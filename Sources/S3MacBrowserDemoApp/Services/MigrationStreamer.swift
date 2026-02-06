import Foundation

struct MigrationStreamer {
    struct EndpointContext {
        let endpoint: StorageEndpoint
        let bucket: String
        let region: String
        let accessKey: String
        let secretKey: String
        let allowInsecure: Bool
    }

    struct RetryPolicy {
        let maxAttempts: Int
        let baseDelay: UInt64
        let jitter: UInt64
    }

    let bufferBytes: Int
    let metricsRecorder: MetricsRecorder
    let sourceProfileName: String
    let targetProfileName: String
    let source: EndpointContext
    let target: EndpointContext
    let retryPolicy = RetryPolicy(maxAttempts: 3, baseDelay: 500_000_000, jitter: 200_000_000)

    struct TransferStats {
        let bytes: Int64
        let requests: Int
    }

    func copyObject(key: String, targetKey: String, contentType: String?, onChunk: @escaping @Sendable (Int64) -> Void) async throws -> TransferStats {
        var attempt = 0
        while true {
            do {
                return try await copyOnce(key: key, targetKey: targetKey, contentType: contentType, onChunk: onChunk)
            } catch {
                attempt += 1
                if attempt >= retryPolicy.maxAttempts {
                    throw error
                }
                let base = retryPolicy.baseDelay * UInt64(1 << min(attempt - 1, 4))
                let jitter = UInt64.random(in: 0...retryPolicy.jitter)
                try? await Task.sleep(nanoseconds: base + jitter)
            }
        }
    }

    private func copyOnce(key: String, targetKey: String, contentType: String?, onChunk: @escaping @Sendable (Int64) -> Void) async throws -> TransferStats {
        let counter = RequestCounter()
        let chunkSize = bufferBytes
        let stream = try await downloadStream(key: key, chunkSize: chunkSize, counter: counter)

        switch target.endpoint.provider {
        case .s3:
            let bytes = try await uploadMultipartS3(stream: stream, key: targetKey, contentType: contentType, counter: counter, onChunk: onChunk)
            return TransferStats(bytes: bytes, requests: counter.count)
        case .azureBlob:
            let bytes = try await uploadBlocksAzure(stream: stream, key: targetKey, counter: counter, onChunk: onChunk)
            return TransferStats(bytes: bytes, requests: counter.count)
        }
    }

    private func downloadStream(key: String, chunkSize: Int, counter: RequestCounter) async throws -> AsyncThrowingStream<Data, Error> {
        let request = try makeDownloadRequest(key: key)
        let session = makeSession(allowInsecure: source.allowInsecure)
        counter.increment()
        let (bytes, response) = try await session.bytes(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode < 400 else {
            throw MigrationError.httpStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return AsyncThrowingStream { continuation in
            Task {
                var buffer = Data()
                buffer.reserveCapacity(chunkSize)
                var downloaded: Int64 = 0
                do {
                    for try await byte in bytes {
                        buffer.append(byte)
                        if buffer.count >= chunkSize {
                            downloaded += Int64(buffer.count)
                            continuation.yield(buffer)
                            buffer = Data()
                            buffer.reserveCapacity(chunkSize)
                        }
                    }
                    if !buffer.isEmpty {
                        downloaded += Int64(buffer.count)
                        continuation.yield(buffer)
                    }
                    continuation.finish()
                    await metricsRecorder.record(category: .get, uploaded: 0, downloaded: downloaded, profileName: sourceProfileName)
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    private func uploadMultipartS3(stream: AsyncThrowingStream<Data, Error>, key: String, contentType: String?, counter: RequestCounter, onChunk: @escaping (Int64) -> Void) async throws -> Int64 {
        let uploadId = try await withRetry { try await createMultipartUploadS3(key: key, contentType: contentType, counter: counter) }
        var partNumber = 1
        var uploaded: Int64 = 0
        var etags: [(Int, String)] = []

        do {
            for try await chunk in stream {
                let etag = try await withRetry {
                    try await uploadPartS3(key: key, uploadId: uploadId, partNumber: partNumber, data: chunk, counter: counter)
                }
                etags.append((partNumber, etag))
                uploaded += Int64(chunk.count)
                onChunk(Int64(chunk.count))
                partNumber += 1
            }
            _ = try await withRetry {
                try await completeMultipartUploadS3(key: key, uploadId: uploadId, etags: etags, counter: counter)
            }
            return uploaded
        } catch {
            _ = try? await abortMultipartUploadS3(key: key, uploadId: uploadId, counter: counter)
            throw error
        }
    }

    private func uploadBlocksAzure(stream: AsyncThrowingStream<Data, Error>, key: String, counter: RequestCounter, onChunk: @escaping (Int64) -> Void) async throws -> Int64 {
        var blockIds: [String] = []
        var uploaded: Int64 = 0
        var index = 0

        for try await chunk in stream {
            let blockId = String(format: "%06d", index)
            let encoded = Data(blockId.utf8).base64EncodedString()
            try await withRetry {
                try await putBlockAzure(key: key, blockId: encoded, data: chunk, counter: counter)
            }
            blockIds.append(encoded)
            uploaded += Int64(chunk.count)
            onChunk(Int64(chunk.count))
            index += 1
        }

        try await withRetry {
            try await commitBlocksAzure(key: key, blockIds: blockIds, counter: counter)
        }
        return uploaded
    }

    private func makeDownloadRequest(key: String) throws -> URLRequest {
        switch source.endpoint.provider {
        case .s3:
            let url = makeS3ObjectURL(endpoint: source.endpoint.baseURL, bucket: source.bucket, key: key)
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            S3Signer.sign(request: &request, region: source.region, accessKey: source.accessKey, secretKey: source.secretKey, payloadHash: S3Signer.sha256Hex(""))
            return request
        case .azureBlob:
            let url = source.endpoint.azureURL(container: source.bucket, blobPath: key)
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            return request
        }
    }

    private func createMultipartUploadS3(key: String, contentType: String?, counter: RequestCounter) async throws -> String {
        let url = makeS3ObjectURL(endpoint: target.endpoint.baseURL, bucket: target.bucket, key: key, queryItems: [
            URLQueryItem(name: "uploads", value: nil)
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        S3Signer.sign(request: &request, region: target.region, accessKey: target.accessKey, secretKey: target.secretKey, payloadHash: S3Signer.sha256Hex(""))
        let (data, response) = try await sessionData(request: request, allowInsecure: target.allowInsecure, counter: counter)
        guard let http = response as? HTTPURLResponse, http.statusCode < 400 else {
            throw MigrationError.httpStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        await metricsRecorder.record(category: .post, uploaded: Int64(data.count), downloaded: 0, profileName: targetProfileName)
        let parser = S3MultipartParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        guard let uploadId = parser.uploadId, !uploadId.isEmpty else {
            throw MigrationError.missingUploadId
        }
        return uploadId
    }

    private func uploadPartS3(key: String, uploadId: String, partNumber: Int, data: Data, counter: RequestCounter) async throws -> String {
        let url = makeS3ObjectURL(endpoint: target.endpoint.baseURL, bucket: target.bucket, key: key, queryItems: [
            URLQueryItem(name: "partNumber", value: String(partNumber)),
            URLQueryItem(name: "uploadId", value: uploadId)
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        S3Signer.sign(request: &request, region: target.region, accessKey: target.accessKey, secretKey: target.secretKey, payloadHash: S3Signer.sha256Hex(data))
        let (responseData, response) = try await sessionUpload(request: request, data: data, allowInsecure: target.allowInsecure, counter: counter)
        guard let http = response as? HTTPURLResponse, http.statusCode < 400 else {
            throw MigrationError.httpStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        await metricsRecorder.record(category: .put, uploaded: Int64(data.count), downloaded: 0, profileName: targetProfileName)
        let etag = (http.value(forHTTPHeaderField: "ETag") ?? "").replacingOccurrences(of: "\"", with: "")
        if etag.isEmpty && !responseData.isEmpty {
            return etag
        }
        return etag.isEmpty ? UUID().uuidString : etag
    }

    private func completeMultipartUploadS3(key: String, uploadId: String, etags: [(Int, String)], counter: RequestCounter) async throws {
        let url = makeS3ObjectURL(endpoint: target.endpoint.baseURL, bucket: target.bucket, key: key, queryItems: [
            URLQueryItem(name: "uploadId", value: uploadId)
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        let body = s3CompleteBody(etags: etags)
        let bodyData = Data(body.utf8)
        S3Signer.sign(request: &request, region: target.region, accessKey: target.accessKey, secretKey: target.secretKey, payloadHash: S3Signer.sha256Hex(bodyData))
        _ = try await sessionUpload(request: request, data: bodyData, allowInsecure: target.allowInsecure, counter: counter)
        await metricsRecorder.record(category: .post, uploaded: Int64(bodyData.count), downloaded: 0, profileName: targetProfileName)
    }

    private func abortMultipartUploadS3(key: String, uploadId: String, counter: RequestCounter) async throws {
        let url = makeS3ObjectURL(endpoint: target.endpoint.baseURL, bucket: target.bucket, key: key, queryItems: [
            URLQueryItem(name: "uploadId", value: uploadId)
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        S3Signer.sign(request: &request, region: target.region, accessKey: target.accessKey, secretKey: target.secretKey, payloadHash: S3Signer.sha256Hex(""))
        _ = try await sessionData(request: request, allowInsecure: target.allowInsecure, counter: counter)
        await metricsRecorder.record(category: .delete, uploaded: 0, downloaded: 0, profileName: targetProfileName)
    }

    private func putBlockAzure(key: String, blockId: String, data: Data, counter: RequestCounter) async throws {
        let url = target.endpoint.azureURL(container: target.bucket, blobPath: key, queryItems: [
            URLQueryItem(name: "comp", value: "block"),
            URLQueryItem(name: "blockid", value: blockId)
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.setValue("BlockBlob", forHTTPHeaderField: "x-ms-blob-type")
        request.setValue("2024-11-04", forHTTPHeaderField: "x-ms-version")
        _ = try await sessionUpload(request: request, data: data, allowInsecure: target.allowInsecure, counter: counter)
        await metricsRecorder.record(category: .put, uploaded: Int64(data.count), downloaded: 0, profileName: targetProfileName)
    }

    private func commitBlocksAzure(key: String, blockIds: [String], counter: RequestCounter) async throws {
        let url = target.endpoint.azureURL(container: target.bucket, blobPath: key, queryItems: [
            URLQueryItem(name: "comp", value: "blocklist")
        ])
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.setValue("2024-11-04", forHTTPHeaderField: "x-ms-version")
        let body = azureBlockListBody(blockIds: blockIds)
        let bodyData = Data(body.utf8)
        _ = try await sessionUpload(request: request, data: bodyData, allowInsecure: target.allowInsecure, counter: counter)
        await metricsRecorder.record(category: .put, uploaded: Int64(bodyData.count), downloaded: 0, profileName: targetProfileName)
    }

    private func makeS3ObjectURL(endpoint: URL, bucket: String, key: String, queryItems: [URLQueryItem] = []) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        let safeKey = key.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? key
        components?.path = "/" + safeBucket + "/" + safeKey
        components?.queryItems = queryItems.isEmpty ? nil : queryItems
        return components?.url ?? endpoint
    }

    private func s3CompleteBody(etags: [(Int, String)]) -> String {
        let parts = etags.sorted { $0.0 < $1.0 }
            .map { part in
                "<Part><PartNumber>\(part.0)</PartNumber><ETag>\"\(part.1)\"</ETag></Part>"
            }
            .joined()
        return "<CompleteMultipartUpload>\(parts)</CompleteMultipartUpload>"
    }

    private func azureBlockListBody(blockIds: [String]) -> String {
        let blocks = blockIds.map { "<Latest>\($0)</Latest>" }.joined()
        return "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>\(blocks)</BlockList>"
    }

    private func withRetry<T>(_ operation: @escaping () async throws -> T) async throws -> T {
        var attempt = 0
        while true {
            do {
                return try await operation()
            } catch {
                attempt += 1
                if attempt >= retryPolicy.maxAttempts {
                    throw error
                }
                let base = retryPolicy.baseDelay * UInt64(1 << min(attempt - 1, 4))
                let jitter = UInt64.random(in: 0...retryPolicy.jitter)
                try? await Task.sleep(nanoseconds: base + jitter)
            }
        }
    }

    private func makeSession(allowInsecure: Bool) -> URLSession {
        if allowInsecure {
            return URLSession(configuration: .ephemeral, delegate: InsecureSessionDelegate(), delegateQueue: nil)
        }
        return URLSession(configuration: .ephemeral)
    }

    private func sessionData(request: URLRequest, allowInsecure: Bool, counter: RequestCounter) async throws -> (Data, URLResponse) {
        let session = makeSession(allowInsecure: allowInsecure)
        counter.increment()
        return try await session.data(for: request)
    }

    private func sessionUpload(request: URLRequest, data: Data, allowInsecure: Bool, counter: RequestCounter) async throws -> (Data, URLResponse) {
        let session = makeSession(allowInsecure: allowInsecure)
        counter.increment()
        return try await session.upload(for: request, from: data)
    }
}

enum MigrationError: Error {
    case httpStatus(Int)
    case missingUploadId
}

final class S3MultipartParser: NSObject, XMLParserDelegate {
    private(set) var uploadId: String?
    private var currentText: String = ""

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        if elementName == "UploadId" {
            uploadId = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        currentText = ""
    }
}

final class RequestCounter {
    private(set) var count: Int = 0

    func increment() {
        count += 1
    }
}
