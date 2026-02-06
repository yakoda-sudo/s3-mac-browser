import Foundation
import CryptoKit

struct ConnectionResult {
    let statusCode: Int?
    let responseText: String?
    let elapsedMs: Int
    let bucketNames: [String]
    let responseHeaders: [String: String]
    let requestSummary: String
    let objectEntries: [S3Object]
    let objectInfo: S3Object?
    let isTruncated: Bool
    let nextContinuationToken: String?

    init(statusCode: Int?,
         responseText: String?,
         elapsedMs: Int,
         bucketNames: [String],
         responseHeaders: [String: String],
         requestSummary: String,
         objectEntries: [S3Object],
         objectInfo: S3Object?,
         isTruncated: Bool = false,
         nextContinuationToken: String? = nil) {
        self.statusCode = statusCode
        self.responseText = responseText
        self.elapsedMs = elapsedMs
        self.bucketNames = bucketNames
        self.responseHeaders = responseHeaders
        self.requestSummary = requestSummary
        self.objectEntries = objectEntries
        self.objectInfo = objectInfo
        self.isTruncated = isTruncated
        self.nextContinuationToken = nextContinuationToken
    }
}

struct ObjectDataResult {
    let data: Data
    let response: ConnectionResult
}

protocol S3ServiceProtocol: Sendable {
    func listBuckets(endpoint: URL, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listObjects(endpoint: URL, bucket: String, prefix: String, continuationToken: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listObjectVersions(endpoint: URL, bucket: String, prefix: String, keyMarker: String?, versionIdMarker: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func headObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func putObject(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func putObjectWithProgress(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult
    func presignGetURL(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresSeconds: Int) -> String
    func deleteObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func deleteObjectVersion(endpoint: URL, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listAllObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> [S3Object]
    func uploadFileWithProgress(endpoint: URL, bucket: String, key: String, fileURL: URL, contentType: String?, settings: UploadParameters, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult
    func getObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ObjectDataResult
    func getObjectWithProgress(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult
    func getObjectVersionWithProgress(endpoint: URL, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult
}

final class S3Service: NSObject, S3ServiceProtocol, @unchecked Sendable {
    private let timeout: TimeInterval = 10
    private let metricsRecorder = MetricsRecorder.shared

    func listBuckets(endpoint: URL, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = ensureRootPath(endpoint)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let bucketEntries = parseBucketEntries(from: data)
        let bucketNames = bucketEntries.map { $0.key }
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        Task { await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: bucketNames,
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: bucketEntries,
            objectInfo: nil
        )
    }

    func listObjects(endpoint: URL, bucket: String, prefix: String, continuationToken: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeListObjectsURL(endpoint: endpoint, bucket: bucket, prefix: prefix, delimiter: "/", continuationToken: continuationToken)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let parsed = parseObjectEntriesWithToken(from: data)
        let objectEntries = parsed.entries
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        Task { await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: objectEntries,
            objectInfo: nil,
            isTruncated: parsed.isTruncated,
            nextContinuationToken: parsed.nextContinuationToken
        )
    }

    func listObjectVersions(endpoint: URL, bucket: String, prefix: String, keyMarker: String?, versionIdMarker: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        var nextKeyMarker = keyMarker
        var nextVersionMarker = versionIdMarker
        var allEntries: [S3Object] = []
        var lastStatusCode: Int?
        var lastResponseText: String?
        var lastHeaders: [String: String] = [:]
        var lastRequestSummary: String = ""
        var totalElapsed = 0

        repeat {
            let start = Date()
            let requestURL = makeListObjectVersionsURL(endpoint: endpoint, bucket: bucket, prefix: prefix, delimiter: "/", keyMarker: nextKeyMarker, versionIdMarker: nextVersionMarker)
            var request = URLRequest(url: requestURL)
            request.httpMethod = "GET"
            request.timeoutInterval = timeout
            signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

            let session = makeSession(allowInsecure: allowInsecure)
            let (data, response) = try await session.data(for: request)

            let elapsed = Int(Date().timeIntervalSince(start) * 1000)
            totalElapsed += elapsed
            let http = response as? HTTPURLResponse
            let text = String(data: data, encoding: .utf8)
            let parsed = parseObjectVersions(from: data)
            let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
                let key = String(describing: entry.key)
                let value = String(describing: entry.value)
                partial[key] = value
            }
            let requestSummary = makeRequestSummary(request: request)
            Task { await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

            allEntries.append(contentsOf: parsed.entries)
            nextKeyMarker = parsed.nextKeyMarker
            nextVersionMarker = parsed.nextVersionIdMarker
            lastStatusCode = http?.statusCode
            lastResponseText = text
            lastHeaders = headerMap
            lastRequestSummary = requestSummary

            if let status = http?.statusCode, status >= 400 {
                break
            }
        } while (nextKeyMarker?.isEmpty == false) || (nextVersionMarker?.isEmpty == false)

        return ConnectionResult(
            statusCode: lastStatusCode,
            responseText: lastResponseText,
            elapsedMs: totalElapsed,
            bucketNames: [],
            responseHeaders: lastHeaders,
            requestSummary: lastRequestSummary,
            objectEntries: allEntries,
            objectInfo: nil
        )
    }

    private func listObjectsPage(endpoint: URL, bucket: String, prefix: String, continuationToken: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> (entries: [S3Object], nextContinuationToken: String?) {
        let requestURL = makeListObjectsURL(endpoint: endpoint, bucket: bucket, prefix: prefix, delimiter: nil, continuationToken: continuationToken)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, _) = try await session.data(for: request)
        Task { await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }
        let parsed = parseObjectEntriesWithToken(from: data)
        return (parsed.entries, parsed.nextContinuationToken)
    }

    func headObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "HEAD"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        let objectInfo = parseHeadObject(key: key, headers: headerMap)
        Task { await metricsRecorder.record(category: .head, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: objectInfo
        )
    }

    func putObject(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "PUT"
        request.timeoutInterval = timeout
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        let payloadHash = sha256Hex(data)
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: payloadHash)

        let session = makeSession(allowInsecure: allowInsecure)
        let (responseData, response) = try await session.upload(for: request, from: data)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: responseData, encoding: .utf8)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        Task { await metricsRecorder.record(category: .put, uploaded: Int64(data.count), downloaded: Int64(responseData.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func putObjectWithProgress(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "PUT"
        request.timeoutInterval = timeout
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        let payloadHash = sha256Hex(data)
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: payloadHash)

        let delegate = ProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress)
        let session = URLSession(configuration: .ephemeral, delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performUpload(session: session, request: request, data: data)
        Task { await metricsRecorder.record(category: .put, uploaded: Int64(data.count), downloaded: 0, profileName: profileName) }
        return result
    }

    func getObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ObjectDataResult {
        let start = Date()
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        let result = ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
        Task { await metricsRecorder.record(category: .get, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }
        return ObjectDataResult(data: data, response: result)
    }

    func getObjectWithProgress(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let delegate = ProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress)
        let session = URLSession(configuration: .ephemeral, delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performDownload(session: session, request: request)
        Task { await metricsRecorder.record(category: .get, uploaded: 0, downloaded: Int64(result.data.count), profileName: profileName) }
        return result
    }

    func getObjectVersionWithProgress(endpoint: URL, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        let requestURL = makeObjectVersionURL(endpoint: endpoint, bucket: bucket, key: key, versionId: versionId)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let delegate = ProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress)
        let session = URLSession(configuration: .ephemeral, delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performDownload(session: session, request: request)
        Task { await metricsRecorder.record(category: .get, uploaded: 0, downloaded: Int64(result.data.count), profileName: profileName) }
        return result
    }

    func presignGetURL(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresSeconds: Int) -> String {
        let safeRegion = region.isEmpty ? "us-east-1" : region
        let url = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        let host = url.host ?? ""
        let port = url.port
        let hostHeader = port == nil ? host : "\(host):\(port ?? 0)"
        let amzDate = iso8601Date()
        let dateStamp = String(amzDate.prefix(8))
        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(safeRegion)/s3/aws4_request"
        let credential = "\(accessKey)/\(credentialScope)"

        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        let expires = min(max(expiresSeconds, 1), 604800)
        let queryItems = [
            URLQueryItem(name: "X-Amz-Algorithm", value: algorithm),
            URLQueryItem(name: "X-Amz-Credential", value: credential),
            URLQueryItem(name: "X-Amz-Date", value: amzDate),
            URLQueryItem(name: "X-Amz-Expires", value: String(expires)),
            URLQueryItem(name: "X-Amz-SignedHeaders", value: "host")
        ]
        components?.queryItems = queryItems
        let encodedPath = URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedPath ?? url.path
        let canonicalUri = encodedPath.isEmpty ? "/" : encodedPath
        let canonicalQuery = canonicalQueryString(from: components ?? URLComponents())
        let canonicalHeaders = "host:\(hostHeader)\n"
        let signedHeaders = "host"
        let payloadHash = "UNSIGNED-PAYLOAD"
        let canonicalRequest = [
            "GET",
            canonicalUri,
            canonicalQuery,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        ].joined(separator: "\n")

        let stringToSign = [
            algorithm,
            amzDate,
            credentialScope,
            sha256Hex(canonicalRequest)
        ].joined(separator: "\n")

        let signingKey = deriveSigningKey(secretKey: secretKey, dateStamp: dateStamp, region: safeRegion, service: "s3")
        let signature = hmacSHA256Hex(key: signingKey, string: stringToSign)

        var finalItems = queryItems
        finalItems.append(URLQueryItem(name: "X-Amz-Signature", value: signature))
        components?.queryItems = finalItems
        return components?.url?.absoluteString ?? url.absoluteString
    }

    func deleteObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeObjectURL(endpoint: endpoint, bucket: bucket, key: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "DELETE"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        Task { await metricsRecorder.record(category: .delete, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func deleteObjectVersion(endpoint: URL, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeObjectVersionURL(endpoint: endpoint, bucket: bucket, key: key, versionId: versionId)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "DELETE"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        Task { await metricsRecorder.record(category: .delete, uploaded: 0, downloaded: Int64(data.count), profileName: profileName) }

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func listAllObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> [S3Object] {
        var all: [S3Object] = []
        var token: String?

        repeat {
            let result = try await listObjectsPage(
                endpoint: endpoint,
                bucket: bucket,
                prefix: prefix,
                continuationToken: token,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: allowInsecure,
                profileName: profileName
            )
            all.append(contentsOf: result.entries)
            token = result.nextContinuationToken
        } while token != nil

        return all
    }

    func uploadFileWithProgress(endpoint: URL, bucket: String, key: String, fileURL: URL, contentType: String?, settings: UploadParameters, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let fileSize = (try? FileManager.default.attributesOfItem(atPath: fileURL.path)[.size] as? NSNumber)?.int64Value ?? 0
        if fileSize <= settings.multipartThresholdBytes {
            let data = try Data(contentsOf: fileURL)
            return try await putObjectWithProgress(
                endpoint: endpoint,
                bucket: bucket,
                key: key,
                data: data,
                contentType: contentType,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: allowInsecure,
                profileName: profileName,
                progress: progress
            )
        }
        return try await multipartUploadFile(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            fileURL: fileURL,
            contentType: contentType,
            settings: settings,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName,
            progress: progress
        )
    }

    private func makeSession(allowInsecure: Bool) -> URLSession {
        if allowInsecure {
            return URLSession(configuration: .ephemeral, delegate: InsecureSessionDelegate(), delegateQueue: nil)
        }
        return URLSession(configuration: .ephemeral)
    }

    private func signRequest(_ request: inout URLRequest, region: String, accessKey: String, secretKey: String, payloadHash: String) {
        guard let url = request.url, !accessKey.isEmpty, !secretKey.isEmpty else {
            return
        }

        let safeRegion = region.isEmpty ? "us-east-1" : region
        let method = request.httpMethod ?? "GET"
        let host = url.host ?? ""
        let port = url.port
        let hostHeader = port == nil ? host : "\(host):\(port ?? 0)"
        let amzDate = iso8601Date()
        let dateStamp = amzDate.prefix(8)

        let encodedPath = URLComponents(url: url, resolvingAgainstBaseURL: false)?.percentEncodedPath ?? url.path
        let canonicalUri = encodedPath.isEmpty ? "/" : encodedPath
        let canonicalQuery = canonicalQueryString(from: url)
        let canonicalHeaders = "host:\(hostHeader)\n" +
            "x-amz-content-sha256:\(payloadHash)\n" +
            "x-amz-date:\(amzDate)\n"
        let signedHeaders = "host;x-amz-content-sha256;x-amz-date"

        let canonicalRequest = [
            method,
            canonicalUri,
            canonicalQuery,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        ].joined(separator: "\n")

        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(safeRegion)/s3/aws4_request"
        let stringToSign = [
            algorithm,
            amzDate,
            credentialScope,
            sha256Hex(canonicalRequest)
        ].joined(separator: "\n")

        let signingKey = deriveSigningKey(secretKey: secretKey, dateStamp: String(dateStamp), region: safeRegion, service: "s3")
        let signature = hmacSHA256Hex(key: signingKey, string: stringToSign)
        let authorization = "\(algorithm) Credential=\(accessKey)/\(credentialScope), SignedHeaders=\(signedHeaders), Signature=\(signature)"

        request.setValue(hostHeader, forHTTPHeaderField: "Host")
        request.setValue(payloadHash, forHTTPHeaderField: "x-amz-content-sha256")
        request.setValue(amzDate, forHTTPHeaderField: "x-amz-date")
        request.setValue(authorization, forHTTPHeaderField: "Authorization")
    }

    private func ensureRootPath(_ url: URL) -> URL {
        guard url.path.isEmpty else { return url }
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        components?.path = "/"
        return components?.url ?? url
    }

    private func makeRequestSummary(request: URLRequest) -> String {
        let method = request.httpMethod ?? "GET"
        let urlString = request.url?.absoluteString ?? "(no url)"
        var lines: [String] = ["\(method) \(urlString)"]
        if let headers = request.allHTTPHeaderFields {
            for (key, value) in headers.sorted(by: { $0.key.lowercased() < $1.key.lowercased() }) {
                if key.lowercased() == "authorization" {
                    lines.append("\(key): (redacted)")
                } else {
                    lines.append("\(key): \(value)")
                }
            }
        }
        return lines.joined(separator: "\n")
    }

    private func parseBucketNames(from data: Data) -> [String] {
        let parser = BucketListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        return parser.bucketNames
    }

    private func parseBucketEntries(from data: Data) -> [S3Object] {
        let parser = BucketListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        return parser.bucketEntries
    }

    private func parseObjectEntries(from data: Data) -> [S3Object] {
        parseObjectEntriesWithToken(from: data).entries
    }

    private func parseObjectEntriesWithToken(from data: Data) -> (entries: [S3Object], nextContinuationToken: String?, isTruncated: Bool) {
        let parser = ObjectListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        return (parser.entries, parser.nextContinuationToken, parser.isTruncated)
    }

    private func parseObjectVersions(from data: Data) -> (entries: [S3Object], nextKeyMarker: String?, nextVersionIdMarker: String?) {
        let parser = ObjectVersionListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        return (parser.entries, parser.nextKeyMarker, parser.nextVersionIdMarker)
    }

    private func iso8601Date() -> String {
        let formatter = DateFormatter()
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return formatter.string(from: Date())
    }

    private func sha256Hex(_ string: String) -> String {
        let data = Data(string.utf8)
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    private func sha256Hex(_ data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    private func hmacSHA256(key: Data, data: Data) -> Data {
        let keySym = SymmetricKey(data: key)
        let signature = HMAC<SHA256>.authenticationCode(for: data, using: keySym)
        return Data(signature)
    }

    private func hmacSHA256Hex(key: Data, string: String) -> String {
        let signature = hmacSHA256(key: key, data: Data(string.utf8))
        return signature.map { String(format: "%02x", $0) }.joined()
    }

    private func deriveSigningKey(secretKey: String, dateStamp: String, region: String, service: String) -> Data {
        let kSecret = Data(("AWS4" + secretKey).utf8)
        let kDate = hmacSHA256(key: kSecret, data: Data(dateStamp.utf8))
        let kRegion = hmacSHA256(key: kDate, data: Data(region.utf8))
        let kService = hmacSHA256(key: kRegion, data: Data(service.utf8))
        let kSigning = hmacSHA256(key: kService, data: Data("aws4_request".utf8))
        return kSigning
    }

    private func canonicalQueryString(from url: URL) -> String {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return ""
        }
        return canonicalQueryString(from: components)
    }

    private func canonicalQueryString(from components: URLComponents) -> String {
        if let items = components.queryItems, !items.isEmpty {
            let sorted = items.sorted {
                if $0.name == $1.name {
                    return ($0.value ?? "") < ($1.value ?? "")
                }
                return $0.name < $1.name
            }
            return sorted.map { item in
                let name = awsEncode(item.name, encodeSlash: true)
                let value = awsEncode(item.value ?? "", encodeSlash: true)
                return "\(name)=\(value)"
            }.joined(separator: "&")
        }

        guard let raw = components.percentEncodedQuery, !raw.isEmpty else {
            return ""
        }
        let pairs: [(name: String, value: String)] = raw.split(separator: "&").map { part in
            let chunks = part.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            let rawName = String(chunks.first ?? "")
            let rawValue = chunks.count > 1 ? String(chunks[1]) : ""
            let name = rawName.removingPercentEncoding ?? rawName
            let value = rawValue.removingPercentEncoding ?? rawValue
            return (name, value)
        }
        let sorted = pairs.sorted {
            if $0.name == $1.name {
                return $0.value < $1.value
            }
            return $0.name < $1.name
        }
        return sorted.map { item in
            let name = awsEncode(item.name, encodeSlash: true)
            let value = awsEncode(item.value, encodeSlash: true)
            return "\(name)=\(value)"
        }.joined(separator: "&")
    }

    private func awsEncode(_ string: String, encodeSlash: Bool) -> String {
        let unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        var allowed = CharacterSet(charactersIn: unreserved)
        if !encodeSlash {
            allowed.insert(charactersIn: "/")
        }
        return string.utf8.map { byte in
            let scalar = UnicodeScalar(Int(byte))
            if let scalar, allowed.contains(scalar) {
                return String(scalar)
            }
            return String(format: "%%%02X", byte)
        }.joined()
    }

    private func makeListObjectsURL(endpoint: URL, bucket: String, prefix: String, delimiter: String?, continuationToken: String? = nil) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        components?.path = "/" + safeBucket
        var parts: [String] = ["list-type=2"]
        if let delimiter {
            parts.append("delimiter=\(awsEncode(delimiter, encodeSlash: true))")
        }
        if !prefix.isEmpty {
            let normalizedPrefix = normalizeKeyForRequest(prefix)
            parts.append("prefix=\(awsEncode(normalizedPrefix, encodeSlash: true))")
        }
        if let continuationToken, !continuationToken.isEmpty {
            parts.append("continuation-token=\(awsEncode(continuationToken, encodeSlash: true))")
        }
        components?.percentEncodedQuery = parts.joined(separator: "&")
        return components?.url ?? endpoint
    }

    private func makeListObjectVersionsURL(endpoint: URL, bucket: String, prefix: String, delimiter: String?, keyMarker: String?, versionIdMarker: String?) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        components?.path = "/" + safeBucket
        var parts: [String] = ["versions"]
        if let delimiter {
            parts.append("delimiter=\(awsEncode(delimiter, encodeSlash: true))")
        }
        if !prefix.isEmpty {
            let normalizedPrefix = normalizeKeyForRequest(prefix)
            parts.append("prefix=\(awsEncode(normalizedPrefix, encodeSlash: true))")
        }
        if let keyMarker, !keyMarker.isEmpty {
            parts.append("key-marker=\(awsEncode(keyMarker, encodeSlash: true))")
        }
        if let versionIdMarker, !versionIdMarker.isEmpty {
            parts.append("version-id-marker=\(awsEncode(versionIdMarker, encodeSlash: true))")
        }
        components?.percentEncodedQuery = parts.joined(separator: "&")
        return components?.url ?? endpoint
    }

    private func makeObjectURL(endpoint: URL, bucket: String, key: String) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        let normalizedKey = normalizeKeyForRequest(key)
        let safeKey = percentEncodedKeyIfNeeded(normalizedKey)
        components?.percentEncodedPath = "/" + safeBucket + "/" + safeKey
        components?.queryItems = nil
        return components?.url ?? endpoint
    }

    private func makeMultipartURL(endpoint: URL, bucket: String, key: String, queryItems: [URLQueryItem]) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        let normalizedKey = normalizeKeyForRequest(key)
        let safeKey = percentEncodedKeyIfNeeded(normalizedKey)
        components?.percentEncodedPath = "/" + safeBucket + "/" + safeKey
        components?.queryItems = queryItems
        return components?.url ?? endpoint
    }

    private func makeObjectVersionURL(endpoint: URL, bucket: String, key: String, versionId: String) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        let normalizedKey = normalizeKeyForRequest(key)
        let safeKey = percentEncodedKeyIfNeeded(normalizedKey)
        components?.percentEncodedPath = "/" + safeBucket + "/" + safeKey
        components?.queryItems = [URLQueryItem(name: "versionId", value: versionId)]
        return components?.url ?? endpoint
    }

    private func normalizeKeyForRequest(_ key: String) -> String {
        var value = key
        var attempts = 0
        while value.contains("%25"), attempts < 2 {
            if let decoded = value.removingPercentEncoding, decoded != value {
                value = decoded
            } else {
                break
            }
            attempts += 1
        }
        return value
    }

    private func percentEncodedKeyIfNeeded(_ key: String) -> String {
        if key.contains("%") {
            return key
        }
        return key.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? key
    }

    private actor AsyncSemaphore {
        private var value: Int
        init(_ value: Int) { self.value = max(value, 1) }
        func acquire() async {
            while value == 0 {
                await Task.yield()
            }
            value -= 1
        }
        func release() {
            value += 1
        }
    }

    private actor ProgressAccumulator {
        private var sent: Int64 = 0
        func add(_ delta: Int64) -> Int64 {
            sent += delta
            return sent
        }
    }

    private func multipartUploadFile(endpoint: URL, bucket: String, key: String, fileURL: URL, contentType: String?, settings: UploadParameters, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let fileAttributes = try FileManager.default.attributesOfItem(atPath: fileURL.path)
        let fileSize = (fileAttributes[.size] as? NSNumber)?.int64Value ?? 0
        let minPartSize = max(settings.multipartChunkBytes, 5 * 1024 * 1024)
        let partSize = minPartSize
        let totalParts = Int((fileSize + Int64(partSize) - 1) / Int64(partSize))
        let uploadId = try await createMultipartUpload(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            contentType: contentType,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure
        )

        let semaphore = AsyncSemaphore(settings.maxConcurrentRequests)
        let accumulator = ProgressAccumulator()
        let bandwidthLimit = settings.maxBandwidthBytesPerSec

        do {
            let parts = try await withThrowingTaskGroup(of: (Int, String).self) { group -> [(Int, String)] in
                for partNumber in 1...totalParts {
                    let offset = Int64(partNumber - 1) * Int64(partSize)
                    let size = Int(min(Int64(partSize), fileSize - offset))
                    await semaphore.acquire()
                    group.addTask { [allowInsecure] in
                        defer { Task { await semaphore.release() } }
                        let start = Date()
                        let handle = try FileHandle(forReadingFrom: fileURL)
                        try handle.seek(toOffset: UInt64(offset))
                        let data = try handle.read(upToCount: size) ?? Data()
                        try handle.close()

                        let etag = try await self.uploadPart(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: key,
                            uploadId: uploadId,
                            partNumber: partNumber,
                            data: data,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: allowInsecure
                        )

                        let totalSent = await accumulator.add(Int64(data.count))
                        progress(totalSent, fileSize)

                        if bandwidthLimit > 0 {
                            let elapsed = Date().timeIntervalSince(start)
                            let minDuration = Double(data.count) / Double(bandwidthLimit)
                            if elapsed < minDuration {
                                let sleepNs = UInt64((minDuration - elapsed) * 1_000_000_000)
                                try? await Task.sleep(nanoseconds: sleepNs)
                            }
                        }

                        return (partNumber, etag)
                    }
                }
                var collected: [(Int, String)] = []
                for try await result in group {
                    collected.append(result)
                }
                return collected
            }

            let sortedParts = parts.sorted { $0.0 < $1.0 }
            let completeResult = try await completeMultipartUpload(
                endpoint: endpoint,
                bucket: bucket,
                key: key,
                uploadId: uploadId,
                parts: sortedParts,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: allowInsecure
            )
            await metricsRecorder.record(category: .put, uploaded: fileSize, downloaded: Int64(completeResult.responseText?.count ?? 0), profileName: profileName)
            return completeResult
        } catch {
            _ = try? await abortMultipartUpload(
                endpoint: endpoint,
                bucket: bucket,
                key: key,
                uploadId: uploadId,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: allowInsecure
            )
            throw error
        }
    }

    private func createMultipartUpload(endpoint: URL, bucket: String, key: String, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> String {
        let requestURL = makeMultipartURL(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            queryItems: [URLQueryItem(name: "uploads", value: nil)]
        )
        var request = URLRequest(url: requestURL)
        request.httpMethod = "POST"
        request.timeoutInterval = timeout
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, _) = try await session.data(for: request)
        let parser = MultipartUploadParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        guard let uploadId = parser.uploadId, !uploadId.isEmpty else {
            throw NSError(domain: "S3Multipart", code: -1, userInfo: [NSLocalizedDescriptionKey: "Missing uploadId"])
        }
        return uploadId
    }

    private func uploadPart(endpoint: URL, bucket: String, key: String, uploadId: String, partNumber: Int, data: Data, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> String {
        let requestURL = makeMultipartURL(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            queryItems: [
                URLQueryItem(name: "partNumber", value: "\(partNumber)"),
                URLQueryItem(name: "uploadId", value: uploadId)
            ]
        )
        var request = URLRequest(url: requestURL)
        request.httpMethod = "PUT"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(data))

        let session = makeSession(allowInsecure: allowInsecure)
        let (responseData, response) = try await session.upload(for: request, from: data)
        let http = response as? HTTPURLResponse
        let etag = http?.allHeaderFields.first { "\($0.key)".lowercased() == "etag" }?.value as? String ?? ""
        if let status = http?.statusCode, status >= 400 {
            let body = String(data: responseData, encoding: .utf8) ?? ""
            throw NSError(domain: "S3Multipart", code: status, userInfo: [NSLocalizedDescriptionKey: body])
        }
        return etag.replacingOccurrences(of: "\"", with: "")
    }

    private func completeMultipartUpload(endpoint: URL, bucket: String, key: String, uploadId: String, parts: [(Int, String)], region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
        let requestURL = makeMultipartURL(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            queryItems: [URLQueryItem(name: "uploadId", value: uploadId)]
        )
        var body = "<CompleteMultipartUpload>"
        for (number, etag) in parts {
            body += "<Part><PartNumber>\(number)</PartNumber><ETag>\"\(etag)\"</ETag></Part>"
        }
        body += "</CompleteMultipartUpload>"
        let data = Data(body.utf8)

        var request = URLRequest(url: requestURL)
        request.httpMethod = "POST"
        request.timeoutInterval = timeout
        request.setValue("application/xml", forHTTPHeaderField: "Content-Type")
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(data))

        let start = Date()
        let session = makeSession(allowInsecure: allowInsecure)
        let (responseData, response) = try await session.upload(for: request, from: data)
        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: responseData, encoding: .utf8),
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    private func abortMultipartUpload(endpoint: URL, bucket: String, key: String, uploadId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
        let requestURL = makeMultipartURL(
            endpoint: endpoint,
            bucket: bucket,
            key: key,
            queryItems: [URLQueryItem(name: "uploadId", value: uploadId)]
        )
        var request = URLRequest(url: requestURL)
        request.httpMethod = "DELETE"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let start = Date()
        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)
        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)
        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    private func parseHeadObject(key: String, headers: [String: String]) -> S3Object {
        let size = Int(headers.first(where: { $0.key.lowercased() == "content-length" })?.value ?? "") ?? 0
        let contentType = headers.first(where: { $0.key.lowercased() == "content-type" })?.value ?? "unknown"
        let etag = headers.first(where: { $0.key.lowercased() == "etag" })?.value.replacingOccurrences(of: "\"", with: "") ?? ""
        let lastModifiedString = headers.first(where: { $0.key.lowercased() == "last-modified" })?.value ?? ""
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
        let lastModified = formatter.date(from: lastModifiedString) ?? Date()
        return S3Object(key: key, sizeBytes: size, lastModified: lastModified, contentType: contentType, eTag: etag)
    }
}

final class InsecureSessionDelegate: NSObject, URLSessionDelegate {
    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        if let trust = challenge.protectionSpace.serverTrust {
            completionHandler(.useCredential, URLCredential(trust: trust))
        } else {
            completionHandler(.performDefaultHandling, nil)
        }
    }
}

private final class ProgressSessionDelegate: NSObject, URLSessionDataDelegate, URLSessionTaskDelegate, @unchecked Sendable {
    private let allowInsecure: Bool
    private let progress: @Sendable (Int64, Int64) -> Void
    private var expectedBytes: Int64 = 0
    private var receivedBytes: Int64 = 0
    private var response: URLResponse?
    private var dataBuffer = Data()
    private var continuation: CheckedContinuation<ObjectDataResult, Error>?
    private var uploadContinuation: CheckedContinuation<ConnectionResult, Error>?

    init(allowInsecure: Bool, progress: @escaping @Sendable (Int64, Int64) -> Void) {
        self.allowInsecure = allowInsecure
        self.progress = progress
    }

    func performDownload(session: URLSession, request: URLRequest) async throws -> ObjectDataResult {
        try await withCheckedThrowingContinuation { continuation in
            self.continuation = continuation
            let task = session.dataTask(with: request)
            task.resume()
        }
    }

    func performUpload(session: URLSession, request: URLRequest, data: Data) async throws -> ConnectionResult {
        try await withCheckedThrowingContinuation { continuation in
            self.uploadContinuation = continuation
            expectedBytes = Int64(data.count)
            progress(0, expectedBytes)
            let task = session.uploadTask(with: request, from: data)
            task.resume()
        }
    }

    func urlSession(_ session: URLSession, didReceive challenge: URLAuthenticationChallenge, completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
        if allowInsecure, let trust = challenge.protectionSpace.serverTrust {
            completionHandler(.useCredential, URLCredential(trust: trust))
        } else {
            completionHandler(.performDefaultHandling, nil)
        }
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
        self.response = response
        expectedBytes = response.expectedContentLength > 0 ? response.expectedContentLength : expectedBytes
        progress(receivedBytes, expectedBytes)
        completionHandler(.allow)
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        dataBuffer.append(data)
        receivedBytes += Int64(data.count)
        progress(receivedBytes, expectedBytes)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
        expectedBytes = totalBytesExpectedToSend
        progress(totalBytesSent, totalBytesExpectedToSend)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        if let error {
            continuation?.resume(throwing: error)
            uploadContinuation?.resume(throwing: error)
            return
        }

        let http = task.response as? HTTPURLResponse
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = (task.originalRequest).map { requestSummaryText($0) } ?? ""
        let text = String(data: dataBuffer, encoding: .utf8)
        let result = ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: 0,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )

        if let continuation {
            continuation.resume(returning: ObjectDataResult(data: dataBuffer, response: result))
        }
        if let uploadContinuation {
            uploadContinuation.resume(returning: result)
        }
    }

    private func requestSummaryText(_ request: URLRequest) -> String {
        let method = request.httpMethod ?? "GET"
        let urlString = request.url?.absoluteString ?? "(no url)"
        var lines: [String] = ["\(method) \(urlString)"]
        if let headers = request.allHTTPHeaderFields {
            for (key, value) in headers.sorted(by: { $0.key.lowercased() < $1.key.lowercased() }) {
                if key.lowercased() == "authorization" {
                    lines.append("\(key): (redacted)")
                } else {
                    lines.append("\(key): \(value)")
                }
            }
        }
        return lines.joined(separator: "\n")
    }
}

private final class MultipartUploadParser: NSObject, XMLParserDelegate {
    private(set) var uploadId: String?
    private var currentText: String = ""

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentText = ""
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        if elementName == "UploadId" {
            let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                uploadId = trimmed
            }
        }
        currentText = ""
    }
}

private final class BucketListParser: NSObject, XMLParserDelegate {
    private(set) var bucketNames: [String] = []
    private(set) var bucketEntries: [S3Object] = []
    private var currentElement: String = ""
    private var currentText: String = ""
    private var currentBucketName: String = ""
    private var currentCreationDate: Date = Date()
    private var insideBucket = false
    private let dateFormatter = ISO8601DateFormatter()
    private let dateFormatterFractional: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Bucket" {
            insideBucket = true
            currentBucketName = ""
            currentCreationDate = Date()
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        if elementName == "Name" && currentElement == "Name" {
            let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                if insideBucket {
                    currentBucketName = trimmed
                } else {
                    bucketNames.append(trimmed)
                }
            }
        }
        if elementName == "CreationDate" && insideBucket {
            let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
            if let parsed = dateFormatterFractional.date(from: trimmed) ?? dateFormatter.date(from: trimmed) {
                currentCreationDate = parsed
            }
        }
        if elementName == "Bucket" && insideBucket {
            if !currentBucketName.isEmpty {
                bucketNames.append(currentBucketName)
                bucketEntries.append(
                    S3Object(
                        key: currentBucketName,
                        sizeBytes: 0,
                        lastModified: currentCreationDate,
                        contentType: "bucket",
                        eTag: ""
                    )
                )
            }
            insideBucket = false
        }
        currentElement = ""
        currentText = ""
    }
}

private final class ObjectListParser: NSObject, XMLParserDelegate {
    private(set) var entries: [S3Object] = []
    private(set) var nextContinuationToken: String?
    private(set) var isTruncated: Bool = false
    private var currentElement: String = ""
    private var currentText: String = ""
    private var currentKey: String = ""
    private var currentSize: Int = 0
    private var currentETag: String = ""
    private var currentStorageClass: String = ""
    private var currentLastModified: Date = Date()
    private var collectingContents = false
    private var collectingPrefix = false
    private var prefixValue: String = ""
    private let dateFormatter = ISO8601DateFormatter()
    private let dateFormatterFractional: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Contents" {
            collectingContents = true
            currentKey = ""
            currentSize = 0
            currentETag = ""
            currentStorageClass = ""
            currentLastModified = Date()
        }
        if elementName == "CommonPrefixes" {
            collectingPrefix = true
            prefixValue = ""
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
        if collectingContents {
            switch elementName {
            case "Key":
                currentKey = decodePercent25(trimmed)
            case "Size":
                currentSize = Int(trimmed) ?? 0
            case "ETag":
                currentETag = trimmed.replacingOccurrences(of: "\"", with: "")
            case "StorageClass":
                currentStorageClass = trimmed
            case "LastModified":
                if let date = dateFormatterFractional.date(from: trimmed) ?? dateFormatter.date(from: trimmed) {
                    currentLastModified = date
                }
            case "Contents":
                let entry = S3Object(
                    key: currentKey,
                    sizeBytes: currentSize,
                    lastModified: currentLastModified,
                    contentType: "object",
                    eTag: currentETag,
                    storageClass: currentStorageClass
                )
                entries.append(entry)
                collectingContents = false
            default:
                break
            }
        } else if collectingPrefix {
            if elementName == "Prefix" {
                prefixValue = decodePercent25(trimmed)
            } else if elementName == "CommonPrefixes" {
                let entry = S3Object(
                    key: prefixValue,
                    sizeBytes: 0,
                    lastModified: Date(),
                    contentType: "folder",
                    eTag: ""
                )
                entries.append(entry)
                collectingPrefix = false
            }
        } else if elementName == "NextContinuationToken" {
            if !trimmed.isEmpty {
                nextContinuationToken = trimmed
            }
        } else if elementName == "IsTruncated" {
            isTruncated = trimmed.lowercased() == "true"
        }
        currentElement = ""
        currentText = ""
    }

    private func decodePercent25(_ value: String) -> String {
        var output = value
        var attempts = 0
        while output.contains("%25"), attempts < 2 {
            if let decoded = output.removingPercentEncoding, decoded != output {
                output = decoded
            } else {
                break
            }
            attempts += 1
        }
        if output.contains("%"), let decoded = output.removingPercentEncoding {
            output = decoded
        }
        return output
    }
}

private final class ObjectVersionListParser: NSObject, XMLParserDelegate {
    private(set) var entries: [S3Object] = []
    private(set) var nextKeyMarker: String?
    private(set) var nextVersionIdMarker: String?
    private var currentElement: String = ""
    private var currentText: String = ""
    private var inVersion = false
    private var inDeleteMarker = false
    private var inCommonPrefix = false
    private var currentKey: String = ""
    private var currentVersionId: String = ""
    private var currentSize: Int = 0
    private var currentETag: String = ""
    private var currentLastModified: Date = Date()
    private var currentIsLatest: Bool = false
    private var prefixValue: String = ""
    private let dateFormatter = ISO8601DateFormatter()
    private let dateFormatterFractional: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Version" {
            inVersion = true
            resetEntry()
        } else if elementName == "DeleteMarker" {
            inDeleteMarker = true
            resetEntry()
        } else if elementName == "CommonPrefixes" {
            inCommonPrefix = true
            prefixValue = ""
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
        if inVersion || inDeleteMarker {
            switch elementName {
            case "Key":
                currentKey = decodePercent25(trimmed)
            case "VersionId":
                currentVersionId = trimmed
            case "IsLatest":
                currentIsLatest = (trimmed.lowercased() == "true")
            case "Size":
                currentSize = Int(trimmed) ?? 0
            case "ETag":
                currentETag = trimmed.replacingOccurrences(of: "\"", with: "")
            case "LastModified":
                if let date = dateFormatterFractional.date(from: trimmed) ?? dateFormatter.date(from: trimmed) {
                    currentLastModified = date
                }
            case "Version":
                let entry = S3Object(
                    key: currentKey,
                    sizeBytes: currentSize,
                    lastModified: currentLastModified,
                    contentType: "object",
                    eTag: currentETag,
                    versionId: currentVersionId,
                    isDeleteMarker: false,
                    isDeleted: false,
                    isVersioned: true,
                    isLatest: currentIsLatest
                )
                entries.append(entry)
                inVersion = false
            case "DeleteMarker":
                let entry = S3Object(
                    key: currentKey,
                    sizeBytes: 0,
                    lastModified: currentLastModified,
                    contentType: "delete-marker",
                    eTag: currentETag,
                    versionId: currentVersionId,
                    isDeleteMarker: true,
                    isDeleted: true,
                    isVersioned: true,
                    isLatest: currentIsLatest
                )
                entries.append(entry)
                inDeleteMarker = false
            default:
                break
            }
        } else if inCommonPrefix {
            if elementName == "Prefix" {
                prefixValue = decodePercent25(trimmed)
            } else if elementName == "CommonPrefixes" {
                let entry = S3Object(
                    key: prefixValue,
                    sizeBytes: 0,
                    lastModified: Date(),
                    contentType: "folder",
                    eTag: ""
                )
                entries.append(entry)
                inCommonPrefix = false
            }
        } else if elementName == "NextKeyMarker" {
            if !trimmed.isEmpty {
                nextKeyMarker = trimmed
            }
        } else if elementName == "NextVersionIdMarker" {
            if !trimmed.isEmpty {
                nextVersionIdMarker = trimmed
            }
        }
        currentElement = ""
        currentText = ""
    }

    private func decodePercent25(_ value: String) -> String {
        var output = value
        var attempts = 0
        while output.contains("%25"), attempts < 2 {
            if let decoded = output.removingPercentEncoding, decoded != output {
                output = decoded
            } else {
                break
            }
            attempts += 1
        }
        if output.contains("%"), let decoded = output.removingPercentEncoding {
            output = decoded
        }
        return output
    }

    private func resetEntry() {
        currentKey = ""
        currentVersionId = ""
        currentSize = 0
        currentETag = ""
        currentLastModified = Date()
        currentIsLatest = false
    }
}
