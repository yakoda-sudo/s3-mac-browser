import Foundation

final class AzureBlobBackend: StorageBackend {
    let provider: StorageProvider = .azureBlob
    private let metricsRecorder: MetricsRecorder
    private let sessionConfigurationProvider: @Sendable () -> URLSessionConfiguration
    private let timeout: TimeInterval = 20
    private let azureApiVersion = "2024-11-04"

    init(metricsRecorder: MetricsRecorder = MetricsRecorder.shared,
         sessionConfigurationProvider: @escaping @Sendable () -> URLSessionConfiguration = { .ephemeral }) {
        self.metricsRecorder = metricsRecorder
        self.sessionConfigurationProvider = sessionConfigurationProvider
    }

    func listBuckets(endpoint: StorageEndpoint, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        if endpoint.isAzureContainerSAS, let container = endpoint.container {
            return ConnectionResult(
                statusCode: 200,
                responseText: nil,
                elapsedMs: 0,
                bucketNames: [container],
                responseHeaders: [:],
                requestSummary: endpoint.redactedURLString(endpoint.baseURL),
                objectEntries: [],
                objectInfo: nil
            )
        }
        return try await listContainers(endpoint: endpoint, allowInsecure: allowInsecure, profileName: profileName)
    }

    func testConnection(endpoint: StorageEndpoint, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        if endpoint.isAzureContainerSAS, let container = endpoint.container {
            let result = try await listBlobs(endpoint: endpoint, container: container, prefix: "", delimiter: "/",
                                             include: nil, marker: nil, allowInsecure: allowInsecure, profileName: profileName)
            return ConnectionResult(
                statusCode: result.statusCode,
                responseText: result.responseText,
                elapsedMs: result.elapsedMs,
                bucketNames: [container],
                responseHeaders: result.responseHeaders,
                requestSummary: result.requestSummary,
                objectEntries: [],
                objectInfo: nil
            )
        }
        return try await listContainers(endpoint: endpoint, allowInsecure: allowInsecure, profileName: profileName)
    }

    func listObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, continuationToken: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        return try await listBlobs(endpoint: endpoint, container: container, prefix: prefix, delimiter: "/", include: nil, marker: continuationToken, allowInsecure: allowInsecure, profileName: profileName)
    }

    func listObjectVersions(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        return try await listBlobs(
            endpoint: endpoint,
            container: container,
            prefix: prefix,
            delimiter: "/",
            include: ["versions", "deleted"],
            marker: nil,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func listAllObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> [S3Object] {
        let container = endpoint.container ?? bucket
        var all: [S3Object] = []
        var marker: String?

        repeat {
            let result = try await listBlobsPage(endpoint: endpoint, container: container, prefix: prefix, delimiter: nil, include: nil, marker: marker, allowInsecure: allowInsecure, profileName: profileName)
            all.append(contentsOf: result.entries)
            marker = result.nextMarker
        } while marker != nil && marker != ""

        return all
    }

    func headObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "HEAD"
        request.timeoutInterval = timeout

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        let objectInfo = parseHeadObject(key: key, headers: headerMap)
        await metricsRecorder.record(category: .head, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: 0,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: objectInfo
        )
    }

    func putObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "PUT"
        request.timeoutInterval = timeout
        request.setValue("BlockBlob", forHTTPHeaderField: "x-ms-blob-type")
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }

        let delegate = AzureProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress, endpoint: endpoint)
        let session = URLSession(configuration: sessionConfigurationProvider(), delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performUpload(session: session, request: request, data: data)
        await metricsRecorder.record(category: .put, uploaded: Int64(data.count), downloaded: 0, profileName: profileName)
        return result
    }

    func uploadFileWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, fileURL: URL, contentType: String?, settings: UploadParameters, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
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
        return try await uploadBlocks(
            endpoint: endpoint,
            container: container,
            key: key,
            fileURL: fileURL,
            contentType: contentType,
            settings: settings,
            allowInsecure: allowInsecure,
            profileName: profileName,
            progress: progress
        )
    }

    func getObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout

        let delegate = AzureProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress, endpoint: endpoint)
        let session = URLSession(configuration: sessionConfigurationProvider(), delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performDownload(session: session, request: request)
        await metricsRecorder.record(category: .get, uploaded: 0, downloaded: Int64(result.data.count), profileName: profileName)
        return result
    }

    func getObjectVersionWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key, queryItems: [
            URLQueryItem(name: "versionid", value: versionId)
        ])
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout

        let delegate = AzureProgressSessionDelegate(allowInsecure: allowInsecure, progress: progress, endpoint: endpoint)
        let session = URLSession(configuration: sessionConfigurationProvider(), delegate: delegate, delegateQueue: nil)
        let result = try await delegate.performDownload(session: session, request: request)
        await metricsRecorder.record(category: .get, uploaded: 0, downloaded: Int64(result.data.count), profileName: profileName)
        return result
    }

    func deleteObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "DELETE"
        request.timeoutInterval = timeout

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        await metricsRecorder.record(category: .delete, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: 0,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func deleteObjectVersion(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        let queryItems: [URLQueryItem] = versionId.isEmpty ? [] : [
            URLQueryItem(name: "versionid", value: versionId)
        ]
        let requestURL = endpoint.azureURL(container: container, blobPath: key, queryItems: queryItems)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "DELETE"
        request.timeoutInterval = timeout
        if versionId.isEmpty {
            request.setValue("true", forHTTPHeaderField: "x-ms-delete-type-permanent")
            request.setValue(azureApiVersion, forHTTPHeaderField: "x-ms-version")
        }

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        await metricsRecorder.record(category: .delete, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: 0,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func undeleteObject(endpoint: StorageEndpoint, bucket: String, key: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let container = endpoint.container ?? bucket
        let requestURL = endpoint.azureURL(container: container, blobPath: key, queryItems: [
            URLQueryItem(name: "comp", value: "undelete")
        ])
        var request = URLRequest(url: requestURL)
        request.httpMethod = "PUT"
        request.timeoutInterval = timeout
        request.setValue(azureApiVersion, forHTTPHeaderField: "x-ms-version")

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        await metricsRecorder.record(category: .put, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: 0,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func shareLink(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresHours: Int) -> String? {
        let container = endpoint.container ?? bucket
        let url = endpoint.azureURL(container: container, blobPath: key)
        return url.absoluteString
    }

    func listContainers(endpoint: StorageEndpoint, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let requestURL = endpoint.azureURL(container: nil, blobPath: nil, queryItems: [
            URLQueryItem(name: "comp", value: "list")
        ])
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout

        let session = makeSession(allowInsecure: allowInsecure)
        let start = Date()
        let (data, response) = try await session.data(for: request)
        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let parser = AzureContainerListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        let containerNames = parser.containerNames.isEmpty ? parseContainerNamesFallback(from: data) : parser.containerNames
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: String(data: data, encoding: .utf8),
            elapsedMs: elapsed,
            bucketNames: containerNames,
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    private func listBlobs(endpoint: StorageEndpoint, container: String, prefix: String, delimiter: String?, include: [String]?, marker: String?, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        let result = try await listBlobsPage(endpoint: endpoint, container: container, prefix: prefix, delimiter: delimiter, include: include, marker: marker, allowInsecure: allowInsecure, profileName: profileName)
        return ConnectionResult(
            statusCode: result.statusCode,
            responseText: result.responseText,
            elapsedMs: result.elapsedMs,
            bucketNames: [],
            responseHeaders: result.responseHeaders,
            requestSummary: result.requestSummary,
            objectEntries: result.entries,
            objectInfo: nil,
            isTruncated: result.nextMarker != nil && result.nextMarker != "",
            nextContinuationToken: result.nextMarker
        )
    }

    private func listBlobsPage(endpoint: StorageEndpoint, container: String, prefix: String, delimiter: String?, include: [String]?, marker: String?, allowInsecure: Bool, profileName: String) async throws -> (entries: [S3Object], nextMarker: String?, statusCode: Int?, responseText: String?, elapsedMs: Int, responseHeaders: [String: String], requestSummary: String) {
        var queryItems: [URLQueryItem] = [
            URLQueryItem(name: "restype", value: "container"),
            URLQueryItem(name: "comp", value: "list"),
            URLQueryItem(name: "maxresults", value: "5000")
        ]
        if let delimiter {
            queryItems.append(URLQueryItem(name: "delimiter", value: delimiter))
        }
        if !prefix.isEmpty {
            queryItems.append(URLQueryItem(name: "prefix", value: prefix))
        }
        if let marker, !marker.isEmpty {
            queryItems.append(URLQueryItem(name: "marker", value: marker))
        }
        if let include, !include.isEmpty {
            queryItems.append(URLQueryItem(name: "include", value: include.joined(separator: ",")))
        }

        let requestURL = endpoint.azureURL(container: container, blobPath: nil, queryItems: queryItems)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout

        let session = makeSession(allowInsecure: allowInsecure)
        let start = Date()
        let (data, response) = try await session.data(for: request)
        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let headerMap = headersMap(from: http)
        let parser = AzureBlobListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
        await metricsRecorder.record(category: .list, uploaded: 0, downloaded: Int64(data.count), profileName: profileName)

        return (parser.entries, parser.nextMarker, http?.statusCode, String(data: data, encoding: .utf8), elapsed, headerMap, requestSummary)
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

    private func uploadBlocks(endpoint: StorageEndpoint, container: String, key: String, fileURL: URL, contentType: String?, settings: UploadParameters, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        let fileAttributes = try FileManager.default.attributesOfItem(atPath: fileURL.path)
        let fileSize = (fileAttributes[.size] as? NSNumber)?.int64Value ?? 0
        let blockSize = max(settings.multipartChunkBytes, 1 * 1024 * 1024)
        let totalBlocks = Int((fileSize + Int64(blockSize) - 1) / Int64(blockSize))
        let semaphore = AsyncSemaphore(settings.maxConcurrentRequests)
        let accumulator = ProgressAccumulator()

        func blockId(for number: Int) -> String {
            let raw = String(format: "block-%06d", number)
            return Data(raw.utf8).base64EncodedString()
        }

        do {
            let blockIds = try await withThrowingTaskGroup(of: (Int, String).self) { group -> [String] in
                for partNumber in 1...totalBlocks {
                    let offset = Int64(partNumber - 1) * Int64(blockSize)
                    let size = Int(min(Int64(blockSize), fileSize - offset))
                    await semaphore.acquire()
                    group.addTask { [self, allowInsecure] in
                        defer { Task { await semaphore.release() } }
                        let handle = try FileHandle(forReadingFrom: fileURL)
                        try handle.seek(toOffset: UInt64(offset))
                        let data = try handle.read(upToCount: size) ?? Data()
                        try handle.close()

                        let id = blockId(for: partNumber)
                        let requestURL = endpoint.azureURL(container: container, blobPath: key, queryItems: [
                            URLQueryItem(name: "comp", value: "block"),
                            URLQueryItem(name: "blockid", value: id)
                        ])
                        var request = URLRequest(url: requestURL)
                        request.httpMethod = "PUT"
                        request.timeoutInterval = self.timeout

                        let session = self.makeSession(allowInsecure: allowInsecure)
                        let (_, response) = try await session.upload(for: request, from: data)
                        if let http = response as? HTTPURLResponse, http.statusCode >= 400 {
                            throw NSError(domain: "AzureBlockUpload", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: "Block upload failed"])
                        }

                        let totalSent = await accumulator.add(Int64(data.count))
                        progress(totalSent, fileSize)
                        return (partNumber, id)
                    }
                }

                var collected: [(Int, String)] = []
                for try await result in group {
                    collected.append(result)
                }
                let sorted = collected.sorted { $0.0 < $1.0 }
                return sorted.map { $0.1 }
            }

            var body = "<BlockList>"
            for id in blockIds {
                body += "<Latest>\(id)</Latest>"
            }
            body += "</BlockList>"
            let data = Data(body.utf8)
            let requestURL = endpoint.azureURL(container: container, blobPath: key, queryItems: [
                URLQueryItem(name: "comp", value: "blocklist")
            ])
            var request = URLRequest(url: requestURL)
            request.httpMethod = "PUT"
            request.timeoutInterval = timeout
            request.setValue("application/xml", forHTTPHeaderField: "Content-Type")
            if let contentType {
                request.setValue(contentType, forHTTPHeaderField: "x-ms-blob-content-type")
            }

            let start = Date()
            let session = makeSession(allowInsecure: allowInsecure)
            let (responseData, response) = try await session.upload(for: request, from: data)
            let elapsed = Int(Date().timeIntervalSince(start) * 1000)
            let http = response as? HTTPURLResponse
            let headerMap = headersMap(from: http)
            let requestSummary = makeRequestSummary(request: request, endpoint: endpoint)
            await metricsRecorder.record(category: .put, uploaded: fileSize, downloaded: Int64(responseData.count), profileName: profileName)

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
        } catch {
            throw error
        }
    }

    private func makeSession(allowInsecure: Bool) -> URLSession {
        let configuration = sessionConfigurationProvider()
        if allowInsecure {
            return URLSession(configuration: configuration, delegate: InsecureSessionDelegate(), delegateQueue: nil)
        }
        return URLSession(configuration: configuration)
    }

    private func headersMap(from response: HTTPURLResponse?) -> [String: String] {
        (response?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
    }

    private func parseHeadObject(key: String, headers: [String: String]) -> S3Object {
        let size = Int(headers.first(where: { $0.key.lowercased() == "content-length" })?.value ?? "") ?? 0
        let contentType = headers.first(where: { $0.key.lowercased() == "content-type" })?.value ?? "application/octet-stream"
        let etag = headers.first(where: { $0.key.lowercased() == "etag" })?.value.replacingOccurrences(of: "\"", with: "") ?? ""
        let lastModifiedString = headers.first(where: { $0.key.lowercased() == "last-modified" })?.value ?? ""
        let lastModified = AzureDateParser.parse(lastModifiedString) ?? Date()
        return S3Object(key: key, sizeBytes: size, lastModified: lastModified, contentType: contentType, eTag: etag)
    }

    private func makeRequestSummary(request: URLRequest, endpoint: StorageEndpoint) -> String {
        let method = request.httpMethod ?? "GET"
        let urlString = endpoint.redactedURLString(request.url ?? endpoint.baseURL)
        var lines: [String] = ["\(method) \(urlString)"]
        if let headers = request.allHTTPHeaderFields {
            for (key, value) in headers.sorted(by: { $0.key.lowercased() < $1.key.lowercased() }) {
                let lower = key.lowercased()
                if lower == "authorization" {
                    lines.append("\(key): (redacted)")
                } else if lower == "x-ms-copy-source" {
                    lines.append("\(key): (redacted)")
                } else {
                    lines.append("\(key): \(value)")
                }
            }
        }
        return lines.joined(separator: "\n")
    }

    private func parseContainerNamesFallback(from data: Data) -> [String] {
        guard let text = String(data: data, encoding: .utf8) else { return [] }
        let pattern = "<Container>.*?<Name>(.*?)</Name>"
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators, .caseInsensitive]) else {
            return []
        }
        let range = NSRange(text.startIndex..<text.endIndex, in: text)
        let matches = regex.matches(in: text, options: [], range: range)
        return matches.compactMap { match in
            guard match.numberOfRanges > 1,
                  let nameRange = Range(match.range(at: 1), in: text) else {
                return nil
            }
            return String(text[nameRange]).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        .filter { !$0.isEmpty }
    }
}

enum AzureDateParser {
    static func parse(_ value: String) -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
        return formatter.date(from: value)
    }
}

final class AzureContainerListParser: NSObject, XMLParserDelegate {
    private(set) var containerNames: [String] = []
    private var currentElement: String = ""
    private var currentText: String = ""
    private var inContainer = false

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Container" {
            inContainer = true
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
        if inContainer && elementName == "Name" {
            if !trimmed.isEmpty {
                containerNames.append(trimmed)
            }
        }
        if elementName == "Container" {
            inContainer = false
        }
        currentElement = ""
        currentText = ""
    }
}

final class AzureBlobListParser: NSObject, XMLParserDelegate {
    private(set) var entries: [S3Object] = []
    private(set) var nextMarker: String?
    private var currentElement: String = ""
    private var currentText: String = ""
    private var inBlob = false
    private var inBlobPrefix = false
    private var currentKey: String = ""
    private var currentSize: Int = 0
    private var currentContentType: String = "application/octet-stream"
    private var currentETag: String = ""
    private var currentLastModified: Date = Date()
    private var currentVersionId: String = ""
    private var currentIsCurrentVersion: Bool = false
    private var currentIsDeleted: Bool = false
    private var currentAccessTier: String = ""
    private var currentBlobType: String = ""
    private var prefixValue: String = ""

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Blob" {
            inBlob = true
            currentKey = ""
            currentSize = 0
            currentContentType = "application/octet-stream"
            currentETag = ""
            currentLastModified = Date()
            currentVersionId = ""
            currentIsCurrentVersion = false
            currentIsDeleted = false
            currentAccessTier = ""
            currentBlobType = ""
        }
        if elementName == "BlobPrefix" {
            inBlobPrefix = true
            prefixValue = ""
        }
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
        if inBlob {
            switch elementName {
            case "Name":
                currentKey = trimmed
            case "Content-Length":
                currentSize = Int(trimmed) ?? 0
            case "Content-Type":
                if !trimmed.isEmpty { currentContentType = trimmed }
            case "Etag":
                currentETag = trimmed.replacingOccurrences(of: "\"", with: "")
            case "Last-Modified":
                if let date = AzureDateParser.parse(trimmed) {
                    currentLastModified = date
                }
            case "AccessTier":
                currentAccessTier = trimmed
            case "BlobType":
                currentBlobType = trimmed
            case "VersionId":
                currentVersionId = trimmed
            case "IsCurrentVersion":
                currentIsCurrentVersion = trimmed.lowercased() == "true"
            case "Deleted":
                currentIsDeleted = trimmed.lowercased() == "true"
            case "Blob":
                let entry = S3Object(
                    key: currentKey,
                    sizeBytes: currentSize,
                    lastModified: currentLastModified,
                    contentType: currentContentType,
                    eTag: currentETag,
                    storageClass: currentAccessTier,
                    blobType: currentBlobType,
                    versionId: currentVersionId.isEmpty ? nil : currentVersionId,
                    isDeleteMarker: false,
                    isDeleted: currentIsDeleted,
                    isVersioned: !currentVersionId.isEmpty,
                    isLatest: currentIsCurrentVersion
                )
                entries.append(entry)
                inBlob = false
            default:
                break
            }
        } else if inBlobPrefix {
            if elementName == "Name" {
                prefixValue = trimmed
            } else if elementName == "BlobPrefix" {
                let entry = S3Object(
                    key: prefixValue,
                    sizeBytes: 0,
                    lastModified: Date(),
                    contentType: "folder",
                    eTag: ""
                )
                entries.append(entry)
                inBlobPrefix = false
            }
        } else if elementName == "NextMarker" {
            if !trimmed.isEmpty {
                nextMarker = trimmed
            }
        }
        currentElement = ""
        currentText = ""
    }
}

private final class AzureProgressSessionDelegate: NSObject, URLSessionDataDelegate, URLSessionTaskDelegate, @unchecked Sendable {
    private let allowInsecure: Bool
    private let progress: @Sendable (Int64, Int64) -> Void
    private let endpoint: StorageEndpoint
    private var expectedBytes: Int64 = 0
    private var receivedBytes: Int64 = 0
    private var dataBuffer = Data()
    private var continuation: CheckedContinuation<ObjectDataResult, Error>?
    private var uploadContinuation: CheckedContinuation<ConnectionResult, Error>?

    init(allowInsecure: Bool, progress: @escaping @Sendable (Int64, Int64) -> Void, endpoint: StorageEndpoint) {
        self.allowInsecure = allowInsecure
        self.progress = progress
        self.endpoint = endpoint
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
        let urlString = endpoint.redactedURLString(request.url ?? endpoint.baseURL)
        var lines: [String] = ["\(method) \(urlString)"]
        if let headers = request.allHTTPHeaderFields {
            for (key, value) in headers.sorted(by: { $0.key.lowercased() < $1.key.lowercased() }) {
                let lower = key.lowercased()
                if lower == "authorization" {
                    lines.append("\(key): (redacted)")
                } else if lower == "x-ms-copy-source" {
                    lines.append("\(key): (redacted)")
                } else {
                    lines.append("\(key): \(value)")
                }
            }
        }
        return lines.joined(separator: "\n")
    }
}
