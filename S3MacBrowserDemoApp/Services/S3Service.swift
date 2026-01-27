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
}

protocol S3ServiceProtocol: Sendable {
    func listBuckets(endpoint: URL, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult
    func listObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult
    func headObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult
    func putObject(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult
    func presignGetURL(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresSeconds: Int) -> String
    func deleteObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult
    func listAllObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> [S3Object]
}

final class S3Service: NSObject, S3ServiceProtocol, @unchecked Sendable {
    private let timeout: TimeInterval = 10

    func listBuckets(endpoint: URL, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
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
        let bucketNames = parseBucketNames(from: data)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: bucketNames,
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: [],
            objectInfo: nil
        )
    }

    func listObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
        let start = Date()
        let requestURL = makeListObjectsURL(endpoint: endpoint, bucket: bucket, prefix: prefix, delimiter: "/")
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, response) = try await session.data(for: request)

        let elapsed = Int(Date().timeIntervalSince(start) * 1000)
        let http = response as? HTTPURLResponse
        let text = String(data: data, encoding: .utf8)
        let objectEntries = parseObjectEntries(from: data)
        let headerMap = (http?.allHeaderFields ?? [:]).reduce(into: [String: String]()) { partial, entry in
            let key = String(describing: entry.key)
            let value = String(describing: entry.value)
            partial[key] = value
        }
        let requestSummary = makeRequestSummary(request: request)

        return ConnectionResult(
            statusCode: http?.statusCode,
            responseText: text,
            elapsedMs: elapsed,
            bucketNames: [],
            responseHeaders: headerMap,
            requestSummary: requestSummary,
            objectEntries: objectEntries,
            objectInfo: nil
        )
    }

    private func listObjectsPage(endpoint: URL, bucket: String, prefix: String, continuationToken: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> (entries: [S3Object], nextContinuationToken: String?) {
        let requestURL = makeListObjectsURL(endpoint: endpoint, bucket: bucket, prefix: prefix, delimiter: nil, continuationToken: continuationToken)
        var request = URLRequest(url: requestURL)
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        signRequest(&request, region: region, accessKey: accessKey, secretKey: secretKey, payloadHash: sha256Hex(""))

        let session = makeSession(allowInsecure: allowInsecure)
        let (data, _) = try await session.data(for: request)
        let parsed = parseObjectEntriesWithToken(from: data)
        return (parsed.entries, parsed.nextContinuationToken)
    }

    func headObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
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

    func putObject(endpoint: URL, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
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
        let canonicalUri = url.path.isEmpty ? "/" : awsEncode(url.path, encodeSlash: false)
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

    func deleteObject(endpoint: URL, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> ConnectionResult {
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

    func listAllObjects(endpoint: URL, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool) async throws -> [S3Object] {
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
                allowInsecure: allowInsecure
            )
            all.append(contentsOf: result.entries)
            token = result.nextContinuationToken
        } while token != nil

        return all
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

        let canonicalUri = url.path.isEmpty ? "/" : awsEncode(url.path, encodeSlash: false)
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

    private func parseObjectEntries(from data: Data) -> [S3Object] {
        parseObjectEntriesWithToken(from: data).entries
    }

    private func parseObjectEntriesWithToken(from data: Data) -> (entries: [S3Object], nextContinuationToken: String?) {
        let parser = ObjectListParser()
        let xml = XMLParser(data: data)
        xml.delegate = parser
        xml.parse()
        return (parser.entries, parser.nextContinuationToken)
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
        guard let items = components.queryItems, !items.isEmpty else {
            return ""
        }
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

    private func awsEncode(_ string: String, encodeSlash: Bool) -> String {
        let unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        var allowed = CharacterSet(charactersIn: unreserved)
        if !encodeSlash {
            allowed.insert(charactersIn: "/")
        }
        return string.unicodeScalars.map { scalar in
            if allowed.contains(scalar) {
                return String(scalar)
            }
            return String(format: "%%%02X", scalar.value)
        }.joined()
    }

    private func makeListObjectsURL(endpoint: URL, bucket: String, prefix: String, delimiter: String?, continuationToken: String? = nil) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        components?.path = "/" + safeBucket
        var items: [URLQueryItem] = [
            URLQueryItem(name: "list-type", value: "2")
        ]
        if let delimiter {
            items.append(URLQueryItem(name: "delimiter", value: delimiter))
        }
        if !prefix.isEmpty {
            items.append(URLQueryItem(name: "prefix", value: prefix))
        }
        if let continuationToken, !continuationToken.isEmpty {
            items.append(URLQueryItem(name: "continuation-token", value: continuationToken))
        }
        components?.queryItems = items
        return components?.url ?? endpoint
    }

    private func makeObjectURL(endpoint: URL, bucket: String, key: String) -> URL {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        let safeBucket = bucket.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? bucket
        let safeKey = key.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? key
        components?.path = "/" + safeBucket + "/" + safeKey
        components?.queryItems = nil
        return components?.url ?? endpoint
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

private final class BucketListParser: NSObject, XMLParserDelegate {
    private(set) var bucketNames: [String] = []
    private var currentElement: String = ""
    private var currentText: String = ""

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
    }

    func parser(_ parser: XMLParser, foundCharacters string: String) {
        currentText += string
    }

    func parser(_ parser: XMLParser, didEndElement elementName: String, namespaceURI: String?, qualifiedName qName: String?) {
        if elementName == "Name" && currentElement == "Name" {
            let trimmed = currentText.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                bucketNames.append(trimmed)
            }
        }
        currentElement = ""
        currentText = ""
    }
}

private final class ObjectListParser: NSObject, XMLParserDelegate {
    private(set) var entries: [S3Object] = []
    private(set) var nextContinuationToken: String?
    private var currentElement: String = ""
    private var currentText: String = ""
    private var currentKey: String = ""
    private var currentSize: Int = 0
    private var currentETag: String = ""
    private var currentLastModified: Date = Date()
    private var collectingContents = false
    private var collectingPrefix = false
    private var prefixValue: String = ""
    private let dateFormatter = ISO8601DateFormatter()

    func parser(_ parser: XMLParser, didStartElement elementName: String, namespaceURI: String?, qualifiedName qName: String?, attributes attributeDict: [String: String]) {
        currentElement = elementName
        currentText = ""
        if elementName == "Contents" {
            collectingContents = true
            currentKey = ""
            currentSize = 0
            currentETag = ""
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
                currentKey = trimmed
            case "Size":
                currentSize = Int(trimmed) ?? 0
            case "ETag":
                currentETag = trimmed.replacingOccurrences(of: "\"", with: "")
            case "LastModified":
                if let date = dateFormatter.date(from: trimmed) {
                    currentLastModified = date
                }
            case "Contents":
                let entry = S3Object(
                    key: currentKey,
                    sizeBytes: currentSize,
                    lastModified: currentLastModified,
                    contentType: "object",
                    eTag: currentETag
                )
                entries.append(entry)
                collectingContents = false
            default:
                break
            }
        } else if collectingPrefix {
            if elementName == "Prefix" {
                prefixValue = trimmed
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
        }
        currentElement = ""
        currentText = ""
    }
}
