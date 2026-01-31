import Foundation

enum StorageProvider: String, Codable {
    case s3
    case azureBlob
}

struct StorageEndpoint {
    let provider: StorageProvider
    let rawInput: String
    let baseURL: URL
    let sasToken: String?
    let container: String?

    var isAzureAccountSAS: Bool {
        provider == .azureBlob && container == nil
    }

    var isAzureContainerSAS: Bool {
        provider == .azureBlob && container != nil
    }

    func azureURL(container: String?, blobPath: String?, queryItems: [URLQueryItem] = []) -> URL {
        var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)
        var pathParts: [String] = []
        if let container, !container.isEmpty {
            pathParts.append(container)
        }
        if let blobPath, !blobPath.isEmpty {
            pathParts.append(blobPath)
        }
        components?.path = "/" + pathParts.joined(separator: "/")
        if let sasToken, !sasToken.isEmpty {
            let extraQuery = encodedQueryString(queryItems)
            let combined = extraQuery.isEmpty ? sasToken : "\(sasToken)&\(extraQuery)"
            components?.percentEncodedQuery = combined
        } else {
            components?.queryItems = queryItems.isEmpty ? nil : queryItems
        }
        return components?.url ?? baseURL
    }

    func redactedURLString(_ url: URL) -> String {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }
        if components.query != nil {
            components.query = "redacted"
        }
        return components.url?.absoluteString ?? url.absoluteString
    }

    private func encodedQueryString(_ items: [URLQueryItem]) -> String {
        guard !items.isEmpty else { return "" }
        var components = URLComponents()
        components.queryItems = items
        return components.percentEncodedQuery ?? ""
    }
}

enum StorageEndpointParser {
    static func parse(input: String) -> StorageEndpoint? {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }

        let hasScheme = trimmed.contains("://")
        let candidate = hasScheme ? trimmed : "https://\(trimmed)"
        guard let url = URL(string: candidate),
              let host = url.host else {
            return nil
        }

        let hostLower = host.lowercased()
        let queryLower = (url.query ?? "").lowercased()
        let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        let hasSig = components?.queryItems?.contains(where: { $0.name.lowercased() == "sig" }) ?? false
        let isAzureHost = hostLower.contains("blob.core.")
        let isAzure = isAzureHost || hasSig || queryLower.contains("sig=")

        if isAzure {
            let scheme = url.scheme ?? "https"
            var baseComponents = URLComponents()
            baseComponents.scheme = scheme
            baseComponents.host = host
            baseComponents.port = url.port
            let baseURL = baseComponents.url ?? url
            let container = url.path.split(separator: "/").first.map(String.init)
            let sasToken = components?.percentEncodedQuery
            return StorageEndpoint(
                provider: .azureBlob,
                rawInput: trimmed,
                baseURL: baseURL,
                sasToken: sasToken,
                container: container
            )
        }

        let scheme: String
        if hasScheme {
            scheme = url.scheme ?? "https"
        } else {
            scheme = isLocalOrPrivateHost(host) ? "http" : "https"
        }
        var baseComponents = URLComponents()
        baseComponents.scheme = scheme
        baseComponents.host = host
        baseComponents.port = url.port
        baseComponents.path = url.path.isEmpty ? "" : url.path
        let baseURL = baseComponents.url ?? url

        return StorageEndpoint(
            provider: .s3,
            rawInput: trimmed,
            baseURL: baseURL,
            sasToken: nil,
            container: nil
        )
    }

    private static func isLocalOrPrivateHost(_ host: String) -> Bool {
        let lower = host.lowercased()
        if lower == "localhost" || lower.hasSuffix(".local") {
            return true
        }
        let hostOnly = lower.split(separator: ":").first.map(String.init) ?? lower
        let parts = hostOnly.split(separator: ".").compactMap { Int($0) }
        guard parts.count == 4 else { return false }
        let a = parts[0]
        let b = parts[1]
        if a == 10 { return true }
        if a == 127 { return true }
        if a == 192 && b == 168 { return true }
        if a == 172 && (16...31).contains(b) { return true }
        return false
    }
}

protocol StorageBackend: Sendable {
    var provider: StorageProvider { get }

    func testConnection(endpoint: StorageEndpoint, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listObjectVersions(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func listAllObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> [S3Object]
    func headObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func putObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult
    func getObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult
    func getObjectVersionWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult
    func deleteObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func deleteObjectVersion(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult
    func shareLink(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresHours: Int) -> String?
}
