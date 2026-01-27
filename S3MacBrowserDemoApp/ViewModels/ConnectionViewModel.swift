import Foundation
import SwiftUI
import UniformTypeIdentifiers

@MainActor
final class ConnectionViewModel: ObservableObject {
    @Published var profileName = "Local MinIO"
    @Published var endpointURL = "s3.amazonaws.com"
    @Published var accessKey = ""
    @Published var secretKey = ""
    @Published var region = "us-east-1"
    @Published var insecureSSL = false

    @Published var statusMessage = "Not connected"
    @Published var lastStatusCode: Int?
    @Published var responseText: String?
    @Published var debugText: String = ""
    @Published var isBusy = false

    @Published var objects: [S3Object] = []
    @Published var breadcrumb: [String] = ["/"]
    @Published var profiles: [ConnectionProfile] = []
    @Published var selectedProfile: ConnectionProfile?
    @Published var currentBucket: String?
    @Published var currentPrefix: String = ""
    @Published var selectedObject: S3Object?
    @Published var selectedObjectInfo: S3Object?

    private let service: S3ServiceProtocol

    init(service: S3ServiceProtocol = S3Service()) {
        self.service = service
    }

    func testConnection() {
        guard let url = normalizedEndpointURL() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        isBusy = true
        statusMessage = "Connecting..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        let service = self.service
        Task {
            do {
                let result = try await service.listBuckets(
                    endpoint: url,
                    region: region,
                    accessKey: accessKey,
                    secretKey: secretKey,
                    allowInsecure: insecureSSL
                )
                lastStatusCode = result.statusCode
                responseText = result.responseText
                if let status = result.statusCode, status >= 400 {
                    statusMessage = "HTTP \(status) - Access Denied or Invalid Credentials"
                } else {
                    statusMessage = "Connected in \(result.elapsedMs) ms"
                }
                currentBucket = nil
                currentPrefix = ""
                breadcrumb = ["/"]
                objects = result.bucketNames.map { name in
                    S3Object(key: name, sizeBytes: 0, lastModified: Date(), contentType: "bucket", eTag: "")
                }
                selectedObject = nil
                selectedObjectInfo = nil
                let headersBlock = result.responseHeaders
                    .sorted { $0.key.lowercased() < $1.key.lowercased() }
                    .map { "\($0.key): \($0.value)" }
                    .joined(separator: "\n")
                let body = result.responseText ?? "(empty response body)"
                debugText = [
                    "Request:",
                    result.requestSummary,
                    "",
                    "Response Status: \(result.statusCode.map(String.init) ?? "nil")",
                    "Response Headers:",
                    headersBlock.isEmpty ? "(none)" : headersBlock,
                    "",
                    "Response Body:",
                    body
                ].joined(separator: "\n")
            } catch {
                statusMessage = "Connection failed: \(error.localizedDescription)"
                debugText = "Connection failed: \(error)"
            }
            isBusy = false
        }
    }

    func saveProfile() {
        let trimmedName = profileName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedName.isEmpty else {
            statusMessage = "Profile name is required"
            return
        }

        let profile = ConnectionProfile(
            name: trimmedName,
            endpoint: endpointURL,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey
        )
        if let index = profiles.firstIndex(where: { $0.name == trimmedName }) {
            profiles[index] = profile
        } else {
            profiles.append(profile)
        }
        selectedProfile = profile
        statusMessage = "Saved profile \(trimmedName)"
    }

    func loadProfile(_ profile: ConnectionProfile) {
        profileName = profile.name
        endpointURL = profile.endpoint
        region = profile.region
        accessKey = profile.accessKey
        secretKey = profile.secretKey
    }

    func openBreadcrumb(at index: Int) {
        guard index >= 0, index < breadcrumb.count else { return }
        if index == 0 {
            navigateBackToBuckets()
        } else if let bucket = currentBucket {
            let depth = index - 1
            let components = Array(breadcrumb.dropFirst(2).prefix(depth))
            let prefix = components.isEmpty ? "" : components.joined(separator: "/") + "/"
            Task {
                await listObjects(bucket: bucket, prefix: prefix)
            }
        }
    }

    func enterFolder(named name: String) {
        guard let bucket = currentBucket else {
            Task {
                await openBucket(name)
            }
            return
        }
        let normalized = name.hasSuffix("/") ? String(name.dropLast()) : name
        let relative = normalized.hasPrefix(currentPrefix) ? String(normalized.dropFirst(currentPrefix.count)) : normalized
        let cleanName = relative.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        breadcrumb.append(cleanName)
        let newPrefix = currentPrefix + cleanName + "/"
        Task {
            await listObjects(bucket: bucket, prefix: newPrefix)
        }
    }

    func openObject(_ object: S3Object) {
        selectedObject = object
        if object.contentType == "bucket" {
            Task { await openBucket(object.key) }
        } else if object.key.hasSuffix("/") {
            enterFolder(named: object.key)
        } else {
            Task { await fetchObjectInfo(object) }
        }
    }

    func selectObject(_ object: S3Object) {
        selectedObject = object
        if object.contentType == "bucket" || object.key.hasSuffix("/") {
            selectedObjectInfo = object
        } else {
            Task { await fetchObjectInfo(object) }
        }
    }

    func openBucket(_ name: String) async {
        currentBucket = name
        currentPrefix = ""
        breadcrumb = ["/", name]
        await listObjects(bucket: name, prefix: "")
    }

    func navigateBack() {
        if currentBucket == nil {
            return
        }
        if currentPrefix.isEmpty {
            navigateBackToBuckets()
            return
        }

        var parts = currentPrefix.split(separator: "/").map(String.init)
        if !parts.isEmpty {
            parts.removeLast()
        }
        let newPrefix = parts.isEmpty ? "" : parts.joined(separator: "/") + "/"
        if let bucket = currentBucket {
            Task {
                await listObjects(bucket: bucket, prefix: newPrefix)
            }
        }
    }

    private func navigateBackToBuckets() {
        currentBucket = nil
        currentPrefix = ""
        breadcrumb = ["/"]
        testConnection()
    }

    private func listObjects(bucket: String, prefix: String) async {
        guard let endpoint = normalizedEndpointURL() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        isBusy = true
        statusMessage = "Listing \(bucket)..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        do {
            let result = try await service.listObjects(
                endpoint: endpoint,
                bucket: bucket,
                prefix: prefix,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: insecureSSL
            )
            lastStatusCode = result.statusCode
            responseText = result.responseText
            currentPrefix = prefix
            let prefixParts = prefix.split(separator: "/").map(String.init)
            breadcrumb = ["/", bucket] + prefixParts
            if let status = result.statusCode, status >= 400 {
                statusMessage = "HTTP \(status) - Access Denied or Invalid Credentials"
            } else {
                statusMessage = "Listed objects in \(result.elapsedMs) ms"
            }
            objects = result.objectEntries.sorted { $0.key < $1.key }
            selectedObject = nil
            selectedObjectInfo = nil

            let headersBlock = result.responseHeaders
                .sorted { $0.key.lowercased() < $1.key.lowercased() }
                .map { "\($0.key): \($0.value)" }
                .joined(separator: "\n")
            let body = result.responseText ?? "(empty response body)"
            debugText = [
                "Request:",
                result.requestSummary,
                "",
                "Response Status: \(result.statusCode.map(String.init) ?? "nil")",
                "Response Headers:",
                headersBlock.isEmpty ? "(none)" : headersBlock,
                "",
                "Response Body:",
                body
            ].joined(separator: "\n")
        } catch {
            statusMessage = "Connection failed: \(error.localizedDescription)"
            debugText = "Connection failed: \(error)"
        }

        isBusy = false
    }

    func fetchObjectInfo(_ object: S3Object) async {
        guard let bucket = currentBucket,
              let endpoint = normalizedEndpointURL() else {
            return
        }

        if object.contentType == "folder" {
            selectedObjectInfo = object
            return
        }

        isBusy = true
        statusMessage = "Fetching info..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        do {
            let result = try await service.headObject(
                endpoint: endpoint,
                bucket: bucket,
                key: object.key,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: insecureSSL
            )
            lastStatusCode = result.statusCode
            responseText = result.responseText
            selectedObjectInfo = result.objectInfo ?? object
            statusMessage = "Info loaded"

            let headersBlock = result.responseHeaders
                .sorted { $0.key.lowercased() < $1.key.lowercased() }
                .map { "\($0.key): \($0.value)" }
                .joined(separator: "\n")
            let body = result.responseText ?? "(empty response body)"
            debugText = [
                "Request:",
                result.requestSummary,
                "",
                "Response Status: \(result.statusCode.map(String.init) ?? "nil")",
                "Response Headers:",
                headersBlock.isEmpty ? "(none)" : headersBlock,
                "",
                "Response Body:",
                body
            ].joined(separator: "\n")
        } catch {
            statusMessage = "Info failed: \(error.localizedDescription)"
            debugText = "Info failed: \(error)"
        }

        isBusy = false
    }

    func presignedURL(for object: S3Object, expiresHours: Int) -> String? {
        guard let bucket = currentBucket,
              let endpoint = normalizedEndpointURL() else {
            return nil
        }
        if object.contentType == "bucket" || object.key.hasSuffix("/") {
            return nil
        }
        let seconds = min(max(expiresHours, 1), 168) * 3600
        return service.presignGetURL(
            endpoint: endpoint,
            bucket: bucket,
            key: object.key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            expiresSeconds: seconds
        )
    }

    func uploadFiles(_ urls: [URL]) {
        guard let bucket = currentBucket else {
            statusMessage = "Select a bucket before uploading"
            return
        }
        guard let endpoint = normalizedEndpointURL() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        Task {
            isBusy = true
            statusMessage = "Uploading..."
            for url in urls {
                do {
                    let prefix = currentPrefix
                    let (data, key, contentType) = try await Task.detached {
                        let data = try Data(contentsOf: url)
                        let key = prefix + url.lastPathComponent
                        let contentType = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType
                        return (data, key, contentType)
                    }.value
                    let result = try await service.putObject(
                        endpoint: endpoint,
                        bucket: bucket,
                        key: key,
                        data: data,
                        contentType: contentType,
                        region: region,
                        accessKey: accessKey,
                        secretKey: secretKey,
                        allowInsecure: insecureSSL
                    )
                    lastStatusCode = result.statusCode
                    responseText = result.responseText
                    let headersBlock = result.responseHeaders
                        .sorted { $0.key.lowercased() < $1.key.lowercased() }
                        .map { "\($0.key): \($0.value)" }
                        .joined(separator: "\n")
                    let body = result.responseText ?? "(empty response body)"
                    debugText = [
                        "Request:",
                        result.requestSummary,
                        "",
                        "Response Status: \(result.statusCode.map(String.init) ?? "nil")",
                        "Response Headers:",
                        headersBlock.isEmpty ? "(none)" : headersBlock,
                        "",
                        "Response Body:",
                        body
                    ].joined(separator: "\n")
                } catch {
                    statusMessage = "Upload failed: \(error.localizedDescription)"
                    debugText = "Upload failed: \(error)"
                }
            }
            if let bucket = currentBucket {
                await listObjects(bucket: bucket, prefix: currentPrefix)
            }
            statusMessage = "Upload complete"
            isBusy = false
        }
    }

    func deleteObjects(_ targets: [S3Object]) {
        guard let bucket = currentBucket else {
            statusMessage = "Select a bucket before deleting"
            return
        }
        guard let endpoint = normalizedEndpointURL() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        Task {
            isBusy = true
            statusMessage = "Deleting..."
            var deletedCount = 0

            for object in targets {
                if object.contentType == "bucket" {
                    continue
                }
                if object.key.hasSuffix("/") {
                    let prefix = object.key
                    do {
                        let allObjects = try await service.listAllObjects(
                            endpoint: endpoint,
                            bucket: bucket,
                            prefix: prefix,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL
                        )
                        for entry in allObjects where !entry.key.hasSuffix("/") {
                            let result = try await service.deleteObject(
                                endpoint: endpoint,
                                bucket: bucket,
                                key: entry.key,
                                region: region,
                                accessKey: accessKey,
                                secretKey: secretKey,
                                allowInsecure: insecureSSL
                            )
                            deletedCount += 1
                            lastStatusCode = result.statusCode
                            responseText = result.responseText
                        }
                        _ = try await service.deleteObject(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: prefix,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL
                        )
                    } catch {
                        statusMessage = "Delete failed: \(error.localizedDescription)"
                        debugText = "Delete failed: \(error)"
                    }
                } else {
                    do {
                        let result = try await service.deleteObject(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: object.key,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL
                        )
                        deletedCount += 1
                        lastStatusCode = result.statusCode
                        responseText = result.responseText
                    } catch {
                        statusMessage = "Delete failed: \(error.localizedDescription)"
                        debugText = "Delete failed: \(error)"
                    }
                }
            }

            if let bucket = currentBucket {
                await listObjects(bucket: bucket, prefix: currentPrefix)
            }
            statusMessage = "Deleted \(deletedCount) object(s)"
            isBusy = false
        }
    }

    func refreshCurrentView() {
        if let bucket = currentBucket {
            Task { await listObjects(bucket: bucket, prefix: currentPrefix) }
        } else {
            testConnection()
        }
    }

    private func normalizedEndpointURL() -> URL? {
        let trimmed = endpointURL.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if trimmed.contains("://") {
            return URL(string: trimmed)
        }
        let scheme = isLocalOrPrivateHost(trimmed) ? "http" : "https"
        return URL(string: "\(scheme)://\(trimmed)")
    }

    private func isLocalOrPrivateHost(_ host: String) -> Bool {
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

    func seedDemoObjects() {
        let now = Date()
        objects = [
            S3Object(key: "photos/", sizeBytes: 0, lastModified: now, contentType: "folder", eTag: ""),
            S3Object(key: "docs/readme.txt", sizeBytes: 2048, lastModified: now.addingTimeInterval(-3600), contentType: "text/plain", eTag: "demo-etag-1"),
            S3Object(key: "backup/archive.zip", sizeBytes: 5242880, lastModified: now.addingTimeInterval(-7200), contentType: "application/zip", eTag: "demo-etag-2")
        ]
    }
}
