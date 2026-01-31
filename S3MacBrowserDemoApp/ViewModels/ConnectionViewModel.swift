import Foundation
import SwiftUI
import UniformTypeIdentifiers
import AppKit

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
    @Published var debugText: String = "" {
        didSet { appendDebugHistory(debugText) }
    }
    @Published var debugHistory: [DebugEntry] = []
    @Published var selectedDebugIndex: Int = 0
    @Published var isBusy = false

    @Published var objects: [S3Object] = []
    @Published var breadcrumb: [String] = ["/"]
    @Published var profiles: [ConnectionProfile] = [] {
        didSet { persistProfiles() }
    }
    @Published var selectedProfile: ConnectionProfile?
    @Published var currentBucket: String?
    @Published var currentPrefix: String = ""
    @Published var selectedObject: S3Object?
    @Published var selectedObjectInfo: S3Object?
    @Published var transferStatus: String = "Idle"
    @Published var transferProgress: Double = 0.0
    @Published var currentTransferItem: String = ""
    @Published var currentTransferProgress: Double = 0.0
    @Published var recentTransfers: [TransferItem] = []
    @Published var provider: StorageProvider = .s3
    @Published private var uploadTotalBytes: Int64 = 0
    @Published private var uploadDoneBytes: Int64 = 0
    @Published private var uploadLastSent: Int64 = 0
    @Published private var downloadTotalBytes: Int64 = 0
    @Published private var downloadDoneBytes: Int64 = 0
    @Published private var downloadLastReceived: Int64 = 0
    @AppStorage("ui.showVersionsDeleted") var showVersionsDeleted: Bool = false

    private let s3Backend: StorageBackend
    private let azureBackend: StorageBackend

    private func appendDebugHistory(_ text: String) {
        guard !text.isEmpty else { return }
        let title = debugTitle(from: text)
        if debugHistory.first?.text == text {
            selectedDebugIndex = 0
            return
        }
        debugHistory.insert(DebugEntry(title: title, text: text), at: 0)
        if debugHistory.count > 12 {
            debugHistory = Array(debugHistory.prefix(12))
        }
        selectedDebugIndex = 0
    }

    private func debugTitle(from text: String) -> String {
        let lines = text.split(separator: "\n", omittingEmptySubsequences: false)
        guard let requestIndex = lines.firstIndex(where: { $0 == "Request:" }),
              requestIndex + 1 < lines.count else {
            return "History"
        }
        let requestLine = lines[requestIndex + 1]
        let parts = requestLine.split(separator: " ", maxSplits: 1)
        let method = parts.first.map(String.init) ?? "REQ"
        if parts.count > 1 {
            return "\(method) \(parts[1])"
        }
        return method
    }

    init(s3Backend: StorageBackend = S3Backend(), azureBackend: StorageBackend = AzureBlobBackend()) {
        self.s3Backend = s3Backend
        self.azureBackend = azureBackend
        loadProfiles()
    }

    func testConnection() {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        provider = endpoint.provider
        isBusy = true
        statusMessage = "Connecting..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        let backend = backend(for: endpoint)
        Task {
            do {
                let result = try await backend.testConnection(
                    endpoint: endpoint,
                    region: region,
                    accessKey: accessKey,
                    secretKey: secretKey,
                    allowInsecure: insecureSSL,
                    profileName: activeProfileName
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
                if endpoint.provider == .azureBlob, result.bucketNames.isEmpty, endpoint.container == nil {
                    statusMessage = "Connected (no containers found). Use a container SAS URL or create a container."
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

    func deleteCurrentProfile() {
        let trimmedName = profileName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedName.isEmpty else {
            statusMessage = "No profile selected"
            return
        }
        if let index = profiles.firstIndex(where: { $0.name == trimmedName }) {
            profiles.remove(at: index)
            if selectedProfile?.name == trimmedName {
                selectedProfile = nil
            }
            statusMessage = "Deleted profile \(trimmedName)"
        } else {
            statusMessage = "Profile not found"
        }
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
        if object.isDeleteMarker || object.isDeleted {
            selectedObjectInfo = object
            return
        }
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

    func openPathInput(_ input: String) {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }

        let path = trimmed.hasPrefix("/") ? String(trimmed.dropFirst()) : trimmed
        let parts = path.split(separator: "/").map(String.init)
        guard !parts.isEmpty else { return }

        let bucket: String
        var prefixParts: [String] = []

        if endpoint.provider == .azureBlob, let fixedContainer = endpoint.container, !fixedContainer.isEmpty {
            bucket = fixedContainer
            if parts.first == fixedContainer {
                prefixParts = Array(parts.dropFirst())
            } else {
                prefixParts = parts
            }
        } else {
            bucket = parts[0]
            prefixParts = Array(parts.dropFirst())
        }

        let prefix = prefixParts.isEmpty ? "" : prefixParts.joined(separator: "/") + "/"
        Task {
            await openBucket(bucket)
            await listObjects(bucket: bucket, prefix: prefix)
        }
    }

    private func navigateBackToBuckets() {
        currentBucket = nil
        currentPrefix = ""
        breadcrumb = ["/"]
        testConnection()
    }

    private func listObjects(bucket: String, prefix: String) async {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }
        provider = endpoint.provider

        isBusy = true
        statusMessage = "Listing \(bucket)..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        do {
            let result: ConnectionResult
            if showVersionsDeleted {
                result = try await backend(for: endpoint).listObjectVersions(
                    endpoint: endpoint,
                    bucket: bucket,
                    prefix: prefix,
                    region: region,
                    accessKey: accessKey,
                    secretKey: secretKey,
                    allowInsecure: insecureSSL,
                    profileName: activeProfileName
                )
            } else {
                result = try await backend(for: endpoint).listObjects(
                    endpoint: endpoint,
                    bucket: bucket,
                    prefix: prefix,
                    region: region,
                    accessKey: accessKey,
                    secretKey: secretKey,
                    allowInsecure: insecureSSL,
                    profileName: activeProfileName
                )
            }
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
            if showVersionsDeleted && endpoint.provider == .s3 {
                objects = result.objectEntries.sorted {
                    if $0.key == $1.key {
                        return $0.lastModified > $1.lastModified
                    }
                    return $0.key < $1.key
                }
            } else {
                objects = result.objectEntries.sorted { $0.key < $1.key }
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

    func fetchObjectInfo(_ object: S3Object) async {
        guard let bucket = currentBucket,
              let endpoint = resolveEndpoint() else {
            return
        }
        provider = endpoint.provider

        if object.contentType == "folder" || object.isDeleteMarker || object.isDeleted || object.isVersioned {
            selectedObjectInfo = object
            return
        }

        isBusy = true
        statusMessage = "Fetching info..."
        lastStatusCode = nil
        responseText = nil
        debugText = ""

        do {
            let result = try await backend(for: endpoint).headObject(
                endpoint: endpoint,
                bucket: bucket,
                key: object.key,
                region: region,
                accessKey: accessKey,
                secretKey: secretKey,
                allowInsecure: insecureSSL,
                profileName: activeProfileName
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

    func shareLink(for object: S3Object, expiresHours: Int) -> String? {
        guard let bucket = currentBucket,
              let endpoint = resolveEndpoint() else {
            return nil
        }
        if object.contentType == "bucket" || object.key.hasSuffix("/") {
            return nil
        }
        if object.isDeleteMarker || object.isDeleted || object.isVersioned {
            return nil
        }
        return backend(for: endpoint).shareLink(
            endpoint: endpoint,
            bucket: bucket,
            key: object.key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            expiresHours: expiresHours
        )
    }


    func uploadFiles(_ urls: [URL]) {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }
        guard let bucket = effectiveBucket(for: endpoint) else {
            statusMessage = "Select a bucket before uploading"
            return
        }
        provider = endpoint.provider

        Task {
            isBusy = true
            statusMessage = "Uploading..."
            transferStatus = "Uploading..."
            transferProgress = 0
            currentTransferItem = ""
            currentTransferProgress = 0

            let targets = expandUploadTargets(urls)
            if targets.isEmpty {
                statusMessage = "No files to upload"
                isBusy = false
                return
            }

            let totalBytes = targets.reduce(0) { $0 + $1.size }
            uploadTotalBytes = totalBytes
            uploadDoneBytes = 0

            for target in targets {
                do {
                    currentTransferItem = target.displayName
                    currentTransferProgress = 0
                    let transferId = UUID()
                    addOrUpdateTransfer(id: transferId, name: target.displayName, progress: 0)
                    uploadLastSent = 0

                    let data = try Data(contentsOf: target.fileURL)
                    let result = try await backend(for: endpoint).putObjectWithProgress(
                        endpoint: endpoint,
                        bucket: bucket,
                        key: target.key,
                        data: data,
                        contentType: target.contentType,
                        region: region,
                        accessKey: accessKey,
                        secretKey: secretKey,
                        allowInsecure: insecureSSL,
                        profileName: activeProfileName
                    ) { [weak self] sent, expected in
                        Task { @MainActor in
                            guard let self else { return }
                            let delta = max(sent - self.uploadLastSent, 0)
                            self.uploadLastSent = sent
                            self.uploadDoneBytes += delta
                            let total = max(expected, 1)
                            self.currentTransferProgress = Double(sent) / Double(total)
                            self.transferProgress = self.uploadTotalBytes > 0 ? Double(self.uploadDoneBytes) / Double(self.uploadTotalBytes) : 0
                            self.transferStatus = "Uploading \(self.formatBytes(self.uploadDoneBytes)) / \(self.formatBytes(self.uploadTotalBytes))"
                            self.addOrUpdateTransfer(id: transferId, name: target.displayName, progress: self.currentTransferProgress)
                        }
                    }

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
                    addOrUpdateTransfer(id: transferId, name: target.displayName, progress: 1.0)
                    currentTransferProgress = 1.0
                } catch {
                    statusMessage = "Upload failed: \(error.localizedDescription)"
                    debugText = "Upload failed: \(error)"
                    addOrUpdateTransfer(id: UUID(), name: target.displayName, progress: 0)
                }
            }

            if let bucket = currentBucket {
                await listObjects(bucket: bucket, prefix: currentPrefix)
            }
            statusMessage = "Upload complete"
            transferStatus = "Upload complete"
            currentTransferItem = ""
            currentTransferProgress = 0
            uploadTotalBytes = 0
            uploadDoneBytes = 0
            uploadLastSent = 0
            isBusy = false
        }
    }


    func deleteObjects(_ targets: [S3Object]) {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }
        guard let bucket = effectiveBucket(for: endpoint) else {
            statusMessage = "Select a bucket before deleting"
            return
        }
        provider = endpoint.provider

        Task {
            isBusy = true
            statusMessage = "Deleting..."
            var deletedCount = 0

            for object in targets {
                if object.contentType == "bucket" {
                    continue
                }
                if endpoint.provider == .s3,
                   object.isVersioned,
                   let versionId = object.versionId,
                   !versionId.isEmpty {
                    do {
                        let result = try await backend(for: endpoint).deleteObjectVersion(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: object.key,
                            versionId: versionId,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL,
                            profileName: activeProfileName
                        )
                        deletedCount += 1
                        lastStatusCode = result.statusCode
                        responseText = result.responseText
                        updateDebug(from: result)
                    } catch {
                        statusMessage = "Delete failed: \(error.localizedDescription)"
                        debugText = "Delete failed: \(error)"
                    }
                    continue
                }
                if endpoint.provider == .azureBlob {
                    let versionId = object.versionId ?? ""
                    if !versionId.isEmpty || object.isDeleted {
                    do {
                        let result = try await backend(for: endpoint).deleteObjectVersion(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: object.key,
                            versionId: versionId,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL,
                            profileName: activeProfileName
                        )
                        deletedCount += 1
                        lastStatusCode = result.statusCode
                        responseText = result.responseText
                        updateDebug(from: result)
                    } catch {
                        statusMessage = "Delete failed: \(error.localizedDescription)"
                        debugText = "Delete failed: \(error)"
                    }
                    continue
                    }
                }
                if object.key.hasSuffix("/") {
                    let prefix = object.key
                    do {
                        let allObjects = try await backend(for: endpoint).listAllObjects(
                            endpoint: endpoint,
                            bucket: bucket,
                            prefix: prefix,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL,
                            profileName: activeProfileName
                        )
                        for entry in allObjects where !entry.key.hasSuffix("/") {
                            let result = try await backend(for: endpoint).deleteObject(
                                endpoint: endpoint,
                                bucket: bucket,
                                key: entry.key,
                                region: region,
                                accessKey: accessKey,
                                secretKey: secretKey,
                                allowInsecure: insecureSSL,
                                profileName: activeProfileName
                            )
                            deletedCount += 1
                            lastStatusCode = result.statusCode
                            responseText = result.responseText
                            updateDebug(from: result)
                        }
                        _ = try await backend(for: endpoint).deleteObject(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: prefix,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL,
                            profileName: activeProfileName
                        )
                    } catch {
                        statusMessage = "Delete failed: \(error.localizedDescription)"
                        debugText = "Delete failed: \(error)"
                    }
                } else {
                    do {
                        let result = try await backend(for: endpoint).deleteObject(
                            endpoint: endpoint,
                            bucket: bucket,
                            key: object.key,
                            region: region,
                            accessKey: accessKey,
                            secretKey: secretKey,
                            allowInsecure: insecureSSL,
                            profileName: activeProfileName
                        )
                        deletedCount += 1
                        lastStatusCode = result.statusCode
                        responseText = result.responseText
                        updateDebug(from: result)
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

    func undeleteObjects(_ targets: [S3Object]) {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }
        guard endpoint.provider == .azureBlob else {
            statusMessage = "Undelete is only available for Azure Blob"
            return
        }
        guard let bucket = effectiveBucket(for: endpoint) else {
            statusMessage = "Select a container before undeleting"
            return
        }
        guard let azureBackend = azureBackend as? AzureBlobBackend else {
            statusMessage = "Azure backend unavailable"
            return
        }

        let deletedTargets = targets.filter { $0.isDeleted }
        guard !deletedTargets.isEmpty else {
            statusMessage = "No deleted blobs selected"
            return
        }

        Task {
            isBusy = true
            statusMessage = "Undeleting..."
            var restoredCount = 0

            for object in deletedTargets {
                do {
                    let result = try await azureBackend.undeleteObject(
                        endpoint: endpoint,
                        bucket: bucket,
                        key: object.key,
                        allowInsecure: insecureSSL,
                        profileName: activeProfileName
                    )
                    restoredCount += 1
                    lastStatusCode = result.statusCode
                    responseText = result.responseText
                    updateDebug(from: result)
                } catch {
                    statusMessage = "Undelete failed: \(error.localizedDescription)"
                    debugText = "Undelete failed: \(error)"
                }
            }

            if let bucket = currentBucket {
                await listObjects(bucket: bucket, prefix: currentPrefix)
            }
            statusMessage = "Undeleted \(restoredCount) object(s)"
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

    func setShowVersionsDeleted(_ isOn: Bool) {
        showVersionsDeleted = isOn
        refreshCurrentView()
    }

    func downloadObjects(_ targets: [S3Object]) {
        guard let endpoint = resolveEndpoint() else {
            statusMessage = "Invalid endpoint URL"
            return
        }
        guard let bucket = effectiveBucket(for: endpoint) else {
            statusMessage = "Select a bucket before downloading"
            return
        }
        provider = endpoint.provider

        let objectsToDownload = targets.filter { object in
            if object.contentType == "bucket" || object.isDeleteMarker {
                return false
            }
            if object.isDeleted, (object.versionId ?? "").isEmpty {
                return false
            }
            return true
        }
        if objectsToDownload.isEmpty {
            statusMessage = "No downloadable objects selected"
            return
        }

        Task {
            isBusy = true
            statusMessage = "Preparing download..."

            let flattened = await expandFolders(
                objectsToDownload,
                endpoint: endpoint,
                bucket: bucket
            )
            if flattened.isEmpty {
                statusMessage = "Nothing to download"
                isBusy = false
                return
            }

            if flattened.count == 1, let only = flattened.first, !only.key.hasSuffix("/") {
                if let destination = chooseSaveLocation(defaultName: downloadDisplayName(for: only, endpoint: endpoint)) {
                    await downloadFiles(flattened, endpoint: endpoint, bucket: bucket, destination: destination, isDirectory: false)
                }
            } else if let directory = chooseFolder() {
                await downloadFiles(flattened, endpoint: endpoint, bucket: bucket, destination: directory, isDirectory: true)
            }

            isBusy = false
        }
    }

    private func expandFolders(_ objects: [S3Object], endpoint: StorageEndpoint, bucket: String) async -> [S3Object] {
        var results: [S3Object] = []
        for object in objects {
            if object.key.hasSuffix("/") {
                do {
                    let allObjects = try await backend(for: endpoint).listAllObjects(
                        endpoint: endpoint,
                        bucket: bucket,
                        prefix: object.key,
                        region: region,
                        accessKey: accessKey,
                        secretKey: secretKey,
                        allowInsecure: insecureSSL,
                        profileName: activeProfileName
                    )
                    results.append(contentsOf: allObjects.filter { !$0.key.hasSuffix("/") })
                } catch {
                    statusMessage = "Download failed: \(error.localizedDescription)"
                }
            } else {
                results.append(object)
            }
        }
        return results
    }

    private func downloadFiles(_ objects: [S3Object], endpoint: StorageEndpoint, bucket: String, destination: URL, isDirectory: Bool) async {
        let totalBytes = objects.reduce(0) { $0 + Int64(max($1.sizeBytes, 0)) }
        downloadTotalBytes = totalBytes
        downloadDoneBytes = 0
        transferStatus = "Downloading..."
        transferProgress = 0
        currentTransferItem = ""
        currentTransferProgress = 0
        var completed = 0

        for object in objects {
            do {
                currentTransferItem = downloadDisplayName(for: object, endpoint: endpoint)
                currentTransferProgress = 0
                let transferId = UUID()
                addOrUpdateTransfer(id: transferId, name: currentTransferItem, progress: 0)
                downloadLastReceived = 0
                let result: ObjectDataResult
                if shouldUseVersionedOperations(for: object, endpoint: endpoint), let versionId = object.versionId {
                    result = try await backend(for: endpoint).getObjectVersionWithProgress(
                        endpoint: endpoint,
                        bucket: bucket,
                        key: object.key,
                        versionId: versionId,
                        region: region,
                        accessKey: accessKey,
                        secretKey: secretKey,
                        allowInsecure: insecureSSL,
                        profileName: activeProfileName
                    ) { [weak self] received, expected in
                        Task { @MainActor in
                            guard let self else { return }
                            let delta = max(received - self.downloadLastReceived, 0)
                            self.downloadLastReceived = received
                            self.downloadDoneBytes += delta
                            let total = max(expected, 1)
                            self.currentTransferProgress = Double(received) / Double(total)
                            self.transferProgress = self.downloadTotalBytes > 0 ? Double(self.downloadDoneBytes) / Double(self.downloadTotalBytes) : Double(completed) / Double(max(objects.count, 1))
                            self.transferStatus = "Downloading \(self.formatBytes(self.downloadDoneBytes)) / \(self.formatBytes(self.downloadTotalBytes))"
                            self.addOrUpdateTransfer(id: transferId, name: self.currentTransferItem, progress: self.currentTransferProgress)
                        }
                    }
                } else {
                    result = try await backend(for: endpoint).getObjectWithProgress(
                        endpoint: endpoint,
                        bucket: bucket,
                        key: object.key,
                        region: region,
                        accessKey: accessKey,
                        secretKey: secretKey,
                        allowInsecure: insecureSSL,
                        profileName: activeProfileName
                    ) { [weak self] received, expected in
                        Task { @MainActor in
                            guard let self else { return }
                            let delta = max(received - self.downloadLastReceived, 0)
                            self.downloadLastReceived = received
                            self.downloadDoneBytes += delta
                            let total = max(expected, 1)
                            self.currentTransferProgress = Double(received) / Double(total)
                            self.transferProgress = self.downloadTotalBytes > 0 ? Double(self.downloadDoneBytes) / Double(self.downloadTotalBytes) : Double(completed) / Double(max(objects.count, 1))
                            self.transferStatus = "Downloading \(self.formatBytes(self.downloadDoneBytes)) / \(self.formatBytes(self.downloadTotalBytes))"
                            self.addOrUpdateTransfer(id: transferId, name: self.currentTransferItem, progress: self.currentTransferProgress)
                        }
                    }
                }
                let targetURL: URL
                if isDirectory {
                    let filePath = destination.appendingPathComponent(downloadRelativePath(for: object, endpoint: endpoint))
                    let dirURL = filePath.deletingLastPathComponent()
                    try FileManager.default.createDirectory(at: dirURL, withIntermediateDirectories: true)
                    targetURL = filePath
                } else {
                    targetURL = destination
                }
                try result.data.write(to: targetURL)
                completed += 1
                downloadDoneBytes = max(downloadDoneBytes, Int64(max(object.sizeBytes, result.data.count)))
                addOrUpdateTransfer(id: transferId, name: currentTransferItem, progress: 1.0)
                currentTransferProgress = 1.0
            } catch {
                statusMessage = "Download failed: \(error.localizedDescription)"
                debugText = "Download failed: \(error)"
                addOrUpdateTransfer(id: UUID(), name: currentTransferItem, progress: 0)
            }
        }

        statusMessage = "Download complete"
        transferStatus = "Download complete"
        currentTransferItem = ""
        currentTransferProgress = 0
        downloadTotalBytes = 0
        downloadDoneBytes = 0
        downloadLastReceived = 0
    }

    private func chooseSaveLocation(defaultName: String) -> URL? {
        let panel = NSSavePanel()
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = defaultName
        return panel.runModal() == .OK ? panel.url : nil
    }

    private func chooseFolder() -> URL? {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true
        return panel.runModal() == .OK ? panel.url : nil
    }

    private func lastPathComponent(_ key: String) -> String {
        let trimmed = key.hasSuffix("/") ? String(key.dropLast()) : key
        return trimmed.split(separator: "/").last.map(String.init) ?? trimmed
    }

    private func shouldUseVersionedOperations(for object: S3Object, endpoint: StorageEndpoint) -> Bool {
        guard let versionId = object.versionId, !versionId.isEmpty else {
            return false
        }
        if endpoint.provider == .s3 {
            return object.isVersioned && !object.isDeleteMarker
        }
        if endpoint.provider == .azureBlob {
            return object.isVersioned || object.isDeleted
        }
        return false
    }

    private func downloadDisplayName(for object: S3Object, endpoint: StorageEndpoint) -> String {
        let baseName = lastPathComponent(object.key)
        guard shouldUseVersionedOperations(for: object, endpoint: endpoint),
              let versionId = object.versionId, !versionId.isEmpty else {
            return baseName
        }
        return versionedFileName(baseName, versionId: versionId)
    }

    private func downloadRelativePath(for object: S3Object, endpoint: StorageEndpoint) -> String {
        let key = object.key
        guard shouldUseVersionedOperations(for: object, endpoint: endpoint),
              let versionId = object.versionId, !versionId.isEmpty else {
            return key
        }
        var parts = key.split(separator: "/").map(String.init)
        guard let last = parts.popLast() else { return key }
        parts.append(versionedFileName(last, versionId: versionId))
        return parts.joined(separator: "/")
    }

    private func versionedFileName(_ baseName: String, versionId: String) -> String {
        let prefix = String(versionId.prefix(8))
        let url = URL(fileURLWithPath: baseName)
        let ext = url.pathExtension
        let stem = url.deletingPathExtension().lastPathComponent
        if ext.isEmpty {
            return "\(stem)-v\(prefix)"
        }
        return "\(stem)-v\(prefix).\(ext)"
    }

    private func updateDebug(from result: ConnectionResult) {
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
    }

    private func formatBytes(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }

    struct TransferItem: Identifiable, Hashable {
        let id: UUID
        var name: String
        var progress: Double
    }

    struct DebugEntry: Identifiable, Hashable {
        let id = UUID()
        let title: String
        let text: String
    }

    private func addOrUpdateTransfer(id: UUID, name: String, progress: Double) {
        let shortName = trimFileName(name)
        if let index = recentTransfers.firstIndex(where: { $0.id == id }) {
            recentTransfers[index].progress = progress
            recentTransfers[index].name = shortName
        } else {
            recentTransfers.insert(TransferItem(id: id, name: shortName, progress: progress), at: 0)
        }
        if recentTransfers.count > 6 {
            recentTransfers = Array(recentTransfers.prefix(6))
        }
    }

    private func trimFileName(_ name: String) -> String {
        if name.count <= 40 { return name }
        let start = name.prefix(20)
        let end = name.suffix(17)
        return "\(start)...\(end)"
    }

    private struct UploadTarget {
        let fileURL: URL
        let key: String
        let size: Int64
        let contentType: String?
        let displayName: String
    }

    private func expandUploadTargets(_ urls: [URL]) -> [UploadTarget] {
        var targets: [UploadTarget] = []
        let fileManager = FileManager.default

        for url in urls {
            var isDirectory: ObjCBool = false
            if fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory), isDirectory.boolValue {
                let base = url
                let baseName = url.lastPathComponent
                if let enumerator = fileManager.enumerator(at: url, includingPropertiesForKeys: [.isRegularFileKey, .fileSizeKey], options: [.skipsHiddenFiles]) {
                    for case let fileURL as URL in enumerator {
                        do {
                            let values = try fileURL.resourceValues(forKeys: [.isRegularFileKey, .fileSizeKey])
                            guard values.isRegularFile == true else { continue }
                            let relativePath = fileURL.path.replacingOccurrences(of: base.path + "/", with: "")
                            let key = currentPrefix + baseName + "/" + relativePath
                            let size = Int64(values.fileSize ?? 0)
                            let contentType = UTType(filenameExtension: fileURL.pathExtension)?.preferredMIMEType
                            targets.append(UploadTarget(
                                fileURL: fileURL,
                                key: key,
                                size: size,
                                contentType: contentType,
                                displayName: baseName + "/" + relativePath
                            ))
                        } catch {
                            continue
                        }
                    }
                }
            } else {
                let size = (try? fileManager.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.int64Value ?? 0
                let key = currentPrefix + url.lastPathComponent
                let contentType = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType
                targets.append(UploadTarget(
                    fileURL: url,
                    key: key,
                    size: size,
                    contentType: contentType,
                    displayName: url.lastPathComponent
                ))
            }
        }
        return targets
    }

    private func persistProfiles() {
        do {
            let data = try JSONEncoder().encode(profiles)
            UserDefaults.standard.set(data, forKey: "s3macbrowser.profiles")
        } catch {
            statusMessage = "Failed to save profiles"
        }
    }

    private func loadProfiles() {
        guard let data = UserDefaults.standard.data(forKey: "s3macbrowser.profiles") else {
            return
        }
        do {
            profiles = try JSONDecoder().decode([ConnectionProfile].self, from: data)
        } catch {
            statusMessage = "Failed to load profiles"
        }
    }

    private func resolveEndpoint() -> StorageEndpoint? {
        StorageEndpointParser.parse(input: endpointURL)
    }

    private func backend(for endpoint: StorageEndpoint) -> StorageBackend {
        endpoint.provider == .azureBlob ? azureBackend : s3Backend
    }

    private var activeProfileName: String {
        let trimmed = profileName.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "default" : trimmed
    }

    private func effectiveBucket(for endpoint: StorageEndpoint) -> String? {
        if let currentBucket {
            return currentBucket
        }
        if endpoint.provider == .azureBlob, let container = endpoint.container {
            currentBucket = container
            breadcrumb = ["/", container]
            return container
        }
        return nil
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
