import Foundation

@MainActor
final class MigrationRunner: ObservableObject {
    struct ProgressSample: Identifiable {
        let id = UUID()
        let timestamp: Date
        let bytesTransferred: Int64
        let requestCount: Int
    }

    @Published private(set) var isRunning = false
    @Published private(set) var statusMessage = "Idle"
    @Published private(set) var totalObjects: Int = 0
    @Published private(set) var completedObjects: Int = 0
    @Published private(set) var bytesCopied: Int64 = 0
    @Published private(set) var throughputBytesPerSec: Int64 = 0
    @Published private(set) var errorMessages: [String] = []
    @Published private(set) var samples: [ProgressSample] = []

    private let settings: MigrationSettings
    private let s3Backend: StorageBackend
    private let azureBackend: StorageBackend
    private var startedAt: Date?
    private var requestCount: Int = 0
    private var progressTimer: Task<Void, Never>?
    private let metricsRecorder = MetricsRecorder.shared

    init(settings: MigrationSettings,
         s3Backend: StorageBackend = S3Backend(),
         azureBackend: StorageBackend = AzureBlobBackend()) {
        self.settings = settings
        self.s3Backend = s3Backend
        self.azureBackend = azureBackend
    }

    func start(job: MigrationJob, profiles: [ConnectionProfile]) {
        guard !isRunning else { return }
        resetProgress()
        isRunning = true
        statusMessage = "Listing source objects..."
        startedAt = Date()

        Task {
            defer { isRunning = false }

            guard let sourceProfile = profiles.first(where: { $0.name == job.sourceProfileName }),
                  let targetProfile = profiles.first(where: { $0.name == job.targetProfileName }),
                  let sourceEndpoint = StorageEndpointParser.parse(input: sourceProfile.endpoint),
                  let targetEndpoint = StorageEndpointParser.parse(input: targetProfile.endpoint) else {
                statusMessage = "Missing source/target profiles"
                return
            }

            let sourcePrefix = normalizedPrefix(job.sourcePrefix)
            let targetPrefix = normalizedPrefix(job.targetPrefix)

            do {
                let objects = try await backend(for: sourceEndpoint).listAllObjects(
                    endpoint: sourceEndpoint,
                    bucket: job.sourceBucket,
                    prefix: sourcePrefix,
                    region: sourceProfile.region,
                    accessKey: sourceProfile.accessKey,
                    secretKey: sourceProfile.secretKey,
                    allowInsecure: false,
                    profileName: sourceProfile.name
                ).filter { !$0.key.hasSuffix("/") }

                requestCount += 1
                totalObjects = objects.count
                statusMessage = "Copying \(totalObjects) objects..."
                startProgressTimer()

                let semaphore = AsyncSemaphore(value: max(settings.maxConcurrentTransfers, 1))
                let checkpoint = MigrationCheckpointStore(jobId: job.id, profileName: sourceProfile.name)
                try await withThrowingTaskGroup(of: Void.self) { group in
                    for object in objects {
                        let alreadyDone = await checkpoint.isCompleted(object.key)
                        if alreadyDone { continue }
                        await semaphore.acquire()
                        group.addTask { [weak self] in
                            defer { Task { await semaphore.release() } }
                            guard let self else { return }
                            try await self.copyObject(
                                object,
                                sourceEndpoint: sourceEndpoint,
                                targetEndpoint: targetEndpoint,
                                sourceProfile: sourceProfile,
                                targetProfile: targetProfile,
                                sourceBucket: job.sourceBucket,
                                targetBucket: job.targetBucket,
                                sourcePrefix: sourcePrefix,
                                targetPrefix: targetPrefix,
                                checkpoint: checkpoint
                            )
                        }
                    }
                    try await group.waitForAll()
                }

                statusMessage = "Migration complete"
            } catch {
                errorMessages.append(error.localizedDescription)
                statusMessage = "Migration failed"
            }

            stopProgressTimer()
        }
    }

    private func copyObject(_ object: S3Object,
                            sourceEndpoint: StorageEndpoint,
                            targetEndpoint: StorageEndpoint,
                            sourceProfile: ConnectionProfile,
                            targetProfile: ConnectionProfile,
                            sourceBucket: String,
                            targetBucket: String,
                            sourcePrefix: String,
                            targetPrefix: String,
                            checkpoint: MigrationCheckpointStore) async throws {
        let relativeKey: String
        if sourcePrefix.isEmpty {
            relativeKey = object.key
        } else {
            relativeKey = object.key.hasPrefix(sourcePrefix) ? String(object.key.dropFirst(sourcePrefix.count)) : object.key
        }
        let targetKey = targetPrefix + relativeKey

        let streamer = MigrationStreamer(
            bufferBytes: settings.bufferBytes,
            metricsRecorder: metricsRecorder,
            sourceProfileName: sourceProfile.name,
            targetProfileName: targetProfile.name,
            source: MigrationStreamer.EndpointContext(
                endpoint: sourceEndpoint,
                bucket: sourceBucket,
                region: sourceProfile.region,
                accessKey: sourceProfile.accessKey,
                secretKey: sourceProfile.secretKey,
                allowInsecure: false
            ),
            target: MigrationStreamer.EndpointContext(
                endpoint: targetEndpoint,
                bucket: targetBucket,
                region: targetProfile.region,
                accessKey: targetProfile.accessKey,
                secretKey: targetProfile.secretKey,
                allowInsecure: false
            )
        )

        let onChunk: @Sendable (Int64) -> Void = { [weak self] delta in
            Task { @MainActor in
                guard let self else { return }
                self.bytesCopied += delta
                self.updateThroughput()
            }
        }
        let stats = try await streamer.copyObject(
            key: object.key,
            targetKey: targetKey,
            contentType: object.contentType,
            onChunk: onChunk
        )
        await MainActor.run {
            self.requestCount += stats.requests
            self.completedObjects += 1
        }
        await checkpoint.markCompleted(object.key)
    }

    private func backend(for endpoint: StorageEndpoint) -> StorageBackend {
        endpoint.provider == .azureBlob ? azureBackend : s3Backend
    }

    private func normalizedPrefix(_ prefix: String) -> String {
        let trimmed = prefix.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "" }
        return trimmed.hasSuffix("/") ? trimmed : trimmed + "/"
    }

    private func resetProgress() {
        statusMessage = "Idle"
        totalObjects = 0
        completedObjects = 0
        bytesCopied = 0
        throughputBytesPerSec = 0
        errorMessages = []
        samples = []
        requestCount = 0
    }

    private func updateThroughput() {
        guard let startedAt else { return }
        let elapsed = Date().timeIntervalSince(startedAt)
        if elapsed > 0 {
            throughputBytesPerSec = Int64(Double(bytesCopied) / elapsed)
        }
    }

    private func startProgressTimer() {
        progressTimer?.cancel()
        progressTimer = Task { [weak self] in
            while let self, !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                await MainActor.run {
                    self.samples.append(ProgressSample(timestamp: Date(), bytesTransferred: self.bytesCopied, requestCount: self.requestCount))
                    if self.samples.count > 300 {
                        self.samples.removeFirst(self.samples.count - 300)
                    }
                }
            }
        }
    }

    private func stopProgressTimer() {
        progressTimer?.cancel()
        progressTimer = nil
    }
}

private actor AsyncSemaphore {
    private var value: Int

    init(value: Int) {
        self.value = value
    }

    func acquire() async {
        while value <= 0 {
            await Task.yield()
        }
        value -= 1
    }

    func release() {
        value += 1
    }
}
