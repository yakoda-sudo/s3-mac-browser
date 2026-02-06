import Foundation

@MainActor
final class MigrationWizardViewModel: ObservableObject {
    enum Step: Int, CaseIterable {
        case source = 0
        case target = 1
        case preview = 2
        case review = 3
    }

    @Published var step: Step = .source
    @Published var sourceProfileName: String = ""
    @Published var sourceBucket: String = ""
    @Published var sourcePrefix: String = ""
    @Published var targetProfileName: String = ""
    @Published var targetBucket: String = ""
    @Published var targetPrefix: String = ""
    @Published var sourceBucketOptions: [String] = []
    @Published var targetBucketOptions: [String] = []
    @Published var isLoadingSourceBuckets = false
    @Published var isLoadingTargetBuckets = false

    let profiles: [ConnectionProfile]
    private let s3Backend: StorageBackend
    private let azureBackend: StorageBackend

    init(profiles: [ConnectionProfile],
         s3Backend: StorageBackend = S3Backend(),
         azureBackend: StorageBackend = AzureBlobBackend()) {
        self.profiles = profiles
        self.s3Backend = s3Backend
        self.azureBackend = azureBackend
        if let first = profiles.first {
            sourceProfileName = first.name
            targetProfileName = first.name
        }
    }

    func refreshSourceBuckets() {
        loadBuckets(isSource: true)
    }

    func refreshTargetBuckets() {
        loadBuckets(isSource: false)
    }

    var isStepValid: Bool {
        switch step {
        case .source:
            return !sourceProfileName.isEmpty && !sourceBucket.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        case .target:
            guard !targetProfileName.isEmpty,
                  !targetBucket.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                return false
            }
            return !isSameSourceAndTarget
        case .preview:
            return true
        case .review:
            return true
        }
    }

    var isSameSourceAndTarget: Bool {
        normalize(sourceProfileName) == normalize(targetProfileName) &&
        normalize(sourceBucket) == normalize(targetBucket) &&
        normalize(sourcePrefix) == normalize(targetPrefix)
    }

    func advance() {
        if step.rawValue < Step.allCases.count - 1 {
            step = Step(rawValue: step.rawValue + 1) ?? step
        }
    }

    func goBack() {
        if step.rawValue > 0 {
            step = Step(rawValue: step.rawValue - 1) ?? step
        }
    }

    func dataFlowPreview(hostName: String) -> String {
        let source = summaryLabel(profileName: sourceProfileName, bucket: sourceBucket)
        let target = summaryLabel(profileName: targetProfileName, bucket: targetBucket)
        return "\(source) --> \(hostName) --> \(target)"
    }

    private func summaryLabel(profileName: String, bucket: String) -> String {
        let bucketLabel = bucket.trimmingCharacters(in: .whitespacesAndNewlines)
        if bucketLabel.isEmpty {
            return profileName
        }
        return "\(profileName)(\(bucketLabel))"
    }

    private func normalize(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private func loadBuckets(isSource: Bool) {
        let profileName = isSource ? sourceProfileName : targetProfileName
        guard let profile = profiles.first(where: { $0.name == profileName }),
              let endpoint = StorageEndpointParser.parse(input: profile.endpoint) else {
            if isSource {
                sourceBucketOptions = []
            } else {
                targetBucketOptions = []
            }
            return
        }

        if isSource {
            isLoadingSourceBuckets = true
        } else {
            isLoadingTargetBuckets = true
        }

        Task {
            do {
                let result = try await backend(for: endpoint).listBuckets(
                    endpoint: endpoint,
                    region: profile.region,
                    accessKey: profile.accessKey,
                    secretKey: profile.secretKey,
                    allowInsecure: false,
                    profileName: profile.name
                )
                let buckets = result.bucketNames.sorted()
                await MainActor.run {
                    if isSource {
                        sourceBucketOptions = buckets
                        if !buckets.isEmpty, !buckets.contains(sourceBucket) {
                            sourceBucket = buckets[0]
                        }
                        isLoadingSourceBuckets = false
                    } else {
                        targetBucketOptions = buckets
                        if !buckets.isEmpty, !buckets.contains(targetBucket) {
                            targetBucket = buckets[0]
                        }
                        isLoadingTargetBuckets = false
                    }
                }
            } catch {
                await MainActor.run {
                    if isSource {
                        sourceBucketOptions = []
                        isLoadingSourceBuckets = false
                    } else {
                        targetBucketOptions = []
                        isLoadingTargetBuckets = false
                    }
                }
            }
        }
    }

    private func backend(for endpoint: StorageEndpoint) -> StorageBackend {
        endpoint.provider == .azureBlob ? azureBackend : s3Backend
    }
}
