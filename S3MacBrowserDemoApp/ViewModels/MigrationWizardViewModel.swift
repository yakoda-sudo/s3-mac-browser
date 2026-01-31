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

    let profiles: [ConnectionProfile]

    init(profiles: [ConnectionProfile]) {
        self.profiles = profiles
        if let first = profiles.first {
            sourceProfileName = first.name
            targetProfileName = first.name
        }
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
}
