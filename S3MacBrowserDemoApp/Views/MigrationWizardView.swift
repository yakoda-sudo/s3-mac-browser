import SwiftUI
import AppKit

struct MigrationWizardView: View {
    @Environment(\.dismiss) private var dismiss
    @StateObject private var viewModel: MigrationWizardViewModel
    @ObservedObject private var settings: MigrationSettings
    @StateObject private var runner: MigrationRunner
    @State private var didStartJob = false
    @State private var showJobCompleteAlert = false
    @State private var jobSummaryText = ""

    init(profiles: [ConnectionProfile], settings: MigrationSettings) {
        _viewModel = StateObject(wrappedValue: MigrationWizardViewModel(profiles: profiles))
        self.settings = settings
        _runner = StateObject(wrappedValue: MigrationRunner(settings: settings))
    }

    var body: some View {
        VStack(spacing: 16) {
            header
            Divider()
            stepContent
            Divider()
            footer
        }
        .padding(20)
        .frame(minWidth: 620, minHeight: 420)
        .alert("Migration complete", isPresented: $showJobCompleteAlert) {
            Button("Copy Summary") {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(jobSummaryText, forType: .string)
            }
            Button("OK", role: .cancel) {}
        } message: {
            Text(jobSummaryText)
        }
        .onChange(of: runner.isRunning) { isRunning in
            if !isRunning, didStartJob, runner.totalObjects > 0, runner.completedObjects == runner.totalObjects {
                jobSummaryText = buildJobSummary()
                showJobCompleteAlert = true
            }
        }
    }

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("Data Migration")
                    .font(.title2)
                Text("Step \(viewModel.step.rawValue + 1) of 4")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            Spacer()
            Button("Close") {
                dismiss()
            }
        }
    }

    @ViewBuilder
    private var stepContent: some View {
        switch viewModel.step {
        case .source:
            MigrationSourceStep(viewModel: viewModel)
        case .target:
            MigrationTargetStep(viewModel: viewModel)
        case .preview:
            MigrationPreviewStep(viewModel: viewModel)
        case .review:
            MigrationReviewStep(viewModel: viewModel, settings: settings, runner: runner)
        }
    }

    private var footer: some View {
        HStack {
            Button("Back") {
                viewModel.goBack()
            }
            .disabled(viewModel.step == .source)

            Spacer()

            if viewModel.step == .review {
                Button("Create Job") {
                    let job = MigrationJob(
                        sourceProfileName: viewModel.sourceProfileName,
                        sourceBucket: viewModel.sourceBucket,
                        sourcePrefix: viewModel.sourcePrefix,
                        targetProfileName: viewModel.targetProfileName,
                        targetBucket: viewModel.targetBucket,
                        targetPrefix: viewModel.targetPrefix
                    )
                    didStartJob = true
                    runner.start(job: job, profiles: viewModel.profiles)
                }
                .buttonStyle(.borderedProminent)
                .disabled(didStartJob || runner.isRunning)
            } else {
                Button("Next") {
                    viewModel.advance()
                }
                .buttonStyle(.borderedProminent)
                .disabled(!viewModel.isStepValid)
            }
        }
    }

    private func buildJobSummary() -> String {
        let bytes = ByteCountFormatter.string(fromByteCount: runner.bytesCopied, countStyle: .file)
        let throughput = ByteCountFormatter.string(fromByteCount: runner.throughputBytesPerSec, countStyle: .file) + "/s"
        return [
            "Source: \(viewModel.sourceProfileName) / \(viewModel.sourceBucket)",
            "Target: \(viewModel.targetProfileName) / \(viewModel.targetBucket)",
            "Objects: \(runner.completedObjects)/\(runner.totalObjects)",
            "Bytes copied: \(bytes)",
            "Throughput: \(throughput)"
        ].joined(separator: "\n")
    }
}

private struct MigrationSourceStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel

    var body: some View {
        Form {
            Section("Source") {
                profilePicker(title: "Profile", selection: $viewModel.sourceProfileName, profiles: viewModel.profiles)
                TextField("Bucket/Container", text: $viewModel.sourceBucket)
                TextField("Prefix (optional)", text: $viewModel.sourcePrefix)
            }
        }
        .formStyle(.grouped)
    }
}

private struct MigrationTargetStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel

    var body: some View {
        Form {
            Section("Target") {
                profilePicker(title: "Profile", selection: $viewModel.targetProfileName, profiles: viewModel.profiles)
                TextField("Bucket/Container", text: $viewModel.targetBucket)
                TextField("Prefix (optional)", text: $viewModel.targetPrefix)
            }
            if viewModel.isSameSourceAndTarget {
                Text("Source and target must be different.")
                    .foregroundColor(.red)
                    .font(.caption)
            }
        }
        .formStyle(.grouped)
    }
}

private struct MigrationPreviewStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Data Flow Preview")
                .font(.headline)
            Text(viewModel.dataFlowPreview(hostName: HostnameProvider.localName))
                .font(.body)
                .foregroundColor(.secondary)
            Spacer()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.top, 8)
    }
}

private struct MigrationReviewStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel
    @ObservedObject var settings: MigrationSettings
    @ObservedObject var runner: MigrationRunner

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Review & Create Job")
                .font(.headline)
            GroupBox("Source") {
                summaryLine("Profile", value: viewModel.sourceProfileName)
                summaryLine("Bucket/Container", value: viewModel.sourceBucket)
                summaryLine("Prefix", value: viewModel.sourcePrefix.isEmpty ? "(none)" : viewModel.sourcePrefix)
            }
            GroupBox("Target") {
                summaryLine("Profile", value: viewModel.targetProfileName)
                summaryLine("Bucket/Container", value: viewModel.targetBucket)
                summaryLine("Prefix", value: viewModel.targetPrefix.isEmpty ? "(none)" : viewModel.targetPrefix)
            }
            GroupBox("Migration Settings") {
                summaryLine("Max concurrent", value: "\(settings.maxConcurrentTransfers)")
                summaryLine("Bandwidth limit", value: "\(settings.bandwidthLimitMBps) MB/s")
                summaryLine("Buffer per stream", value: "\(settings.bufferSizeMB) MB")
                Text("This job uses the current Migration Settings.")
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }
            GroupBox("Job Progress") {
                summaryLine("Status", value: runner.statusMessage)
                summaryLine("Objects", value: "\(runner.completedObjects)/\(runner.totalObjects)")
                summaryLine("Bytes copied", value: ByteCountFormatter.string(fromByteCount: runner.bytesCopied, countStyle: .file))
                summaryLine("Throughput", value: ByteCountFormatter.string(fromByteCount: runner.throughputBytesPerSec, countStyle: .file) + "/s")
                if let lastError = runner.errorMessages.last {
                    Text("Last error: \(lastError)")
                        .font(.caption2)
                        .foregroundColor(.red)
                }
            }
            Spacer()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

private func profilePicker(title: String, selection: Binding<String>, profiles: [ConnectionProfile]) -> some View {
    Picker(title, selection: selection) {
        ForEach(profiles, id: \.name) { profile in
            Text(profile.name).tag(profile.name)
        }
    }
}

private func summaryLine(_ title: String, value: String) -> some View {
    HStack {
        Text(title)
            .foregroundColor(.secondary)
            .frame(width: 140, alignment: .leading)
        Text(value.isEmpty ? "-" : value)
        Spacer()
    }
    .font(.caption)
}

private enum HostnameProvider {
    static let localName: String = {
        Host.current().localizedName ?? "your_mac"
    }()
}
