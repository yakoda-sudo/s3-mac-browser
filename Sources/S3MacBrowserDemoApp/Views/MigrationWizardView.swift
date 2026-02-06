import SwiftUI
import AppKit

struct MigrationWizardView: View {
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var language: LanguageManager
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
        .alert(language.t("migration.completeTitle"), isPresented: $showJobCompleteAlert) {
            Button(language.t("button.copySummary")) {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(jobSummaryText, forType: .string)
            }
            Button(language.t("button.ok"), role: .cancel) {}
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
                Text(language.t("migration.title"))
                    .font(.title2)
                Text(String(format: language.t("migration.stepOf"), viewModel.step.rawValue + 1, 4))
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            Spacer()
            Button(language.t("button.close")) {
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
            Button(language.t("button.back")) {
                viewModel.goBack()
            }
            .disabled(viewModel.step == .source)

            Spacer()

            if viewModel.step == .review {
                Button(language.t("button.createJob")) {
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
                Button(language.t("button.next")) {
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
            String(format: language.t("migration.summary.source"), viewModel.sourceProfileName, viewModel.sourceBucket),
            String(format: language.t("migration.summary.target"), viewModel.targetProfileName, viewModel.targetBucket),
            String(format: language.t("migration.summary.objects"), runner.completedObjects, runner.totalObjects),
            String(format: language.t("migration.summary.bytes"), bytes),
            String(format: language.t("migration.summary.throughput"), throughput)
        ].joined(separator: "\n")
    }
}

private struct MigrationSourceStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel
    @EnvironmentObject private var language: LanguageManager

    var body: some View {
        Form {
            Section(language.t("migration.source")) {
                profilePicker(title: language.t("label.profile"), selection: $viewModel.sourceProfileName, profiles: viewModel.profiles)
                bucketPicker(
                    title: language.t("label.bucketContainer"),
                    selection: $viewModel.sourceBucket,
                    buckets: viewModel.sourceBucketOptions,
                    isLoading: viewModel.isLoadingSourceBuckets,
                    onRefresh: { viewModel.refreshSourceBuckets() }
                )
                TextField(language.t("label.prefixOptional"), text: $viewModel.sourcePrefix)
            }
        }
        .formStyle(.grouped)
        .onAppear {
            viewModel.refreshSourceBuckets()
        }
        .onChange(of: viewModel.sourceProfileName) { _ in
            viewModel.refreshSourceBuckets()
        }
    }
}

private struct MigrationTargetStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel
    @EnvironmentObject private var language: LanguageManager

    var body: some View {
        Form {
            Section(language.t("migration.target")) {
                profilePicker(title: language.t("label.profile"), selection: $viewModel.targetProfileName, profiles: viewModel.profiles)
                bucketPicker(
                    title: language.t("label.bucketContainer"),
                    selection: $viewModel.targetBucket,
                    buckets: viewModel.targetBucketOptions,
                    isLoading: viewModel.isLoadingTargetBuckets,
                    onRefresh: { viewModel.refreshTargetBuckets() }
                )
                TextField(language.t("label.prefixOptional"), text: $viewModel.targetPrefix)
            }
            if viewModel.isSameSourceAndTarget {
                Text(language.t("migration.sourceTargetDifferent"))
                    .foregroundColor(.red)
                    .font(.caption)
            }
        }
        .formStyle(.grouped)
        .onAppear {
            viewModel.refreshTargetBuckets()
        }
        .onChange(of: viewModel.targetProfileName) { _ in
            viewModel.refreshTargetBuckets()
        }
    }
}

private struct MigrationPreviewStep: View {
    @ObservedObject var viewModel: MigrationWizardViewModel
    @EnvironmentObject private var language: LanguageManager

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(language.t("migration.dataFlowPreview"))
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
    @EnvironmentObject private var language: LanguageManager

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(language.t("migration.reviewCreate"))
                .font(.headline)
            GroupBox(language.t("migration.source")) {
                summaryLine(language.t("label.profile"), value: viewModel.sourceProfileName)
                summaryLine(language.t("label.bucketContainer"), value: viewModel.sourceBucket)
                summaryLine(language.t("label.prefix"), value: viewModel.sourcePrefix.isEmpty ? language.t("general.none") : viewModel.sourcePrefix)
            }
            GroupBox(language.t("migration.target")) {
                summaryLine(language.t("label.profile"), value: viewModel.targetProfileName)
                summaryLine(language.t("label.bucketContainer"), value: viewModel.targetBucket)
                summaryLine(language.t("label.prefix"), value: viewModel.targetPrefix.isEmpty ? language.t("general.none") : viewModel.targetPrefix)
            }
            GroupBox(language.t("migration.settingsTitle")) {
                summaryLine(language.t("migration.maxConcurrentShort"), value: "\(settings.maxConcurrentTransfers)")
                summaryLine(language.t("migration.bandwidthLimit"), value: "\(settings.bandwidthLimitMBps) MB/s")
                summaryLine(language.t("migration.bufferPerStream"), value: "\(settings.bufferSizeMB) MB")
                Text(language.t("migration.settingsNote"))
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }
            GroupBox(language.t("migration.jobProgress")) {
                summaryLine(language.t("label.status"), value: runner.statusMessage)
                summaryLine(language.t("label.objects"), value: "\(runner.completedObjects)/\(runner.totalObjects)")
                summaryLine(language.t("label.bytesCopied"), value: ByteCountFormatter.string(fromByteCount: runner.bytesCopied, countStyle: .file))
                summaryLine(language.t("label.throughput"), value: ByteCountFormatter.string(fromByteCount: runner.throughputBytesPerSec, countStyle: .file) + "/s")
                if let lastError = runner.errorMessages.last {
                    Text(String(format: language.t("label.lastError"), lastError))
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

private func bucketPicker(title: String,
                          selection: Binding<String>,
                          buckets: [String],
                          isLoading: Bool,
                          onRefresh: @escaping () -> Void) -> some View {
    HStack {
        if buckets.isEmpty {
            TextField(title, text: selection)
        } else {
            Picker(title, selection: selection) {
                ForEach(buckets, id: \.self) { bucket in
                    Text(bucket).tag(bucket)
                }
            }
            .pickerStyle(.menu)
        }

        Button(action: onRefresh) {
            Image(systemName: "arrow.clockwise")
        }
        .buttonStyle(BorderlessButtonStyle())

        if isLoading {
            ProgressView()
                .scaleEffect(0.7)
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
