import SwiftUI

struct UploadParametersView: View {
    @EnvironmentObject private var language: LanguageManager
    @Environment(\.dismiss) private var dismiss
    let profileName: String

    @State private var multipartThresholdMB: Int = 8
    @State private var multipartChunkSizeMB: Int = 4
    @State private var maxConcurrentRequests: Int = 2
    @State private var maxBandwidthMBps: Int = 4

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(language.t("menu.uploadParameters"))
                    .font(.title2)
                Spacer()
                Button(language.t("button.close")) {
                    persistSettings()
                    dismiss()
                }
                .buttonStyle(.bordered)
            }

            Form {
                Section {
                    Stepper(value: $multipartThresholdMB, in: 8...4096) {
                        HStack {
                            Text(language.t("field.multipartThreshold"))
                            Spacer()
                            Text("\(multipartThresholdMB) MB")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $multipartChunkSizeMB, in: 5...4096) {
                        HStack {
                            Text(language.t("field.multipartChunkSize"))
                            Spacer()
                            Text("\(multipartChunkSizeMB) MB")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $maxConcurrentRequests, in: 1...512) {
                        HStack {
                            Text(language.t("field.maxConcurrentRequests"))
                            Spacer()
                            Text("\(maxConcurrentRequests)")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $maxBandwidthMBps, in: 1...2048) {
                        HStack {
                            Text(language.t("field.maxBandwidth"))
                            Spacer()
                            Text("\(maxBandwidthMBps) MB/s")
                                .foregroundColor(.secondary)
                        }
                    }
                }
            }
            .formStyle(.grouped)
        }
        .padding()
        .frame(minWidth: 460, minHeight: 300)
        .onAppear { loadSettings() }
        .onChange(of: profileName) { _ in loadSettings() }
    }

    private func storageKey(_ suffix: String) -> String {
        let trimmed = profileName.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileKey = trimmed.isEmpty ? "default" : trimmed
        return "upload.\(profileKey).\(suffix)"
    }

    private func loadSettings() {
        let defaults = UserDefaults.standard
        let threshold = defaults.integer(forKey: storageKey("multipartThresholdMB"))
        let chunk = defaults.integer(forKey: storageKey("multipartChunkSizeMB"))
        let concurrent = defaults.integer(forKey: storageKey("maxConcurrentRequests"))
        let bandwidth = defaults.integer(forKey: storageKey("maxBandwidthMBps"))

        multipartThresholdMB = threshold > 0 ? threshold : 8
        multipartChunkSizeMB = chunk > 0 ? chunk : 4
        maxConcurrentRequests = concurrent > 0 ? concurrent : 2
        maxBandwidthMBps = bandwidth > 0 ? bandwidth : 4
    }

    private func persistSettings() {
        let defaults = UserDefaults.standard
        defaults.set(multipartThresholdMB, forKey: storageKey("multipartThresholdMB"))
        defaults.set(multipartChunkSizeMB, forKey: storageKey("multipartChunkSizeMB"))
        defaults.set(maxConcurrentRequests, forKey: storageKey("maxConcurrentRequests"))
        defaults.set(maxBandwidthMBps, forKey: storageKey("maxBandwidthMBps"))
    }
}
