import SwiftUI

struct MigrationSettingsView: View {
    @ObservedObject var settings: MigrationSettings
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var language: LanguageManager

    private let maxConcurrentRange = 1...8
    private let bandwidthRange = 1...50
    private let bufferRange = 128...512

    var body: some View {
        VStack(spacing: 16) {
            HStack {
                Text(language.t("migration.settingsTitle"))
                    .font(.title2)
                Spacer()
                Button(language.t("button.close")) { dismiss() }
            }
            Form {
                Section(language.t("migration.globalSettings")) {
                    Stepper(value: $settings.maxConcurrentTransfers, in: maxConcurrentRange) {
                        HStack {
                            Text(language.t("migration.maxConcurrent"))
                            Spacer()
                            Text("\(settings.maxConcurrentTransfers)")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $settings.bandwidthLimitMBps, in: bandwidthRange) {
                        HStack {
                            Text(language.t("migration.bandwidthLimit"))
                            Spacer()
                            Text("\(settings.bandwidthLimitMBps) MB/s")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $settings.bufferSizeMB, in: bufferRange, step: 32) {
                        HStack {
                            Text(language.t("migration.bufferPerStream"))
                            Spacer()
                            Text("\(settings.bufferSizeMB) MB")
                                .foregroundColor(.secondary)
                        }
                    }
                }
                Section {
                    Text(language.t("migration.settingsNote"))
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
            .formStyle(.grouped)
        }
        .padding(20)
        .frame(minWidth: 480, minHeight: 320)
    }
}
