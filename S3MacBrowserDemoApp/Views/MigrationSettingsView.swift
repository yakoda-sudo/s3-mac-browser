import SwiftUI

struct MigrationSettingsView: View {
    @ObservedObject var settings: MigrationSettings
    @Environment(\.dismiss) private var dismiss

    private let maxConcurrentRange = 1...8
    private let bandwidthRange = 1...50
    private let bufferRange = 128...512

    var body: some View {
        VStack(spacing: 16) {
            HStack {
                Text("Migration Settings")
                    .font(.title2)
                Spacer()
                Button("Close") { dismiss() }
            }
            Form {
                Section("Global Settings") {
                    Stepper(value: $settings.maxConcurrentTransfers, in: maxConcurrentRange) {
                        HStack {
                            Text("Max concurrent transfers")
                            Spacer()
                            Text("\(settings.maxConcurrentTransfers)")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $settings.bandwidthLimitMBps, in: bandwidthRange) {
                        HStack {
                            Text("Bandwidth limit")
                            Spacer()
                            Text("\(settings.bandwidthLimitMBps) MB/s")
                                .foregroundColor(.secondary)
                        }
                    }
                    Stepper(value: $settings.bufferSizeMB, in: bufferRange, step: 32) {
                        HStack {
                            Text("Memory buffer per stream")
                            Spacer()
                            Text("\(settings.bufferSizeMB) MB")
                                .foregroundColor(.secondary)
                        }
                    }
                }
                Section {
                    Text("These settings apply to all migration jobs. Jobs do not override them.")
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
