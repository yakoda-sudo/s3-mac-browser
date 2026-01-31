import SwiftUI
import AppKit

struct UsageStatsView: View {
    let profileName: String

    @State private var totals: MetricsTotals = .empty
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Requests Metrics")
                    .font(.title2)
                Spacer()
                Button("Copy") {
                    copyToClipboard()
                }
                Button("Close") {
                    dismiss()
                }
            }
            Text("Profile: \(displayProfileName)")
                .font(.caption)
                .foregroundColor(.secondary)
            Text("Last 72 hours (UTC)")
                .font(.caption)
                .foregroundColor(.secondary)

            HStack(spacing: 24) {
                statBlock(title: "Total Requests", value: "\(totals.totalRequests)")
                statBlock(title: "Total Upload", value: MetricsByteFormatter.string(from: totals.totalUpload))
                statBlock(title: "Total Download", value: MetricsByteFormatter.string(from: totals.totalDownload))
            }

            Divider()

            VStack(alignment: .leading, spacing: 6) {
                HStack {
                    Text("Category")
                        .frame(width: 90, alignment: .leading)
                    Text("Requests")
                        .frame(width: 90, alignment: .trailing)
                    Text("Upload")
                        .frame(width: 140, alignment: .trailing)
                    Text("Download")
                        .frame(width: 140, alignment: .trailing)
                    Spacer()
                }
                .font(.caption)
                .foregroundColor(.secondary)

                Divider()

                ForEach(MetricsCategory.allCases, id: \.self) { category in
                    let counts = totals.byCategory[category] ?? MetricsCounts(count: 0, up: 0, down: 0)
                    HStack {
                        Text(category.displayName)
                            .frame(width: 90, alignment: .leading)
                        Text("\(counts.count)")
                            .frame(width: 90, alignment: .trailing)
                        Text(MetricsByteFormatter.string(from: counts.up))
                            .frame(width: 140, alignment: .trailing)
                        Text(MetricsByteFormatter.string(from: counts.down))
                            .frame(width: 140, alignment: .trailing)
                        Spacer()
                    }
                    .font(.caption)
                }
            }

            Spacer()
        }
        .padding(16)
        .frame(minWidth: 520, minHeight: 360)
        .task(id: profileName) {
            totals = MetricsAggregator.loadLast72Hours(profileName: displayProfileName)
        }
    }

    private var displayProfileName: String {
        let trimmed = profileName.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "default" : trimmed
    }

    private func statBlock(title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundColor(.secondary)
            Text(value)
                .font(.headline)
        }
    }

    private func copyToClipboard() {
        let text = metricsText()
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
    }

    private func metricsText() -> String {
        var lines: [String] = []
        lines.append("Requests Metrics")
        lines.append("Profile: \(displayProfileName)")
        lines.append("Last 72 hours (UTC)")
        lines.append("")
        lines.append("Total Requests: \(totals.totalRequests)")
        lines.append("Total Upload: \(MetricsByteFormatter.string(from: totals.totalUpload))")
        lines.append("Total Download: \(MetricsByteFormatter.string(from: totals.totalDownload))")
        lines.append("")
        lines.append("Category | Requests | Upload | Download")
        for category in MetricsCategory.allCases {
            let counts = totals.byCategory[category] ?? MetricsCounts(count: 0, up: 0, down: 0)
            let upload = MetricsByteFormatter.string(from: counts.up)
            let download = MetricsByteFormatter.string(from: counts.down)
            lines.append("\(category.displayName) | \(counts.count) | \(upload) | \(download)")
        }
        return lines.joined(separator: "\n")
    }
}
