import SwiftUI
import AppKit

struct UsageStatsView: View {
    let profileName: String

    @State private var totals: MetricsTotals = .empty
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var language: LanguageManager

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(language.t("metrics.title"))
                    .font(.title2)
                Spacer()
                Button(language.t("button.copy")) {
                    copyToClipboard()
                }
                Button(language.t("button.close")) {
                    dismiss()
                }
            }
            Text(String(format: language.t("metrics.profile"), displayProfileName))
                .font(.caption)
                .foregroundColor(.secondary)
            Text(language.t("metrics.last72h"))
                .font(.caption)
                .foregroundColor(.secondary)

            HStack(spacing: 24) {
                statBlock(title: language.t("metrics.totalRequests"), value: "\(totals.totalRequests)")
                statBlock(title: language.t("metrics.totalUpload"), value: MetricsByteFormatter.string(from: totals.totalUpload))
                statBlock(title: language.t("metrics.totalDownload"), value: MetricsByteFormatter.string(from: totals.totalDownload))
            }

            Divider()

            VStack(alignment: .leading, spacing: 6) {
                HStack {
                    Text(language.t("metrics.category"))
                        .frame(width: 90, alignment: .leading)
                    Text(language.t("metrics.requests"))
                        .frame(width: 90, alignment: .trailing)
                    Text(language.t("metrics.upload"))
                        .frame(width: 140, alignment: .trailing)
                    Text(language.t("metrics.download"))
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
        return trimmed.isEmpty ? language.t("general.default") : trimmed
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
        lines.append(language.t("metrics.title"))
        lines.append(String(format: language.t("metrics.profile"), displayProfileName))
        lines.append(language.t("metrics.last72h"))
        lines.append("")
        lines.append(String(format: language.t("metrics.totalRequestsLine"), totals.totalRequests))
        lines.append(String(format: language.t("metrics.totalUploadLine"), MetricsByteFormatter.string(from: totals.totalUpload)))
        lines.append(String(format: language.t("metrics.totalDownloadLine"), MetricsByteFormatter.string(from: totals.totalDownload)))
        lines.append("")
        lines.append(language.t("metrics.tableHeader"))
        for category in MetricsCategory.allCases {
            let counts = totals.byCategory[category] ?? MetricsCounts(count: 0, up: 0, down: 0)
            let upload = MetricsByteFormatter.string(from: counts.up)
            let download = MetricsByteFormatter.string(from: counts.down)
            lines.append("\(category.displayName) | \(counts.count) | \(upload) | \(download)")
        }
        return lines.joined(separator: "\n")
    }
}
