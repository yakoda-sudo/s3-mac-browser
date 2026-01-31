import Foundation

enum MetricsCategory: String, CaseIterable, Codable {
    case put
    case copy
    case post
    case list
    case get
    case select
    case head
    case delete

    var displayName: String {
        rawValue.uppercased()
    }
}

struct MetricsHourRecord: Codable, Equatable {
    var h: String
    var put: Int
    var copy: Int
    var post: Int
    var list: Int
    var get: Int
    var select: Int
    var head: Int
    var delete: Int
    var up: Int64
    var down: Int64

    init(hourStart: Date) {
        h = MetricsTime.hourString(for: hourStart)
        put = 0
        copy = 0
        post = 0
        list = 0
        get = 0
        select = 0
        head = 0
        delete = 0
        up = 0
        down = 0
    }

    init(hourStart: Date, put: Int, copy: Int, post: Int, list: Int, get: Int, select: Int, head: Int = 0, delete: Int = 0, up: Int64, down: Int64) {
        self.h = MetricsTime.hourString(for: hourStart)
        self.put = put
        self.copy = copy
        self.post = post
        self.list = list
        self.get = get
        self.select = select
        self.head = head
        self.delete = delete
        self.up = up
        self.down = down
    }

    mutating func add(category: MetricsCategory, uploaded: Int64, downloaded: Int64) {
        switch category {
        case .put:
            put += 1
        case .copy:
            copy += 1
        case .post:
            post += 1
        case .list:
            list += 1
        case .get:
            get += 1
        case .select:
            select += 1
        case .head:
            head += 1
        case .delete:
            delete += 1
        }
        up += max(uploaded, 0)
        down += max(downloaded, 0)
    }

    enum CodingKeys: String, CodingKey {
        case h, put, copy, post, list, get, select, head, delete, up, down
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        h = try container.decode(String.self, forKey: .h)
        put = try container.decodeIfPresent(Int.self, forKey: .put) ?? 0
        copy = try container.decodeIfPresent(Int.self, forKey: .copy) ?? 0
        post = try container.decodeIfPresent(Int.self, forKey: .post) ?? 0
        list = try container.decodeIfPresent(Int.self, forKey: .list) ?? 0
        get = try container.decodeIfPresent(Int.self, forKey: .get) ?? 0
        select = try container.decodeIfPresent(Int.self, forKey: .select) ?? 0
        head = try container.decodeIfPresent(Int.self, forKey: .head) ?? 0
        delete = try container.decodeIfPresent(Int.self, forKey: .delete) ?? 0
        up = try container.decodeIfPresent(Int64.self, forKey: .up) ?? 0
        down = try container.decodeIfPresent(Int64.self, forKey: .down) ?? 0
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(h, forKey: .h)
        try container.encode(put, forKey: .put)
        try container.encode(copy, forKey: .copy)
        try container.encode(post, forKey: .post)
        try container.encode(list, forKey: .list)
        try container.encode(get, forKey: .get)
        try container.encode(select, forKey: .select)
        try container.encode(head, forKey: .head)
        try container.encode(delete, forKey: .delete)
        try container.encode(up, forKey: .up)
        try container.encode(down, forKey: .down)
    }
}

struct MetricsCounts: Equatable {
    var count: Int
    var up: Int64
    var down: Int64
}

struct MetricsTotals: Equatable {
    var byCategory: [MetricsCategory: MetricsCounts]
    var totalRequests: Int
    var totalUpload: Int64
    var totalDownload: Int64

    static var empty: MetricsTotals {
        MetricsTotals(
            byCategory: MetricsCategory.allCases.reduce(into: [:]) { partial, category in
                partial[category] = MetricsCounts(count: 0, up: 0, down: 0)
            },
            totalRequests: 0,
            totalUpload: 0,
            totalDownload: 0
        )
    }
}

enum MetricsTime {
    private static let utcCalendar: Calendar = {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") ?? .current
        return calendar
    }()

    static func hourStart(for date: Date) -> Date {
        let components = utcCalendar.dateComponents([.year, .month, .day, .hour], from: date)
        return utcCalendar.date(from: components) ?? date
    }

    static func hourString(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.string(from: hourStart(for: date))
    }

    static func parseHourString(_ value: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.date(from: value)
    }

    static func monthStamp(for date: Date) -> String {
        let components = utcCalendar.dateComponents([.year, .month], from: date)
        let year = components.year ?? 0
        let month = components.month ?? 1
        return String(format: "%04d-%02d", year, month)
    }
}

enum MetricsPaths {
    static func metricsRoot(appName: String) throws -> URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
        let appDir = (base ?? URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true))
            .appendingPathComponent(appName, isDirectory: true)
        let metricsDir = appDir.appendingPathComponent("metrics", isDirectory: true)
        try FileManager.default.createDirectory(at: metricsDir, withIntermediateDirectories: true)
        return metricsDir
    }

    static func sanitizeProfileName(_ name: String) -> String {
        let trimmed = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "default" }
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
        let sanitized = trimmed.unicodeScalars.map { allowed.contains($0) ? Character($0) : "_" }
        return String(sanitized)
    }

    static func profileDirectory(root: URL, profileName: String) -> URL {
        root.appendingPathComponent(sanitizeProfileName(profileName), isDirectory: true)
    }

    static func monthlyFileURL(root: URL, profileName: String, date: Date) -> URL {
        let monthStamp = MetricsTime.monthStamp(for: date)
        let profileDir = profileDirectory(root: root, profileName: profileName)
        return profileDir.appendingPathComponent("metrics-\(monthStamp).ndjson")
    }
}

struct MetricsByteFormatter {
    static func string(from bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useBytes, .useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesActualByteCount = false
        return formatter.string(fromByteCount: bytes)
    }
}

actor MetricsRecorder {
    static let shared = MetricsRecorder()

    private struct HourState {
        var hourStart: Date
        var record: MetricsHourRecord
    }

    private let appName: String
    private let metricsRoot: URL?
    private var perProfile: [String: HourState] = [:]
    private var flushTask: Task<Void, Never>?

    init(appName: String = "s3-mac-browser", rootURL: URL? = nil) {
        self.appName = appName
        if let rootURL {
            metricsRoot = rootURL
        } else {
            metricsRoot = try? MetricsPaths.metricsRoot(appName: appName)
        }
        if let metricsRoot {
            try? FileManager.default.createDirectory(at: metricsRoot, withIntermediateDirectories: true)
        }
        Task { [weak self] in
            await self?.bootstrap()
        }
    }

    func record(category: MetricsCategory, uploaded: Int64, downloaded: Int64, timestamp: Date = Date(), profileName: String) {
        guard metricsRoot != nil else { return }
        let profileKey = MetricsPaths.sanitizeProfileName(profileName)
        let hourStart = MetricsTime.hourStart(for: timestamp)

        if var state = perProfile[profileKey] {
            if state.hourStart != hourStart {
                flush(state: state, profileKey: profileKey)
                state = HourState(hourStart: hourStart, record: MetricsHourRecord(hourStart: hourStart))
            }
            state.record.add(category: category, uploaded: uploaded, downloaded: downloaded)
            perProfile[profileKey] = state
        } else {
            var record = MetricsHourRecord(hourStart: hourStart)
            record.add(category: category, uploaded: uploaded, downloaded: downloaded)
            perProfile[profileKey] = HourState(hourStart: hourStart, record: record)
        }
    }

    func flushAll() {
        for (profileKey, state) in perProfile {
            flush(state: state, profileKey: profileKey)
        }
    }

    func clearProfile(profileName: String) {
        guard let metricsRoot else { return }
        let profileKey = MetricsPaths.sanitizeProfileName(profileName)
        let profileDir = MetricsPaths.profileDirectory(root: metricsRoot, profileName: profileKey)
        _ = try? FileManager.default.removeItem(at: profileDir)
        _ = try? FileManager.default.createDirectory(at: profileDir, withIntermediateDirectories: true)
        perProfile.removeValue(forKey: profileKey)
    }

    private func bootstrap() async {
        cleanupOldFiles()
        startAutoFlush()
    }

    private func flush(state: HourState, profileKey: String) {
        guard let metricsRoot else { return }
        do {
            let fileURL = MetricsPaths.monthlyFileURL(root: metricsRoot, profileName: profileKey, date: state.hourStart)
            try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            try writeRecord(state.record, to: fileURL)
        } catch {
            return
        }
    }

    private func writeRecord(_ record: MetricsHourRecord, to fileURL: URL) throws {
        let encoder = JSONEncoder()
        let data = try encoder.encode(record)
        guard let line = String(data: data, encoding: .utf8) else { return }
        let hourKey = record.h

        var lines: [String] = []
        if let existing = try? String(contentsOf: fileURL, encoding: .utf8) {
            lines = existing
                .split(separator: "\n")
                .map(String.init)
                .filter { !$0.isEmpty && !$0.contains("\"\(hourKey)\"") }
        }
        lines.append(line)
        let output = lines.joined(separator: "\n") + "\n"

        let tempURL = fileURL.appendingPathExtension("tmp")
        try output.write(to: tempURL, atomically: true, encoding: .utf8)
        if FileManager.default.fileExists(atPath: fileURL.path) {
            _ = try? FileManager.default.removeItem(at: fileURL)
        }
        try FileManager.default.moveItem(at: tempURL, to: fileURL)
    }

    private func cleanupOldFiles() {
        guard let metricsRoot else { return }
        let cutoff = Date().addingTimeInterval(-30 * 24 * 60 * 60)
        let fileManager = FileManager.default
        let enumerator = fileManager.enumerator(
            at: metricsRoot,
            includingPropertiesForKeys: [URLResourceKey.contentModificationDateKey],
            options: [.skipsHiddenFiles]
        )
        if let enumerator {
            for case let fileURL as URL in enumerator {
                guard fileURL.lastPathComponent.hasPrefix("metrics-"),
                      fileURL.pathExtension == "ndjson" else { continue }
                let values = try? fileURL.resourceValues(forKeys: [URLResourceKey.contentModificationDateKey])
                if let modified = values?.contentModificationDate, modified < cutoff {
                    _ = try? fileManager.removeItem(at: fileURL)
                }
            }
        }
    }

    private func startAutoFlush() {
        flushTask?.cancel()
        flushTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 60 * 1_000_000_000)
                await self?.flushAll()
            }
        }
    }
}

enum MetricsAggregator {
    static func loadLast72Hours(profileName: String, now: Date = Date(), rootURL: URL? = nil, appName: String = "s3-mac-browser") -> MetricsTotals {
        let root = rootURL ?? (try? MetricsPaths.metricsRoot(appName: appName))
        guard let root else { return .empty }
        let profileDir = MetricsPaths.profileDirectory(root: root, profileName: profileName)
        let startDate = now.addingTimeInterval(-72 * 60 * 60)

        let months = monthStamps(from: startDate, to: now)
        var totals = MetricsTotals.empty
        var totalRequests = 0
        var totalUpload: Int64 = 0
        var totalDownload: Int64 = 0
        let decoder = JSONDecoder()

        for month in months {
            let fileURL = profileDir.appendingPathComponent("metrics-\(month).ndjson")
            guard let contents = try? String(contentsOf: fileURL, encoding: .utf8) else { continue }
            for line in contents.split(separator: "\n") {
                guard let data = line.data(using: .utf8),
                      let record = try? decoder.decode(MetricsHourRecord.self, from: data),
                      let hourDate = MetricsTime.parseHourString(record.h),
                      hourDate >= startDate else { continue }
                if hourDate > now { continue }
                totalRequests += record.put + record.copy + record.post + record.list + record.get + record.select + record.head + record.delete
                totalUpload += record.up
                totalDownload += record.down
                totals = totalsByAdding(record: record, into: totals)
            }
        }
        totals.totalRequests = totalRequests
        totals.totalUpload = totalUpload
        totals.totalDownload = totalDownload
        return totals
    }

    private static func totalsByAdding(record: MetricsHourRecord, into totals: MetricsTotals) -> MetricsTotals {
        var updated = totals
        let uploadCount = record.put + record.copy + record.post
        let downloadCount = record.get + record.list + record.select + record.head
        add(category: .put, count: record.put, up: apportionedBytes(total: record.up, part: record.put, sum: uploadCount), down: 0, totals: &updated)
        add(category: .copy, count: record.copy, up: apportionedBytes(total: record.up, part: record.copy, sum: uploadCount), down: 0, totals: &updated)
        add(category: .post, count: record.post, up: apportionedBytes(total: record.up, part: record.post, sum: uploadCount), down: 0, totals: &updated)
        add(category: .list, count: record.list, up: 0, down: apportionedBytes(total: record.down, part: record.list, sum: downloadCount), totals: &updated)
        add(category: .get, count: record.get, up: 0, down: apportionedBytes(total: record.down, part: record.get, sum: downloadCount), totals: &updated)
        add(category: .select, count: record.select, up: 0, down: apportionedBytes(total: record.down, part: record.select, sum: downloadCount), totals: &updated)
        add(category: .head, count: record.head, up: 0, down: apportionedBytes(total: record.down, part: record.head, sum: downloadCount), totals: &updated)
        add(category: .delete, count: record.delete, up: 0, down: 0, totals: &updated)
        return updated
    }

    private static func add(category: MetricsCategory, count: Int, up: Int64, down: Int64, totals: inout MetricsTotals) {
        guard count > 0 || up > 0 || down > 0 else { return }
        var existing = totals.byCategory[category] ?? MetricsCounts(count: 0, up: 0, down: 0)
        existing.count += count
        existing.up += up
        existing.down += down
        totals.byCategory[category] = existing
    }

    private static func apportionedBytes(total: Int64, part: Int, sum: Int) -> Int64 {
        guard total > 0, sum > 0, part > 0 else { return 0 }
        let ratio = Double(part) / Double(sum)
        return Int64(Double(total) * ratio)
    }

    private static func monthStamps(from start: Date, to end: Date) -> [String] {
        var stamps: [String] = []
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? TimeZone(identifier: "UTC") ?? .current
        var current = calendar.date(from: calendar.dateComponents([.year, .month], from: start)) ?? start
        let endMonth = calendar.date(from: calendar.dateComponents([.year, .month], from: end)) ?? end

        while current <= endMonth {
            stamps.append(MetricsTime.monthStamp(for: current))
            guard let next = calendar.date(byAdding: .month, value: 1, to: current) else { break }
            current = next
        }
        return stamps
    }
}
