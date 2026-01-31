import Foundation

actor MigrationCheckpointStore {
    private let fileURL: URL
    private var completedKeys: Set<String> = []

    init(jobId: UUID, profileName: String) {
        let base = Self.checkpointDirectory()
        let safeProfile = profileName.replacingOccurrences(of: " ", with: "_")
        let filename = "checkpoint-\(safeProfile)-\(jobId.uuidString).ndjson"
        fileURL = base.appendingPathComponent(filename)
        completedKeys = Self.loadExisting(from: fileURL)
    }

    func isCompleted(_ key: String) -> Bool {
        completedKeys.contains(key)
    }

    func markCompleted(_ key: String) {
        guard !completedKeys.contains(key) else { return }
        completedKeys.insert(key)
        appendLine(key)
    }

    private static func loadExisting(from url: URL) -> Set<String> {
        guard let data = try? Data(contentsOf: url),
              let text = String(data: data, encoding: .utf8) else {
            return []
        }
        let lines = text.split(separator: "\n").map(String.init)
        return Set(lines)
    }

    private func appendLine(_ key: String) {
        let line = key + "\n"
        if let data = line.data(using: .utf8) {
            if FileManager.default.fileExists(atPath: fileURL.path) {
                if let handle = try? FileHandle(forWritingTo: fileURL) {
                    defer { try? handle.close() }
                    _ = try? handle.seekToEnd()
                    try? handle.write(contentsOf: data)
                }
            } else {
                try? data.write(to: fileURL, options: .atomic)
            }
        }
    }

    private static func checkpointDirectory() -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first ?? FileManager.default.temporaryDirectory
        let dir = root.appendingPathComponent("s3-mac-browser/migration", isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }
}
