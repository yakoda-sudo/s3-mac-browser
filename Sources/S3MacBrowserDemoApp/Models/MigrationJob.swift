import Foundation

struct MigrationJob: Identifiable, Hashable {
    let id = UUID()
    let createdAt = Date()
    let sourceProfileName: String
    let sourceBucket: String
    let sourcePrefix: String
    let targetProfileName: String
    let targetBucket: String
    let targetPrefix: String
}
