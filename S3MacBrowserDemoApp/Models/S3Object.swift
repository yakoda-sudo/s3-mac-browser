import Foundation

struct S3Object: Identifiable, Hashable {
    let id = UUID()
    let key: String
    let sizeBytes: Int
    let lastModified: Date
    let contentType: String
    let eTag: String
}
