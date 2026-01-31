import Foundation

struct S3Object: Identifiable, Hashable {
    let id = UUID()
    let key: String
    let sizeBytes: Int
    let lastModified: Date
    let contentType: String
    let eTag: String
    let versionId: String?
    let isDeleteMarker: Bool
    let isDeleted: Bool
    let isVersioned: Bool
    let isLatest: Bool

    init(key: String,
         sizeBytes: Int,
         lastModified: Date,
         contentType: String,
         eTag: String,
         versionId: String? = nil,
         isDeleteMarker: Bool = false,
         isDeleted: Bool = false,
         isVersioned: Bool = false,
         isLatest: Bool = false) {
        self.key = key
        self.sizeBytes = sizeBytes
        self.lastModified = lastModified
        self.contentType = contentType
        self.eTag = eTag
        self.versionId = versionId
        self.isDeleteMarker = isDeleteMarker
        self.isDeleted = isDeleted
        self.isVersioned = isVersioned
        self.isLatest = isLatest
    }
}
