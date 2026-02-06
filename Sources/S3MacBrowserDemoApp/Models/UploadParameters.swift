import Foundation

struct UploadParameters: Sendable {
    let multipartThresholdBytes: Int64
    let multipartChunkBytes: Int
    let maxConcurrentRequests: Int
    let maxBandwidthBytesPerSec: Int64
}
