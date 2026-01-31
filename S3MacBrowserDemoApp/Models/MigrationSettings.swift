import SwiftUI

@MainActor
final class MigrationSettings: ObservableObject {
    @AppStorage("migration.maxConcurrentTransfers") var maxConcurrentTransfers: Int = 2
    @AppStorage("migration.bandwidthLimitMBps") var bandwidthLimitMBps: Int = 2
    @AppStorage("migration.bufferSizeMB") var bufferSizeMB: Int = 256

    var bandwidthBytesPerSecond: Int64 {
        Int64(max(bandwidthLimitMBps, 1)) * 1024 * 1024
    }

    var bufferBytes: Int {
        max(bufferSizeMB, 128) * 1024 * 1024
    }
}
