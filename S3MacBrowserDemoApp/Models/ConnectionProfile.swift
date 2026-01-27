import Foundation

struct ConnectionProfile: Identifiable, Hashable {
    let id = UUID()
    var name: String
    var endpoint: String
    var region: String
    var accessKey: String
    var secretKey: String
}
