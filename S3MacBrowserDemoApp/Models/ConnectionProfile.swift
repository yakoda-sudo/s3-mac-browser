import Foundation

struct ConnectionProfile: Identifiable, Hashable, Codable {
    let id: UUID
    var name: String
    var endpoint: String
    var region: String
    var accessKey: String
    var secretKey: String

    init(id: UUID = UUID(), name: String, endpoint: String, region: String, accessKey: String, secretKey: String) {
        self.id = id
        self.name = name
        self.endpoint = endpoint
        self.region = region
        self.accessKey = accessKey
        self.secretKey = secretKey
    }
}
