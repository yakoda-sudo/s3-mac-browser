import Foundation
import CryptoKit

enum S3Signer {
    static func sign(request: inout URLRequest, region: String, accessKey: String, secretKey: String, payloadHash: String) {
        guard let url = request.url, !accessKey.isEmpty, !secretKey.isEmpty else {
            return
        }

        let safeRegion = region.isEmpty ? "us-east-1" : region
        let method = request.httpMethod ?? "GET"
        let host = url.host ?? ""
        let port = url.port
        let hostHeader = port == nil ? host : "\(host):\(port ?? 0)"
        let amzDate = iso8601Date()
        let dateStamp = amzDate.prefix(8)

        let canonicalUri = url.path.isEmpty ? "/" : awsEncode(url.path, encodeSlash: false)
        let canonicalQuery = canonicalQueryString(from: url)
        let canonicalHeaders = "host:\(hostHeader)\n" +
            "x-amz-content-sha256:\(payloadHash)\n" +
            "x-amz-date:\(amzDate)\n"
        let signedHeaders = "host;x-amz-content-sha256;x-amz-date"

        let canonicalRequest = [
            method,
            canonicalUri,
            canonicalQuery,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        ].joined(separator: "\n")

        let algorithm = "AWS4-HMAC-SHA256"
        let credentialScope = "\(dateStamp)/\(safeRegion)/s3/aws4_request"
        let stringToSign = [
            algorithm,
            amzDate,
            credentialScope,
            sha256Hex(canonicalRequest)
        ].joined(separator: "\n")

        let signingKey = deriveSigningKey(secretKey: secretKey, dateStamp: String(dateStamp), region: safeRegion, service: "s3")
        let signature = hmacSHA256Hex(key: signingKey, string: stringToSign)
        let authorization = "\(algorithm) Credential=\(accessKey)/\(credentialScope), SignedHeaders=\(signedHeaders), Signature=\(signature)"

        request.setValue(hostHeader, forHTTPHeaderField: "Host")
        request.setValue(payloadHash, forHTTPHeaderField: "x-amz-content-sha256")
        request.setValue(amzDate, forHTTPHeaderField: "x-amz-date")
        request.setValue(authorization, forHTTPHeaderField: "Authorization")
    }

    static func sha256Hex(_ data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    static func sha256Hex(_ string: String) -> String {
        let data = Data(string.utf8)
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    private static func iso8601Date() -> String {
        let formatter = DateFormatter()
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return formatter.string(from: Date())
    }

    private static func deriveSigningKey(secretKey: String, dateStamp: String, region: String, service: String) -> Data {
        let kSecret = Data(("AWS4" + secretKey).utf8)
        let kDate = hmacSHA256(key: kSecret, data: Data(dateStamp.utf8))
        let kRegion = hmacSHA256(key: kDate, data: Data(region.utf8))
        let kService = hmacSHA256(key: kRegion, data: Data(service.utf8))
        let kSigning = hmacSHA256(key: kService, data: Data("aws4_request".utf8))
        return kSigning
    }

    private static func hmacSHA256(key: Data, data: Data) -> Data {
        let keySym = SymmetricKey(data: key)
        let signature = HMAC<SHA256>.authenticationCode(for: data, using: keySym)
        return Data(signature)
    }

    private static func hmacSHA256Hex(key: Data, string: String) -> String {
        let signature = hmacSHA256(key: key, data: Data(string.utf8))
        return signature.map { String(format: "%02x", $0) }.joined()
    }

    private static func canonicalQueryString(from url: URL) -> String {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return ""
        }
        guard let items = components.queryItems, !items.isEmpty else {
            return ""
        }
        let sorted = items.sorted {
            if $0.name == $1.name {
                return ($0.value ?? "") < ($1.value ?? "")
            }
            return $0.name < $1.name
        }
        return sorted.map { item in
            let name = awsEncode(item.name, encodeSlash: true)
            let value = awsEncode(item.value ?? "", encodeSlash: true)
            return "\(name)=\(value)"
        }.joined(separator: "&")
    }

    private static func awsEncode(_ string: String, encodeSlash: Bool) -> String {
        let unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        var allowed = CharacterSet(charactersIn: unreserved)
        if !encodeSlash {
            allowed.insert(charactersIn: "/")
        }
        return string.unicodeScalars.map { scalar in
            if allowed.contains(scalar) {
                return String(scalar)
            }
            return String(format: "%%%02X", scalar.value)
        }.joined()
    }
}
