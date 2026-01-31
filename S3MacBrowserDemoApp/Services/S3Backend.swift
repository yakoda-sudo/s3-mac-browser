import Foundation

final class S3Backend: StorageBackend {
    let provider: StorageProvider = .s3
    private let service: S3ServiceProtocol

    init(service: S3ServiceProtocol = S3Service()) {
        self.service = service
    }

    func testConnection(endpoint: StorageEndpoint, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.listBuckets(
            endpoint: endpoint.baseURL,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func listObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.listObjects(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            prefix: prefix,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func listObjectVersions(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.listObjectVersions(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            prefix: prefix,
            keyMarker: nil,
            versionIdMarker: nil,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func listAllObjects(endpoint: StorageEndpoint, bucket: String, prefix: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> [S3Object] {
        try await service.listAllObjects(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            prefix: prefix,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func headObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.headObject(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func putObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, data: Data, contentType: String?, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ConnectionResult {
        try await service.putObjectWithProgress(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            data: data,
            contentType: contentType,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName,
            progress: progress
        )
    }

    func getObjectWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        try await service.getObjectWithProgress(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName,
            progress: progress
        )
    }

    func getObjectVersionWithProgress(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String, progress: @escaping @Sendable (Int64, Int64) -> Void) async throws -> ObjectDataResult {
        try await service.getObjectVersionWithProgress(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            versionId: versionId,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName,
            progress: progress
        )
    }

    func deleteObject(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.deleteObject(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func deleteObjectVersion(endpoint: StorageEndpoint, bucket: String, key: String, versionId: String, region: String, accessKey: String, secretKey: String, allowInsecure: Bool, profileName: String) async throws -> ConnectionResult {
        try await service.deleteObjectVersion(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            versionId: versionId,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            allowInsecure: allowInsecure,
            profileName: profileName
        )
    }

    func shareLink(endpoint: StorageEndpoint, bucket: String, key: String, region: String, accessKey: String, secretKey: String, expiresHours: Int) -> String? {
        let seconds = min(max(expiresHours, 1), 168) * 3600
        return service.presignGetURL(
            endpoint: endpoint.baseURL,
            bucket: bucket,
            key: key,
            region: region,
            accessKey: accessKey,
            secretKey: secretKey,
            expiresSeconds: seconds
        )
    }
}
