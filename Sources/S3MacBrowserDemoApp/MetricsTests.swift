import XCTest
@testable import S3MacBrowserDemoApp

final class MetricsTests: XCTestCase {
    func testHourBucketing() {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        let date = formatter.date(from: "2026-01-29T21:34:45Z")!
        let hourStart = MetricsTime.hourStart(for: date)
        XCTAssertEqual(MetricsTime.hourString(for: hourStart), "2026-01-29T21:00:00Z")
    }

    func testByteFormatter() {
        let formatted = MetricsByteFormatter.string(from: 1024)
        XCTAssertTrue(formatted.contains("KB") || formatted.contains("kB"))
        XCTAssertTrue(formatted.hasPrefix("1"))
    }

    func testAggregationAcrossMonthBoundary() throws {
        let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)

        let profileName = "Test Profile"
        let janDate = ISO8601DateFormatter().date(from: "2026-01-31T23:00:00Z")!
        let febDate = ISO8601DateFormatter().date(from: "2026-02-01T00:00:00Z")!
        let febDate2 = ISO8601DateFormatter().date(from: "2026-02-02T00:00:00Z")!

        try writeRecord(MetricsHourRecord(hourStart: janDate, put: 1, copy: 0, post: 0, list: 0, get: 1, select: 0, up: 100, down: 200), root: tempRoot, profile: profileName, date: janDate)
        try writeRecord(MetricsHourRecord(hourStart: febDate, put: 0, copy: 0, post: 0, list: 2, get: 0, select: 0, up: 0, down: 50), root: tempRoot, profile: profileName, date: febDate)
        try writeRecord(MetricsHourRecord(hourStart: febDate2, put: 0, copy: 0, post: 0, list: 0, get: 3, select: 0, up: 0, down: 300), root: tempRoot, profile: profileName, date: febDate2)

        let now = ISO8601DateFormatter().date(from: "2026-02-02T01:00:00Z")!
        let totals = MetricsAggregator.loadLast72Hours(profileName: profileName, now: now, rootURL: tempRoot, appName: "s3-mac-browser")

        XCTAssertEqual(totals.totalRequests, 7)
        XCTAssertEqual(totals.totalUpload, 100)
        XCTAssertEqual(totals.totalDownload, 550)
        XCTAssertEqual(totals.byCategory[.put]?.count, 1)
        XCTAssertEqual(totals.byCategory[.get]?.count, 4)
        XCTAssertEqual(totals.byCategory[.list]?.count, 2)
        XCTAssertEqual(totals.byCategory[.head]?.count, 0)
    }

    private func writeRecord(_ record: MetricsHourRecord, root: URL, profile: String, date: Date) throws {
        let encoder = JSONEncoder()
        let data = try encoder.encode(record)
        guard let line = String(data: data, encoding: .utf8) else { return }
        let fileURL = MetricsPaths.monthlyFileURL(root: root, profileName: profile, date: date)
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let existing = (try? String(contentsOf: fileURL, encoding: .utf8)) ?? ""
        let output = existing + line + "\n"
        try output.write(to: fileURL, atomically: true, encoding: .utf8)
    }
}

final class StorageEndpointTests: XCTestCase {
    func testAzureDetectionByHostAndSig() {
        let input = "https://account.blob.core.windows.net/?sv=2023-11-03&sig=abc"
        let endpoint = StorageEndpointParser.parse(input: input)
        XCTAssertEqual(endpoint?.provider, .azureBlob)
        XCTAssertEqual(endpoint?.container, nil)
        XCTAssertNotNil(endpoint?.sasToken)
    }

    func testAzureDetectionWithoutScheme() {
        let input = "account.blob.core.windows.net/container?sig=abc"
        let endpoint = StorageEndpointParser.parse(input: input)
        XCTAssertEqual(endpoint?.provider, .azureBlob)
        XCTAssertEqual(endpoint?.container, "container")
    }

    func testS3DetectionLocal() {
        let input = "10.0.0.10:9000"
        let endpoint = StorageEndpointParser.parse(input: input)
        XCTAssertEqual(endpoint?.provider, .s3)
        XCTAssertEqual(endpoint?.baseURL.scheme, "http")
    }
}

final class AzureParserTests: XCTestCase {
    func testContainerListParsing() {
        let xml = """
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults>
          <Containers>
            <Container><Name>alpha</Name></Container>
            <Container><Name>beta</Name></Container>
          </Containers>
        </EnumerationResults>
        """
        let parser = AzureContainerListParser()
        let xmlParser = XMLParser(data: Data(xml.utf8))
        xmlParser.delegate = parser
        xmlParser.parse()
        XCTAssertEqual(parser.containerNames, ["alpha", "beta"])
    }

    func testBlobListParsing() {
        let xml = """
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults>
          <Blobs>
            <Blob>
              <Name>folder/file.txt</Name>
              <Properties>
                <Last-Modified>Wed, 28 Jan 2026 21:36:22 GMT</Last-Modified>
                <Content-Length>12</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Etag>"abc"</Etag>
              </Properties>
            </Blob>
            <BlobPrefix>
              <Name>folder/</Name>
            </BlobPrefix>
          </Blobs>
          <NextMarker></NextMarker>
        </EnumerationResults>
        """
        let parser = AzureBlobListParser()
        let xmlParser = XMLParser(data: Data(xml.utf8))
        xmlParser.delegate = parser
        xmlParser.parse()
        XCTAssertEqual(parser.entries.count, 2)
        XCTAssertEqual(parser.entries.first?.key, "folder/file.txt")
        XCTAssertEqual(parser.entries.last?.contentType, "folder")
    }
}

final class AzureMetricsTests: XCTestCase {
    func testAzureListMetricsIncrement() async throws {
        let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)

        let recorder = MetricsRecorder(appName: "s3-mac-browser", rootURL: tempRoot)
        let configProvider: () -> URLSessionConfiguration = {
            let config = URLSessionConfiguration.ephemeral
            config.protocolClasses = [MockURLProtocol.self]
            return config
        }
        let backend = AzureBlobBackend(metricsRecorder: recorder, sessionConfigurationProvider: configProvider)

        let xml = """
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults>
          <Containers>
            <Container><Name>alpha</Name></Container>
          </Containers>
        </EnumerationResults>
        """
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/xml"]
            )!
            return (response, Data(xml.utf8))
        }

        let endpoint = StorageEndpointParser.parse(input: "https://account.blob.core.windows.net/?sig=abc")!
        _ = try await backend.testConnection(
            endpoint: endpoint,
            region: "",
            accessKey: "",
            secretKey: "",
            allowInsecure: false,
            profileName: "TestProfile"
        )

        await recorder.flushAll()
        let metricsFile = MetricsPaths.monthlyFileURL(root: tempRoot, profileName: "TestProfile", date: Date())
        let contents = try String(contentsOf: metricsFile, encoding: .utf8)
        let firstLine = contents.split(separator: "\n").first
        XCTAssertNotNil(firstLine)
        if let line = firstLine {
            let record = try JSONDecoder().decode(MetricsHourRecord.self, from: Data(line.utf8))
            XCTAssertEqual(record.list, 1)
        }
    }
}

final class MockURLProtocol: URLProtocol {
    static var requestHandler: ((URLRequest) -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let handler = MockURLProtocol.requestHandler else {
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        let (response, data) = handler(request)
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
