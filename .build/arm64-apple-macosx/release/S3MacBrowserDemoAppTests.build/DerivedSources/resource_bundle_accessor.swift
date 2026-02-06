import Foundation

extension Foundation.Bundle {
    static let module: Bundle = {
        let mainPath = Bundle.main.bundleURL.appendingPathComponent("s3-mac-browser_S3MacBrowserDemoAppTests.bundle").path
        let buildPath = "/Users/ykou/Documents/s3-mac-browser/S3MacBrowserDemo/.build/arm64-apple-macosx/release/s3-mac-browser_S3MacBrowserDemoAppTests.bundle"

        let preferredBundle = Bundle(path: mainPath)

        guard let bundle = preferredBundle ?? Bundle(path: buildPath) else {
            // Users can write a function called fatalError themselves, we should be resilient against that.
            Swift.fatalError("could not load resource bundle: from \(mainPath) or \(buildPath)")
        }

        return bundle
    }()
}