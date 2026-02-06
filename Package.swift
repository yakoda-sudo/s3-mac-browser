// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "s3-mac-browser",
    defaultLocalization: "en",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .executable(name: "s3-mac-browser", targets: ["S3MacBrowserDemoApp"])
    ],
    targets: [
        .executableTarget(
            name: "S3MacBrowserDemoApp",
            path: "Sources/S3MacBrowserDemoApp",
            exclude: ["MetricsTests.swift"],
            resources: [
                .process("Resources")
            ]
        ),
        .testTarget(
            name: "S3MacBrowserDemoAppTests",
            dependencies: ["S3MacBrowserDemoApp"],
            path: "Sources/S3MacBrowserDemoApp",
            exclude: ["Localization", "Models", "Services", "ViewModels", "Views", "S3MacBrowserDemoApp.swift"],
            sources: ["MetricsTests.swift"]
        )
    ]
)
