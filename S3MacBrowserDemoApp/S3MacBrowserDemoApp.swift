import SwiftUI
import AppKit

@main
struct S3MacBrowserDemoApp: App {
    @AppStorage("presignExpiryHours") private var presignExpiryHours: Int = 4
    private let appVersion = "0.2"

    init() {
        NSApplication.shared.setActivationPolicy(.regular)
    }

    var body: some Scene {
        WindowGroup {
            ConnectionView()
                .onAppear {
                    NSApplication.shared.activate(ignoringOtherApps: true)
                }
        }
        .commands {
            CommandGroup(after: .textEditing) {
                Menu("Presigned URL Expiry") {
                    Button("1 hour") { setPresignHours(1) }
                    Button("4 hours") { setPresignHours(4) }
                    Button("24 hours") { setPresignHours(24) }
                    Button("7 days (168 hours)") { setPresignHours(168) }
                }
            }
            CommandGroup(after: .help) {
                Button("Version Info") {
                    let alert = NSAlert()
                    alert.messageText = "s3-mac-browser"
                    alert.informativeText = "Version: \(appVersion)\nAuthor: yangqi kou\nDate: 2026-01-27"
                    alert.runModal()
                }
            }
        }
    }

    private func setPresignHours(_ hours: Int) {
        presignExpiryHours = min(max(hours, 1), 168)
    }
}
