import SwiftUI
import AppKit

@main
struct S3MacBrowserDemoApp: App {
    @AppStorage("presignExpiryHours") private var presignExpiryHours: Int = 4
    private let appVersion = "0.9.2"
    @StateObject private var viewModel = ConnectionViewModel()
    @StateObject private var migrationSettings = MigrationSettings()
    @StateObject private var languageManager = LanguageManager()
    @State private var showUsageStats = false
    @State private var showMigrationWizard = false
    @State private var showMigrationSettings = false
    @State private var showUploadParameters = false

    init() {
        NSApplication.shared.setActivationPolicy(.regular)
    }

    var body: some Scene {
        WindowGroup {
            ConnectionView(viewModel: viewModel)
                .environmentObject(languageManager)
                .environment(\.locale, languageManager.locale)
                .id(languageManager.language)
                .onAppear {
                    NSApplication.shared.activate(ignoringOtherApps: true)
                }
                .sheet(isPresented: $showUsageStats) {
                    UsageStatsView(profileName: viewModel.profileName)
                        .environmentObject(languageManager)
                        .environment(\.locale, languageManager.locale)
                }
                .sheet(isPresented: $showMigrationWizard) {
                    MigrationWizardView(
                        profiles: viewModel.profiles,
                        settings: migrationSettings
                    )
                    .environmentObject(languageManager)
                    .environment(\.locale, languageManager.locale)
                }
                .sheet(isPresented: $showMigrationSettings) {
                    MigrationSettingsView(settings: migrationSettings)
                        .environmentObject(languageManager)
                        .environment(\.locale, languageManager.locale)
                }
                .sheet(isPresented: $showUploadParameters) {
                    UploadParametersView(profileName: viewModel.profileName)
                        .environmentObject(languageManager)
                        .environment(\.locale, languageManager.locale)
                }
        }
        .commands {
            CommandMenu(languageManager.t("menu.migration")) {
                Button(languageManager.t("menu.dataMigration")) {
                    showMigrationWizard = true
                }
                .keyboardShortcut("m", modifiers: [.command, .option])

                Button(languageManager.t("menu.migrationSettings")) {
                    showMigrationSettings = true
                }
            }
            CommandGroup(after: .textEditing) {
                Button(languageManager.t("menu.resetLayout")) {
                    NotificationCenter.default.post(name: .uiResetLayout, object: nil)
                }
                .keyboardShortcut("0", modifiers: [.command, .option])
            }
            CommandGroup(after: .textEditing) {
                Button(languageManager.t("menu.uploadParameters")) {
                    showUploadParameters = true
                }
            }
            CommandGroup(after: .textEditing) {
                Menu(languageManager.t("menu.presignExpiry")) {
                    Button(languageManager.t("menu.presign1h")) { setPresignHours(1) }
                    Button(languageManager.t("menu.presign4h")) { setPresignHours(4) }
                    Button(languageManager.t("menu.presign24h")) { setPresignHours(24) }
                    Button(languageManager.t("menu.presign7d")) { setPresignHours(168) }
                }
            }
            CommandMenu(languageManager.t("menu.metrics")) {
                Button(languageManager.t("menu.requestsMetrics")) {
                    showUsageStats = true
                }
                .keyboardShortcut("m", modifiers: [.command, .shift])

                Button(languageManager.t("menu.clearUsageMetrics")) {
                    let profileName = viewModel.profileName.trimmingCharacters(in: .whitespacesAndNewlines)
                    let displayName = profileName.isEmpty ? languageManager.t("general.default") : profileName
                    let alert = NSAlert()
                    alert.messageText = languageManager.t("alert.clearMetricsTitle")
                    alert.informativeText = String(format: languageManager.t("alert.clearMetricsBody"), displayName)
                    alert.addButton(withTitle: languageManager.t("button.clear"))
                    alert.addButton(withTitle: languageManager.t("button.cancel"))
                    let response = alert.runModal()
                    if response == .alertFirstButtonReturn {
                        Task { await MetricsRecorder.shared.clearProfile(profileName: displayName) }
                    }
                }
            }
            CommandMenu(languageManager.t("menu.language")) {
                ForEach(languageManager.options) { option in
                    Button(languageManager.displayName(for: option.code)) {
                        languageManager.language = option.code
                    }
                }
            }
            CommandGroup(after: .help) {
                Button(languageManager.t("menu.contact")) {
                    let alert = NSAlert()
                    alert.messageText = languageManager.t("menu.contact")
                    alert.informativeText = "yangqi.kou@gmail.com"
                    alert.addButton(withTitle: languageManager.t("button.copy"))
                    alert.addButton(withTitle: languageManager.t("button.close"))
                    let response = alert.runModal()
                    if response == .alertFirstButtonReturn {
                        NSPasteboard.general.clearContents()
                        NSPasteboard.general.setString("yangqi.kou@gmail.com", forType: .string)
                    }
                }
                Button(languageManager.t("menu.versionInfo")) {
                    let alert = NSAlert()
                    alert.messageText = languageManager.t("menu.versionInfo")
                    alert.informativeText = String(format: languageManager.t("menu.versionInfoBody"), appVersion)
                    alert.runModal()
                }
            }
        }
    }

    private func setPresignHours(_ hours: Int) {
        presignExpiryHours = min(max(hours, 1), 168)
    }
}
