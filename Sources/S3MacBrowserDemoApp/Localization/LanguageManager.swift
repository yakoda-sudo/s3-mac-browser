import Foundation
import SwiftUI

final class LanguageManager: ObservableObject {
    @AppStorage("ui.language") var language: String = "en" {
        didSet { objectWillChange.send() }
    }

    var locale: Locale {
        Locale(identifier: language)
    }

    func t(_ key: String) -> String {
        localizedString(key)
    }

    func localizedString(_ key: String) -> String {
        bundle().localizedString(forKey: key, value: key, table: nil)
    }

    func bundle() -> Bundle {
        let normalized = language.isEmpty ? "en" : language
        for candidate in bundleCandidates() {
            if let path = candidate.path(forResource: normalized, ofType: "lproj"),
               let bundle = Bundle(path: path) {
                return bundle
            }
        }
        for candidate in bundleCandidates() {
            if let path = candidate.path(forResource: "en", ofType: "lproj"),
               let bundle = Bundle(path: path) {
                return bundle
            }
        }
        return Bundle.main
    }

    func displayName(for code: String) -> String {
        for candidate in bundleCandidates() {
            if let path = candidate.path(forResource: code, ofType: "lproj"),
               let bundle = Bundle(path: path) {
                return bundle.localizedString(forKey: "lang.\(code)", value: "lang.\(code)", table: nil)
            }
        }
        return localizedString("lang.\(code)")
    }

    var options: [LanguageOption] {
        [
            .init(code: "en", key: "lang.en"),
            .init(code: "ar", key: "lang.ar"),
            .init(code: "da", key: "lang.da"),
            .init(code: "nl", key: "lang.nl"),
            .init(code: "fi", key: "lang.fi"),
            .init(code: "fr", key: "lang.fr"),
            .init(code: "de", key: "lang.de"),
            .init(code: "el", key: "lang.el"),
            .init(code: "he", key: "lang.he"),
            .init(code: "it", key: "lang.it"),
            .init(code: "ja", key: "lang.ja"),
            .init(code: "zh-Hant", key: "lang.zh-Hant"),
            .init(code: "zh-Hans", key: "lang.zh-Hans"),
            .init(code: "ko", key: "lang.ko"),
            .init(code: "pl", key: "lang.pl"),
            .init(code: "ru", key: "lang.ru"),
            .init(code: "es", key: "lang.es"),
            .init(code: "th", key: "lang.th")
        ]
    }

    private func bundleCandidates() -> [Bundle] {
        var bundles: [Bundle] = [Bundle.module, Bundle.main]
        if let directURL = Bundle.main.url(forResource: "s3-mac-browser_S3MacBrowserDemoApp", withExtension: "bundle"),
           let bundle = Bundle(url: directURL) {
            bundles.append(bundle)
        } else if let resourcesURL = Bundle.main.resourceURL,
                  let contents = try? FileManager.default.contentsOfDirectory(at: resourcesURL, includingPropertiesForKeys: nil) {
            for url in contents where url.pathExtension == "bundle" && url.lastPathComponent.contains("S3MacBrowserDemoApp") {
                if let bundle = Bundle(url: url) {
                    bundles.append(bundle)
                }
            }
        }
        return bundles
    }
}

struct LanguageOption: Identifiable {
    let code: String
    let key: String
    var id: String { code }
}
