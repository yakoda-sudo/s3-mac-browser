import SwiftUI
import AppKit

struct ConnectionView: View {
    @StateObject private var viewModel = ConnectionViewModel()
    @State private var searchText = ""
    @State private var dropHint = "Drop files here to simulate upload"
    @State private var droppedFiles: [String] = []
    @State private var presignedAlert: AlertPayload?
    @State private var accessKeyField = ""
    @AppStorage("presignExpiryHours") private var presignExpiryHours: Int = 4
    @State private var configSplitRatio: CGFloat = 0.75
    @State private var contentSplitRatio: CGFloat = 0.75
    @State private var selection = Set<S3Object.ID>()
    @State private var lastSelectedID: S3Object.ID?

    var body: some View {
        HSplitView {
            configPanel
                .overlay(alignment: .trailing) {
                    Rectangle()
                        .fill(Color(NSColor.separatorColor))
                        .frame(width: 1)
                }
            content
                .overlay(alignment: .leading) {
                    Rectangle()
                        .fill(Color(NSColor.separatorColor))
                        .frame(width: 1)
                }
        }
        .frame(minWidth: 1100, minHeight: 700)
        .alert(item: $presignedAlert) { payload in
            Alert(
                title: Text(payload.title),
                message: Text(payload.message),
                primaryButton: .default(Text("Copy")) {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(payload.message, forType: .string)
                },
                secondaryButton: .cancel()
            )
        }
        .onAppear {
            accessKeyField = viewModel.accessKey
        }
        .onChange(of: viewModel.accessKey) { value in
            accessKeyField = value
        }
    }

    private var configPanel: some View {
        GeometryReader { proxy in
            VSplitView {
                configFormSection
                    .frame(height: max(200, proxy.size.height * configSplitRatio))
                debugSection
                    .frame(height: max(140, proxy.size.height * (1 - configSplitRatio)))
                    .overlay(alignment: .top) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(height: 1)
                    }
            }
            .onChange(of: viewModel.selectedProfile) { profile in
                guard let profile else { return }
                viewModel.loadProfile(profile)
            }
            .onAppear {
                configSplitRatio = 0.75
            }
        }
        .frame(minWidth: 320)
    }

    private var configFormSection: some View {
        VStack(spacing: 12) {
            HStack {
                Text("s3-mac-browser")
                    .font(.title2)
                Spacer()
            }
            Text(viewModel.statusMessage)
                .foregroundColor(viewModel.lastStatusCode == nil ? .secondary : .primary)
                .frame(maxWidth: .infinity, alignment: .leading)

            Form {
                Section("Profiles") {
                    TextField("Profile Name", text: $viewModel.profileName)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                    Picker("Saved Profiles", selection: $viewModel.selectedProfile) {
                        Text("None").tag(ConnectionProfile?.none)
                        ForEach(viewModel.profiles) { profile in
                            Text(profile.name).tag(Optional(profile))
                        }
                    }
                }

                Section("Connection") {
                    TextField("Endpoint URL", text: $viewModel.endpointURL)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                    TextField("Region", text: $viewModel.region)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                }

                Section("Credentials") {
                    TextField("Access Key", text: $accessKeyField)
                        .onChange(of: accessKeyField) { value in
                            viewModel.accessKey = value
                        }
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                    SecureField("Secret Key", text: $viewModel.secretKey)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                }

                Section("Security") {
                    Toggle("Ignore SSL Verification (Local Only)", isOn: $viewModel.insecureSSL)
                        .help("Use only with local MinIO/LocalStack testing")
                }
            }
            .formStyle(.grouped)

            HStack {
                Button("Save Profile") {
                    viewModel.saveProfile()
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button("Connect") {
                    viewModel.testConnection()
                }
                .disabled(viewModel.isBusy)
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .tint(.blue)

                if let status = viewModel.lastStatusCode {
                    Text("HTTP \(status)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }

    private var debugSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Debug Response")
                .font(.caption)
                .foregroundColor(.secondary)
            ScrollView {
                Text(viewModel.debugText.isEmpty ? "(empty)" : viewModel.debugText)
                    .font(.caption)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var content: some View {
        VStack(spacing: 0) {
            breadcrumbBar
            Divider()
            GeometryReader { proxy in
                VSplitView {
                    objectList
                        .frame(height: max(200, proxy.size.height * contentSplitRatio))
                        .overlay(alignment: .bottom) {
                            Rectangle()
                                .fill(Color(NSColor.separatorColor))
                                .frame(height: 1)
                        }
                    detailsPanel
                        .frame(height: max(140, proxy.size.height * (1 - contentSplitRatio)))
                        .overlay(alignment: .top) {
                            Rectangle()
                                .fill(Color(NSColor.separatorColor))
                                .frame(height: 1)
                        }
                }
                .onAppear {
                    contentSplitRatio = 0.75
                }
            }
            Divider()
            uploadArea
        }
    }

    private var breadcrumbBar: some View {
        HStack(spacing: 8) {
            ForEach(Array(viewModel.breadcrumb.enumerated()), id: \.offset) { index, item in
                Button(item) {
                    viewModel.openBreadcrumb(at: index)
                }
                if index < viewModel.breadcrumb.count - 1 {
                    Text(">")
                        .foregroundColor(.secondary)
                }
            }
            Spacer()
            Button {
                viewModel.navigateBack()
            } label: {
                Image(systemName: "chevron.backward")
            }
            .disabled(viewModel.currentBucket == nil && viewModel.breadcrumb.count <= 1)
        }
        .padding(10)
    }

    private var objectList: some View {
        List(filteredObjects, selection: $selection) { object in
            HStack {
                Image(systemName: iconName(for: object))
                VStack(alignment: .leading) {
                    Text(displayKey(for: object))
                    Text("\(object.sizeBytes) bytes")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                Spacer()
                Text(object.lastModified.formatted(date: .abbreviated, time: .shortened))
                    .foregroundColor(.secondary)
            }
            .contentShape(Rectangle())
            .tag(object.id)
            .onTapGesture {
                handleSingleClick(object)
            }
            .onTapGesture(count: 2) {
                viewModel.openObject(object)
            }
            .contextMenu {
                Button("Presigned URL (\(clampedPresignHours) hours)") {
                    if let url = viewModel.presignedURL(for: object, expiresHours: clampedPresignHours) {
                        presignedAlert = AlertPayload(title: "Presigned URL", message: url)
                    } else {
                        presignedAlert = AlertPayload(title: "Presigned URL", message: "Select a file object inside a bucket.")
                    }
                }
                Button("Delete") {
                    viewModel.deleteObjects(selectedTargets(for: object))
                }
            }
        }
        .searchable(text: $searchText)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Button {
                    viewModel.refreshCurrentView()
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
                .help("Refresh")
            }
        }
        .onChange(of: selection) { _ in
            let selected = selectedTargets()
            if selected.count == 1, let first = selected.first {
                viewModel.selectObject(first)
            } else {
                viewModel.selectedObject = nil
                viewModel.selectedObjectInfo = nil
            }
        }
    }

    private var detailsPanel: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Object Properties")
                .font(.headline)
            if let info = viewModel.selectedObjectInfo ?? viewModel.selectedObject {
                PropertyRow(title: "Key", value: info.key)
                PropertyRow(title: "Content-Type", value: info.contentType)
                PropertyRow(title: "Size", value: "\(info.sizeBytes) bytes")
                PropertyRow(title: "Last Modified", value: info.lastModified.formatted(date: .abbreviated, time: .standard))
                PropertyRow(title: "ETag", value: info.eTag)
            } else {
                Text("Select an object to see details.")
                    .foregroundColor(.secondary)
            }
            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var uploadArea: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(dropHint)
                .font(.caption)
                .foregroundColor(.secondary)
            if !droppedFiles.isEmpty {
                Text("Queued: \(droppedFiles.joined(separator: ", "))")
                    .font(.caption)
            }
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(NSColor.windowBackgroundColor))
        .onDrop(of: [.fileURL], isTargeted: nil) { providers in
            droppedFiles = []
            var urls: [URL] = []
            let group = DispatchGroup()
            for provider in providers {
                group.enter()
                provider.loadItem(forTypeIdentifier: "public.file-url", options: nil) { item, _ in
                    guard let data = item as? Data,
                          let url = URL(dataRepresentation: data, relativeTo: nil) else {
                        DispatchQueue.main.async { group.leave() }
                        return
                    }
                    let name = url.lastPathComponent
                    DispatchQueue.main.async {
                        droppedFiles.append(name)
                        urls.append(url)
                        group.leave()
                    }
                }
            }
            group.notify(queue: .main) {
                if !urls.isEmpty {
                    viewModel.uploadFiles(urls)
                }
            }
            return true
        }
    }

    private var filteredObjects: [S3Object] {
        guard !searchText.isEmpty else { return viewModel.objects }
        return viewModel.objects.filter { $0.key.localizedCaseInsensitiveContains(searchText) }
    }

    private var clampedPresignHours: Int {
        min(max(presignExpiryHours, 1), 168)
    }


    private func iconName(for object: S3Object) -> String {
        if object.contentType == "bucket" {
            return "tray.full"
        }
        if object.key.hasSuffix("/") {
            return "folder"
        }
        return "doc"
    }

    private func displayKey(for object: S3Object) -> String {
        let prefix = viewModel.currentPrefix
        if !prefix.isEmpty, object.key.hasPrefix(prefix) {
            return String(object.key.dropFirst(prefix.count))
        }
        return object.key
    }

    private func selectedTargets(for object: S3Object) -> [S3Object] {
        if selection.isEmpty {
            return [object]
        }
        let selectedSet = Set(selection)
        return viewModel.objects.filter { selectedSet.contains($0.id) }
    }

    private func selectedTargets() -> [S3Object] {
        if selection.isEmpty {
            return []
        }
        let selectedSet = Set(selection)
        return viewModel.objects.filter { selectedSet.contains($0.id) }
    }

    private func handleSingleClick(_ object: S3Object) {
        let flags = NSEvent.modifierFlags.intersection(.deviceIndependentFlagsMask)
        let isShift = flags.contains(.shift)
        let isToggle = flags.contains(.command) || flags.contains(.control)

        if isShift, let anchor = lastSelectedID,
           let rangeIDs = selectionRange(from: anchor, to: object.id) {
            selection = Set(rangeIDs)
        } else if isToggle {
            if selection.contains(object.id) {
                selection.remove(object.id)
            } else {
                selection.insert(object.id)
            }
            lastSelectedID = object.id
        } else {
            selection = [object.id]
            lastSelectedID = object.id
        }

        let selected = selectedTargets()
        if selected.count == 1, let first = selected.first {
            viewModel.selectObject(first)
        } else {
            viewModel.selectedObject = nil
            viewModel.selectedObjectInfo = nil
        }
    }

    private func selectionRange(from start: S3Object.ID, to end: S3Object.ID) -> [S3Object.ID]? {
        let ids = filteredObjects.map { $0.id }
        guard let startIndex = ids.firstIndex(of: start),
              let endIndex = ids.firstIndex(of: end) else {
            return nil
        }
        if startIndex <= endIndex {
            return Array(ids[startIndex...endIndex])
        } else {
            return Array(ids[endIndex...startIndex])
        }
    }
}

private struct PropertyRow: View {
    let title: String
    let value: String

    var body: some View {
        HStack(alignment: .firstTextBaseline) {
            Text(title)
                .foregroundColor(.secondary)
                .frame(width: 120, alignment: .leading)
            Text(value.isEmpty ? "-" : value)
                .textSelection(.enabled)
            Spacer()
        }
    }
}

private struct AlertPayload: Identifiable {
    let id = UUID()
    let title: String
    let message: String
}
