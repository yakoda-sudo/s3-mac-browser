import SwiftUI
import AppKit

struct ConnectionView: View {
    @ObservedObject var viewModel: ConnectionViewModel
    @State private var searchText = ""
    @State private var dropHint = "Drop files here to simulate upload"
    @State private var droppedFiles: [String] = []
    @State private var presignedAlert: AlertPayload?
    @State private var showUndeleteConfirm = false
    @State private var pendingUndeleteTargets: [S3Object] = []
    @State private var pathInput = ""
    @State private var accessKeyField = ""
    @AppStorage("presignExpiryHours") private var presignExpiryHours: Int = 4
    @AppStorage("ui.mainSplitRatio") private var mainSplitRatio: Double = 0.45
    @AppStorage("ui.configSplitRatio") private var configSplitRatio: Double = 0.75
    @AppStorage("ui.contentSplitRatio") private var contentSplitRatio: Double = 0.75
    @AppStorage("ui.contentVerticalRatio") private var contentVerticalRatio: Double = 0.625
    @AppStorage("ui.didBoostTransferHeight") private var didBoostTransferHeight: Bool = false
    @AppStorage("ui.transferSplitRatio") private var transferSplitRatio: Double = 0.6
    @State private var selection = Set<S3Object.ID>()
    @State private var lastSelectedID: S3Object.ID?

    init(viewModel: ConnectionViewModel = ConnectionViewModel()) {
        self.viewModel = viewModel
    }

    var body: some View {
        GeometryReader { proxy in
            HSplitView {
                configPanel
                    .frame(width: max(320, proxy.size.width * CGFloat(mainSplitRatio)))
                    .background(GeometryReader { inner in
                        Color.clear
                            .onAppear { updateMainSplit(leftWidth: inner.size.width, totalWidth: proxy.size.width) }
                            .onChange(of: inner.size.width) { newValue in
                                updateMainSplit(leftWidth: newValue, totalWidth: proxy.size.width)
                            }
                    })
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
        .alert("Undelete selected blobs?", isPresented: $showUndeleteConfirm) {
            Button("Cancel", role: .cancel) {}
            Button("Undelete") {
                let targets = pendingUndeleteTargets
                pendingUndeleteTargets = []
                viewModel.undeleteObjects(targets)
            }
        } message: {
            Text("This will restore the selected soft-deleted blobs in the current container.")
        }
        .onAppear {
            accessKeyField = viewModel.accessKey
            if !didBoostTransferHeight {
                let transferRatio = 1.0 - contentVerticalRatio
                let boostedTransfer = min(max(transferRatio * 1.5, 0.2), 0.8)
                contentVerticalRatio = 1.0 - boostedTransfer
                didBoostTransferHeight = true
            }
        }
        .onChange(of: viewModel.accessKey) { value in
            accessKeyField = value
        }
    }

    private var configPanel: some View {
        GeometryReader { proxy in
            VSplitView {
                configFormSection
                    .frame(height: max(200, proxy.size.height * CGFloat(configSplitRatio)))
                    .background(GeometryReader { inner in
                        Color.clear
                            .onAppear { updateConfigSplit(topHeight: inner.size.height, totalHeight: proxy.size.height) }
                            .onChange(of: inner.size.height) { newValue in
                                updateConfigSplit(topHeight: newValue, totalHeight: proxy.size.height)
                            }
                    })
                debugSection
                    .frame(height: max(140, proxy.size.height * CGFloat(1 - configSplitRatio)))
            }
            .onChange(of: viewModel.selectedProfile) { profile in
                guard let profile else { return }
                viewModel.loadProfile(profile)
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
                    HStack(spacing: 8) {
                        TextField("Profile Name", text: $viewModel.profileName)
                            .textFieldStyle(.roundedBorder)
                            .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                        Button {
                            let profileName = viewModel.profileName.trimmingCharacters(in: .whitespacesAndNewlines)
                            let displayName = profileName.isEmpty ? "default" : profileName
                            let alert = NSAlert()
                            alert.messageText = "Delete profile?"
                            alert.informativeText = "This will permanently delete the profile \"\(displayName)\"."
                            alert.addButton(withTitle: "Delete")
                            alert.addButton(withTitle: "Cancel")
                            let response = alert.runModal()
                            if response == .alertFirstButtonReturn {
                                viewModel.deleteCurrentProfile()
                            }
                        } label: {
                            Image(systemName: "trash")
                        }
                        .help("Delete current profile")
                        .disabled(!viewModel.profiles.contains(where: { $0.name == viewModel.profileName.trimmingCharacters(in: .whitespacesAndNewlines) }))
                    }
                    Picker("Saved Profiles", selection: $viewModel.selectedProfile) {
                        Text("None").tag(ConnectionProfile?.none)
                        ForEach(viewModel.profiles) { profile in
                            Text(profile.name).tag(Optional(profile))
                        }
                    }
                }

                Section("Connection") {
                    TextField("Endpoint URL / SAS", text: $viewModel.endpointURL)
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
            HStack {
                Text("Debug Response")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Spacer()
                if !viewModel.debugHistory.isEmpty {
                    Picker("History", selection: $viewModel.selectedDebugIndex) {
                        ForEach(0..<viewModel.debugHistory.count, id: \.self) { index in
                            Text(viewModel.debugHistory[index].title).tag(index)
                        }
                    }
                    .pickerStyle(.menu)
                    .font(.caption)
                }
            }
            ScrollView {
                Text(currentDebugText())
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
                    objectDetailsContainer
                        .frame(height: max(200, proxy.size.height * CGFloat(contentVerticalRatio)))
                        .background(GeometryReader { inner in
                            Color.clear
                                .onAppear { updateContentVerticalSplit(topHeight: inner.size.height, totalHeight: proxy.size.height) }
                                .onChange(of: inner.size.height) { newValue in
                                    updateContentVerticalSplit(topHeight: newValue, totalHeight: proxy.size.height)
                                }
                        })
                    transferPanel
                        .frame(height: max(180, proxy.size.height * CGFloat(1 - contentVerticalRatio)))
                }
            }
        }
    }

    private var objectDetailsContainer: some View {
        GeometryReader { proxy in
            VSplitView {
                objectList
                    .frame(height: max(200, proxy.size.height * CGFloat(contentSplitRatio)))
                    .overlay(alignment: .bottom) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(height: 1)
                    }
                    .background(GeometryReader { inner in
                        Color.clear
                            .onAppear { updateContentSplit(topHeight: inner.size.height, totalHeight: proxy.size.height) }
                            .onChange(of: inner.size.height) { newValue in
                                updateContentSplit(topHeight: newValue, totalHeight: proxy.size.height)
                            }
                    })
                detailsPanel
                    .frame(height: max(140, proxy.size.height * CGFloat(1 - contentSplitRatio)))
                    .overlay(alignment: .top) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(height: 1)
                    }
            }
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
            TextField("/bucket/path", text: $pathInput)
                .textFieldStyle(.roundedBorder)
                .frame(width: 260)
                .onSubmit {
                    viewModel.openPathInput(pathInput)
                    pathInput = ""
                }
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
        List(displayRows, selection: $selection) { row in
            let object = row.object
            HStack {
                Image(systemName: iconName(for: object))
                VStack(alignment: .leading) {
                    Text(displayKey(for: object))
                        .foregroundColor(row.isChild ? .secondary : .primary)
                        .font(row.isChild ? .subheadline : .body)
                    Text("\(object.sizeBytes) bytes")
                        .font(row.isChild ? .caption2 : .caption)
                        .foregroundColor(.secondary)
                    if object.isDeleteMarker || object.isDeleted || object.isVersioned {
                        Text(versionBadgeText(for: object))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                            .italic()
                    }
                }
                Spacer()
                Text(object.lastModified.formatted(date: .abbreviated, time: .shortened))
                    .foregroundColor(.secondary)
                    .font(row.isChild ? .caption2 : .caption)
            }
            .padding(.leading, row.indent)
            .contentShape(Rectangle())
            .tag(object.id)
            .opacity(rowOpacity(for: row))
            .onTapGesture {
                handleSingleClick(object)
            }
            .onTapGesture(count: 2) {
                viewModel.openObject(object)
            }
            .contextMenu {
                if viewModel.provider == .s3 && object.isVersioned {
                    if object.isDeleteMarker {
                        Button("Remove Delete Marker") {
                            viewModel.deleteObjects(selectedTargets(for: object))
                        }
                    } else {
                        Button("Download Version") {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                        Button("Delete Version") {
                            viewModel.deleteObjects(selectedTargets(for: object))
                        }
                    }
                } else if viewModel.provider == .azureBlob && (object.isVersioned || object.isDeleted) {
                    if object.isDeleted {
                        Button("Undelete") {
                            pendingUndeleteTargets = selectedTargets(for: object)
                            showUndeleteConfirm = true
                        }
                    }
                    if let versionId = object.versionId, !versionId.isEmpty {
                        Button("Download Version") {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                    } else if !object.isDeleted {
                        Button("Download") {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                    }
                    Button(object.isDeleted ? "Permanently Delete" : "Delete") {
                        viewModel.deleteObjects(selectedTargets(for: object))
                    }
                } else {
                    if viewModel.provider == .azureBlob {
                        Button("Share Link") {
                            if let url = viewModel.shareLink(for: object, expiresHours: clampedPresignHours) {
                                presignedAlert = AlertPayload(title: "Share Link", message: url)
                            } else {
                                presignedAlert = AlertPayload(title: "Share Link", message: "Select a file object inside a container.")
                            }
                        }
                    } else {
                        Button("Presigned URL (\(clampedPresignHours) hours)") {
                            if let url = viewModel.shareLink(for: object, expiresHours: clampedPresignHours) {
                                presignedAlert = AlertPayload(title: "Presigned URL", message: url)
                            } else {
                                presignedAlert = AlertPayload(title: "Presigned URL", message: "Select a file object inside a bucket.")
                            }
                        }
                    }
                    Button("Download") {
                        viewModel.downloadObjects(selectedTargets(for: object))
                    }
                    Button("Delete") {
                        viewModel.deleteObjects(selectedTargets(for: object))
                    }
                }
            }
        }
        .searchable(text: $searchText)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Button {
                    viewModel.setShowVersionsDeleted(!viewModel.showVersionsDeleted)
                } label: {
                    Image(systemName: viewModel.showVersionsDeleted ? "eye.fill" : "eye")
                }
                .help("Show Versions / Deleted Objects")
            }
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
            Text("Upload / Drop")
                .font(.caption)
                .foregroundColor(.secondary)
            Text(dropHint)
                .font(.caption)
                .foregroundColor(.secondary)
            if !droppedFiles.isEmpty {
                Text("Queued: \(droppedFiles.joined(separator: ", "))")
                    .font(.caption)
            }
            Spacer(minLength: 0)
        }
        .padding(12)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(Color(NSColor.controlBackgroundColor))
        .overlay(
            Rectangle()
                .stroke(Color(NSColor.separatorColor), lineWidth: 1)
        )
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

    private var transferPanel: some View {
        GeometryReader { proxy in
            HSplitView {
                // Left: drop/upload area
                uploadArea
                    .frame(width: max(200, proxy.size.width * CGFloat(transferSplitRatio)))
                    .background(GeometryReader { inner in
                        Color.clear
                            .onAppear { updateTransferSplit(leftWidth: inner.size.width, totalWidth: proxy.size.width) }
                            .onChange(of: inner.size.width) { newValue in
                                updateTransferSplit(leftWidth: newValue, totalWidth: proxy.size.width)
                            }
                    })
                    .overlay(alignment: .trailing) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(width: 1)
                    }
                // Right: transfer status + progress
                transferStatusPanel
                    .overlay(alignment: .leading) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(width: 1)
                    }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    private var transferStatusPanel: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 6) {
                Text("Transfer Status")
                    .font(.headline)
                Text(viewModel.transferStatus)
                    .font(.caption)
                    .foregroundColor(.secondary)
                ProgressView(value: viewModel.transferProgress)
                Text("\(Int(viewModel.transferProgress * 100))%")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Divider()
                ForEach(viewModel.recentTransfers) { item in
                    HStack(spacing: 8) {
                        Text(item.name)
                            .font(.caption)
                            .frame(width: 150, alignment: .leading)
                            .lineLimit(1)
                        ProgressView(value: item.progress)
                            .frame(maxWidth: .infinity)
                        Text("\(Int(item.progress * 100))%")
                            .font(.caption)
                            .frame(width: 40, alignment: .trailing)
                            .foregroundColor(.secondary)
                    }
                }
                Spacer()
            }
            .padding()
        }
        .scrollIndicators(.visible)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .overlay(
            Rectangle()
                .stroke(Color(NSColor.separatorColor), lineWidth: 1)
        )
    }

    private var filteredObjects: [S3Object] {
        guard !searchText.isEmpty else { return viewModel.objects }
        return viewModel.objects.filter { $0.key.localizedCaseInsensitiveContains(searchText) }
    }

    private var displayRows: [ObjectRowItem] {
        let source = filteredObjects
        guard viewModel.showVersionsDeleted else {
            return source.map { ObjectRowItem(object: $0, indent: 0, isChild: false) }
        }
        return groupedRows(from: source)
    }

    private var displayedObjects: [S3Object] {
        displayRows.map { $0.object }
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
        let ids = displayedObjects.map { $0.id }
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

    private func updateMainSplit(leftWidth: CGFloat, totalWidth: CGFloat) {
        guard totalWidth > 0 else { return }
        let ratio = min(max(Double(leftWidth / totalWidth), 0.2), 0.8)
        if ratio != mainSplitRatio {
            mainSplitRatio = ratio
        }
    }

    private func updateConfigSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != configSplitRatio {
            configSplitRatio = ratio
        }
    }

    private func updateContentVerticalSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != contentVerticalRatio {
            contentVerticalRatio = ratio
        }
    }

    private func updateContentSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != contentSplitRatio {
            contentSplitRatio = ratio
        }
    }

    private func updateTransferSplit(leftWidth: CGFloat, totalWidth: CGFloat) {
        guard totalWidth > 0 else { return }
        let ratio = min(max(Double(leftWidth / totalWidth), 0.2), 0.8)
        if ratio != transferSplitRatio {
            transferSplitRatio = ratio
        }
    }

    private func versionBadgeText(for object: S3Object) -> String {
        var labels: [String] = []
        if object.isDeleteMarker {
            labels.append("Delete Marker")
        } else if object.isDeleted {
            labels.append("Deleted")
        } else if object.isVersioned {
            labels.append("Version")
        }
        if object.isLatest {
            labels.append("Latest")
        }
        if let versionId = object.versionId, !versionId.isEmpty {
            labels.append("ID: \(versionId.prefix(8))")
        }
        return labels.joined(separator: " • ")
    }

    private func currentDebugText() -> String {
        if viewModel.debugHistory.isEmpty {
            return viewModel.debugText.isEmpty ? "(empty)" : viewModel.debugText
        }
        let index = min(max(viewModel.selectedDebugIndex, 0), viewModel.debugHistory.count - 1)
        let text = viewModel.debugHistory[index].text
        return text.isEmpty ? "(empty)" : text
    }

    private func rowOpacity(for row: ObjectRowItem) -> Double {
        var opacity = row.isChild ? 0.7 : 1.0
        if row.object.isDeleteMarker || row.object.isDeleted {
            opacity *= 0.75
        }
        return opacity
    }

    private func groupedRows(from objects: [S3Object]) -> [ObjectRowItem] {
        var versionGroups: [String: [S3Object]] = [:]
        for object in objects where object.isVersioned || object.isDeleteMarker || object.isDeleted {
            versionGroups[object.key, default: []].append(object)
        }

        let groupedKeys = Set(versionGroups.filter { $0.value.count > 1 }.map { $0.key })
        var topLevel: [S3Object] = []
        var childrenByKey: [String: [S3Object]] = [:]

        for object in objects {
            if groupedKeys.contains(object.key) {
                continue
            }
            topLevel.append(object)
        }

        for (key, group) in versionGroups where groupedKeys.contains(key) {
            let sorted = group.sorted { lhs, rhs in
                if lhs.isLatest != rhs.isLatest {
                    return lhs.isLatest && !rhs.isLatest
                }
                if lhs.lastModified != rhs.lastModified {
                    return lhs.lastModified > rhs.lastModified
                }
                return lhs.id.uuidString < rhs.id.uuidString
            }
            guard let parent = sorted.first else { continue }
            topLevel.append(parent)
            let children = Array(sorted.dropFirst()).sorted { $0.lastModified > $1.lastModified }
            childrenByKey[key] = children
        }

        let sortedTop = topLevel.sorted { $0.key < $1.key }
        var rows: [ObjectRowItem] = []
        for object in sortedTop {
            rows.append(ObjectRowItem(object: object, indent: 0, isChild: false))
            if let children = childrenByKey[object.key], !children.isEmpty {
                for child in children {
                    rows.append(ObjectRowItem(object: child, indent: 18, isChild: true))
                }
            }
        }
        return rows
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

private struct ObjectRowItem: Identifiable {
    let object: S3Object
    let indent: CGFloat
    let isChild: Bool

    var id: UUID { object.id }
}

private struct AlertPayload: Identifiable {
    let id = UUID()
    let title: String
    let message: String
}
