# s3-mac-browser

<!-- Badges: replace links once GitHub repo is created -->
[![macOS](https://img.shields.io/badge/macOS-13%2B-000000?logo=apple&logoColor=white)](#requirements)
[![Swift](https://img.shields.io/badge/Swift-6.0-F05138?logo=swift&logoColor=white)](#requirements)

A lightweight  macOS SwiftUI professinal S3 browser that supports local endpoints (MinIO/LocalStack), AWS-compatible services, and basic object management.
<img width="1492" height="993" alt="image" src="https://github.com/user-attachments/assets/186d9b33-fee6-43e2-8a28-56c54b3017c3" />

## Features

- Connect to S3-compatible endpoints (AWS/MinIO/Wasabi/PureStorage(local)/DELLEMC)
- List buckets and browse prefixes
- Object metadata (HEAD) in a properties panel
- Presigned URL generation 
- Upload via drag & drop
- Delete single or multiple objects/folders (press shift for multi-selection)
- Search/filter in current view

## Requirements

- macOS 13+ (Ventura)
- Swift 6 toolchain
- Network access to your endpoint


## Usage

1. Enter endpoint URL (e.g. `http://localhost:9000` for MinIO 'http://IP_address'for your Local S3-compatible storage e.g. Pure Flashblade or `https://s3.us-east-1.wasabisys.com` for Wasabi).
2. Set region and access/secret keys.
3. Click **Connect** to list buckets.
4. Double‑click a bucket or folder to navigate.
5. Click an object to view metadata in the **Object Properties** panel.
6. Right‑click an object to get a presigned URL or delete. (presigned URL timeout is configuratble in Edit menu)
7. Drag & drop files into the bottom area to upload to the current prefix.

## Multi‑selection

- **Command‑click** (or **Control‑click**) to toggle selection of multiple objects.
- **Shift‑click** to select a range between the last selected item and the clicked item.
- Right‑click any selected item and choose **Delete** to delete all selected items.

## Notes

- Presigned URL expiry is configurable via **Edit → Presigned URL Expiry**.
- For HTTPS endpoints, keep **Ignore SSL Verification** OFF unless you are testing a local endpoint with self‑signed certs.

## Project Structure

```
S3MacBrowserDemo/
  Package.swift
  Sources/
    S3MacBrowserDemoApp/
      Models/
      Services/
      ViewModels/
      Views/
```

## License

Copyright © 2026. All rights reserved.

## Releases

When the GitHub repo is ready, add releases here:

1. Go to **Releases** → **Draft a new release**.
2. Tag version (e.g. `v0.2`) and title (e.g. `s3-mac-browser 0.2`).
3. Upload the `.app` bundle or zipped release artifacts.
4. Add release notes (features, fixes, known issues).

## Screenshots

Add screenshots here after upload. Example markdown:

```
![Main View](docs/screenshots/main.png)
![Object Details](docs/screenshots/details.png)
```
