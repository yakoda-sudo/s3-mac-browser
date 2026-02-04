# s3-mac-browser
<img width="256" height="256" alt="image" src="https://github.com/user-attachments/assets/767cc93e-c83c-49cb-8660-dc4557f2fc2b" />



<!-- Badges: replace links once GitHub repo is created -->
[![macOS](https://img.shields.io/badge/macOS-13%2B-000000?logo=apple&logoColor=white)](#requirements)
[![Swift](https://img.shields.io/badge/Swift-6.0-F05138?logo=swift&logoColor=white)](#requirements)

A lightweight  macOS SwiftUI professinal S3 browser that supports local endpoints (MinIO/LocalStack/Flashblade object /DellEMC object ...), AWS-compatible services (e.g. wasabi/GCP object store), Azure Storage Account and free all-platform Data migration. Also support Microsoft Storage Account container blob store
<img width="1454" height="879" alt="image" src="https://github.com/user-attachments/assets/18ef6dc1-64e6-4448-af2a-914a8d2977fd" />

API call metrics/Debug options

<img width="520" height="368" alt="image" src="https://github.com/user-attachments/assets/95336b05-13fd-496c-9e4f-8b7640dd32c0" />



## Features

- Connect to S3-compatible endpoints (AWS/MinIO/Wasabi/PureStorage(local)/DELLEMC)
- Azure Blob Storage via SAS URL (account or container SAS)
- List buckets and browse prefixes ,toggle display versions/delete markers/ soft deletes
- Object metadata (HEAD) in a properties panel.
- Adding client side request metrics display (72hrs) for the s3 account . Classify by request type .  You can copy the metrics for calculating billing purpose :-)
- Presigned URL generation, storage account blob sharing
- Upload via drag & drop ,right click objects to download
- Delete single or multiple objects/folders (press shift for multi-selection) ,can also restore object remove delete markers
- Easy and strate-forward UI to configure any object storage types. display history of full debugging API calls
- Free Cross-Vendors/Protocol Replication Job between AWS S3/ S3-compatible object / Azure Storage Account
 


## Requirements

- macOS 13+ (Ventura)
- Swift 6 toolchain
- Network access to your endpoint / or azure storage account SAS


## Usage

1. Enter endpoint URL (e.g. `http://localhost:9000` for MinIO 'http://IP_address'for your Local S3-compatible storage e.g. Pure Flashblade or `https://s3.us-east-1.wasabisys.com` for Wasabi) and SAS URL for azure storage account.
2. Set region and access/secret keys. (no need for azure SAS URL which contains token )
3. Click **Connect** to list buckets.
4. Double‑click a bucket or folder to navigate.
5. Click an object to view metadata in the **Object Properties** panel.
6. Right‑click an object to get a presigned URL or delete / download. (presigned URL timeout is configuratble in Edit menu)
7. Drag & drop files into the bottom area to upload to the current prefix.
8. Data Migration menu and wizard creating data migration job on any local object store / aws s3/ s3 compatible /Microsoft azure storage account 

Please note , this app is not signed, you need to allow it from macos ,running command to trust:
xattr -dr com.apple.quarantine "/Applications/s3-mac-browser.app" 

## Multi‑selection

- **Command‑click** (or **Control‑click**) to toggle selection of multiple objects.
- **Shift‑click** to select a range between the last selected item and the clicked item.
- Right‑click any selected item and choose **Delete** to delete all selected items.

## Data-Migration
- Support Migration for all types of profiels (AWS S3/ S3 compatible object/ Local object store/ Azure Storage account )
- Easy strate-forward wizard will help to configure the source and distinations for migrate job
- All metrics during migration will also be saved in metrics
<img width="700" height="1534" alt="data_migration" src="https://github.com/user-attachments/assets/6c141d5c-50b5-42c2-a8ad-dd4fed1c8ba6" />



## Notes

- Presigned URL expiry is configurable via **Edit → Presigned URL Expiry**.
- For HTTPS endpoints, keep **Ignore SSL Verification** OFF unless you are testing a local endpoint with self‑signed certs.
- For DataMigration, the parameters can be configured in the Migration settings menu, need to configured according to the local memory/internet bandwidth etc

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

