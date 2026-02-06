# Release Notes

## 0.9.2 — 2026-02-05
- File list now shows columns for Name, Size, Last Modified, Storage Class, plus Blob Type for Azure.
- Azure AccessTier maps to the Storage Class column.

## 0.9.0 — 2026-02-03

### Added
- Multi-language UI with in-app language selection and instant switching.
- Migration wizard bucket/container dropdowns with live listing per profile.

### Improved
- Data Migration menu UX with clearer source/target selection and discovery.

### Fixed
- Localization bundle loading in release builds.

## 0.7.0 — 2026-01-31

### Added
- One-time Data Migration wizard to copy objects across S3, S3-compatible, and Azure Blob backends.
- Global migration settings with conservative defaults (concurrency, bandwidth, buffer size).
- Streaming migration engine with retries and checkpointing for stability.

### Improved
- Optimized cross vendor-protocol migration with no restriction on direction (any supported source/target).

## 0.6.0 — 2026-01-31

### Added
- “Show Versions/Deleted” toggle to include S3 versions/delete markers and Azure versions/soft-deleted blobs.
- Version-aware context menu actions (download version, delete version/remove delete marker).
- Azure soft-delete support with “Undelete” and permanent delete actions.

### Improved
- Versioned entries display as a simple tree with the latest object as parent and older versions indented.
- Version downloads append `-v<id>` to filenames to avoid overwriting.

## 0.4.0 — 2026-01-30

### Added
- Azure Blob Storage browsing via SAS URL (account or container).
- Azure blob upload/download/delete and share link support.
- Azure request metrics (LIST/GET/PUT/DELETE/COPY/HEAD) tracked per profile.

## 0.3 — 2026-01-30

### Added
- Per-profile S3 usage metrics (hourly NDJSON, 30-day retention).
- Requests Metrics menu and 72-hour usage summary view.

## 0.2 — 2026-01-28

### Added
- Resizable split panes (left/right and vertical) with persisted ratios across launches.
- Drag-and-drop uploads plus per-file and overall transfer progress.
- Multi-select with modifier keys; context menu actions for download and delete.
- Object details panel showing metadata (size, content-type, last modified, ETag).
- Toolbar refresh button and search filtering for the current view.

### Improved
- Endpoint handling for local IPs and public S3-compatible domains.
- Debug response panel now supports full text selection/copy.
- Presigned URL generation uses full AWS Signature V4.
- UI divider lines and panel layout consistency for clearer separation.

### Fixed
- Access key display masking no longer alters stored credentials.
- Last modified parsing and display for object listings.
