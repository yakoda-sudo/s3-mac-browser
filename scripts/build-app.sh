#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="s3-mac-browser"
ICON_SRC="$ROOT_DIR/newicon/icon2-1024x1024.png"
ICONSET_DIR="$ROOT_DIR/AppIcon.iconset"
ICNS_OUT="$ROOT_DIR/${APP_NAME}.icns"
APP_DIR="$ROOT_DIR/${APP_NAME}.app"

if [[ ! -f "$ICON_SRC" ]]; then
  echo "Missing icon: $ICON_SRC" >&2
  exit 1
fi

cd "$ROOT_DIR"
swift build -c release

rm -rf "$ICONSET_DIR"
mkdir -p "$ICONSET_DIR"
for size in 16 32 64 128 256 512 1024; do
  sips -z $size $size "$ICON_SRC" --out "$ICONSET_DIR/icon_${size}x${size}.png" >/dev/null
 done
cp "$ICONSET_DIR/icon_32x32.png" "$ICONSET_DIR/icon_16x16@2x.png"
cp "$ICONSET_DIR/icon_64x64.png" "$ICONSET_DIR/icon_32x32@2x.png"
cp "$ICONSET_DIR/icon_256x256.png" "$ICONSET_DIR/icon_128x128@2x.png"
cp "$ICONSET_DIR/icon_512x512.png" "$ICONSET_DIR/icon_256x256@2x.png"
cp "$ICONSET_DIR/icon_1024x1024.png" "$ICONSET_DIR/icon_512x512@2x.png"

iconutil -c icns "$ICONSET_DIR" -o "$ICNS_OUT"

rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"
cp "$ROOT_DIR/.build/release/${APP_NAME}" "$APP_DIR/Contents/MacOS/"
cp "$ICNS_OUT" "$APP_DIR/Contents/Resources/"

cat > "$APP_DIR/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>s3-mac-browser</string>
  <key>CFBundleExecutable</key><string>s3-mac-browser</string>
  <key>CFBundleIdentifier</key><string>com.yangkikou.s3macbrowser</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>0.7.0</string>
  <key>CFBundleVersion</key><string>7</string>
  <key>LSMinimumSystemVersion</key><string>13.0</string>
  <key>CFBundleIconFile</key><string>s3-mac-browser.icns</string>
</dict>
</plist>
PLIST

echo "Built: $APP_DIR"
