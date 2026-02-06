#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="s3-mac-browser"
VERSION="0.9.2"
DIST_DIR="$ROOT_DIR/dist"
STAGING="$ROOT_DIR/.dmg-staging"
DMG_RW="$DIST_DIR/${APP_NAME}-${VERSION}-rw.dmg"
DMG_FINAL="$DIST_DIR/${APP_NAME}-${VERSION}.dmg"

if [[ "${1:-}" == "--clean-profiles" ]]; then
  defaults delete com.yangkikou.s3macbrowser s3macbrowser.profiles >/dev/null 2>&1 || true
  rm -rf "$HOME/Library/Application Support/${APP_NAME}" >/dev/null 2>&1 || true
  echo "Cleared local profiles and metrics."
fi

mkdir -p "$DIST_DIR"
rm -rf "$STAGING"
mkdir -p "$STAGING"

if [[ ! -d "$ROOT_DIR/${APP_NAME}.app" ]]; then
  echo "Missing app: $ROOT_DIR/${APP_NAME}.app" >&2
  exit 1
fi

cp -R "$ROOT_DIR/${APP_NAME}.app" "$STAGING/"
ln -s /Applications "$STAGING/Applications"

rm -f "$DMG_RW" "$DMG_FINAL"

hdiutil create -volname "$APP_NAME" -srcfolder "$STAGING" -ov -format UDRW "$DMG_RW" >/dev/null
hdiutil convert "$DMG_RW" -format UDZO -o "$DMG_FINAL" >/dev/null
rm -f "$DMG_RW"

echo "Built: $DMG_FINAL"
