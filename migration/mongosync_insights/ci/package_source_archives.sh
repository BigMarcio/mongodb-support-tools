#!/usr/bin/env bash
# Create source .tar.gz and .zip archives of migration/mongosync_insights only.
#
# Usage:
#   ./ci/package_source_archives.sh <version> [output-dir]
#
# Archives contain top-level mongosync_insights/ (not the full monorepo).
# Build artifacts and local venv/cache dirs are excluded.
#
# Examples:
#   ./ci/package_source_archives.sh 0.9.1.15
#   ./ci/package_source_archives.sh 0.9.1.15 /tmp/out
set -euo pipefail

usage() {
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

VERSION=$1
OUT_DIR=${2:-dist}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MI_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
MIGRATION_DIR=$(cd "${MI_DIR}/.." && pwd)
MI_NAME=$(basename "$MI_DIR")

if [[ ! -d "$MI_DIR" ]]; then
    echo "ERROR: source directory not found: ${MI_DIR}" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

BASENAME="mongosync-insights-${VERSION}-source"
TAR_PATH="${OUT_DIR}/${BASENAME}.tar.gz"
ZIP_PATH="${OUT_DIR}/${BASENAME}.zip"

TAR_EXCLUDES=(
    --exclude="${MI_NAME}/dist"
    --exclude="${MI_NAME}/build"
    --exclude="${MI_NAME}/.venv"
    --exclude="${MI_NAME}/.pytest_cache"
    --exclude="${MI_NAME}/__pycache__"
    --exclude="${MI_NAME}/*/__pycache__"
    --exclude="${MI_NAME}/*/*/__pycache__"
    --exclude="${MI_NAME}/*/*/*/__pycache__"
    --exclude="${MI_NAME}/*.pyc"
    --exclude="${MI_NAME}/*/*.pyc"
    --exclude="${MI_NAME}/*/*/*.pyc"
    --exclude="${MI_NAME}/.build_venv_*"
)

ZIP_EXCLUDES=(
    -x "${MI_NAME}/dist/*"
    -x "${MI_NAME}/build/*"
    -x "${MI_NAME}/.venv/*"
    -x "${MI_NAME}/.pytest_cache/*"
    -x "${MI_NAME}/__pycache__/*"
    -x "${MI_NAME}/*/__pycache__/*"
    -x "${MI_NAME}/*/*/__pycache__/*"
    -x "${MI_NAME}/*/*/*/__pycache__/*"
    -x "${MI_NAME}/*.pyc"
    -x "${MI_NAME}/*/*.pyc"
    -x "${MI_NAME}/*/*/*.pyc"
    -x "${MI_NAME}/.build_venv_*/*"
)

rm -f "$TAR_PATH" "$ZIP_PATH"

tar -C "$MIGRATION_DIR" -czf "$TAR_PATH" "${TAR_EXCLUDES[@]}" "$MI_NAME"
(
    cd "$MIGRATION_DIR"
    zip -qr "$ZIP_PATH" "$MI_NAME" "${ZIP_EXCLUDES[@]}"
)

echo "==> Created ${TAR_PATH}"
echo "==> Created ${ZIP_PATH}"
