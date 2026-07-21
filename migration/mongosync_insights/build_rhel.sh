#!/usr/bin/env bash
# =============================================================================
# build_rhel.sh — RPM for Red Hat Enterprise Linux, CentOS, Rocky, AlmaLinux
#
# Build on the same or older RHEL major version as your targets
# (e.g. build on RHEL 8 to support RHEL 8; a RHEL 9 build may not run on RHEL 8).
#
# Prerequisites:
#   sudo dnf install -y python3.11 python3.11-pip ruby rubygems rpm-build gcc
#   sudo gem install fpm
#
# Usage:
#   cd migration/mongosync_insights
#   ./build_rhel.sh
#   ./build_rhel.sh --el-major 8
#   ./build_rhel.sh --el-major 9
#
# Output:
#   dist/mongosync-insights-<version>-1.el.<arch>.rpm
#   dist/mongosync-insights-<version>-1.el8.<arch>.rpm   (--el-major 8)
#   dist/mongosync-insights-<version>-1.el9.<arch>.rpm   (--el-major 9)
# =============================================================================
set -euo pipefail

RHEL_EL_MAJOR="${RHEL_EL_MAJOR:-}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --el-major)
            RHEL_EL_MAJOR="${2:?--el-major requires 8 or 9}"
            shift 2
            ;;
        -h | --help)
            sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "ERROR: unknown option: $1" >&2
            exit 1
            ;;
    esac
done
if [[ -n "${RHEL_EL_MAJOR}" && "${RHEL_EL_MAJOR}" != "8" && "${RHEL_EL_MAJOR}" != "9" ]]; then
    echo "ERROR: --el-major must be 8 or 9 (got ${RHEL_EL_MAJOR})" >&2
    exit 1
fi
export RHEL_EL_MAJOR

LINUX_DISTRO=rhel
PACKAGE_FORMAT=rpm

# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_build_linux_common.sh"
linux_build_main
