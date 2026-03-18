#!/usr/bin/env bash
# download_container.sh
# Downloads datamill.sif from a GitHub Release of AgriscaleContainer.
#
# Requirements:
#   - curl
#
# Usage:
#   # Download the latest release:
#   bash scripts/download_container.sh
#
#   # Download a specific version:
#   AGRISCALE_VERSION=v1.2.0 bash scripts/download_container.sh
#
# For private repositories, set GITHUB_TOKEN:
#   export GITHUB_TOKEN=ghp_your_token_here
#
# The script places datamill.sif in the repository root.

set -euo pipefail

REPO="CropModelingPlatform/AgriscaleContainer"
ASSET_NAME="datamill.sif"
OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_FILE="${OUT_DIR}/${ASSET_NAME}"
VERSION="${AGRISCALE_VERSION:-latest}"

if [[ "$VERSION" == "latest" ]]; then
    DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/${ASSET_NAME}"
else
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET_NAME}"
fi

echo "Downloading ${ASSET_NAME} (${VERSION}) from:"
echo "  ${DOWNLOAD_URL}"
echo ""

CURL_ARGS=(-fL --progress-bar -o "$OUT_FILE")

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    CURL_ARGS+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

curl "${CURL_ARGS[@]}" "$DOWNLOAD_URL"

echo ""
echo "Container saved to: ${OUT_FILE}"
echo "Done."
