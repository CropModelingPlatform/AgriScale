#!/usr/bin/env bash
# download_container.sh
# Downloads the latest pre-built datamill.sif from AgriscaleContainer CI artifacts.
#
# Requirements:
#   - curl
#   - jq
#   - unzip
#   - A GitHub personal access token with actions:read scope set as GITHUB_TOKEN,
#     OR the GitHub CLI (gh) authenticated.
#
# Usage:
#   export GITHUB_TOKEN=ghp_your_token_here
#   bash scripts/download_container.sh
#
# The script places datamill.sif in the repository root.

set -euo pipefail

REPO="CropModelingPlatform/AgriscaleContainer"
WORKFLOW_NAME="build-container.yml"
ARTIFACT_NAME="datamill-container"
OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_FILE="${OUT_DIR}/datamill.sif"

# ── Resolve authentication ───────────────────────────────────────────────────

if command -v gh &>/dev/null && gh auth status &>/dev/null 2>&1; then
    USE_GH=1
    echo "Using GitHub CLI (gh) for authentication."
else
    USE_GH=0
    if [[ -z "${GITHUB_TOKEN:-}" ]]; then
        echo "ERROR: Set GITHUB_TOKEN or authenticate with the GitHub CLI (gh auth login)." >&2
        exit 1
    fi
    AUTH_HEADER="Authorization: Bearer ${GITHUB_TOKEN}"
    echo "Using GITHUB_TOKEN for authentication."
fi

api_get() {
    local url="$1"
    if [[ $USE_GH -eq 1 ]]; then
        gh api "$url"
    else
        curl -fsSL -H "$AUTH_HEADER" -H "Accept: application/vnd.github+json" "https://api.github.com/${url}"
    fi
}

# ── Find the latest successful workflow run ───────────────────────────────────

echo "Looking for the latest successful run of '${WORKFLOW_NAME}'..."

RUN_ID=$(api_get "repos/${REPO}/actions/workflows/${WORKFLOW_NAME}/runs?status=success&per_page=1" \
    | jq -r '.workflow_runs[0].id')

if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
    echo "ERROR: No successful workflow runs found for '${WORKFLOW_NAME}'." >&2
    exit 1
fi

echo "Latest successful run ID: ${RUN_ID}"

# ── Find the artifact ─────────────────────────────────────────────────────────

echo "Looking for artifact '${ARTIFACT_NAME}'..."

ARTIFACT_ID=$(api_get "repos/${REPO}/actions/runs/${RUN_ID}/artifacts" \
    | jq -r --arg name "$ARTIFACT_NAME" '.artifacts[] | select(.name == $name) | .id')

if [[ -z "$ARTIFACT_ID" || "$ARTIFACT_ID" == "null" ]]; then
    echo "ERROR: Artifact '${ARTIFACT_NAME}' not found in run ${RUN_ID}." >&2
    exit 1
fi

echo "Artifact ID: ${ARTIFACT_ID}"

# ── Download and extract ───────────────────────────────────────────────────────

TMPDIR_DL=$(mktemp -d)
trap 'rm -rf "$TMPDIR_DL"' EXIT

ZIP_FILE="${TMPDIR_DL}/${ARTIFACT_NAME}.zip"

echo "Downloading artifact..."

if [[ $USE_GH -eq 1 ]]; then
    gh api "repos/${REPO}/actions/artifacts/${ARTIFACT_ID}/zip" > "$ZIP_FILE"
else
    curl -fsSL \
        -H "$AUTH_HEADER" \
        -H "Accept: application/vnd.github+json" \
        -L \
        "https://api.github.com/repos/${REPO}/actions/artifacts/${ARTIFACT_ID}/zip" \
        -o "$ZIP_FILE"
fi

echo "Extracting..."
unzip -q "$ZIP_FILE" -d "$TMPDIR_DL"

# The artifact contains a tar.gz with the SIF inside
TARBALL=$(find "$TMPDIR_DL" -name "*.tar.gz" | head -n1)
if [[ -z "$TARBALL" ]]; then
    # Artifact may directly contain the .sif
    SIF=$(find "$TMPDIR_DL" -name "*.sif" | head -n1)
    if [[ -z "$SIF" ]]; then
        echo "ERROR: Could not find datamill.sif or a .tar.gz in the artifact." >&2
        exit 1
    fi
    cp "$SIF" "$OUT_FILE"
else
    tar -xzf "$TARBALL" -C "$TMPDIR_DL"
    SIF=$(find "$TMPDIR_DL" -name "*.sif" | head -n1)
    if [[ -z "$SIF" ]]; then
        echo "ERROR: Could not find datamill.sif inside the tarball." >&2
        exit 1
    fi
    cp "$SIF" "$OUT_FILE"
fi

echo ""
echo "Container saved to: ${OUT_FILE}"
echo "Done."
