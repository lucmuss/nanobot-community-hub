#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASE_URL="${NANOBOT_HUB_BASE_URL:-http://host.docker.internal:18811}"
OUTPUT_DIR="${NANOBOT_HUB_SCREENSHOT_DIR:-${REPO_ROOT}/output/gui-screenshots}"
IMAGE="${NANOBOT_HUB_SCREENSHOT_IMAGE:-mcr.microsoft.com/playwright:v1.58.2-noble}"

docker run --rm \
  --entrypoint bash \
  --add-host host.docker.internal:host-gateway \
  -e NANOBOT_HUB_BASE_URL="${BASE_URL}" \
  -e NANOBOT_HUB_SCREENSHOT_DIR="/repo/output/gui-screenshots" \
  -e NANOBOT_HUB_ADMIN_IDENTIFIER="${NANOBOT_HUB_ADMIN_IDENTIFIER:-}" \
  -e NANOBOT_HUB_ADMIN_PASSWORD="${NANOBOT_HUB_ADMIN_PASSWORD:-}" \
  -v "${REPO_ROOT}:/repo" \
  -w /repo \
  "${IMAGE}" \
  -lc "mkdir -p /tmp/nanobot-hub-screens && cd /tmp/nanobot-hub-screens && npm init -y >/dev/null 2>&1 && npm install playwright@1.58.2 >/dev/null 2>&1 && NODE_PATH=/tmp/nanobot-hub-screens/node_modules node /repo/scripts/capture_hub_screenshots.js"

echo "Updated screenshots in ${OUTPUT_DIR}"
