#!/usr/bin/env bash
# Pull the CURRENT cloud draft state down into the repo, so local dev starts
# from what the editors actually have (the mirror of seed-azure.sh):
#   content-dev → app/data/*.json
#   media-dev   → app/media-content
#
# Run this before a local work session: ./scripts/pull-azure.sh <storage-account>
# Then bash start-local.sh seeds Azurite from the freshly pulled files.
#
# Downloads only (never writes to the cloud). --overwrite refreshes local copies.
set -euo pipefail
ACCT="${1:?usage: pull-azure.sh <storage-account>}"
APP="$(cd "$(dirname "$0")/.." && pwd)/app"

KEY=$(az storage account keys list --account-name "$ACCT" --query "[0].value" -o tsv)

az storage blob download-batch --account-name "$ACCT" --account-key "$KEY" \
  -s content-dev -d "$APP/data" --pattern "*.json" --overwrite -o none
echo "✓ pulled content-dev → app/data"

az storage blob download-batch --account-name "$ACCT" --account-key "$KEY" \
  -s media-dev -d "$APP/media-content" --overwrite -o none
echo "✓ pulled media-dev → app/media-content"

echo "pull complete — run 'bash start-local.sh' to work against this state locally"
