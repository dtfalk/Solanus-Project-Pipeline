#!/usr/bin/env bash
# start-local.sh — run the Pagesmith editor prototype entirely locally (no cloud).
#
#   ./start-local.sh
#
# Starts Azurite (local Azure Blob), seeds it from app/data + app/media-content,
# then launches the SWA emulator + Functions at http://localhost:4280.
#
# To use the editor: open http://localhost:4280 → you'll be sent to a login
# screen → sign in as user "dev" and ADD THE ROLE: editor  → the ✎ Edit toolbar
# appears. Toggle Edit, change text/photos, Save, then Publish Changes.
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p .azurite

# 1) Azurite on :10000 (skip if already up)
if curl -s -m2 -o /dev/null "http://127.0.0.1:10000/devstoreaccount1"; then
  echo "→ Azurite already running on :10000"
else
  echo "→ starting Azurite…"
  ( azurite --silent --location .azurite --blobPort 10000 --queuePort 10001 --tablePort 10002 >/dev/null 2>&1 & )
  for _ in $(seq 1 30); do
    curl -s -m2 -o /dev/null "http://127.0.0.1:10000/devstoreaccount1" && break || sleep 0.5
  done
fi

# 2) API deps
if [ ! -d api/node_modules ]; then
  echo "→ installing API dependencies…"
  ( cd api && npm install )
fi

# 3) Seed Azurite (idempotent — overwrites with current app/data + app/media-content)
echo "→ seeding Azurite…"
node api/scripts/seed-azurite.js

# 4) SWA emulator + Functions
echo ""
echo "→ open http://localhost:4280  (log in as 'dev', role: editor)"
echo ""
exec swa start app --api-location api
