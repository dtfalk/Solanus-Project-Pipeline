#!/usr/bin/env bash
# Install this repo's Claude skills into ~/.claude/skills/ on THIS machine.
# Run once per machine after `git pull`:  bash pipeline_v3/skills/install_skills.sh
set -e
SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="$HOME/.claude/skills"
mkdir -p "$DEST"
for d in "$SRC"/*/ ; do
  name="$(basename "$d")"
  rm -rf "$DEST/$name"
  cp -r "$d" "$DEST/$name"
  echo "installed skill: $name -> $DEST/$name"
done
echo "done. Restart Claude Code (or open a new session) to pick up new skills."
