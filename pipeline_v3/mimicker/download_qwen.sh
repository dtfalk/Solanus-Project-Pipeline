#!/usr/bin/env bash
# download_qwen.sh — fetch Qwen2.5 models as PLAIN FOLDERS into the cache (matches David's registry layout).
# Run on a Midway LOGIN node (internet). Qwen is UNGATED — no token needed.
#
#   ./download_qwen.sh 7b      # Qwen2.5-7B-Instruct  -> $HF_HOME/qwen_7b   (the smoke model; default)
#   ./download_qwen.sh 72b     # Qwen2.5-72B-Instruct -> $HF_HOME/qwen_72b
#   ./download_qwen.sh both
set -euo pipefail

WHICH="${1:-7b}"
export HF_HOME="${HF_HOME:-/project/smbowdre/dtfalk/models/cache}"
# Plain HTTP, few workers: Xet high-performance mode gets OOM-killed by the login node's per-process
# memory cap. This is slower but stays under the limit. Downloads auto-resume if interrupted.
export HF_HUB_DISABLE_XET=1
HF="$(command -v hf || command -v huggingface-cli)"
[ -n "$HF" ] || { echo "ERROR: no 'hf' CLI on PATH"; exit 1; }

dl () {  # repo  dest_folder
  echo ">> downloading $1  ->  $HF_HOME/$2   (resumable — just re-run if it dies)"
  "$HF" download "$1" --local-dir "$HF_HOME/$2" --max-workers 2
  if [ -f "$HF_HOME/$2/config.json" ]; then
    n=$(ls "$HF_HOME/$2"/*.safetensors 2>/dev/null | wc -l)
    echo "   OK: $HF_HOME/$2  ($n safetensors shards)"
  else
    echo "   ERROR: no config.json in $HF_HOME/$2 — download did not complete"; exit 1
  fi
}

case "$WHICH" in
  7b)   dl Qwen/Qwen2.5-7B-Instruct  qwen_7b ;;
  72b)  dl Qwen/Qwen2.5-72B-Instruct qwen_72b ;;
  both) dl Qwen/Qwen2.5-7B-Instruct qwen_7b; dl Qwen/Qwen2.5-72B-Instruct qwen_72b ;;
  *)    echo "usage: $0 [7b|72b|both]"; exit 2 ;;
esac

echo ">> submit with:  PROFILE=qwen_fast sbatch submit_job.sh   (or qwen_top for 72b)"
