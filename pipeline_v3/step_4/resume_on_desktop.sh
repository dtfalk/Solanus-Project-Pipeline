#!/usr/bin/env bash
# ONE-SHOT DESKTOP RESUME — 2026-06-09 work-laptop handoff.
# Finishes the Volume_4 per-person re-label (pages 190-272 — the laptop run was killed
# mid-flight at David's request; pages 1-189 already carry correct-demo labels), then
# QA + triage. Designed to run unattended:   nohup bash resume_on_desktop.sh &
#
# Prereqs on the desktop (one-time):
#   git fetch origin && git reset --hard origin/master   # history was rewritten long ago; reset > pull
#   pipeline_v3/step_4 needs venv + .env (copy from pipeline_v2/step_4 if missing:
#     python3 -m venv venv && ./venv/bin/pip install -r requirements.txt ; cp ../../pipeline_v2/step_4/.env .)
#   polygon_cropped_pdfs/ must exist here (copy/rsync from pipeline_v2/step_4 — not in git).
set -e
cd "$(dirname "$0")"
LOG=resume_run.log
PINS="Volume_4/page_002,Volume_4/page_003,Volume_4/page_004,Volume_4/page_005,Volume_4/page_006,Volume_4/page_008,Volume_4/page_009,Volume_4/page_010,Volume_4/page_011,Volume_4/page_012,Volume_4/page_013"

echo "=== $(date) preflight ==="                                  | tee -a "$LOG"
./venv/bin/python -c "import os,sys;sys.path.insert(0,'.');from dotenv import load_dotenv;load_dotenv('.env');from google import genai;genai.Client(api_key=os.getenv('GEMINI_API_KEY')).models.generate_content(model='gemini-3.1-flash-lite',contents='ping');print('API OK')" | tee -a "$LOG"

# Few-shot uploads expire after 48h (last upload 2026-06-09 12:18). Refresh if stale:
if [ -z "$(find file_uris.json -mtime -2 2>/dev/null)" ]; then
  echo "=== refreshing pool uploads (stale >48h) ==="              | tee -a "$LOG"
  ./venv/bin/python upload_examples.py                             >> "$LOG" 2>&1
fi

echo "=== $(date) labeling Volume_4 remainder 190-272 (~\$8) ==="  | tee -a "$LOG"
./venv/bin/python auto_labeler.py --volume Volume_4 --start 190 --end 272 --overwrite \
    --num-fewshot 11 --pin-examples "$PINS"                        >> "$LOG" 2>&1

echo "=== $(date) qa_report ==="                                   | tee -a "$LOG"
./venv/bin/python qa_report.py Volume_4                            >> "$LOG" 2>&1

echo "=== $(date) triage (worst-first worklist, ~70 min) ==="      | tee -a "$LOG"
./venv/bin/python triage.py Volume_4                               >> "$LOG" 2>&1

echo "=== $(date) ALL DONE — review with: ./venv/bin/python normalized_editor.py ===" | tee -a "$LOG"
echo "(then commit+push: git add -A . && git commit -m 'Volume_4 run-3 complete + qa + triage' && git push)" | tee -a "$LOG"
