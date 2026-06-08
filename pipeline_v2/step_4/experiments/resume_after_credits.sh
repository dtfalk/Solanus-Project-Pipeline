#!/usr/bin/env bash
# RESUME after the AI Studio Gemini credits ran out (2026-06-07 mid-session).
# FIRST: top up prepaid credits at https://ai.studio/projects  (the GEMINI_API_KEY account).
# Verify it's back:  ./venv/bin/python -c "import os,sys;sys.path.insert(0,'.');from dotenv import load_dotenv;load_dotenv('.env');from google import genai;print(genai.Client(api_key=os.getenv('GEMINI_API_KEY')).models.generate_content(model='gemini-3.1-flash-lite',contents='ping').text)"
set -e
cd "$(dirname "$0")/.."

# ── Step 1 (optional, ~$5, 12 pages): the few-shot A/B that got cut off — does upping
#    the demo count or layout-similarity selection beat the current 12/random selection?
#    Read its table; if c20 clearly wins, add  --num-fewshot 20  to Step 3. If c12_sim wins,
#    ping Claude to wire similarity-selection into auto_labeler before Step 3 (not yet wired).
./venv/bin/python experiments/fewshot_ab.py

# ── Step 2: refresh pool uploads (Volume_1's 271 pages were promoted but never uploaded,
#    so labeling would otherwise fall back to slow inline demo images).
./venv/bin/python upload_examples.py

# ── Step 3: label the smallest unlabelled volume — Volume_4 (272 pages) — with production
#    defaults (gemini-3.5-flash + page-type routing + the new per-line clip-fix snap). ~$15-20.
./venv/bin/python auto_labeler.py --volume Volume_4

# ── Step 4: QA + triage so the review is ordered worst-first.
./venv/bin/python qa_report.py Volume_4
./venv/bin/python triage.py Volume_4

echo
echo "Volume_4 ready. Review with:"
echo "  EDITOR_DOCUMENT=Volume_4 ./venv/bin/python normalized_editor.py"
