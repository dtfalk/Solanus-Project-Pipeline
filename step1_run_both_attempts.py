"""
Step 1 Runner — runs the correction pipeline N times
This script simply just calls step_1_correct_ocr_errors.py N times.

Running two independent attempts lets us compare results later.
If both attempts agree on a correction, we have high confidence.
If they disagree, we can investigate further (steps 3-6).

Usage:
    python step1_run_both_attempts.py
"""

import subprocess
import sys
import os

CORRECTION_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "step1_correct_ocr_errors.py")

for attempt_number in (1, 2):
    print(f"\n{'=' * 70}")
    print(f"  STARTING ATTEMPT {attempt_number}")
    print(f"{'=' * 70}\n")

    # Pass the attempt number as an environment variable so the script
    # knows which attempt it is and saves to separate progress files.
    environment = {**os.environ, "ATTEMPT_NUMBER": str(attempt_number)}
    result = subprocess.run([sys.executable, CORRECTION_SCRIPT], env=environment)
    print(f"\n  Attempt {attempt_number} finished with exit code {result.returncode}")

print("\n\nDone — both attempts complete.")
