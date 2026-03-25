"""
Step 1 Orchestrator — runs the correction pipeline N times
This script simply just calls step1_get_corrections.py NUM_ATTEMPTS times.

Running N independent attempts lets us compare results later.
If attempts agree on a correction, we have high confidence.
If they disagree, we can investigate further (steps 3-6).

Usage:
    python step_1_orchestrator.py
"""

import subprocess
import sys
import os
from dotenv import load_dotenv
import logging

# The script that makes api calls to identify errors
CORRECTION_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "step_1_get_corrections.py")

# Load the environment variables and extract the number of attempts to run
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
NUM_ATTEMPTS = int(os.getenv("NUM_ATTEMPTS"))

# Run the pipeline the NUM_ATTEMPTS number of times
for attempt_number in range(1, NUM_ATTEMPTS + 1):

    # Log the current pipeline run
    logging.info(f"\n{'=' * 70}")
    logging.info(f"  STARTING ATTEMPT {attempt_number}")
    logging.info(f"{'=' * 70}\n")
    
    # Set the environment variable for the attempt number
    os.environ["ATTEMPT_NUMBER"] = str(attempt_number)

    # Run the correction pipeline
    result = subprocess.run([sys.executable, CORRECTION_SCRIPT])

    # Print that the current attempt completed successfully
    logging.info(f"\n  Attempt {attempt_number} finished with exit code {result.returncode}")

logging.info("\n\nStep 1 Complete.")
