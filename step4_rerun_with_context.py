"""
Step 4: Rerun With Context — re-process disagreements with prior attempt info.

After step 3 moves disagreeing error folders to data/reruns/, this script
re-processes each one. It works like step 1, but with two key differences:

  1. The prompt includes PRIOR ATTEMPT RESULTS so the LLM knows what
     previous runs concluded (and that they disagreed).
  2. The response includes a new field: semantic_difference (bool) — whether
     the prior disagreement actually matters for meaning/interpretation.

This gives the LLM more context to make a better-informed decision on
the second pass.

Input:  data/reruns/ (populated by step 3)
Output: Fix files saved into data/reruns/ alongside existing files

Usage:
    python step4_rerun_with_context.py

Set ATTEMPT_NUMBER=3 (or 4, 5, etc.) in .env before running.
"""

import os
import csv
import json
import re
import asyncio
import logging
from time import time
from datetime import datetime
from dotenv import load_dotenv
from openai import AsyncAzureOpenAI, APIError
from itertools import cycle
from pydantic import BaseModel

from image_helpers import render_pdf_page_as_image, crop_image_to_bounding_box, convert_image_to_base64
from prompts import SYSTEM_PROMPT, ERROR_TYPE_HINTS, DEFAULT_HINT
from cost_tracker import tracker


# ── Response model (adds semantic_difference field for reruns) ──────────────────
class RerunCorrectionResponse(BaseModel):
    needs_correction: bool
    needs_error_correction: bool
    needs_context_correction: bool
    corrected_line: str
    semantic_difference: bool


load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR       = os.path.join(SCRIPT_DIR, "data")
PDF_PAGES_DIR  = os.path.join(DATA_DIR, "pdf-pages")
OCR_OUTPUT_DIR = os.path.join(DATA_DIR, "output")
RERUNS_DIR     = os.path.join(DATA_DIR, "reruns")
METADATA_DIR   = os.path.join(DATA_DIR, "metadata")
RUN_LOG_CSV    = os.path.join(METADATA_DIR, "run_log_reruns.csv")

# ── Settings ───────────────────────────────────────────────────────────────────
DOTS_PER_INCH   = int(os.getenv("DPI"))
PADDING_INCHES  = float(os.getenv("PAD_INCHES"))
MAX_RETRIES     = int(os.getenv("MAX_RETRIES"))
MAX_CONCURRENT  = int(os.getenv("MAX_CONCURRENT"))
ATTEMPT_NUMBER  = int(os.getenv("ATTEMPT_NUMBER"))
LOG_EVERY_N     = int(os.getenv("LOG_EVERY"))

# Per-attempt tracking files
FAILED_REQUESTS_FILE = os.path.join(METADATA_DIR, f"failed_requests_rerun_attempt_{ATTEMPT_NUMBER}.json")
PROGRESS_FILE        = os.path.join(METADATA_DIR, f"progress_rerun_attempt_{ATTEMPT_NUMBER}.json")

# ── Azure OpenAI client pool ───────────────────────────────────────────────────
MODEL_NAME  = os.getenv("MODEL")
API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

ENDPOINT_CONFIGS = [
    (os.getenv(f"AZURE_OPENAI_ENDPOINT_{i}"), os.getenv(f"AZURE_OPENAI_KEY_{i}"), MODEL_NAME)
    for i in range(1, 6)
]

CLIENT_POOL = cycle([
    (AsyncAzureOpenAI(api_key=api_key, api_version=API_VERSION, azure_endpoint=endpoint), model, endpoint)
    for endpoint, api_key, model in ENDPOINT_CONFIGS
])

# ── Logging ────────────────────────────────────────────────────────────────────
log = logging.getLogger("rerun-pipeline")
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
log.setLevel(logging.INFO)

# ── Global tracking state ──────────────────────────────────────────────────────
failed_requests_list = []
failure_lock = None
progress_lock = None
total_corrections_needed = 0
total_no_correction_needed = 0
completed_error_ids = []


# ── Enhanced system prompt for reruns ──────────────────────────────────────────
# Extends the base prompt with:
#   1. Extra emphasis on faithful transcription (don't "fix" author's spelling)
#   2. The semantic_difference field definition

RERUN_SYSTEM_PROMPT = (
    SYSTEM_PROMPT.rstrip()
    + "\n\n"
    "CRITICAL — FAITHFUL EXTRACTION:\n"
    "The original author made spelling errors. WE DO NOT CARE ABOUT SPELLING. "
    "Your job is FAITHFUL EXTRACTION — preserve EXACTLY what the author wrote, "
    "including misspellings. If the handwriting shows 'recieve', transcribe "
    "'recieve', NOT 'receive'. Only correct OCR mistakes where the digital "
    "text doesn't match the handwriting.\n\n"
    "ADDITIONAL FIELD FOR RERUNS:\n"
    "- semantic_difference: true if the different corrected_line values from "
    "prior attempts change the meaning or archival significance of the text "
    "(different words, names, numbers, or meaning-affecting punctuation). "
    "false if differences are trivial (whitespace, formatting). "
    "If no prior attempts exist or they all agree, set to false.\n\n"
    'Updated schema: {"needs_correction": bool, "needs_error_correction": bool, '
    '"needs_context_correction": bool, "corrected_line": string, '
    '"semantic_difference": bool}\n'
    "Include semantic_difference in every response. No extra keys."
)


# ── Failure/progress tracking (same pattern as step 1) ─────────────────────────

def _find_existing_failure(error_id, source_name):
    for index, entry in enumerate(failed_requests_list):
        if entry.get("error_id") == error_id and entry.get("name") == source_name:
            return index
    return -1


async def record_failure(failure_entry):
    async with failure_lock:
        existing_index = _find_existing_failure(failure_entry["error_id"], failure_entry["name"])
        if existing_index == -1:
            failure_entry["resolved"] = False
            failed_requests_list.append(failure_entry)
        else:
            failed_requests_list[existing_index].update(failure_entry)
            failed_requests_list[existing_index]["resolved"] = False
        with open(FAILED_REQUESTS_FILE, "w") as file:
            json.dump(failed_requests_list, file, indent=2)


async def mark_failure_as_resolved(error_id, source_name):
    async with failure_lock:
        existing_index = _find_existing_failure(error_id, source_name)
        if existing_index != -1:
            failed_requests_list[existing_index]["resolved"] = True
            with open(FAILED_REQUESTS_FILE, "w") as file:
                json.dump(failed_requests_list, file, indent=2)


async def save_progress(error_id=None, source_name=None, status="success"):
    async with progress_lock:
        if error_id is not None:
            completed_error_ids.append({"error_id": error_id, "name": source_name, "status": status})
        cost_summary = tracker.get_summary()
        progress_data = {
            "completed": len(completed_error_ids),
            "corrections_needed": total_corrections_needed,
            "corrections_not_needed": total_no_correction_needed,
            "failures": cost_summary["total_failed"],
            "total_input_tokens": cost_summary["total_input_tokens"],
            "total_output_tokens": cost_summary["total_output_tokens"],
            "total_cost_usd": round(cost_summary["total_cost_usd"], 6),
            "completed_errors": completed_error_ids,
        }
        with open(PROGRESS_FILE, "w") as file:
            json.dump(progress_data, file, indent=2)


# ── Response validation ────────────────────────────────────────────────────────

def validate_rerun_response(raw_text):
    """Validate the LLM response for reruns (includes semantic_difference field)."""
    parsed = json.loads(raw_text)
    expected_keys = {"needs_correction", "needs_error_correction", "needs_context_correction", "corrected_line", "semantic_difference"}
    assert set(parsed.keys()) == expected_keys
    assert isinstance(parsed["needs_correction"], bool)
    assert isinstance(parsed["needs_error_correction"], bool)
    assert isinstance(parsed["needs_context_correction"], bool)
    assert isinstance(parsed["corrected_line"], str)
    assert isinstance(parsed["semantic_difference"], bool)
    if not parsed["needs_correction"]:
        assert parsed["corrected_line"] == "NULL"
    return parsed


# ── Discover errors from the reruns directory ──────────────────────────────────
# Each error folder in reruns/ contains an error_{name}_{eid}.json metadata file

METADATA_FILE_PATTERN = re.compile(r"^error_([a-zA-Z]+)_(\d+)\.json$")


def discover_rerun_errors():
    """
    Walk data/reruns/ and find all error metadata files.

    Also loads decision_summary.json (the comprehensive context from step 3)
    so we can include prior attempt info in the prompt.

    Returns a list of (error_dict, source_name, output_directory, decision_summary) tuples.
    """
    errors_to_rerun = []
    for root, _, files in os.walk(RERUNS_DIR):
        # Load decision_summary.json if available (contains prior attempt context)
        decision_summary = None
        if "decision_summary.json" in files:
            with open(os.path.join(root, "decision_summary.json"), encoding="utf-8") as file_handle:
                decision_summary = json.load(file_handle)

        # Find error metadata files
        for filename in files:
            regex_match = METADATA_FILE_PATTERN.match(filename)
            if not regex_match:
                continue
            with open(os.path.join(root, filename), encoding="utf-8") as file_handle:
                error_record = json.load(file_handle)
            source_name = regex_match.group(1)
            errors_to_rerun.append((error_record, source_name, root, decision_summary))

    return errors_to_rerun


# ── Build prior attempts context text ──────────────────────────────────────────

def build_prior_attempts_text(decision_summary):
    """
    Build a text block summarizing what prior attempts concluded.

    This is appended to the user prompt so the LLM knows the history
    and can make a more informed decision.
    """
    if not decision_summary or "attempts" not in decision_summary:
        return ""

    attempt_lines = []
    for attempt in decision_summary["attempts"]:
        response = attempt.get("full_response", {})
        corrected_line = response.get("corrected_line", "NULL")
        needs_correction = response.get("needs_correction", False)
        attempt_lines.append(
            f"  - Attempt {attempt.get('attempt_number', '?')} "
            f"(model: {attempt.get('model', '?')}): "
            f"needs_correction={needs_correction}, corrected_line=\"{corrected_line}\""
        )

    if not attempt_lines:
        return ""

    # Show the distinct corrected_line values and their vote counts
    distinct_values = decision_summary.get("corrected_line_analysis", {}).get("distinct_values", [])
    distinct_text = ", ".join(
        f'"{entry["corrected_line"]}" (x{entry["vote_count"]})'
        for entry in distinct_values
    )

    # Warn if there are semantically meaningful differences
    has_different_values = len(distinct_values) > 1
    semantic_note = ""
    if has_different_values:
        semantic_note = (
            "\n\n**IMPORTANT: The prior attempts produced DIFFERENT corrected_line values. "
            "Examine whether these differences are semantically meaningful or just trivial variations.**"
        )

    return (
        "\n\nPRIOR ATTEMPTS (this error was rerun because previous attempts disagreed):\n"
        + "\n".join(attempt_lines)
        + f"\n\nDistinct corrected_line values from prior runs: {distinct_text}"
        + semantic_note
        + "\n\nPlease carefully re-examine the handwriting and make your own "
        "independent judgment. Use the prior attempts as additional context "
        "but base your answer on what you see in the images."
    )


# ── Process a single error ─────────────────────────────────────────────────────

async def process_single_error(error_record, source_name, output_directory, concurrency_semaphore, decision_summary=None):
    """
    Re-process one error with enhanced context from prior attempts.

    Same flow as step 1, but with:
      - Prior attempt results included in the prompt
      - Enhanced system prompt emphasizing faithful transcription
      - semantic_difference field in the response
    """
    async with concurrency_semaphore:
        error_id      = error_record["error_id"]
        error_type    = error_record["error_type"]
        document_name = error_record["source_document"].replace(".pdf", "")
        page_number   = error_record["page_number"]
        line_number   = error_record["line_number"]

        if int(error_id) % LOG_EVERY_N == 0:
            log.info(f"[rerun/{source_name}] error {error_id} | {error_type} | {document_name} p{page_number} L{line_number}")

        # Build file paths
        pdf_path = os.path.join(PDF_PAGES_DIR, document_name, f"{document_name}-page-{page_number}.pdf")

        # Render page and crop images
        full_page_image    = await asyncio.to_thread(render_pdf_page_as_image, pdf_path, DOTS_PER_INCH)
        context_line_image = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, error_record["bounding_boxes"]["context"], DOTS_PER_INCH, PADDING_INCHES)
        error_token_image  = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, error_record["bounding_boxes"]["error"], DOTS_PER_INCH, PADDING_INCHES)

        context_image_base64     = convert_image_to_base64(context_line_image)
        error_token_image_base64 = convert_image_to_base64(error_token_image)

        hint_text = ERROR_TYPE_HINTS.get(error_type, DEFAULT_HINT)

        # Build the prior attempts context (empty string if no decision_summary)
        prior_attempts_text = build_prior_attempts_text(decision_summary)

        # Build chat messages
        messages = [
            {"role": "system", "content": RERUN_SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": (
                    f"Error type: {error_type}\n"
                    f"Flagged token: \"{error_record['error']}\"\n"
                    f"OCR line text: \"{error_record['context']['line_text']}\"\n"
                    f"Hint: {hint_text}"
                    f"{prior_attempts_text}\n\n"
                    "Image 1: full line context. Image 2: flagged token.\n"
                    "Does the line need correction? Respond with EXACT JSON only."
                )},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{context_image_base64}"}},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{error_token_image_base64}"}},
            ]},
        ]

        # Send to LLM with retries
        for pass_number in range(1, MAX_RETRIES + 1):
            try:
                chosen_client, chosen_model, chosen_endpoint = next(CLIENT_POOL)
                response = await chosen_client.chat.completions.parse(
                    messages=messages,
                    model=chosen_model,
                    response_format=RerunCorrectionResponse,
                )
                raw_response_text = response.choices[0].message.content.strip()
                validated_result = validate_rerun_response(raw_response_text)
                tracker.record_api_call(chosen_endpoint, chosen_model, response.usage.prompt_tokens, response.usage.completion_tokens, error_id=error_id)

            except APIError as api_error:
                tracker.record_failure()
                log.warning(f"[rerun/{source_name}] error {error_id} API failure: {api_error}")
                await record_failure({"error_id": error_id, "name": source_name, "type": "api_error", "reason": str(api_error)})
                await save_progress(error_id, source_name, status="api_error")
                return

            except (json.JSONDecodeError, AssertionError) as validation_error:
                log.warning(f"[rerun/{source_name}] error {error_id} validation fail pass {pass_number}: {validation_error}")
                if pass_number == MAX_RETRIES:
                    await record_failure({"error_id": error_id, "name": source_name, "type": "validation_failure", "reason": str(validation_error)})
                    await save_progress(error_id, source_name, status="validation_failure")
                    return
                continue

            # Success — update counters and save
            global total_corrections_needed, total_no_correction_needed
            if validated_result["needs_correction"]:
                total_corrections_needed += 1
            else:
                total_no_correction_needed += 1

            await mark_failure_as_resolved(error_id, source_name)

            # Save fix file into the reruns folder
            fix_filename = f"error_{source_name}_{error_id}_fix_{ATTEMPT_NUMBER}_{chosen_model}_{pass_number}.json"
            with open(os.path.join(output_directory, fix_filename), "w") as file:
                json.dump(validated_result, file, indent=2)

            await save_progress(error_id, source_name, status="success")

            if error_id % LOG_EVERY_N == 0:
                log.info(f"[rerun/{source_name}] error {error_id} done (pass {pass_number})")
            return


# ── Main ───────────────────────────────────────────────────────────────────────

async def run_pipeline():
    global failure_lock, progress_lock
    failure_lock = asyncio.Lock()
    progress_lock = asyncio.Lock()
    concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    os.makedirs(METADATA_DIR, exist_ok=True)
    with open(FAILED_REQUESTS_FILE, "w") as file:
        json.dump([], file)
    with open(PROGRESS_FILE, "w") as file:
        json.dump({"completed": 0, "corrections_needed": 0, "corrections_not_needed": 0, "failures": 0}, file, indent=2)

    # Discover errors from the reruns directory
    errors_to_rerun = discover_rerun_errors()
    if not errors_to_rerun:
        print("No error folders found in", RERUNS_DIR)
        print("Run step3_move_disagreements.py first.")
        return

    print(f"Found {len(errors_to_rerun)} errors to rerun")
    log.info(f"Found {len(errors_to_rerun)} errors to rerun from {RERUNS_DIR}")

    # Process all errors concurrently
    tasks = [
        process_single_error(error_record, source_name, output_dir, concurrency_semaphore, decision_summary)
        for error_record, source_name, output_dir, decision_summary in errors_to_rerun
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Report unhandled exceptions
    unhandled = [r for r in results if isinstance(r, Exception)]
    for exception in unhandled:
        log.error(f"Unhandled exception: {exception}")

    await save_progress()
    tracker.print_summary()
    log.info("Rerun pipeline complete\n\n")


def main():
    print("\n" + "=" * 70)
    print("STEP 4: RERUN DISAGREEMENTS WITH CONTEXT")
    print("=" * 70)
    print(f"  Attempt Number:  {ATTEMPT_NUMBER}")
    print(f"  Model:           {MODEL_NAME}")
    print(f"  Max Concurrent:  {MAX_CONCURRENT}")
    print(f"  Reruns Dir:      {RERUNS_DIR}")
    print("=" * 70 + "\n")

    start_time = time()
    asyncio.run(run_pipeline())
    total_runtime = time() - start_time

    print("\n" + "=" * 70)
    print("RERUN SUMMARY")
    print("=" * 70)
    print(f"  Corrections needed:    {total_corrections_needed}")
    print(f"  No correction needed:  {total_no_correction_needed}")
    print(f"  Total runtime:         {total_runtime:.2f} seconds")

    # Append to run log CSV
    cost_summary = tracker.get_summary()
    os.makedirs(os.path.dirname(RUN_LOG_CSV), exist_ok=True)
    write_header = not os.path.exists(RUN_LOG_CSV)
    with open(RUN_LOG_CSV, "a", newline="") as file:
        csv_writer = csv.writer(file)
        if write_header:
            csv_writer.writerow([
                "timestamp", "model", "dpi", "pad_inches", "max_retries",
                "max_concurrent", "attempt_number",
                "errors_processed", "corrections_needed", "no_correction_needed",
                "failures", "input_tokens", "output_tokens", "total_tokens",
                "total_cost_usd", "cost_per_error_usd", "runtime_seconds",
            ])
        csv_writer.writerow([
            datetime.now().isoformat(), MODEL_NAME, DOTS_PER_INCH, PADDING_INCHES,
            MAX_RETRIES, MAX_CONCURRENT, ATTEMPT_NUMBER,
            cost_summary["total_errors_processed"],
            total_corrections_needed, total_no_correction_needed,
            cost_summary["total_failed"],
            cost_summary["total_input_tokens"], cost_summary["total_output_tokens"],
            cost_summary["total_input_tokens"] + cost_summary["total_output_tokens"],
            f"{cost_summary['total_cost_usd']:.6f}",
            f"{cost_summary['cost_per_error_usd']:.8f}",
            f"{total_runtime:.2f}",
        ])
    log.info(f"Run logged to {RUN_LOG_CSV}")


if __name__ == "__main__":
    main()
