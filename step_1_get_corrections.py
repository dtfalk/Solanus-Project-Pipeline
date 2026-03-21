"""
Step 1: Correct OCR Errors — the main pipeline workhorse.

For each flagged OCR error, this script:
  1. Renders the PDF page as an image
  2. Crops the full line context and the flagged token
  3. Sends both images + metadata to Azure OpenAI
  4. Asks the LLM: "Does this line need correction?"
  5. Validates the JSON response
  6. Saves the result alongside the original error metadata

This script is designed to be run NUM_ATTEMPTS times (attempt 1, attempt 2, ... attempt n) so we
can compare the results later. This file is for running a single attempt. The orchestrator file that calls
this file is named step_1_orchestrator.py

Input:
  - raw_data/errors_*.json  (error manifests from independent approaches)
  - data/pdf-pages/         (single-page PDFs for each document page)
  - data/output/            (Azure OCR layout.json and read.json per page)

Output:
  - data/corrected_files/{doc}/page_{p}/line_{l}/error_id_{id}/
      ├── error_{name}_{id}.json                              — Data about the original error
      ├── error_{name}_{id}_fix_{attempt}_{model}_{pass}.json — LLM result
      ├── error_{name}_{id}_context.png                       — line context image
      ├── error_{name}_{id}_error.png                         — flagged token image
      ├── layout.json                                         — copy of OCR layout
      └── read.json                                           — copy of OCR read

Usage:
    python step1_correct_ocr_errors.py
"""

import os
import csv
import json
import glob
import shutil
import asyncio
import logging
from time import time
from datetime import datetime
from openai import AsyncAzureOpenAI, APIError
from itertools import cycle
from pydantic import BaseModel

from image_helpers import render_pdf_page_as_image, crop_image_to_bounding_box, convert_image_to_base64
from prompts import SYSTEM_PROMPT, ERROR_TYPE_HINTS, DEFAULT_HINT
from cost_tracker import tracker

# ── Pydantic model to enforce structured JSON output from the LLM ──────────────
class CorrectionResponse(BaseModel):
    needs_correction: bool
    needs_error_correction: bool
    needs_context_correction: bool
    corrected_line: str

# ── File paths (all relative to this script's directory) ───────────────────────
SCRIPT_DIR        = os.path.dirname(os.path.abspath(__file__))
DATA_DIR          = os.path.join(SCRIPT_DIR, "..", "raw_data")
PDF_PAGES_DIR     = os.path.join(DATA_DIR, "pdf_pages")
OCR_OUTPUT_DIR    = os.path.join(DATA_DIR, "ocr_output")
CORRECTED_DIR     = os.path.join(SCRIPT_DIR, "output", "corrected_files")
RUN_LOG_CSV       = os.path.join(SCRIPT_DIR, "output", "run_log.csv")

# ── Settings from environment variables ────────────────────────────────────────
DOTS_PER_INCH     = int(os.getenv("DPI"))
PADDING_INCHES    = float(os.getenv("PAD_INCHES"))
MAX_RETRIES       = int(os.getenv("MAX_RETRIES"))
MAX_CONCURRENT    = int(os.getenv("MAX_CONCURRENT"))
ATTEMPT_NUMBER    = int(os.getenv("ATTEMPT_NUMBER"))
LOG_EVERY_N       = int(os.getenv("LOG_EVERY"))
PROCESS_FIRST_N   = int(os.getenv("PROCESS_FIRST_N"))

# Progress/failure tracking files (one per attempt so runs don't overwrite each other)
FAILED_REQUESTS_FILE = os.path.join(SCRIPT_DIR, f"failed_requests_attempt_{ATTEMPT_NUMBER}.json")
PROGRESS_FILE        = os.path.join(SCRIPT_DIR, f"progress_attempt_{ATTEMPT_NUMBER}.json")

# ── Azure OpenAI client pool  ───────────────────────────────────────────────────
NUM_ENDPOINTS = int(os.getenv("NUM_ENDPOINTS"))
MODEL_NAME    = os.getenv("MODEL")
API_VERSION   = os.getenv("AZURE_OPENAI_API_VERSION")

ENDPOINT_CONFIGS = [
    (os.getenv(f"AZURE_OPENAI_ENDPOINT_{i}"), os.getenv(f"AZURE_OPENAI_KEY_{i}"), MODEL_NAME)
    for i in range(1, NUM_ENDPOINTS + 1)
]

# Build a round-robin cycle of (Azure OpenAI client, model_name, endpoint_url) tuples.
# Each endpoint gets its own AsyncAzureOpenAI client for parallelism.
CLIENT_POOL = cycle([
    (
        AsyncAzureOpenAI(api_key = api_key, api_version = API_VERSION, azure_endpoint = endpoint),
        model,
        endpoint,
    )
    for endpoint, api_key, model in ENDPOINT_CONFIGS
])

# ── Logging ────────────────────────────────────────────────────────────────────
log = logging.getLogger("pipeline")
logging.basicConfig(level = logging.WARNING, format = "%(levelname)s %(message)s")
log.setLevel(logging.INFO)

# ── Global state for tracking progress across async tasks ──────────────────────
failed_requests_list = []
failure_lock = None       # initialized in run()
progress_lock = None      # initialized in run()
total_corrections_needed = 0
total_no_correction_needed = 0
completed_error_ids = []


# ── Failure tracking helpers ───────────────────────────────────────────────────

def _find_existing_failure(error_id, source_name):
    """Find a failure entry by error_id + source_name. Returns index or -1."""
    for index, entry in enumerate(failed_requests_list):
        if entry.get("error_id") == error_id and entry.get("name") == source_name:
            return index
    return -1


async def record_failure(failure_entry):
    """Add or update a failure record and save."""
    
    # Ensure that only one thread is writing/reading the failures file at a time
    async with failure_lock:

        # Search for the index of the call failure, returning either the index or -1 if not found
        existing_index = _find_existing_failure(failure_entry["error_id"], failure_entry["name"])
        
        # If failure is not found, then create a new entry for it in the failure file
        if existing_index == -1:
            failure_entry["resolved"] = False
            failed_requests_list.append(failure_entry)
        
        # Otherwise, update the failure
        else:
            failed_requests_list[existing_index].update(failure_entry)
            failed_requests_list[existing_index]["resolved"] = False
        
        # After making changes to the failure entry, write back to the error file
        with open(FAILED_REQUESTS_FILE, "w") as file:
            json.dump(failed_requests_list, file, indent = 2)


async def mark_failure_as_resolved(error_id, source_name):
    """If this error previously failed but has now succeeded, then mark it resolved."""
    
    # Ensure that only one thread is writing/reading the failures file at a time
    async with failure_lock:

        # Search for the index of the call failure, returning either the index or -1 if not found
        existing_index = _find_existing_failure(error_id, source_name)
        
        # If we find the failure, then update the failure to "resolved" and write back to failure file
        if existing_index != -1:
            failed_requests_list[existing_index]["resolved"] = True
            with open(FAILED_REQUESTS_FILE, "w") as file:
                json.dump(failed_requests_list, file, indent = 2)


async def save_progress(error_id = None, source_name = None, status = "success"):
    """Save current progress counters and cost info to disk."""
    
    # Ensure that only one thread is writing/reading at a time
    async with progress_lock:

        # Ensure a valid error and add it to the list of successfully processed errors
        if error_id is not None:
            completed_error_ids.append({"error_id": error_id, "name": source_name, "status": status})
        
        # Get current cost summary
        cost_summary = tracker.get_summary()
        
        # Update cost summary with new stats from this error
        progress_data = {
            "completed":              len(completed_error_ids),
            "corrections_needed":     total_corrections_needed,
            "corrections_not_needed": total_no_correction_needed,
            "failures":               cost_summary["total_failed"],
            "total_input_tokens":     cost_summary["total_input_tokens"],
            "total_output_tokens":    cost_summary["total_output_tokens"],
            "total_cost_usd":         round(cost_summary["total_cost_usd"], 6),
            "completed_errors":       completed_error_ids,
        }

        # Write the updated summary back to disk
        with open(PROGRESS_FILE, "w") as file:
            json.dump(progress_data, file, indent = 2)


# ── Response validation ────────────────────────────────────────────────────────

def validate_llm_response(raw_text):
    """
    Parse and validate the LLM's JSON response.

    Checks:
      - Exactly the right keys are present
      - Boolean fields are booleans, string field is string
      - If needs_correction is False, corrected_line must be "NULL"

    Raises AssertionError or json.JSONDecodeError on invalid input.
    """

    # Parse text to JSON
    parsed = json.loads(raw_text)
    
    # Define the expected keys
    expected_keys = {"needs_correction", "needs_error_correction", "needs_context_correction", "corrected_line"}
    
    # Assert all keys and datatypes are correct
    assert set(parsed.keys()) == expected_keys
    assert isinstance(parsed["needs_correction"], bool)
    assert isinstance(parsed["needs_error_correction"], bool)
    assert isinstance(parsed["needs_context_correction"], bool)
    assert isinstance(parsed["corrected_line"], str)
    
    # Check that if no correction needed then it returned NULL
    if not parsed["needs_correction"]:
        assert parsed["corrected_line"] == "NULL"
    
    # If all checks pass then return the parsed JSON response
    return parsed


# ── Process a single error ─────────────────────────────────────────────────────

async def process_single_error(error_record, source_name, concurrency_semaphore):
    """
    Process one flagged OCR error:
      1. Render the PDF page and crop the relevant regions
      2. Send images + prompt to Azure OpenAI
      3. Validate the response
      4. Save all outputs to disk

    This function is called concurrently for many errors at once,
    with the semaphore limiting how many requests are "in flight" simultaneously.
    """

    # Performs the following operations with the concurrent request semaphore
    async with concurrency_semaphore:

        # ── Extract metadata from the error record ──
        error_id       = error_record["error_id"]
        error_type     = error_record["error_type"]
        document_name  = error_record["source_document"].replace(".pdf", "")
        page_number    = error_record["page_number"]
        line_number    = error_record["line_number"]

        # If the error_id is a multiple of the "log every n errors" value, then log the report
        if int(error_id) % LOG_EVERY_N == 0:
            log.info(f"[{source_name}] error {error_id} | {error_type} | {document_name} p{page_number} L{line_number}")

        # ── Build file paths ──
        pdf_path         = os.path.join(PDF_PAGES_DIR, document_name, f"{document_name}-page-{page_number}.pdf")
        layout_path      = os.path.join(OCR_OUTPUT_DIR, document_name, f"page-{page_number}", "layout.json")
        read_path        = os.path.join(OCR_OUTPUT_DIR, document_name, f"page-{page_number}", "read.json")
        output_directory = os.path.join(CORRECTED_DIR, document_name, f"page_{page_number}", f"line_{line_number}", f"error_id_{error_id}")

        # ── Render page and crop images (run in thread pool to avoid blocking) ──
        full_page_image      = await asyncio.to_thread(render_pdf_page_as_image, pdf_path, DOTS_PER_INCH)
        context_line_image   = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, error_record["bounding_boxes"]["context"], DOTS_PER_INCH, PADDING_INCHES)
        error_token_image    = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, error_record["bounding_boxes"]["error"], DOTS_PER_INCH, PADDING_INCHES)

        # ── Encode images as base64 for the Azure OpenAI API ──
        context_image_base64     = convert_image_to_base64(context_line_image)
        error_token_image_base64 = convert_image_to_base64(error_token_image)

        # ── Get the hint for this error type (reminder: the hint is the additional prompt context for the given type of error) ──
        hint_text = ERROR_TYPE_HINTS.get(error_type, DEFAULT_HINT)

        # ── Build the chat messages for Azure OpenAI ──
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": (
                    f"Error type: {error_type}\n"
                    f"Flagged token: \"{error_record['error']}\"\n"
                    f"OCR line text: \"{error_record['context']['line_text']}\"\n"
                    f"Hint: {hint_text}\n\n"
                    "Image 1: full line context. Image 2: token flagged for review.\n"
                    "Does the line need correction? Respond with EXACT JSON only."
                )},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{context_image_base64}"}},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{error_token_image_base64}"}},
            ]},
        ]

        # ── Send to LLM (with retries for failures such as JSON schema validation, rate limits, content violations, etc...) ──
        # Make the request to the LLM up to MAX_RETRIES number of requests
        for pass_number in range(1, MAX_RETRIES + 1):
            
            # "Try" making the call to Azure OpenAI LLM 
            try:

                # Pick the next client from the round-robin pool
                chosen_client, chosen_model, chosen_endpoint = next(CLIENT_POOL)

                # Call Azure OpenAI with structured output
                response = await chosen_client.chat.completions.parse(
                    messages        = messages,
                    model           = chosen_model,
                    response_format = CorrectionResponse,
                )

                # Validate the response
                raw_response_text = response.choices[0].message.content.strip()
                validated_result  = validate_llm_response(raw_response_text)

                # Record token usage for cost tracking
                tracker.record_api_call(
                    endpoint_url  = chosen_endpoint, 
                    model_name    = chosen_model,
                    input_tokens  = response.usage.prompt_tokens, 
                    output_tokens = response.usage.completion_tokens,
                    error_id      = error_id,
                )

            # Handles API failures (rate limits, server errors)
            except APIError as api_error:

                # Print a warning about the failure
                log.warning(f"[{source_name}] error {error_id} API failure: {api_error}")

                # If we have hit max number of retries then stop trying
                if pass_number == MAX_RETRIES:

                    # Record the failure to the failure file
                    await record_failure({"error_id": error_id, "name": source_name, "type": "api_error", "reason": str(api_error)})
                    
                    # Update the progress file (token usage, successes, failures, etc...)
                    await save_progress(error_id, source_name, status = "api_error")

                    # Exit since we have exceeded max attempts
                    return
                
                # If we have NOT hit max number of retries, then try again
                continue

            # Handles response format JSON failures
            # (same logic as above)
            except (json.JSONDecodeError, AssertionError) as validation_error:
                log.warning(f"[{source_name}] error {error_id} validation fail pass {pass_number}: {validation_error}")
                if pass_number == MAX_RETRIES:
                    await record_failure({"error_id": error_id, "name": source_name, "type": "validation_failure", "reason": str(validation_error)})
                    await save_progress(error_id, source_name, status="validation_failure")
                    return
                continue

            # If none of the above errors, then it succeeded! Now we update the counters and save results for this error
            global total_corrections_needed, total_no_correction_needed
            if validated_result["needs_correction"]:
                total_corrections_needed += 1
            else:
                total_no_correction_needed += 1

            await mark_failure_as_resolved(error_id, source_name)

            # Create output directory and save all artifacts
            os.makedirs(output_directory, exist_ok=True)

            # Save original error metadata
            with open(os.path.join(output_directory, f"error_{source_name}_{error_id}.json"), "w") as file:
                json.dump(error_record, file, indent=2)

            # Copy OCR layout and read files for reference
            for source_path, dest_name in [(layout_path, "layout.json"), (read_path, "read.json")]:
                shutil.copy2(source_path, os.path.join(output_directory, dest_name))

            # Save cropped images
            context_line_image.save(os.path.join(output_directory, f"error_{source_name}_{error_id}_context.png"))
            error_token_image.save(os.path.join(output_directory, f"error_{source_name}_{error_id}_error.png"))

            # Save the LLM's correction result
            fix_filename = f"error_{source_name}_{error_id}_fix_{ATTEMPT_NUMBER}_{chosen_model}_{pass_number}.json"
            with open(os.path.join(output_directory, fix_filename), "w") as file:
                json.dump(validated_result, file, indent=2)

            # Update progress 
            await save_progress(error_id, source_name, status = "success")

            # Log if it is that time again
            if error_id % LOG_EVERY_N == 0:
                log.info(f"[{source_name}] error {error_id} done (pass {pass_number})")
            return


# ── Main pipeline orchestration ────────────────────────────────────────────────

async def run_pipeline():
    """
    Load all error manifests, schedule concurrent processing, and wait for completion.
    """

    # Define global file locks. This ensures no two processes are messing with a file at the same time
    global failure_lock, progress_lock
    failure_lock = asyncio.Lock()
    progress_lock = asyncio.Lock()

    # Define a semaphore to handle traffic across different threads 
    # (think a police officer on traffic duty telling car when they can go)
    concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # Initialize tracking files
    with open(FAILED_REQUESTS_FILE, "w") as file:
        json.dump([], file)
    with open(PROGRESS_FILE, "w") as file:
        json.dump(
            {
                "completed": 0,
                "corrections_needed": 0,
                "corrections_not_needed": 0,
                "failures": 0
            },
            file, 
            indent = 2)

    # ── Discover and load all error manifests ──
    # Error files are named errors_{source_name}.json (e.g. errors_stella.json, errors_nathan.json)
    all_tasks = []
    errors_processed_count = 0
    done_loading = False

    # Load all errors (or the first N errors if we set PROCESS_FIRST_N > 0)
    for manifest_path in sorted(glob.glob(os.path.join(DATA_DIR, "errors_*.json"))):
        
        # Extract the source name from the filename (e.g. "stella" from "errors_stella.json")
        source_name = os.path.basename(manifest_path).removeprefix("errors_").removesuffix(".json")

        # Open and load the file with info about the OCR errors for the given person
        with open(manifest_path, "r", encoding="utf-8") as file:
            manifest_data = json.load(file)

        # Log that we found have the errors
        log.info(f"Loaded {len(manifest_data['errors'])} errors from errors_{source_name}.json")

        # Iterate over all of the extracted errors for this person
        for error_record in manifest_data["errors"]:

            # Optionally limit how many errors we process (for testing)
            if PROCESS_FIRST_N >= 0 and errors_processed_count >= PROCESS_FIRST_N:
                done_loading = True
                break
            
            # Add the current error to the list of "tasks" we will need to complete
            all_tasks.append(process_single_error(error_record, source_name, concurrency_semaphore))
            errors_processed_count += 1

        if done_loading:
            break

    # ── Run all tasks concurrently ──
    results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Report any unhandled exceptions
    unhandled_exceptions = [result for result in results if isinstance(result, Exception)]
    for exception in unhandled_exceptions:
        log.error(f"Unhandled exception: {exception}")

    # Save the progress logs
    await save_progress()

    # Report any errors in the console
    if unhandled_exceptions:
        log.error(f"{len(unhandled_exceptions)} errors had unhandled exceptions (see above)")

    # Print summary statistics from the tracker
    tracker.log_summary()

    # Log that the pipeline is complete
    log.info(f"Pipeline complete for attempt #{ATTEMPT_NUMBER}.\n\n")


def run_one_attempt():
    """Entry point: run the async pipeline and print a summary."""
    
    # Grab the start time so we can see total runtime
    start_time = time()

    # Run the run_pipeline() function asynchronously (akin to what you find in JavaScript)
    asyncio.run(run_pipeline())

    # Get the total runtime
    total_runtime_seconds = time() - start_time

    # {Print }
    logging.info("\n" + "=" * 70)
    logging.info(f"SUMMARY: Attempt #{ATTEMPT_NUMBER}")
    logging.info("=" * 70)
    logging.info(f"  Corrections needed:      {total_corrections_needed}")
    logging.info(f"  No correction needed:    {total_no_correction_needed}")
    label = PROCESS_FIRST_N if PROCESS_FIRST_N >= 0 else "all"
    logging.info(f"  Total runtime ({label} errors): {total_runtime_seconds:.2f} seconds")

    # Append a row to the run log CSV for record-keeping
    cost_summary = tracker.get_summary()
    write_csv_header = not os.path.exists(RUN_LOG_CSV)
    with open(RUN_LOG_CSV, "a", newline="") as file:
        csv_writer = csv.writer(file)
        
        # If this is the first attempt, then write the header for the CSV
        if write_csv_header:
            csv_writer.writerow([
                "timestamp", "model", "dpi", "pad_inches", "max_retries",
                "max_concurrent", "attempt_number", "process_first_n",
                "errors_processed", "corrections_needed", "no_correction_needed",
                "failures", "input_tokens", "output_tokens", "total_tokens",
                "total_cost_usd", "cost_per_error_usd", "runtime_seconds",
            ])
        
        # Regardless of attempt, write this attempt's information to the CSV
        csv_writer.writerow([
            datetime.now().isoformat(),
            MODEL_NAME, DOTS_PER_INCH, PADDING_INCHES, MAX_RETRIES,
            MAX_CONCURRENT, ATTEMPT_NUMBER, PROCESS_FIRST_N,
            cost_summary["total_errors_processed"],
            total_corrections_needed, total_no_correction_needed,
            cost_summary["total_failed"],
            cost_summary["total_input_tokens"], cost_summary["total_output_tokens"],
            cost_summary["total_input_tokens"] + cost_summary["total_output_tokens"],
            f"{cost_summary['total_cost_usd']:.6f}",
            f"{cost_summary['cost_per_error_usd']:.8f}",
            f"{total_runtime_seconds:.2f}",
        ])
    
    # Log that we have saved this attempt's info and where to find the file with that info
    log.info(f"Run logged to {RUN_LOG_CSV}")

def main():
    run_one_attempt()

if __name__ == "__main__":
    main()
