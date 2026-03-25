"""
Step 6: Resolve Three-Way Splits — LLM tiebreaker for unresolved cases.

After step 5b identifies THREE_WAY_SPLIT cases (where the rerun produced
a third, different answer instead of siding with A or B), this script
sends all three options plus the original images to the LLM and asks
it to pick the best one.

Input:
    analysis_output/deep_decision_analysis.json (THREE_WAY_SPLIT cases)
    data/reruns/{path}/decision_summary.json    (images and context)

Output:
    data/reruns/{path}/tiebreaker_result.json   (per-case result)
    analysis_output/tiebreaker_results.csv      (summary CSV)

Usage:
    python step6_resolve_three_way_splits.py
    python step6_resolve_three_way_splits.py --dry-run
    python step6_resolve_three_way_splits.py --reset
"""

import os
import csv
import json
import asyncio
import logging
import argparse
from time import time
from datetime import datetime
from dotenv import load_dotenv
from openai import AsyncAzureOpenAI, APIError
from itertools import cycle
from pydantic import BaseModel

from image_helpers import render_pdf_page_as_image, crop_image_to_bounding_box, convert_image_to_base64
from cost_tracker import tracker

load_dotenv()


# ── Pydantic model for the tiebreaker response ────────────────────────────────

class TiebreakerResponse(BaseModel):
    chosen_option: str   # "A", "B", "C", or "NONE"
    confidence: str      # "high", "medium", or "low"
    reasoning: str       # Brief explanation of the choice


# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_DIR         = os.path.join(SCRIPT_DIR, "data")
PDF_PAGES_DIR    = os.path.join(DATA_DIR, "pdf-pages")
RERUNS_DIR       = os.path.join(DATA_DIR, "reruns")
METADATA_DIR     = os.path.join(DATA_DIR, "metadata")
ANALYSIS_DIR     = os.path.join(SCRIPT_DIR, "analysis_output")

ANALYSIS_JSON    = os.path.join(ANALYSIS_DIR, "deep_decision_analysis.json")
PROGRESS_FILE    = os.path.join(METADATA_DIR, "progress_tiebreaker.json")
RESULTS_CSV      = os.path.join(ANALYSIS_DIR, "tiebreaker_results.csv")

# ── Settings ───────────────────────────────────────────────────────────────────
DOTS_PER_INCH  = int(os.getenv("DPI", "150"))
PADDING_INCHES = float(os.getenv("PAD_INCHES", "0.1"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "5"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "55"))
LOG_EVERY_N    = int(os.getenv("LOG_EVERY", "50"))

# ── Azure OpenAI client pool ──────────────────────────────────────────────────
MODEL_NAME  = os.getenv("MODEL", "gpt-5-nano")
API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

ENDPOINT_CONFIGS = [
    (os.getenv(f"AZURE_OPENAI_ENDPOINT_{i}"), os.getenv(f"AZURE_OPENAI_KEY_{i}"), MODEL_NAME)
    for i in range(1, 6)
]
CLIENT_POOL = cycle([
    (AsyncAzureOpenAI(api_key=api_key, api_version=API_VERSION, azure_endpoint=endpoint), model, endpoint)
    for endpoint, api_key, model in ENDPOINT_CONFIGS
    if endpoint and api_key
])

# ── Logging ────────────────────────────────────────────────────────────────────
log = logging.getLogger("tiebreaker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Global progress tracking ──────────────────────────────────────────────────
progress_lock = None
progress_counters = {"completed": 0, "successes": 0, "failures": 0, "skipped": 0, "total": 0}
completed_items = []
pipeline_start_time = None


# ── TIEBREAKER PROMPTS ─────────────────────────────────────────────────────────

def build_tiebreaker_prompts(three_way_case):
    """
    Build the system and user prompts for choosing between options A, B, and C.

    Returns (system_prompt, user_prompt).
    """

    system_prompt = """You are an OCR transcription reviewer for handwritten historical documents.

CONTEXT ON HOW WE GOT HERE:
We ran an LLM twice to correct OCR errors on this line, and the two attempts DISAGREED:
- Option A: First LLM attempt's correction
- Option B: Second LLM attempt's correction (different from A)

Because they disagreed, we ran a THIRD attempt (Option C) with enhanced context including:
- Knowledge of the prior disagreement
- The original images again
- Emphasis on faithful transcription

Option C is the most recent attempt with the most context, BUT it produced yet another 
different answer instead of siding with A or B. Now we have three conflicting options.

YOUR TASK:
Choose which option (A, B, or C) most faithfully represents the original handwriting.

GUIDELINES:
1. FAITHFUL TRANSCRIPTION is paramount - choose what the author actually wrote, INCLUDING 
   any spelling errors, unusual punctuation, or unconventional grammar.
2. Do NOT choose based on "correct" spelling/grammar - choose based on what's in the image.
3. MAKE THE SAFEST BET: If you're uncertain, prefer the option that:
   - Makes the fewest changes from the original OCR
   - Is most conservative/literal
   - Avoids "fixing" things that might not need fixing
4. Option C had more context when it was generated, so it may be more informed - but don't 
   blindly trust it. It could also have overcorrected. Judge by the images.
5. If two options are nearly identical and one differs significantly, the two similar ones 
   are probably more likely correct.
6. If the image shows NOTHING legible, or the flagged region appears blank/empty/unreadable,
   choose "NONE" - meaning keep the original OCR unchanged.

Respond with EXACT JSON only. No markdown. No explanation outside the JSON.
Schema: {"chosen_option": "A"|"B"|"C"|"NONE", "confidence": "high"|"medium"|"low", "reasoning": "<brief explanation>"}"""

    original_ocr_line = three_way_case.get("ocr_original", "")
    option_a          = three_way_case.get("option_a", "")
    option_b          = three_way_case.get("option_b", "")
    option_c          = three_way_case.get("rerun_line", "")
    error_type        = three_way_case.get("error_type", "")
    flagged_token     = three_way_case.get("flagged_token", "")
    similarity_a_b    = three_way_case.get("sim_a_to_b", "")
    similarity_c_a    = three_way_case.get("sim_rerun_to_a", "")
    similarity_c_b    = three_way_case.get("sim_rerun_to_b", "")

    user_prompt = f"""Error type: {error_type}
Flagged token: "{flagged_token}"

ORIGINAL OCR TEXT (before any corrections):
"{original_ocr_line}"

THREE CORRECTION OPTIONS:
Option A (1st attempt): "{option_a}"
Option B (2nd attempt): "{option_b}"
Option C (3rd attempt with enhanced context): "{option_c}"

SIMILARITY ANALYSIS:
- A vs B similarity: {similarity_a_b}
- C vs A similarity: {similarity_c_a}
- C vs B similarity: {similarity_c_b}
(1.0 = identical, 0.0 = completely different)

Image 1: Full line context from original document.
Image 2: Flagged token/region cropped from document.

Examine the images carefully. Which option most faithfully represents what appears in the original handwriting? Make the safest bet.

Respond with JSON only: {{"chosen_option": "A"|"B"|"C", "confidence": "high"|"medium"|"low", "reasoning": "..."}}"""

    return system_prompt, user_prompt


# ── Progress tracking ─────────────────────────────────────────────────────────

async def save_progress(item_path=None, status="success"):
    """Save current progress to the progress file."""
    async with progress_lock:
        if item_path is not None:
            completed_items.append({"path": item_path, "status": status})

        cost_summary = tracker.get_summary()
        progress_data = {
            "completed": progress_counters["completed"],
            "successes": progress_counters["successes"],
            "failures": progress_counters["failures"],
            "skipped": progress_counters["skipped"],
            "total": progress_counters["total"],
            "total_input_tokens": cost_summary["total_input_tokens"],
            "total_output_tokens": cost_summary["total_output_tokens"],
            "total_cost_usd": round(cost_summary["total_cost_usd"], 6),
            "last_updated": datetime.now().isoformat(),
            "completed_items": completed_items,
        }
        if pipeline_start_time:
            progress_data["elapsed_seconds"] = round(time() - pipeline_start_time, 1)

        with open(PROGRESS_FILE, "w") as file_handle:
            json.dump(progress_data, file_handle, indent=2)


# ── Load THREE_WAY_SPLIT cases from deep_decision_analysis.json ───────────────

def load_three_way_split_cases():
    """
    Read the deep decision analysis output and return only the THREE_WAY_SPLIT cases.
    These are the ones that need a tiebreaker.
    """
    if not os.path.exists(ANALYSIS_JSON):
        log.error(f"Analysis file not found: {ANALYSIS_JSON}")
        log.error("Run step5b_deep_decision_analysis.py first.")
        return []

    with open(ANALYSIS_JSON, "r", encoding="utf-8") as file_handle:
        analysis_data = json.load(file_handle)

    three_way_cases = [
        result for result in analysis_data.get("results", [])
        if result.get("outcome") == "THREE_WAY_SPLIT"
    ]
    return three_way_cases


# ── Process a single tiebreaker case ──────────────────────────────────────────

async def process_single_tiebreaker(three_way_case, concurrency_semaphore, dry_run=False):
    """
    Send a single THREE_WAY_SPLIT case to the LLM for tiebreaking.

    Renders the original PDF page, crops the relevant images,
    and asks the LLM to pick between options A, B, and C.
    """
    folder_path = three_way_case.get("folder_path")
    error_id    = three_way_case.get("error_id")

    # Check if already processed (skip if result file exists)
    tiebreaker_result_path = os.path.join(folder_path, "tiebreaker_result.json")
    if os.path.exists(tiebreaker_result_path):
        async with progress_lock:
            progress_counters["skipped"] += 1
        return {"status": "skipped", "error_id": error_id, "reason": "already processed"}

    if dry_run:
        return {"status": "dry_run", "error_id": error_id}

    # Load decision_summary.json for bounding box / image info
    summary_file_path = os.path.join(folder_path, "decision_summary.json")
    if not os.path.exists(summary_file_path):
        log.error(f"error_id={error_id}: no decision_summary.json in {folder_path}")
        return {"status": "error", "error_id": error_id, "reason": "no decision_summary.json"}

    try:
        with open(summary_file_path, "r", encoding="utf-8") as file_handle:
            decision_summary = json.load(file_handle)
    except (json.JSONDecodeError, OSError) as read_error:
        log.error(f"error_id={error_id}: failed to read decision_summary: {read_error}")
        return {"status": "error", "error_id": error_id, "reason": str(read_error)}

    # Get bounding boxes for image cropping
    error_metadata     = decision_summary.get("error_metadata", {})
    bounding_boxes     = error_metadata.get("bounding_boxes", {})
    context_polygon    = bounding_boxes.get("context", [])
    error_token_polygon = bounding_boxes.get("error", [])

    if not context_polygon or not error_token_polygon:
        log.error(f"error_id={error_id}: missing bounding boxes")
        return {"status": "error", "error_id": error_id, "reason": "missing bounding boxes"}

    # Find the PDF page file
    document_name = error_metadata.get("source_document", "").replace(".pdf", "")
    page_number   = error_metadata.get("page_number")
    pdf_file_path = os.path.join(PDF_PAGES_DIR, document_name, f"{document_name}-page-{page_number}.pdf")

    if not os.path.exists(pdf_file_path):
        log.error(f"error_id={error_id}: PDF not found: {pdf_file_path}")
        return {"status": "error", "error_id": error_id, "reason": f"PDF not found: {pdf_file_path}"}

    # Render and crop images
    try:
        full_page_image    = await asyncio.to_thread(render_pdf_page_as_image, pdf_file_path, DOTS_PER_INCH)
        context_line_image = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, context_polygon, DOTS_PER_INCH, PADDING_INCHES)
        error_token_image  = await asyncio.to_thread(crop_image_to_bounding_box, full_page_image, error_token_polygon, DOTS_PER_INCH, PADDING_INCHES)
        context_image_base64     = convert_image_to_base64(context_line_image)
        error_token_image_base64 = convert_image_to_base64(error_token_image)
    except Exception as image_error:
        log.error(f"error_id={error_id}: image processing failed: {image_error}")
        return {"status": "error", "error_id": error_id, "reason": f"image processing: {image_error}"}

    # Build prompt
    system_prompt_text, user_prompt_text = build_tiebreaker_prompts(three_way_case)

    # Send to LLM with retries
    async with concurrency_semaphore:
        for retry_number in range(MAX_RETRIES):
            chosen_client, chosen_model, chosen_endpoint = next(CLIENT_POOL)
            try:
                api_response = await chosen_client.chat.completions.parse(
                    model=chosen_model,
                    messages=[
                        {"role": "system", "content": system_prompt_text},
                        {"role": "user", "content": [
                            {"type": "text", "text": user_prompt_text},
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{context_image_base64}"}},
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{error_token_image_base64}"}},
                        ]},
                    ],
                    response_format=TiebreakerResponse,
                    max_completion_tokens=300,
                    temperature=0.1,
                )

                # Parse and validate the response
                raw_response_text = api_response.choices[0].message.content
                parsed_result = json.loads(raw_response_text)

                assert parsed_result.get("chosen_option") in ("A", "B", "C", "NONE"), \
                    f"chosen_option must be A, B, C, or NONE, got {parsed_result.get('chosen_option')}"
                assert parsed_result.get("confidence") in ("high", "medium", "low"), \
                    f"confidence must be high, medium, or low, got {parsed_result.get('confidence')}"

                # Map the chosen option letter to the actual corrected line text
                option_to_line = {
                    "A": three_way_case.get("option_a"),
                    "B": three_way_case.get("option_b"),
                    "C": three_way_case.get("rerun_line"),
                    "NONE": three_way_case.get("ocr_original"),  # Keep original unchanged
                }
                chosen_line = option_to_line.get(parsed_result["chosen_option"])

                # Track API cost
                input_tokens  = api_response.usage.prompt_tokens
                output_tokens = api_response.usage.completion_tokens
                tracker.record_api_call(chosen_endpoint, chosen_model, input_tokens, output_tokens, error_id=error_id)

                # Build the full result object
                tiebreaker_result = {
                    "chosen_option": parsed_result["chosen_option"],
                    "confidence": parsed_result["confidence"],
                    "reasoning": parsed_result["reasoning"],
                    "chosen_line": chosen_line,
                    "option_a": three_way_case.get("option_a"),
                    "option_b": three_way_case.get("option_b"),
                    "option_c": three_way_case.get("rerun_line"),
                    "ocr_original": three_way_case.get("ocr_original"),
                    "error_id": error_id,
                    "model": chosen_model,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "timestamp": datetime.now().isoformat(),
                }

                # Save result to file
                with open(tiebreaker_result_path, "w", encoding="utf-8") as file_handle:
                    json.dump(tiebreaker_result, file_handle, indent=2, ensure_ascii=False)

                # Update progress
                async with progress_lock:
                    progress_counters["completed"] += 1
                    progress_counters["successes"] += 1

                await save_progress(item_path=folder_path, status="success")

                # Log periodically
                if progress_counters["completed"] % LOG_EVERY_N == 0:
                    elapsed = time() - pipeline_start_time if pipeline_start_time else 0
                    rate = progress_counters["completed"] / elapsed if elapsed > 0 else 0
                    remaining = progress_counters["total"] - progress_counters["completed"] - progress_counters["skipped"]
                    estimated_time_remaining = remaining / rate if rate > 0 else 0
                    log.info(
                        f"Progress: {progress_counters['completed']}/{progress_counters['total']} done, "
                        f"{progress_counters['successes']} success, {progress_counters['failures']} fail, "
                        f"{progress_counters['skipped']} skip | {rate:.1f}/s, "
                        f"ETA {estimated_time_remaining:.0f}s"
                    )

                return {"status": "success", "error_id": error_id, "result": tiebreaker_result}

            except (APIError, json.JSONDecodeError, AssertionError, KeyError) as call_error:
                tracker.record_failure()
                if retry_number == MAX_RETRIES - 1:
                    log.error(f"error_id={error_id}: all {MAX_RETRIES} retries failed: {call_error}")
                    async with progress_lock:
                        progress_counters["completed"] += 1
                        progress_counters["failures"] += 1
                    await save_progress(item_path=folder_path, status="error")
                    return {"status": "error", "error_id": error_id, "reason": str(call_error)}
                await asyncio.sleep(1)


# ── Async entry point ─────────────────────────────────────────────────────────

async def run_tiebreaker_pipeline(cases, dry_run=False):
    """Process all THREE_WAY_SPLIT cases concurrently."""
    global progress_lock, pipeline_start_time
    progress_lock = asyncio.Lock()
    pipeline_start_time = time()
    progress_counters["total"] = len(cases)

    concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    log.info(f"Starting {len(cases)} tiebreaker cases with {MAX_CONCURRENT} concurrent workers")

    tasks = [
        process_single_tiebreaker(case, concurrency_semaphore, dry_run)
        for case in cases
    ]
    results = await asyncio.gather(*tasks)

    await save_progress()
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Resolve THREE_WAY_SPLIT cases with LLM tiebreaker")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making LLM calls")
    parser.add_argument("--reset", action="store_true", help="Delete existing results and reprocess all")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("STEP 6: RESOLVE THREE-WAY SPLITS (TIEBREAKER)")
    print("=" * 70)
    print(f"  Model:          {MODEL_NAME}")
    print(f"  Max Concurrent: {MAX_CONCURRENT}")
    print(f"  Max Retries:    {MAX_RETRIES}")
    print(f"  Analysis JSON:  {ANALYSIS_JSON}")
    print("=" * 70 + "\n")

    # Load the THREE_WAY_SPLIT cases from step 5b output
    three_way_cases = load_three_way_split_cases()
    print(f"Found {len(three_way_cases)} THREE_WAY_SPLIT cases")

    if not three_way_cases:
        print("Nothing to process. Run step5b_deep_decision_analysis.py first.")
        return

    # Reset if requested: delete existing tiebreaker results
    if args.reset:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        for case in three_way_cases:
            result_file = os.path.join(case["folder_path"], "tiebreaker_result.json")
            if os.path.exists(result_file):
                os.remove(result_file)
        print("Reset complete — will reprocess all cases")

    if args.dry_run:
        print("DRY RUN — no LLM calls will be made\n")

    # Run the tiebreaker pipeline
    start_time = time()
    all_results = asyncio.run(run_tiebreaker_pipeline(three_way_cases, args.dry_run))
    total_runtime = time() - start_time

    # Count statuses
    status_counts = {}
    for result in all_results:
        status = result.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    print(f"\n=== Results ({total_runtime:.1f}s) ===")
    for status, count in sorted(status_counts.items(), key=lambda pair: -pair[1]):
        print(f"  {status}: {count}")

    # ── Write summary CSV from all tiebreaker result files ─────────────────────
    csv_rows = []
    for case in three_way_cases:
        result_file = os.path.join(case["folder_path"], "tiebreaker_result.json")
        if not os.path.exists(result_file):
            continue
        try:
            with open(result_file, "r", encoding="utf-8") as file_handle:
                result_data = json.load(file_handle)
            csv_rows.append({
                "folder_path": case["folder_path"],
                "error_id": case["error_id"],
                "error_type": case["error_type"],
                "ocr_original": result_data.get("ocr_original", ""),
                "option_a": result_data.get("option_a", ""),
                "option_b": result_data.get("option_b", ""),
                "option_c": result_data.get("option_c", ""),
                "chosen_option": result_data.get("chosen_option", ""),
                "chosen_line": result_data.get("chosen_line", ""),
                "confidence": result_data.get("confidence", ""),
                "reasoning": result_data.get("reasoning", ""),
            })
        except (json.JSONDecodeError, OSError):
            pass

    if csv_rows:
        os.makedirs(ANALYSIS_DIR, exist_ok=True)
        csv_columns = [
            "folder_path", "error_id", "error_type", "ocr_original",
            "option_a", "option_b", "option_c",
            "chosen_option", "chosen_line", "confidence", "reasoning",
        ]
        with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as file_handle:
            csv_writer = csv.DictWriter(file_handle, fieldnames=csv_columns)
            csv_writer.writeheader()
            csv_writer.writerows(csv_rows)
        print(f"\nCSV report: {RESULTS_CSV}")

        # Show choice distribution
        choice_distribution = {}
        confidence_distribution = {}
        for row in csv_rows:
            choice = row.get("chosen_option", "?")
            choice_distribution[choice] = choice_distribution.get(choice, 0) + 1
            confidence = row.get("confidence", "?")
            confidence_distribution[confidence] = confidence_distribution.get(confidence, 0) + 1

        print("\n=== Choice Distribution ===")
        for option_letter in ["A", "B", "C", "NONE"]:
            count = choice_distribution.get(option_letter, 0)
            percentage = 100 * count / len(csv_rows) if csv_rows else 0
            print(f"  Option {option_letter}: {count} ({percentage:.1f}%)")

        print("\n=== Confidence Distribution ===")
        for confidence_level in ["high", "medium", "low"]:
            count = confidence_distribution.get(confidence_level, 0)
            percentage = 100 * count / len(csv_rows) if csv_rows else 0
            print(f"  {confidence_level}: {count} ({percentage:.1f}%)")

    # Print cost summary
    tracker.print_summary()


if __name__ == "__main__":
    main()
