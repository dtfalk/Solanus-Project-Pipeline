"""
Step 3: Move Disagreements — isolate errors where correction attempts disagree.

After step 2 shows which errors have different corrected_line text across
attempts, this script copies those error folders to data/reruns/ so they
can be re-processed with additional context in step 4.

A "disagreement" means the LLM gave different corrected_line text in
attempt 1 vs attempt 2. This includes:
  - One attempt says NULL (no correction), the other gives actual text
  - Both attempts give different corrected text

For each moved folder, a comprehensive decision_summary.json is generated
containing everything needed for re-prompting:
  - Full original error metadata
  - Every fix attempt result
  - Voting breakdown
  - The original prompt text
  - OCR context

Usage:
    python step3_move_disagreements.py              # move disagreeing folders
    python step3_move_disagreements.py --dry-run    # preview only
"""

import os
import re
import json
import shutil
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from prompts import ERROR_TYPE_HINTS, SYSTEM_PROMPT, DEFAULT_HINT

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR       = os.path.join(SCRIPT_DIR, "data")
CORRECTED_DIR  = os.path.join(DATA_DIR, "corrected_files")
RERUNS_DIR     = os.path.join(DATA_DIR, "reruns")

# Regex patterns for matching fix files and error metadata files
FIX_FILENAME_PATTERN  = re.compile(r"^error_(?:([a-zA-Z]+)_)?(\d+)_fix_(\d+)_(.+)_(\d+)\.json$")
META_FILENAME_PATTERN = re.compile(r"^error_(?:[a-zA-Z]+_)?\d+\.json$")


def normalize_corrected_line(line_text):
    """Normalize a corrected_line value for comparison."""
    if line_text is None:
        return "NULL"
    line_text = str(line_text).strip()
    return line_text if line_text else "NULL"


def scan_directory_for_disagreements(directory_path, file_list):
    """
    Read all fix files from a single error directory and check if the
    corrected_line values disagree across attempts.

    Returns None if fewer than 2 fix files or all corrected_lines agree.
    Otherwise returns (directory_path, summary_dict).
    """
    # ── Find the error metadata JSON ──
    error_metadata = None
    metadata_filename = None
    for filename in file_list:
        if filename.endswith(".json") and "_fix_" not in filename and filename not in ("layout.json", "read.json"):
            if META_FILENAME_PATTERN.match(filename):
                metadata_filename = filename
                with open(os.path.join(directory_path, filename)) as file_handle:
                    error_metadata = json.load(file_handle)
                break

    # ── Read all fix files ──
    fix_results = []
    for filename in sorted(file_list):
        regex_match = FIX_FILENAME_PATTERN.match(filename)
        if not regex_match:
            continue
        with open(os.path.join(directory_path, filename)) as file_handle:
            fix_data = json.load(file_handle)
        fix_results.append({
            "filename": filename,
            "name": regex_match.group(1) or "unknown",
            "error_id": int(regex_match.group(2)),
            "attempt": int(regex_match.group(3)),
            "model": regex_match.group(4),
            "pass_num": int(regex_match.group(5)),
            "response": fix_data,
        })

    # Need at least 2 fix files to compare
    if len(fix_results) < 2:
        return None

    # ── Check if corrected_line values disagree ──
    corrected_lines = [normalize_corrected_line(fix["response"].get("corrected_line")) for fix in fix_results]
    unique_lines = set(corrected_lines)
    if len(unique_lines) <= 1:
        return None  # All attempts agree — nothing to move

    # ── Build voting breakdown ──
    yes_votes = sum(1 for fix in fix_results if fix["response"].get("needs_correction"))
    no_votes = len(fix_results) - yes_votes
    yes_error = sum(1 for fix in fix_results if fix["response"].get("needs_error_correction"))
    yes_context = sum(1 for fix in fix_results if fix["response"].get("needs_context_correction"))

    # ── Reconstruct the original prompt ──
    error_type  = error_metadata.get("error_type", "") if error_metadata else ""
    error_token = error_metadata.get("error", "") if error_metadata else ""
    ocr_line    = error_metadata.get("context", {}).get("line_text", "") if error_metadata else ""
    hint_text   = ERROR_TYPE_HINTS.get(error_type, DEFAULT_HINT)

    user_prompt_text = (
        f"Error type: {error_type}\n"
        f"Flagged token: \"{error_token}\"\n"
        f"OCR line text: \"{ocr_line}\"\n"
        f"Hint: {hint_text}\n\n"
        "Image 1: full line context. Image 2: flagged token.\n"
        "Does the line need correction? Respond with EXACT JSON only."
    )

    # ── Count votes for each distinct corrected_line ──
    line_vote_counts = defaultdict(int)
    for fix in fix_results:
        corrected = normalize_corrected_line(fix["response"].get("corrected_line"))
        line_vote_counts[corrected] += 1

    # ── Build the comprehensive summary ──
    summary = {
        "folder_path": directory_path,
        "fix_file_count": len(fix_results),
        "error_metadata": error_metadata,
        "meta_filename": metadata_filename,
        "attempts": [
            {
                "filename": fix["filename"],
                "name": fix["name"],
                "error_id": fix["error_id"],
                "attempt_number": fix["attempt"],
                "model": fix["model"],
                "pass_num": fix["pass_num"],
                "full_response": fix["response"],
            }
            for fix in fix_results
        ],
        "voting_breakdown": {
            "total_attempts": len(fix_results),
            "needs_correction_yes": yes_votes,
            "needs_correction_no": no_votes,
            "needs_error_correction_yes": yes_error,
            "needs_context_correction_yes": yes_context,
        },
        "corrected_line_analysis": {
            "all_agree": False,
            "unique_corrected_lines": len(unique_lines),
            "distinct_values": [
                {"corrected_line": line, "vote_count": count}
                for line, count in sorted(line_vote_counts.items(), key=lambda x: -x[1])
            ],
        },
        "original_prompt": {
            "system_prompt": SYSTEM_PROMPT,
            "user_prompt_text": user_prompt_text,
            "hint_used": hint_text,
            "note": "Images (context + error crop) were also sent but are stored as .png files in this folder.",
        },
        "ocr_context": {
            "ocr_line_text": ocr_line,
            "flagged_token": error_token,
            "error_type": error_type,
            "source_document": error_metadata.get("source_document", "") if error_metadata else "",
            "page_number": error_metadata.get("page_number", "") if error_metadata else "",
            "line_number": error_metadata.get("line_number", "") if error_metadata else "",
        },
    }
    return (directory_path, summary)


def main():
    parser = argparse.ArgumentParser(description="Move corrected_line disagreements to reruns/")
    parser.add_argument("--dry-run", action="store_true", help="Preview moves without executing them")
    args = parser.parse_args()

    print("Scanning corrected_files for corrected_line disagreements...", flush=True)

    # Collect all directories in the corrected_files tree
    all_directories = [(root, files) for root, _, files in os.walk(CORRECTED_DIR)]
    print(f"  Found {len(all_directories):,} directories to scan.", flush=True)

    # Scan in parallel for speed
    folders_to_move = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(scan_directory_for_disagreements, root, files): root
            for root, files in all_directories
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                folders_to_move.append(result)

    print(f"\n  Found {len(folders_to_move):,} error folders with corrected_line disagreements.", flush=True)

    if not folders_to_move:
        print("  Nothing to move.")
        return

    folders_to_move.sort(key=lambda x: x[0])

    # Show breakdown by fix file count
    count_breakdown = defaultdict(int)
    for _, summary in folders_to_move:
        count_breakdown[summary["fix_file_count"]] += 1
    print("\n  Disagreement breakdown by fix-file count:")
    for count in sorted(count_breakdown.keys()):
        print(f"    {count} fix files: {count_breakdown[count]:,} folders")

    if args.dry_run:
        print("\n  DRY RUN — the following folders would be moved:\n")
        for source_path, summary in folders_to_move[:20]:
            relative_path = os.path.relpath(source_path, CORRECTED_DIR)
            votes = summary["voting_breakdown"]
            lines = summary["corrected_line_analysis"]["distinct_values"]
            print(f"    {relative_path}")
            print(f"      fix_files: {summary['fix_file_count']}  "
                  f"yes: {votes['needs_correction_yes']}  no: {votes['needs_correction_no']}")
            for line_vote in lines:
                preview = line_vote["corrected_line"][:60] + "..." if len(line_vote["corrected_line"]) > 60 else line_vote["corrected_line"]
                print(f"      -> \"{preview}\" (x{line_vote['vote_count']})")
        if len(folders_to_move) > 20:
            print(f"    ... and {len(folders_to_move) - 20} more.")
        return

    # ── Actually copy the folders ──
    moved_count = 0
    for source_path, summary in folders_to_move:
        relative_path = os.path.relpath(source_path, CORRECTED_DIR)
        destination_path = os.path.join(RERUNS_DIR, relative_path)

        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.copytree(source_path, destination_path, dirs_exist_ok=True)

        # Write the decision_summary.json into the copied folder
        with open(os.path.join(destination_path, "decision_summary.json"), "w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2, ensure_ascii=False)

        moved_count += 1
        if moved_count % 500 == 0:
            print(f"  Moved {moved_count:,}/{len(folders_to_move):,}...", flush=True)

    print(f"\n  Done: moved {moved_count:,} error folders to {RERUNS_DIR}")
    print(f"  Each folder contains:")
    print(f"    - Original error metadata JSON")
    print(f"    - All previous fix attempt JSONs (preserved)")
    print(f"    - Context + error crop images (.png)")
    print(f"    - decision_summary.json (voting, prompts, all responses, OCR context)")
    print(f"\n  Next step: run step4_rerun_with_context.py to re-process them.")


if __name__ == "__main__":
    main()
