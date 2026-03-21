"""
Step 7: Apply Corrections to OCR Output — the final step.

This is the last step in the pipeline. It:

  1. Crawls corrected_files/ and reruns/ to collect every fix result
  2. Groups fixes by OCR line location (document, page, line number)
  3. Votes on the best corrected_line for each line:
       - All NULL → no correction needed (stable false positive)
       - One value has majority → use it
       - Tie or split → skip (marked as unresolved)
  4. Copies the OCR output to data/patched_output/ and patches lines there
  5. Writes detailed reports of every decision

Input:
    data/corrected_files/  (step 1 output)
    data/reruns/           (step 4 output)
    data/output/           (original OCR output)

Output:
    data/patched_output/               (patched copy of OCR output)
    analysis_output/patch_report.csv   (every line considered + decision)
    analysis_output/patch_summary.csv  (aggregate stats)

Usage:
    python step7_apply_corrections.py             # apply patches
    python step7_apply_corrections.py --dry-run   # preview without writing
    python step7_apply_corrections.py --report    # generate report only
"""

import os
import json
import csv
import re
import shutil
import argparse
from collections import defaultdict


# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR          = os.path.dirname(os.path.abspath(__file__))
DATA_DIR            = os.path.join(SCRIPT_DIR, "data")
CORRECTED_FILES_DIR = os.path.join(DATA_DIR, "corrected_files")
RERUNS_DIR          = os.path.join(DATA_DIR, "reruns")
OCR_OUTPUT_DIR      = os.path.join(DATA_DIR, "output")
PATCHED_OUTPUT_DIR  = os.path.join(DATA_DIR, "patched_output")
ANALYSIS_DIR        = os.path.join(SCRIPT_DIR, "analysis_output")
os.makedirs(ANALYSIS_DIR, exist_ok=True)

# Regex to match fix result filenames
# Format: error_{name}_{eid}_fix_{attempt}_{model}_{pass}.json
FIX_FILENAME_PATTERN = re.compile(
    r"^error_(?:([a-zA-Z]+)_)?(\d+)_fix_(\d+)_(.+)_(\d+)\.json$"
)

# Regex to match error metadata filenames (NOT fix files)
METADATA_FILENAME_PATTERN = re.compile(
    r"^error_(?:[a-zA-Z]+_)?\d+\.json$"
)


# ── Helper functions ───────────────────────────────────────────────────────────

def normalize_line(raw_value):
    """Normalize a corrected_line value to a consistent format."""
    if raw_value is None:
        return "NULL"
    cleaned = str(raw_value).strip()
    return cleaned if cleaned else "NULL"


def collect_all_fix_results_from_directory(base_directory):
    """
    Walk a directory tree (corrected_files/ or reruns/) and collect every
    fix result file found.

    For each fix file, we also load the corresponding error metadata file
    (same folder) to get line location info (document, page, line number).

    Returns a list of dicts, one per fix file.
    """
    collected_results = []

    for folder_root, _, folder_files in os.walk(base_directory):

        # First, find and load the error metadata file in this folder
        error_metadata = None
        for filename in folder_files:
            # Skip fix files, layout/read JSON, and decision summaries
            if filename.endswith(".json") and "_fix_" not in filename:
                if filename not in ("layout.json", "read.json", "decision_summary.json"):
                    if METADATA_FILENAME_PATTERN.match(filename):
                        try:
                            with open(os.path.join(folder_root, filename), encoding="utf-8") as file_handle:
                                error_metadata = json.load(file_handle)
                        except (json.JSONDecodeError, OSError):
                            pass
                        break

        if error_metadata is None:
            continue  # No metadata = can't identify which line this error belongs to

        # Now process each fix file in the same folder
        for filename in folder_files:
            regex_match = FIX_FILENAME_PATTERN.match(filename)
            if not regex_match:
                continue

            try:
                with open(os.path.join(folder_root, filename), encoding="utf-8") as file_handle:
                    fix_response = json.load(file_handle)
            except (json.JSONDecodeError, OSError):
                continue

            collected_results.append({
                "source_document": error_metadata.get("source_document", ""),
                "page": error_metadata.get("page_number", 0),
                "line": error_metadata.get("line_number", 0),
                "error_id": error_metadata.get("error_id", 0),
                "error_type": error_metadata.get("error_type", ""),
                "error_token": error_metadata.get("error", ""),
                "ocr_line_text": error_metadata.get("context", {}).get("line_text", ""),
                "source_name": regex_match.group(1) or "unknown",
                "attempt_number": int(regex_match.group(3)),
                "model": regex_match.group(4),
                "needs_correction": fix_response.get("needs_correction", False),
                "needs_error_correction": fix_response.get("needs_error_correction", False),
                "needs_context_correction": fix_response.get("needs_context_correction", False),
                "corrected_line": fix_response.get("corrected_line", "NULL"),
                "source_tree": os.path.basename(base_directory),
                "fix_filename": filename,
            })

    return collected_results


def decide_best_correction_for_line(all_fixes_for_line):
    """
    Given all fix results for a single OCR line, vote on the best correction.

    Voting strategy:
      1. Count votes for each distinct corrected_line value
      2. All NULL → no correction needed (stable false positive)
      3. One non-NULL value has strict majority → use it
      4. Tie or split → unresolved (needs manual review)

    Returns: (decision, corrected_line, confidence, explanation)
      decision:       "patch" | "no_correction" | "unresolved"
      corrected_line: the chosen text (or "NULL" if no correction)
      confidence:     float 0.0-1.0 (fraction of votes that agree)
      explanation:    human-readable string explaining the decision
    """
    # Count votes for each distinct corrected_line value
    vote_counts = defaultdict(int)
    for fix in all_fixes_for_line:
        normalized = normalize_line(fix["corrected_line"])
        vote_counts[normalized] += 1

    total_votes = len(all_fixes_for_line)
    sorted_votes = sorted(vote_counts.items(), key=lambda pair: -pair[1])

    # Case 1: Every single fix said NULL → this line doesn't need correction
    if len(sorted_votes) == 1 and sorted_votes[0][0] == "NULL":
        return ("no_correction", "NULL", 1.0,
                f"All {total_votes} attempts say no correction needed")

    # Separate NULL votes from actual correction votes
    non_null_votes = [(line_text, count) for line_text, count in sorted_votes if line_text != "NULL"]
    null_vote_count = vote_counts.get("NULL", 0)

    if not non_null_votes:
        return ("no_correction", "NULL", 1.0,
                f"All {total_votes} attempts returned NULL")

    best_line_text, best_vote_count = non_null_votes[0]

    # Case 2: One non-NULL value has strict majority of ALL votes (including NULLs)
    if best_vote_count > total_votes / 2:
        confidence = best_vote_count / total_votes
        return ("patch", best_line_text, confidence,
                f"{best_vote_count}/{total_votes} attempts agree on correction "
                f"(confidence {confidence:.0%})")

    # Case 3: Best non-NULL value beats NULL count, and it's the only non-NULL value
    if best_vote_count > null_vote_count and len(non_null_votes) == 1:
        confidence = best_vote_count / total_votes
        return ("patch", best_line_text, confidence,
                f"{best_vote_count}/{total_votes} for correction vs {null_vote_count} NULL "
                f"(weak majority, confidence {confidence:.0%})")

    # Case 4: Genuine tie or multi-way split → unresolved
    detail_parts = []
    for line_text, count in sorted_votes:
        preview = line_text[:40] + "..." if len(line_text) > 40 else line_text
        detail_parts.append(f'"{preview}" x{count}')
    return ("unresolved", "", 0.0,
            f"No majority: {'; '.join(detail_parts)}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Apply stable corrections to OCR output")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview patches without writing any files")
    parser.add_argument("--report", action="store_true",
                        help="Only generate report CSVs, don't patch files")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("STEP 7: APPLY CORRECTIONS TO OCR OUTPUT")
    print("=" * 70 + "\n")

    # ── Collect all fix results ────────────────────────────────────────────────
    print("Collecting fix results from corrected_files/ and reruns/...", flush=True)

    all_fix_results = []
    for source_directory in [CORRECTED_FILES_DIR, RERUNS_DIR]:
        if os.path.isdir(source_directory):
            fixes_from_dir = collect_all_fix_results_from_directory(source_directory)
            all_fix_results.extend(fixes_from_dir)
            print(f"  {os.path.basename(source_directory)}: {len(fixes_from_dir):,} fix files", flush=True)

    if not all_fix_results:
        print("No fix files found. Nothing to patch.")
        return

    # ── Group by OCR line ──────────────────────────────────────────────────────
    # Multiple error_ids can point to the same line — group them
    fixes_grouped_by_line = defaultdict(list)
    for fix in all_fix_results:
        line_key = (fix["source_document"], fix["page"], fix["line"])
        fixes_grouped_by_line[line_key].append(fix)

    print(f"\n  {len(all_fix_results):,} total fix files -> {len(fixes_grouped_by_line):,} unique OCR lines\n", flush=True)

    # ── Vote on best correction for each line ──────────────────────────────────
    patch_report_rows = []
    for (document, page, line), line_fixes in sorted(fixes_grouped_by_line.items()):
        decision, corrected_line, confidence, explanation = decide_best_correction_for_line(line_fixes)

        # Collect metadata about what errors affect this line
        error_ids    = sorted(set(fix["error_id"] for fix in line_fixes))
        error_types  = sorted(set(fix["error_type"] for fix in line_fixes if fix["error_type"]))
        error_tokens = sorted(set(fix["error_token"] for fix in line_fixes if fix["error_token"]))
        source_names = sorted(set(fix["source_name"] for fix in line_fixes))
        attempts     = sorted(set(fix["attempt_number"] for fix in line_fixes))
        models       = sorted(set(fix["model"] for fix in line_fixes))
        source_trees = sorted(set(fix["source_tree"] for fix in line_fixes))
        ocr_line_text = line_fixes[0]["ocr_line_text"]

        # All distinct corrected_line values with their vote counts
        vote_counts = defaultdict(int)
        for fix in line_fixes:
            vote_counts[normalize_line(fix["corrected_line"])] += 1

        yes_votes = sum(1 for fix in line_fixes if fix["needs_correction"])
        no_votes  = len(line_fixes) - yes_votes

        patch_report_rows.append({
            "source_document": document,
            "page": page,
            "line": line,
            "ocr_line_text": ocr_line_text,
            "decision": decision,
            "corrected_line": corrected_line,
            "confidence": f"{confidence:.4f}",
            "detail": explanation,
            "total_fix_files": len(line_fixes),
            "needs_correction_yes": yes_votes,
            "needs_correction_no": no_votes,
            "error_ids": "; ".join(str(eid) for eid in error_ids),
            "error_types": "; ".join(error_types),
            "error_tokens": "; ".join(error_tokens),
            "sources": "; ".join(source_names),
            "attempts": "; ".join(str(a) for a in attempts),
            "models": "; ".join(models),
            "from_trees": "; ".join(source_trees),
            "distinct_corrections": "; ".join(
                f'"{line_text}" x{count}'
                for line_text, count in sorted(vote_counts.items(), key=lambda pair: -pair[1])
            ),
        })

    # ── Write patch_report.csv ─────────────────────────────────────────────────
    report_csv_columns = [
        "source_document", "page", "line", "ocr_line_text",
        "decision", "corrected_line", "confidence", "detail",
        "total_fix_files", "needs_correction_yes", "needs_correction_no",
        "error_ids", "error_types", "error_tokens",
        "sources", "attempts", "models", "from_trees", "distinct_corrections",
    ]
    report_csv_path = os.path.join(ANALYSIS_DIR, "patch_report.csv")
    with open(report_csv_path, "w", newline="", encoding="utf-8") as file_handle:
        csv_writer = csv.DictWriter(file_handle, fieldnames=report_csv_columns)
        csv_writer.writeheader()
        csv_writer.writerows(patch_report_rows)

    # ── Compute and write patch_summary.csv ────────────────────────────────────
    lines_to_patch     = sum(1 for row in patch_report_rows if row["decision"] == "patch")
    lines_no_correction = sum(1 for row in patch_report_rows if row["decision"] == "no_correction")
    lines_unresolved   = sum(1 for row in patch_report_rows if row["decision"] == "unresolved")

    summary_csv_path = os.path.join(ANALYSIS_DIR, "patch_summary.csv")
    with open(summary_csv_path, "w", newline="", encoding="utf-8") as file_handle:
        csv_writer = csv.writer(file_handle)
        csv_writer.writerow(["metric", "value"])
        csv_writer.writerow(["total_ocr_lines_analyzed", len(patch_report_rows)])
        csv_writer.writerow(["lines_to_patch", lines_to_patch])
        csv_writer.writerow(["lines_no_correction_needed", lines_no_correction])
        csv_writer.writerow(["lines_unresolved", lines_unresolved])
        csv_writer.writerow(["patch_rate",
                             f"{lines_to_patch / len(patch_report_rows):.4f}" if patch_report_rows else "N/A"])
        csv_writer.writerow(["no_correction_rate",
                             f"{lines_no_correction / len(patch_report_rows):.4f}" if patch_report_rows else "N/A"])
        csv_writer.writerow(["unresolved_rate",
                             f"{lines_unresolved / len(patch_report_rows):.4f}" if patch_report_rows else "N/A"])
        patch_confidences = [float(row["confidence"]) for row in patch_report_rows if row["decision"] == "patch"]
        csv_writer.writerow(["avg_patch_confidence",
                             f"{sum(patch_confidences) / len(patch_confidences):.4f}" if patch_confidences else "N/A"])

    # ── Print summary ──────────────────────────────────────────────────────────
    print(f"  Patch decisions:")
    print(f"    Patch (stable correction):  {lines_to_patch:,}")
    print(f"    No correction needed:       {lines_no_correction:,}")
    print(f"    Unresolved (need review):   {lines_unresolved:,}")
    if patch_confidences:
        print(f"    Avg patch confidence:       {sum(patch_confidences) / len(patch_confidences):.1%}")

    if args.report:
        print(f"\n  Report-only mode. CSVs written to {ANALYSIS_DIR}/")
        return

    # ── Apply patches ──────────────────────────────────────────────────────────
    patches_to_apply = [row for row in patch_report_rows if row["decision"] == "patch"]
    if not patches_to_apply:
        print("\n  No patches to apply.")
        return

    if args.dry_run:
        print(f"\n  DRY RUN — would patch {len(patches_to_apply)} lines:\n")
        for row in patches_to_apply[:15]:
            ocr_preview = row["ocr_line_text"][:60] + "..." if len(row["ocr_line_text"]) > 60 else row["ocr_line_text"]
            fix_preview = row["corrected_line"][:60] + "..." if len(row["corrected_line"]) > 60 else row["corrected_line"]
            print(f"    {row['source_document']} p{row['page']} L{row['line']}")
            print(f"      OCR: \"{ocr_preview}\"")
            print(f"      FIX: \"{fix_preview}\" (confidence {row['confidence']})")
        if len(patches_to_apply) > 15:
            print(f"    ... and {len(patches_to_apply) - 15} more.")
        return

    # Group patches by (document, page) to minimize file reads
    patches_by_page = defaultdict(list)
    for row in patches_to_apply:
        document_name = row["source_document"].replace(".pdf", "")
        patches_by_page[(document_name, row["page"])].append(row)

    # Copy original OCR output to patched_output/ (fresh copy every time)
    print(f"\n  Copying OCR output to {PATCHED_OUTPUT_DIR}/ for patching...", flush=True)
    if os.path.exists(PATCHED_OUTPUT_DIR):
        shutil.rmtree(PATCHED_OUTPUT_DIR)
    shutil.copytree(OCR_OUTPUT_DIR, PATCHED_OUTPUT_DIR)

    lines_patched = 0
    lines_skipped = 0

    for (document_name, page_number), page_patches in sorted(patches_by_page.items()):
        page_directory = os.path.join(PATCHED_OUTPUT_DIR, document_name, f"page-{page_number}")

        # Patch both layout.json and read.json
        for json_filename in ("layout.json", "read.json"):
            json_file_path = os.path.join(page_directory, json_filename)
            if not os.path.exists(json_file_path):
                continue

            with open(json_file_path, "r", encoding="utf-8") as file_handle:
                ocr_data = json.load(file_handle)

            ocr_lines = ocr_data.get("pages", [{}])[0].get("lines", [])

            for row in page_patches:
                line_index = int(row["line"]) - 1  # Convert 1-indexed to 0-indexed
                if 0 <= line_index < len(ocr_lines):
                    ocr_lines[line_index]["content"] = row["corrected_line"]
                    if json_filename == "layout.json":  # Count once per line, not twice
                        lines_patched += 1
                else:
                    if json_filename == "layout.json":
                        lines_skipped += 1

            with open(json_file_path, "w", encoding="utf-8") as file_handle:
                json.dump(ocr_data, file_handle, indent=2, ensure_ascii=False)

    # ── Final report ───────────────────────────────────────────────────────────
    print(f"\n  Applied {lines_patched:,} line patches to {PATCHED_OUTPUT_DIR}/")
    if lines_skipped:
        print(f"  Skipped {lines_skipped:,} (line index out of range in JSON)")
    print(f"  Reports written to {ANALYSIS_DIR}/")
    print(f"\n  Original OCR:  {OCR_OUTPUT_DIR}/")
    print(f"  Patched OCR:   {PATCHED_OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
