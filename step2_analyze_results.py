"""
Step 2: Analyze Results — compare correction attempts and generate reports.

After running step 1 twice, this script crawls the corrected_files/ directory,
collects all LLM fix results, and produces detailed CSV reports:

  - summary.csv:                         Overall stats + per-attempt false positive rates
  - per_error.csv:                       Every error with each attempt's result
  - disagreements.csv:                   Errors where attempts disagree
  - by_error_type.csv:                   Breakdown by error type
  - convergence.csv:                     Lines flagged by BOTH stella and nathan
  - convergence_summary.csv:             Aggregate convergence stats
  - corrected_line_agreement.csv:        Per-error text agreement across attempts
  - corrected_line_disagreements.csv:    Errors where corrected_line text differs
  - corrected_line_summary.csv:          Aggregate text agreement stats
  - corrected_line_by_error_type.csv:    Text disagreements by error type
  - folder_fix_counts.csv:              How many fix files each error folder has

Usage:
    python step2_analyze_results.py
"""

import os
import json
import csv
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR       = os.path.join(SCRIPT_DIR, "data")
CORRECTED_DIR  = os.path.join(DATA_DIR, "corrected_files")
OUTPUT_DIR     = os.path.join(SCRIPT_DIR, "analysis_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Regex to parse fix filenames like: error_stella_42_fix_1_gpt-5-nano_1.json
FIX_FILENAME_PATTERN = re.compile(r"^error_(?:([a-zA-Z]+)_)?(\d+)_fix_(\d+)_(.+)_(\d+)\.json$")


def normalize_corrected_line(line_text):
    """Normalize a corrected_line value for comparison (strip whitespace)."""
    if line_text is None:
        return "NULL"
    line_text = str(line_text).strip()
    return line_text if line_text else "NULL"


# ── Phase 1: Read all fix files from the directory tree ────────────────────────

def read_fix_files_from_directory(directory_path, file_list):
    """
    Read the error metadata and all fix result files from a single error directory.
    Returns a list of row dicts, one per fix file found.
    """
    rows_from_this_directory = []

    # Find the error metadata JSON (not a fix file, not layout/read)
    error_metadata = {}
    for filename in file_list:
        if filename.endswith(".json") and "_fix_" not in filename and filename not in ("layout.json", "read.json"):
            with open(os.path.join(directory_path, filename)) as file_handle:
                error_metadata = json.load(file_handle)
            break

    # Read each fix file
    fix_file_count = 0
    for filename in file_list:
        regex_match = FIX_FILENAME_PATTERN.match(filename)
        if not regex_match:
            continue
        fix_file_count += 1

        with open(os.path.join(directory_path, filename)) as file_handle:
            fix_result = json.load(file_handle)

        rows_from_this_directory.append({
            "name":             regex_match.group(1) or "unknown",
            "error_id":         int(regex_match.group(2)),
            "attempt":          int(regex_match.group(3)),
            "model":            regex_match.group(4),
            "pass_num":         int(regex_match.group(5)),
            "needs_correction": fix_result["needs_correction"],
            "corrected_line":   fix_result["corrected_line"],
            "error_type":       error_metadata.get("error_type", ""),
            "source_document":  error_metadata.get("source_document", ""),
            "page":             error_metadata.get("page_number", ""),
            "line":             error_metadata.get("line_number", ""),
            "error_token":      error_metadata.get("error", ""),
            "ocr_line":         error_metadata.get("context", {}).get("line_text", ""),
            "folder_path":      directory_path,
            "fix_file_count":   0,  # filled in below
        })

    # Set fix_file_count on all rows from this directory
    for row in rows_from_this_directory:
        row["fix_file_count"] = fix_file_count
    return rows_from_this_directory


# ── Walk directory tree and load all data ──────────────────────────────────────
print("  Walking directory tree...", flush=True)
all_directories = [(root, files) for root, _, files in os.walk(CORRECTED_DIR)]
print(f"  Found {len(all_directories):,} directories. Loading JSON files...", flush=True)

all_rows = []
directories_completed = 0
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = {
        executor.submit(read_fix_files_from_directory, root, files): index
        for index, (root, files) in enumerate(all_directories)
    }
    for future in as_completed(futures):
        all_rows.extend(future.result())
        directories_completed += 1
        if directories_completed % 10000 == 0:
            print(f"  Processed {directories_completed:,}/{len(all_directories):,} directories "
                  f"({len(all_rows):,} fix files loaded)", flush=True)

print(f"  Done: {len(all_rows):,} fix files loaded from {len(all_directories):,} directories.", flush=True)

if not all_rows:
    print("No fix files found in", CORRECTED_DIR)
    exit()

# ── Group rows by (source_name, error_id) ──────────────────────────────────────
errors_grouped_by_id = defaultdict(list)
for row in all_rows:
    errors_grouped_by_id[(row["name"], row["error_id"])].append(row)

all_attempt_numbers = sorted(set(row["attempt"] for row in all_rows))
print(f"Found {len(all_rows)} fix files for {len(errors_grouped_by_id)} unique errors across attempts {all_attempt_numbers}")

# ── folder_fix_counts.csv ──────────────────────────────────────────────────────
# Distribution: how many fix files does each error folder have?
fix_count_distribution = defaultdict(int)
for (source_name, error_id), entries in errors_grouped_by_id.items():
    fix_count_distribution[entries[0]["fix_file_count"]] += 1

with open(os.path.join(OUTPUT_DIR, "folder_fix_counts.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["fix_files_in_folder", "num_folders", "pct_of_total"])
    total_unique_errors = len(errors_grouped_by_id)
    for count in sorted(fix_count_distribution.keys()):
        num_folders = fix_count_distribution[count]
        writer.writerow([count, num_folders, f"{num_folders / total_unique_errors:.4f}"])

print(f"\n  Fix-file-count distribution:")
for count in sorted(fix_count_distribution.keys()):
    num_folders = fix_count_distribution[count]
    print(f"    {count} fix files: {num_folders:,} folders ({num_folders / total_unique_errors:.1%})")

# ── per_error.csv ──────────────────────────────────────────────────────────────
with open(os.path.join(OUTPUT_DIR, "per_error.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    header = [
        "name", "error_id", "error_type", "source_document", "page", "line",
        "error_token", "ocr_line", "num_attempts", "fix_file_count", "all_agree",
        "majority_correction",
    ]
    for attempt_num in all_attempt_numbers:
        header += [f"att{attempt_num}_correction", f"att{attempt_num}_corrected_line", f"att{attempt_num}_model"]
    writer.writerow(header)

    for (source_name, error_id), entries in sorted(errors_grouped_by_id.items()):
        entries_by_attempt = {row["attempt"]: row for row in entries}
        correction_votes = [row["needs_correction"] for row in entries]
        first_entry = entries[0]
        csv_row = [
            source_name, error_id, first_entry["error_type"], first_entry["source_document"],
            first_entry["page"], first_entry["line"], first_entry["error_token"], first_entry["ocr_line"],
            len(entries), first_entry["fix_file_count"],
            len(set(correction_votes)) == 1,     # all_agree
            sum(correction_votes) > len(correction_votes) / 2,  # majority_correction
        ]
        for attempt_num in all_attempt_numbers:
            entry = entries_by_attempt.get(attempt_num)
            csv_row += [entry["needs_correction"], entry["corrected_line"], entry["model"]] if entry else ["", "", ""]
        writer.writerow(csv_row)

# ── summary.csv ────────────────────────────────────────────────────────────────
errors_that_agree = sum(
    1 for entries in errors_grouped_by_id.values()
    if len(set(row["needs_correction"] for row in entries)) == 1
)
errors_that_disagree = len(errors_grouped_by_id) - errors_that_agree

with open(os.path.join(OUTPUT_DIR, "summary.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["metric", "value"])
    writer.writerow(["total_unique_errors", len(errors_grouped_by_id)])
    writer.writerow(["attempts_found", str(all_attempt_numbers)])
    writer.writerow(["all_agree", errors_that_agree])
    writer.writerow(["disagreements", errors_that_disagree])
    writer.writerow(["agreement_rate", f"{errors_that_agree / len(errors_grouped_by_id):.4f}"])
    for count in sorted(fix_count_distribution.keys()):
        writer.writerow([f"folders_with_{count}_fix_files", fix_count_distribution[count]])
    for attempt_num in all_attempt_numbers:
        attempt_rows = [row for row in all_rows if row["attempt"] == attempt_num]
        total_in_attempt = len(attempt_rows)
        needed_correction = sum(1 for row in attempt_rows if row["needs_correction"])
        no_correction = total_in_attempt - needed_correction
        writer.writerow([f"att{attempt_num}_total", total_in_attempt])
        writer.writerow([f"att{attempt_num}_needs_correction", needed_correction])
        writer.writerow([f"att{attempt_num}_no_correction", no_correction])
        writer.writerow([f"att{attempt_num}_false_positive_rate", f"{no_correction / total_in_attempt:.4f}" if total_in_attempt else "N/A"])

# ── disagreements.csv ──────────────────────────────────────────────────────────
with open(os.path.join(OUTPUT_DIR, "disagreements.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "name", "error_id", "error_type", "source_document", "page", "line",
        "error_token", "yes_votes", "no_votes",
    ])
    for (source_name, error_id), entries in sorted(errors_grouped_by_id.items()):
        votes = [row["needs_correction"] for row in entries]
        if len(set(votes)) == 1:
            continue  # skip agreements
        first_entry = entries[0]
        writer.writerow([
            source_name, error_id, first_entry["error_type"], first_entry["source_document"],
            first_entry["page"], first_entry["line"], first_entry["error_token"],
            sum(votes), len(votes) - sum(votes),
        ])

# ── by_error_type.csv ──────────────────────────────────────────────────────────
stats_by_error_type = defaultdict(lambda: {"total": 0, "correction": 0, "no_correction": 0, "disagree": 0})
for entries in errors_grouped_by_id.values():
    error_type = entries[0]["error_type"] or "unknown"
    stats_by_error_type[error_type]["total"] += 1
    votes = [row["needs_correction"] for row in entries]
    if len(set(votes)) > 1:
        stats_by_error_type[error_type]["disagree"] += 1
    elif votes[0]:
        stats_by_error_type[error_type]["correction"] += 1
    else:
        stats_by_error_type[error_type]["no_correction"] += 1

with open(os.path.join(OUTPUT_DIR, "by_error_type.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["error_type", "total", "needs_correction", "no_correction", "disagreements", "false_positive_rate"])
    for error_type, stats in sorted(stats_by_error_type.items()):
        false_positive_rate = stats["no_correction"] / stats["total"] if stats["total"] else 0
        writer.writerow([error_type, stats["total"], stats["correction"], stats["no_correction"], stats["disagree"], f"{false_positive_rate:.4f}"])

# ── Convergence analysis (lines flagged by BOTH stella AND nathan) ─────────────
# Build lookup: (doc, page, line) → {source_name → [entries]}
errors_by_document_location = defaultdict(lambda: defaultdict(list))
for (source_name, error_id), entries in errors_grouped_by_id.items():
    first_entry = entries[0]
    location_key = (first_entry["source_document"], first_entry["page"], first_entry["line"])
    errors_by_document_location[location_key][source_name].extend(entries)

# Find lines where BOTH sources flagged errors
convergence_rows = []
for location_key, entries_by_source in sorted(errors_by_document_location.items()):
    sources_present = set(name.lower() for name in entries_by_source.keys())
    if "stella" not in sources_present or "nathan" not in sources_present:
        continue

    document, page, line = location_key

    # For each source, take majority vote across attempts
    source_results = {}
    for source_name, entries in entries_by_source.items():
        votes = [row["needs_correction"] for row in entries]
        majority_says_correction = sum(votes) > len(votes) / 2
        # Find the corrected_line from a matching attempt
        corrected_line_text = ""
        for row in entries:
            if row["needs_correction"] == majority_says_correction and row["corrected_line"] != "NULL":
                corrected_line_text = row["corrected_line"]
                break
        source_results[source_name.lower()] = {
            "error_ids": sorted(set(row["error_id"] for row in entries)),
            "error_types": sorted(set(row["error_type"] for row in entries if row["error_type"])),
            "needs_correction": majority_says_correction,
            "corrected_line": corrected_line_text,
            "error_tokens": sorted(set(row["error_token"] for row in entries if row["error_token"])),
        }

    stella_result = source_results.get("stella", {})
    nathan_result = source_results.get("nathan", {})
    both_sources_agree = stella_result.get("needs_correction") == nathan_result.get("needs_correction")

    convergence_rows.append({
        "source_document": document, "page": page, "line": line,
        "ocr_line": entries[0]["ocr_line"],
        "stella_error_ids": "; ".join(str(x) for x in stella_result.get("error_ids", [])),
        "nathan_error_ids": "; ".join(str(x) for x in nathan_result.get("error_ids", [])),
        "stella_error_types": "; ".join(stella_result.get("error_types", [])),
        "nathan_error_types": "; ".join(nathan_result.get("error_types", [])),
        "stella_error_tokens": "; ".join(stella_result.get("error_tokens", [])),
        "nathan_error_tokens": "; ".join(nathan_result.get("error_tokens", [])),
        "stella_needs_correction": stella_result.get("needs_correction", ""),
        "nathan_needs_correction": nathan_result.get("needs_correction", ""),
        "sources_agree": both_sources_agree,
        "stella_corrected_line": stella_result.get("corrected_line", ""),
        "nathan_corrected_line": nathan_result.get("corrected_line", ""),
    })

# Write convergence.csv
convergence_header = [
    "source_document", "page", "line", "ocr_line",
    "stella_error_ids", "nathan_error_ids", "stella_error_types", "nathan_error_types",
    "stella_error_tokens", "nathan_error_tokens",
    "stella_needs_correction", "nathan_needs_correction", "sources_agree",
    "stella_corrected_line", "nathan_corrected_line",
]
with open(os.path.join(OUTPUT_DIR, "convergence.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=convergence_header)
    writer.writeheader()
    writer.writerows(convergence_rows)

# Convergence summary stats
total_convergence_lines = len(convergence_rows)
convergence_agree = sum(1 for row in convergence_rows if row["sources_agree"])
both_need_correction = sum(1 for row in convergence_rows if row["stella_needs_correction"] and row["nathan_needs_correction"])
both_no_correction = sum(1 for row in convergence_rows if not row["stella_needs_correction"] and not row["nathan_needs_correction"])
stella_only_correction = sum(1 for row in convergence_rows if row["stella_needs_correction"] and not row["nathan_needs_correction"])
nathan_only_correction = sum(1 for row in convergence_rows if not row["stella_needs_correction"] and row["nathan_needs_correction"])

with open(os.path.join(OUTPUT_DIR, "convergence_summary.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["metric", "value"])
    writer.writerow(["lines_flagged_by_both", total_convergence_lines])
    writer.writerow(["sources_agree", convergence_agree])
    writer.writerow(["sources_disagree", total_convergence_lines - convergence_agree])
    writer.writerow(["agreement_rate", f"{convergence_agree / total_convergence_lines:.4f}" if total_convergence_lines else "N/A"])
    writer.writerow(["both_need_correction", both_need_correction])
    writer.writerow(["both_no_correction", both_no_correction])
    writer.writerow(["stella_only_correction", stella_only_correction])
    writer.writerow(["nathan_only_correction", nathan_only_correction])

    # Lines flagged by only one source
    stella_only_lines = sum(1 for loc, by_name in errors_by_document_location.items()
                           if any(n.lower() == "stella" for n in by_name)
                           and not any(n.lower() == "nathan" for n in by_name))
    nathan_only_lines = sum(1 for loc, by_name in errors_by_document_location.items()
                           if any(n.lower() == "nathan" for n in by_name)
                           and not any(n.lower() == "stella" for n in by_name))
    writer.writerow(["stella_only_lines_total", stella_only_lines])
    writer.writerow(["nathan_only_lines_total", nathan_only_lines])
    writer.writerow(["overlap_lines_total", total_convergence_lines])

# ── Corrected-line text agreement analysis ─────────────────────────────────────
# Compare the actual corrected_line STRINGS across attempts (not just the boolean).
agreement_rows = []
disagreement_rows = []

for (source_name, error_id), entries in sorted(errors_grouped_by_id.items()):
    if len(entries) < 2:
        continue  # need at least 2 attempts to compare

    entries_by_attempt = {row["attempt"]: row for row in entries}
    corrected_lines_by_attempt = {attempt: normalize_corrected_line(row["corrected_line"]) for attempt, row in entries_by_attempt.items()}
    unique_corrected_lines = set(corrected_lines_by_attempt.values())
    all_lines_match = len(unique_corrected_lines) == 1

    first_entry = entries[0]
    row_data = {
        "name": source_name, "error_id": error_id,
        "error_type": first_entry["error_type"], "source_document": first_entry["source_document"],
        "page": first_entry["page"], "line": first_entry["line"],
        "error_token": first_entry["error_token"], "ocr_line": first_entry["ocr_line"],
        "num_attempts": len(entries), "corrected_lines_agree": all_lines_match,
        "unique_corrected_lines": len(unique_corrected_lines),
    }
    for attempt_num in all_attempt_numbers:
        row_data[f"att{attempt_num}_needs_correction"] = entries_by_attempt[attempt_num]["needs_correction"] if attempt_num in entries_by_attempt else ""
        row_data[f"att{attempt_num}_corrected_line"] = entries_by_attempt[attempt_num]["corrected_line"] if attempt_num in entries_by_attempt else ""

    agreement_rows.append(row_data)
    if not all_lines_match:
        disagreement_rows.append(row_data)

# Write corrected_line CSVs
corrected_line_header = [
    "name", "error_id", "error_type", "source_document", "page", "line",
    "error_token", "ocr_line", "num_attempts", "corrected_lines_agree", "unique_corrected_lines",
]
for attempt_num in all_attempt_numbers:
    corrected_line_header += [f"att{attempt_num}_needs_correction", f"att{attempt_num}_corrected_line"]

with open(os.path.join(OUTPUT_DIR, "corrected_line_agreement.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=corrected_line_header)
    writer.writeheader()
    writer.writerows(agreement_rows)

with open(os.path.join(OUTPUT_DIR, "corrected_line_disagreements.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=corrected_line_header)
    writer.writeheader()
    writer.writerows(disagreement_rows)

# Categorize corrected_line agreement/disagreement
both_null_count = 0
both_same_text_count = 0
mixed_null_and_text_count = 0
different_text_count = 0

for (source_name, error_id), entries in sorted(errors_grouped_by_id.items()):
    if len(entries) < 2:
        continue
    normalized_lines = [normalize_corrected_line(row["corrected_line"]) for row in entries]
    unique_lines = set(normalized_lines)
    if len(unique_lines) == 1:
        if "NULL" in unique_lines:
            both_null_count += 1
        else:
            both_same_text_count += 1
    else:
        if "NULL" in unique_lines:
            mixed_null_and_text_count += 1
        else:
            different_text_count += 1

total_compared = len(agreement_rows)
total_agree = both_null_count + both_same_text_count
total_disagree = mixed_null_and_text_count + different_text_count

# Write corrected_line_summary.csv
with open(os.path.join(OUTPUT_DIR, "corrected_line_summary.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["metric", "value"])
    writer.writerow(["errors_with_multiple_attempts", total_compared])
    writer.writerow(["corrected_line_agree", total_agree])
    writer.writerow(["corrected_line_disagree", total_disagree])
    writer.writerow(["agreement_rate", f"{total_agree / total_compared:.4f}" if total_compared else "N/A"])
    writer.writerow(["both_null_agree", both_null_count])
    writer.writerow(["both_same_text_agree", both_same_text_count])
    writer.writerow(["mixed_null_vs_text_disagree", mixed_null_and_text_count])
    writer.writerow(["different_text_disagree", different_text_count])

# By error type disagree breakdown
disagree_by_type = defaultdict(lambda: {"total": 0, "agree": 0, "disagree": 0, "mixed_null": 0, "diff_text": 0})
for (source_name, error_id), entries in errors_grouped_by_id.items():
    if len(entries) < 2:
        continue
    error_type = entries[0]["error_type"] or "unknown"
    normalized_lines = [normalize_corrected_line(row["corrected_line"]) for row in entries]
    unique_lines = set(normalized_lines)
    disagree_by_type[error_type]["total"] += 1
    if len(unique_lines) == 1:
        disagree_by_type[error_type]["agree"] += 1
    else:
        disagree_by_type[error_type]["disagree"] += 1
        if "NULL" in unique_lines:
            disagree_by_type[error_type]["mixed_null"] += 1
        else:
            disagree_by_type[error_type]["diff_text"] += 1

with open(os.path.join(OUTPUT_DIR, "corrected_line_by_error_type.csv"), "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["error_type", "total_compared", "agree", "disagree", "mixed_null_vs_text", "different_text", "disagree_rate"])
    for error_type, stats in sorted(disagree_by_type.items()):
        disagree_rate = stats["disagree"] / stats["total"] if stats["total"] else 0
        writer.writerow([error_type, stats["total"], stats["agree"], stats["disagree"], stats["mixed_null"], stats["diff_text"], f"{disagree_rate:.4f}"])

# ── Print summary to console ──────────────────────────────────────────────────
print(f"\n  Agree: {errors_that_agree}  |  Disagree: {errors_that_disagree}  |  Rate: {errors_that_agree / len(errors_grouped_by_id):.1%}")
for attempt_num in all_attempt_numbers:
    attempt_rows = [row for row in all_rows if row["attempt"] == attempt_num]
    needed = sum(1 for row in attempt_rows if row["needs_correction"])
    print(f"  Attempt {attempt_num}: {len(attempt_rows)} errors, {needed} need correction, {len(attempt_rows) - needed} false positives ({(len(attempt_rows) - needed) / len(attempt_rows):.1%})")

if total_convergence_lines:
    print(f"\n  Convergence (lines flagged by both stella & nathan): {total_convergence_lines}")
    print(f"    Both agree: {convergence_agree} ({convergence_agree / total_convergence_lines:.1%})")
    print(f"    Both need correction: {both_need_correction}  |  Both no correction: {both_no_correction}")
    print(f"    Stella-only correction: {stella_only_correction}  |  Nathan-only correction: {nathan_only_correction}")
    print(f"    Stella-only lines (no nathan): {stella_only_lines}  |  Nathan-only lines (no stella): {nathan_only_lines}")

print(f"\n  Corrected-line text agreement (errors with 2+ attempts): {total_compared}")
if total_compared:
    print(f"    Text agrees: {total_agree} ({total_agree / total_compared:.1%})")
    print(f"      Both NULL: {both_null_count}  |  Same text: {both_same_text_count}")
    print(f"    Text disagrees: {total_disagree} ({total_disagree / total_compared:.1%})")
    print(f"      NULL vs sentence: {mixed_null_and_text_count}  |  Different sentences: {different_text_count}")

print(f"\nCSVs written to {OUTPUT_DIR}/")
