"""
Step 5b: Deep Decision Analysis — Did the rerun break the tie?

Background:
  Folders land in data/reruns/ because the first two attempts DISAGREED.
  So each folder has exactly two prior options (A and B) that differ.
  The rerun (step 4) produced a third result (C).

This script asks: does C match A or B (breaking the tie), or does it
introduce yet another answer (creating a three-way split)?

Possible outcomes:
  TIE_BROKEN       — C exactly matches A or B → 2-1 vote, winner is clear
  TIE_BROKEN_FUZZY — C ≈ A or B (>98% similar) → effectively 2-1
  TRIVIAL_DIFF     — A and B were >95% similar anyway → trivial disagreement
  THREE_WAY_SPLIT  — C differs from both → all 3 are different, needs tiebreaker

Outputs:
  analysis_output/deep_decision_analysis.csv
  analysis_output/deep_decision_analysis.json

Usage:
    python step5b_deep_decision_analysis.py
    python step5b_deep_decision_analysis.py --attempt 4
"""

import os
import re
import json
import csv
import argparse
from collections import Counter
from difflib import SequenceMatcher


# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(SCRIPT_DIR, "data")
RERUNS_DIR   = os.path.join(DATA_DIR, "reruns")
ANALYSIS_DIR = os.path.join(SCRIPT_DIR, "analysis_output")
os.makedirs(ANALYSIS_DIR, exist_ok=True)

# Pattern for fix filenames: error_{name}_{eid}_fix_{attempt}_{model}_{pass}.json
FIX_FILENAME_PATTERN = re.compile(r"error_(\w+)_(\d+)_fix_(\d+)_([^_]+)_(\d+)\.json")


# ── Helper functions ───────────────────────────────────────────────────────────

def normalize_line(raw_value):
    """
    Normalize a corrected_line value.
    Returns None for NULL/empty (meaning "no correction"), or the stripped string.
    """
    if raw_value is None:
        return None
    cleaned = str(raw_value).strip()
    if cleaned.upper() == "NULL" or cleaned == "":
        return None
    return cleaned


def compute_similarity(text_a, text_b):
    """Compute SequenceMatcher ratio between two strings (0.0 to 1.0)."""
    if text_a is None and text_b is None:
        return 1.0
    if text_a is None or text_b is None:
        return 0.0
    return SequenceMatcher(None, text_a, text_b).ratio()


def extract_word_tokens(text):
    """Extract lowercase alphanumeric tokens from a string."""
    if text is None:
        return []
    return re.findall(r"[A-Za-z0-9]+", text.lower())


def compute_token_differences(text_a, text_b):
    """Find tokens present in one string but not the other."""
    tokens_a = set(extract_word_tokens(text_a))
    tokens_b = set(extract_word_tokens(text_b))
    return tokens_a.symmetric_difference(tokens_b)


def load_decision_summary(folder_path):
    """Load decision_summary.json from a rerun folder, or return None."""
    summary_path = os.path.join(folder_path, "decision_summary.json")
    if not os.path.exists(summary_path):
        return None
    try:
        with open(summary_path, "r", encoding="utf-8") as file_handle:
            return json.load(file_handle)
    except (json.JSONDecodeError, OSError):
        return None


def find_fix_files_in_folder(folder_path, filter_attempt_number=None):
    """
    Find all fix result files in a folder.
    Optionally filter to a specific attempt number.

    Returns a list of dicts with fix file metadata + parsed response.
    """
    fix_files = []
    for filename in os.listdir(folder_path):
        regex_match = FIX_FILENAME_PATTERN.match(filename)
        if not regex_match:
            continue

        fix_info = {
            "filename": filename,
            "source_name": regex_match.group(1),
            "error_id": int(regex_match.group(2)),
            "attempt_number": int(regex_match.group(3)),
            "model": regex_match.group(4),
            "pass_number": int(regex_match.group(5)),
        }

        if filter_attempt_number is not None and fix_info["attempt_number"] != filter_attempt_number:
            continue

        try:
            with open(os.path.join(folder_path, filename), "r", encoding="utf-8") as file_handle:
                fix_info["response"] = json.load(file_handle)
        except (json.JSONDecodeError, OSError):
            fix_info["response"] = None

        fix_files.append(fix_info)

    return fix_files


def gather_all_attempts(decision_summary, folder_path):
    """
    Combine prior attempt results (from decision_summary) with rerun fix files.

    Returns a list of attempt dicts, each with:
      attempt_number, needs_correction, corrected_line, semantic_difference, source
    """
    all_attempts = []

    # Prior attempts (from decision_summary.json, populated by step 3)
    if decision_summary:
        for prior_attempt in decision_summary.get("attempts", []):
            full_response = prior_attempt.get("full_response", {})
            all_attempts.append({
                "attempt_number": prior_attempt.get("attempt_number"),
                "needs_correction": full_response.get("needs_correction"),
                "needs_error_correction": full_response.get("needs_error_correction"),
                "needs_context_correction": full_response.get("needs_context_correction"),
                "corrected_line": normalize_line(full_response.get("corrected_line")),
                "semantic_difference": full_response.get("semantic_difference"),
                "source": "prior",
            })

    # Rerun attempts (from fix files with attempt_number >= 3)
    all_fix_files = find_fix_files_in_folder(folder_path)
    for fix_file in all_fix_files:
        if fix_file["attempt_number"] >= 3 and fix_file.get("response"):
            response = fix_file["response"]
            all_attempts.append({
                "attempt_number": fix_file["attempt_number"],
                "needs_correction": response.get("needs_correction"),
                "needs_error_correction": response.get("needs_error_correction"),
                "needs_context_correction": response.get("needs_context_correction"),
                "corrected_line": normalize_line(response.get("corrected_line")),
                "semantic_difference": response.get("semantic_difference"),
                "source": "rerun",
            })

    return all_attempts


def analyze_single_folder(folder_path):
    """
    Analyze one rerun folder and determine whether the rerun broke the tie.

    This is the core logic of the script. For each error:
      - Option A = what prior attempt 1 concluded
      - Option B = what prior attempt 2 concluded (differs from A by definition)
      - Option C = what the rerun concluded

    We check: Does C match A? Does C match B? Or is C something entirely new?
    """
    decision_summary = load_decision_summary(folder_path)
    if not decision_summary:
        return None

    all_attempts = gather_all_attempts(decision_summary, folder_path)
    if not all_attempts:
        return None

    # Get the original OCR line text
    ocr_context = decision_summary.get("ocr_context", {})
    original_ocr_line = ocr_context.get("ocr_line_text", "")
    if not original_ocr_line:
        error_metadata = decision_summary.get("error_metadata", {})
        original_ocr_line = error_metadata.get("context", {}).get("line_text", "")

    error_metadata = decision_summary.get("error_metadata", {})

    # Separate prior attempts from rerun attempts
    prior_attempts = [a for a in all_attempts if a["source"] == "prior"]
    rerun_attempts = [a for a in all_attempts if a["source"] == "rerun"]

    # Get the "effective" line for each attempt
    # (NULL means "no correction needed" → the effective text IS the original OCR)
    def get_effective_line(attempt):
        corrected = attempt["corrected_line"]
        return corrected if corrected else original_ocr_line

    prior_effective_lines = [get_effective_line(a) for a in prior_attempts]

    # For reruns: pick the most common line, or the latest if there's a tie
    rerun_effective_lines = [get_effective_line(a) for a in rerun_attempts if get_effective_line(a)]
    if rerun_effective_lines:
        line_vote_counts = {}
        for line in rerun_effective_lines:
            line_vote_counts[line] = line_vote_counts.get(line, 0) + 1
        rerun_chosen_line = max(
            line_vote_counts.keys(),
            key=lambda line: (line_vote_counts[line], rerun_effective_lines[::-1].index(line) if line in rerun_effective_lines else 0)
        )
    else:
        rerun_chosen_line = None

    # The two disagreeing options from prior attempts
    unique_prior_lines = list(set(prior_effective_lines))
    option_a = unique_prior_lines[0] if len(unique_prior_lines) >= 1 else None
    option_b = unique_prior_lines[1] if len(unique_prior_lines) >= 2 else None

    # Check if the rerun flagged a semantic difference
    rerun_flagged_semantic_diff = any(a.get("semantic_difference") for a in rerun_attempts)

    # Compute similarity scores
    similarity_rerun_to_a = compute_similarity(rerun_chosen_line, option_a) if rerun_chosen_line and option_a else 0
    similarity_rerun_to_b = compute_similarity(rerun_chosen_line, option_b) if rerun_chosen_line and option_b else 0
    similarity_a_to_b     = compute_similarity(option_a, option_b) if option_a and option_b else 0

    # Token-level diffs
    tokens_diff_a_vs_b = compute_token_differences(option_a, option_b)

    # ── Decision logic ─────────────────────────────────────────────────────────
    outcome = "UNKNOWN"
    winner = None
    reason = ""

    if option_b is None:
        # Edge case: prior attempts actually agreed (shouldn't be in reruns)
        outcome = "ERROR_SHOULDNT_BE_HERE"
        reason = "Prior attempts agreed — this shouldn't be in reruns/"
        winner = option_a

    elif rerun_chosen_line == option_a:
        # Rerun matches option A exactly → A wins 2-1
        outcome = "TIE_BROKEN"
        winner = option_a
        reason = "Rerun matches option A exactly (2-1 vote)"

    elif rerun_chosen_line == option_b:
        # Rerun matches option B exactly → B wins 2-1
        outcome = "TIE_BROKEN"
        winner = option_b
        reason = "Rerun matches option B exactly (2-1 vote)"

    elif similarity_rerun_to_a > 0.98:
        # Rerun is essentially option A (tiny difference like whitespace)
        outcome = "TIE_BROKEN_FUZZY"
        winner = option_a
        reason = f"Rerun ≈ option A (similarity {similarity_rerun_to_a:.2f})"

    elif similarity_rerun_to_b > 0.98:
        # Rerun is essentially option B
        outcome = "TIE_BROKEN_FUZZY"
        winner = option_b
        reason = f"Rerun ≈ option B (similarity {similarity_rerun_to_b:.2f})"

    elif similarity_a_to_b > 0.95:
        # Options A and B were nearly identical anyway — trivial disagreement
        outcome = "TRIVIAL_DIFF"
        winner = option_a  # Pick either since they're basically the same
        reason = "Options A and B are 95%+ similar — trivial punctuation diff"

    else:
        # Rerun introduced a third option — now we have a genuine 3-way split
        outcome = "THREE_WAY_SPLIT"
        winner = None
        reason = f"Rerun differs from both prior options (sim to A: {similarity_rerun_to_a:.2f}, to B: {similarity_rerun_to_b:.2f})"

    # Voting breakdown on needs_correction
    votes_needs_correction = sum(1 for a in all_attempts if a.get("needs_correction"))
    votes_no_correction    = len(all_attempts) - votes_needs_correction

    return {
        "folder_path": folder_path,
        "error_id": error_metadata.get("error_id"),
        "error_type": error_metadata.get("error_type"),
        "source_document": error_metadata.get("source_document"),
        "page_number": error_metadata.get("page_number"),
        "line_number": error_metadata.get("line_number"),
        "flagged_token": ocr_context.get("flagged_token", ""),
        "ocr_original": original_ocr_line,
        "option_a": option_a,
        "option_b": option_b,
        "rerun_line": rerun_chosen_line,
        "outcome": outcome,
        "winner": winner,
        "reason": reason,
        "sim_rerun_to_a": f"{similarity_rerun_to_a:.4f}",
        "sim_rerun_to_b": f"{similarity_rerun_to_b:.4f}",
        "sim_a_to_b": f"{similarity_a_to_b:.4f}",
        "tokens_diff_a_vs_b": "; ".join(sorted(tokens_diff_a_vs_b)[:10]),
        "rerun_semantic_diff_flag": rerun_flagged_semantic_diff,
        "needs_correction_yes": votes_needs_correction,
        "needs_correction_no": votes_no_correction,
        "total_attempts": len(all_attempts),
        "all_attempts": all_attempts,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deep decision analysis for rerun corrections")
    parser.add_argument("--attempt", type=int, default=4,
                        help="Latest attempt number to include in analysis (default: 4)")
    args = parser.parse_args()

    print("=" * 70)
    print("STEP 5b: DEEP DECISION ANALYSIS")
    print("=" * 70)
    print(f"  Scanning: {RERUNS_DIR}")
    print(f"  Attempt:  {args.attempt}")
    print("=" * 70)
    print()

    # Analyze every folder that has a decision_summary.json
    analysis_results = []
    for folder_root, folder_dirs, folder_files in os.walk(RERUNS_DIR):
        if "decision_summary.json" in folder_files:
            result = analyze_single_folder(folder_root)
            if result:
                analysis_results.append(result)

    if not analysis_results:
        print("No folders with decision_summary.json found in", RERUNS_DIR)
        print("Run step 3 and step 4 first.")
        return

    # ── Summary statistics ─────────────────────────────────────────────────────
    outcome_counts = Counter(result["outcome"] for result in analysis_results)

    print(f"Analyzed {len(analysis_results)} error folders\n")
    print("=== Outcome Summary ===")
    print("(Did the rerun break the tie?)")
    for outcome, count in sorted(outcome_counts.items(), key=lambda pair: -pair[1]):
        percentage = 100 * count / len(analysis_results)
        print(f"  {outcome}: {count} ({percentage:.1f}%)")
    print()

    tie_broken_count  = sum(1 for r in analysis_results if r["outcome"] in ("TIE_BROKEN", "TIE_BROKEN_FUZZY"))
    three_way_count   = sum(1 for r in analysis_results if r["outcome"] == "THREE_WAY_SPLIT")
    trivial_count     = sum(1 for r in analysis_results if r["outcome"] == "TRIVIAL_DIFF")
    semantic_flagged  = sum(1 for r in analysis_results if r["rerun_semantic_diff_flag"])

    print("=== Summary ===")
    print(f"  TIE BROKEN (rerun sided with A or B): {tie_broken_count}")
    print(f"  THREE-WAY SPLIT (rerun introduced new option): {three_way_count}")
    print(f"  TRIVIAL DIFF (A and B were basically same): {trivial_count}")
    print(f"  Rerun flagged semantic_difference=true: {semantic_flagged}")
    print()

    # ── By error type ──────────────────────────────────────────────────────────
    by_error_type = {}
    for result in analysis_results:
        error_type = result["error_type"] or "unknown"
        if error_type not in by_error_type:
            by_error_type[error_type] = {"count": 0, "tie_broken": 0, "three_way": 0, "trivial": 0}
        by_error_type[error_type]["count"] += 1
        if result["outcome"] in ("TIE_BROKEN", "TIE_BROKEN_FUZZY"):
            by_error_type[error_type]["tie_broken"] += 1
        if result["outcome"] == "THREE_WAY_SPLIT":
            by_error_type[error_type]["three_way"] += 1
        if result["outcome"] == "TRIVIAL_DIFF":
            by_error_type[error_type]["trivial"] += 1

    print("=== By Error Type ===")
    for error_type, stats in sorted(by_error_type.items(), key=lambda pair: -pair[1]["count"]):
        print(f"  {error_type}: {stats['count']} total, {stats['tie_broken']} tie broken, "
              f"{stats['three_way']} 3-way, {stats['trivial']} trivial")
    print()

    # ── Write CSV report ───────────────────────────────────────────────────────
    csv_output_path = os.path.join(ANALYSIS_DIR, "deep_decision_analysis.csv")
    csv_columns = [
        "folder_path", "error_id", "error_type", "source_document", "page_number",
        "line_number", "flagged_token", "ocr_original", "option_a", "option_b",
        "rerun_line", "outcome", "winner", "reason",
        "sim_rerun_to_a", "sim_rerun_to_b", "sim_a_to_b", "tokens_diff_a_vs_b",
        "rerun_semantic_diff_flag", "needs_correction_yes", "needs_correction_no",
        "total_attempts",
    ]
    with open(csv_output_path, "w", newline="", encoding="utf-8") as file_handle:
        csv_writer = csv.DictWriter(file_handle, fieldnames=csv_columns, extrasaction="ignore")
        csv_writer.writeheader()
        csv_writer.writerows(analysis_results)
    print(f"CSV report: {csv_output_path}")

    # ── Write JSON with full details ───────────────────────────────────────────
    json_output_path = os.path.join(ANALYSIS_DIR, "deep_decision_analysis.json")

    # Exclude all_attempts from JSON to keep file size manageable
    json_safe_results = []
    for result in analysis_results:
        safe_copy = {key: value for key, value in result.items() if key != "all_attempts"}
        json_safe_results.append(safe_copy)

    with open(json_output_path, "w", encoding="utf-8") as file_handle:
        json.dump({
            "summary": {
                "total_analyzed": len(analysis_results),
                "outcomes": dict(outcome_counts),
                "tie_broken_count": tie_broken_count,
                "three_way_split_count": three_way_count,
                "trivial_diff_count": trivial_count,
                "semantic_diff_flagged": semantic_flagged,
                "by_error_type": by_error_type,
            },
            "results": json_safe_results,
        }, file_handle, indent=2, ensure_ascii=False)
    print(f"Full JSON: {json_output_path}")

    # ── Print example cases ────────────────────────────────────────────────────
    three_way_cases = [r for r in analysis_results if r["outcome"] == "THREE_WAY_SPLIT"]
    if three_way_cases:
        print()
        print("=== Sample THREE_WAY_SPLIT Cases ===")
        for result in three_way_cases[:5]:
            option_a_preview = (result["option_a"][:70] + "...") if result["option_a"] and len(result["option_a"]) > 70 else (result["option_a"] or "None")
            option_b_preview = (result["option_b"][:70] + "...") if result["option_b"] and len(result["option_b"]) > 70 else (result["option_b"] or "None")
            rerun_preview    = (result["rerun_line"][:70] + "...") if result["rerun_line"] and len(result["rerun_line"]) > 70 else (result["rerun_line"] or "None")
            print(f"\n  Error ID: {result['error_id']} ({result['error_type']})")
            print(f"  OCR Original: {(result['ocr_original'][:70] + '...') if len(result['ocr_original']) > 70 else result['ocr_original']}")
            print(f"  Option A:     {option_a_preview}")
            print(f"  Option B:     {option_b_preview}")
            print(f"  Rerun:        {rerun_preview}")
            print(f"  Sim to A: {result['sim_rerun_to_a']}, Sim to B: {result['sim_rerun_to_b']}")


if __name__ == "__main__":
    main()
