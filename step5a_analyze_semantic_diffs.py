"""
Step 5a: Analyze Semantic Disagreements in Rerun Results.

After the reruns (step 4), this script compares the distinct corrected_line
values from each error's decision_summary.json. It computes:

  - String similarity (SequenceMatcher ratio)
  - Token-level diffs (which words changed)
  - Optional embedding similarity (sentence-transformers)

It then writes:
  - analysis_output/semantic_disagreement_report.csv
  - Updates each decision_summary.json with a "semantic_analysis" block

This helps identify which disagreements actually matter (real word differences)
versus which are trivial (whitespace, punctuation, capitalisation).

Usage:
    python step5a_analyze_semantic_diffs.py
    python step5a_analyze_semantic_diffs.py --use-embeddings
    python step5a_analyze_semantic_diffs.py --use-embeddings --threshold 0.92

Notes:
    - String analysis always runs and is deterministic.
    - Embedding analysis requires the sentence-transformers package.
"""

import os
import re
import json
import csv
import argparse
from difflib import SequenceMatcher
from itertools import combinations

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR       = os.path.join(SCRIPT_DIR, "data")
RERUNS_DIR     = os.path.join(DATA_DIR, "reruns")
ANALYSIS_DIR   = os.path.join(SCRIPT_DIR, "analysis_output")
os.makedirs(ANALYSIS_DIR, exist_ok=True)

# Regex to extract alphanumeric words from a string
WORD_PATTERN = re.compile(r"[A-Za-z0-9]+")


# ── Helper functions ───────────────────────────────────────────────────────────

def normalize_line(raw_value):
    """Normalize a corrected_line value: strip whitespace, treat empties as NULL."""
    if raw_value is None:
        return "NULL"
    cleaned = str(raw_value).strip()
    return cleaned if cleaned else "NULL"


def extract_words(text):
    """Extract lowercase alphanumeric tokens from a string."""
    return WORD_PATTERN.findall(text.lower())


def compute_string_similarity(text_a, text_b):
    """
    Compare two strings and return similarity metrics.

    Returns a dict with:
      - sequence_ratio: float 0.0-1.0 (SequenceMatcher overall similarity)
      - token_diff_count: int (number of tokens that differ between the two)
      - changed_tokens: list of tokens present in one but not the other
      - exact_token_match: bool (are the word lists identical?)
    """
    sequence_ratio = SequenceMatcher(None, text_a, text_b).ratio()

    words_a = extract_words(text_a)
    words_b = extract_words(text_b)
    word_set_a = set(words_a)
    word_set_b = set(words_b)

    # Symmetric difference = words in A but not B, plus words in B but not A
    changed_tokens = sorted(word_set_a.symmetric_difference(word_set_b))
    token_diff_count = len(changed_tokens)
    exact_token_match = (words_a == words_b)

    return {
        "sequence_ratio": sequence_ratio,
        "token_diff_count": token_diff_count,
        "changed_tokens": changed_tokens,
        "exact_token_match": exact_token_match,
    }


def load_sentence_transformer_model(device_name):
    """
    Try to load the sentence-transformers model.
    Returns None if the package isn't installed.
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None
    return SentenceTransformer("all-MiniLM-L6-v2", device=device_name)


def compute_cosine_similarity(vector_a, vector_b):
    """Compute cosine similarity between two vectors."""
    import math
    dot_product = sum(x * y for x, y in zip(vector_a, vector_b))
    magnitude_a = math.sqrt(sum(x * x for x in vector_a))
    magnitude_b = math.sqrt(sum(y * y for y in vector_b))
    if magnitude_a == 0 or magnitude_b == 0:
        return 0.0
    return dot_product / (magnitude_a * magnitude_b)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Analyze semantic disagreements in rerun results")
    parser.add_argument("--use-embeddings", action="store_true",
                        help="Also compute embedding similarity (requires sentence-transformers)")
    parser.add_argument("--threshold", type=float, default=0.90,
                        help="Embedding similarity threshold below which to flag as 'semantic difference'")
    parser.add_argument("--device", type=str, default="cuda",
                        help="Device for embedding model (cuda or cpu)")
    args = parser.parse_args()

    # Load embedding model if requested
    embedding_model = None
    if args.use_embeddings:
        embedding_model = load_sentence_transformer_model(args.device)
        if embedding_model is None:
            print("WARNING: sentence-transformers is not installed. Embedding analysis disabled.")
            args.use_embeddings = False

    report_rows = []
    summaries_updated = 0

    # Walk every folder in reruns/ looking for decision_summary.json
    for folder_root, _, folder_files in os.walk(RERUNS_DIR):
        if "decision_summary.json" not in folder_files:
            continue

        summary_file_path = os.path.join(folder_root, "decision_summary.json")
        try:
            with open(summary_file_path, "r", encoding="utf-8") as file_handle:
                decision_summary = json.load(file_handle)
        except (json.JSONDecodeError, OSError):
            continue

        # Get the distinct corrected_line values from prior analysis
        corrected_line_data = decision_summary.get("corrected_line_analysis", {})
        distinct_values = corrected_line_data.get("distinct_values", [])

        # Get the original OCR line text (used when corrected_line is NULL)
        ocr_context = decision_summary.get("ocr_context", {})
        original_ocr_line = ocr_context.get("ocr_line_text", "")
        if not original_ocr_line:
            error_metadata = decision_summary.get("error_metadata", {})
            original_ocr_line = error_metadata.get("context", {}).get("line_text", "")

        # Build list of "effective lines" — substitute the original OCR for NULL values
        # (NULL means "no correction needed", so the effective text is the original)
        effective_lines = []
        null_substitution_count = 0
        for entry in distinct_values:
            corrected_line = entry.get("corrected_line")
            if corrected_line is None or str(corrected_line).strip().upper() == "NULL" or str(corrected_line).strip() == "":
                effective_lines.append(original_ocr_line if original_ocr_line else "[ORIGINAL]")
                null_substitution_count += 1
            else:
                effective_lines.append(str(corrected_line).strip())

        unique_effective_lines = sorted(set(effective_lines))

        # If all lines resolve to the same text, no disagreement to analyze
        if len(unique_effective_lines) <= 1:
            continue

        # Compute embedding vectors if using embeddings
        embedding_vectors = {}
        if args.use_embeddings:
            embeddable_lines = [line for line in unique_effective_lines if line and line != "[ORIGINAL]"]
            if embeddable_lines:
                raw_vectors = embedding_model.encode(embeddable_lines, normalize_embeddings=False)
                for line, vector in zip(embeddable_lines, raw_vectors):
                    embedding_vectors[line] = vector.tolist() if hasattr(vector, "tolist") else list(vector)

        # Compare every pair of distinct lines
        pairwise_comparisons = []
        has_semantic_difference = False

        for line_a, line_b in combinations(unique_effective_lines, 2):
            string_metrics = compute_string_similarity(line_a, line_b)

            embedding_similarity = ""
            if args.use_embeddings:
                if line_a in embedding_vectors and line_b in embedding_vectors:
                    embedding_similarity = compute_cosine_similarity(embedding_vectors[line_a], embedding_vectors[line_b])
                else:
                    embedding_similarity = 0.0

            # Rule: flag as semantically different if actual words changed,
            #        or if embedding similarity is below threshold
            if string_metrics["token_diff_count"] > 0:
                has_semantic_difference = True
            elif args.use_embeddings and isinstance(embedding_similarity, float) and embedding_similarity < args.threshold:
                has_semantic_difference = True

            pairwise_comparisons.append({
                "line_a": line_a,
                "line_b": line_b,
                "sequence_ratio": f"{string_metrics['sequence_ratio']:.4f}",
                "token_diff_count": string_metrics["token_diff_count"],
                "changed_tokens": "; ".join(string_metrics["changed_tokens"]),
                "embedding_similarity": f"{embedding_similarity:.4f}" if isinstance(embedding_similarity, float) else "",
            })

        # Add semantic_analysis block to the decision_summary
        decision_summary["semantic_analysis"] = {
            "use_embeddings": bool(args.use_embeddings),
            "embedding_threshold": args.threshold if args.use_embeddings else "",
            "semantic_difference": has_semantic_difference,
            "ocr_original_line": original_ocr_line,
            "null_substitution_count": null_substitution_count,
            "note": ("NULL values were replaced with the original OCR line for comparison"
                     if null_substitution_count > 0 else ""),
            "pairwise": pairwise_comparisons,
        }

        # Write updated decision_summary back to disk
        try:
            with open(summary_file_path, "w", encoding="utf-8") as file_handle:
                json.dump(decision_summary, file_handle, indent=2, ensure_ascii=False)
            summaries_updated += 1
        except OSError:
            pass

        # Add row to CSV report
        report_rows.append({
            "folder_path": folder_root,
            "source_document": ocr_context.get("source_document", ""),
            "page": ocr_context.get("page_number", ""),
            "line": ocr_context.get("line_number", ""),
            "error_type": ocr_context.get("error_type", ""),
            "error_token": ocr_context.get("flagged_token", ""),
            "ocr_line_text": ocr_context.get("ocr_line_text", ""),
            "distinct_corrected_lines": "; ".join(unique_effective_lines),
            "semantic_difference": has_semantic_difference,
            "pair_count": len(pairwise_comparisons),
        })

    # ── Write the CSV report ───────────────────────────────────────────────────
    report_csv_path = os.path.join(ANALYSIS_DIR, "semantic_disagreement_report.csv")
    csv_columns = [
        "folder_path", "source_document", "page", "line", "error_type",
        "error_token", "ocr_line_text", "distinct_corrected_lines",
        "semantic_difference", "pair_count",
    ]
    with open(report_csv_path, "w", newline="", encoding="utf-8") as file_handle:
        csv_writer = csv.DictWriter(file_handle, fieldnames=csv_columns)
        csv_writer.writeheader()
        csv_writer.writerows(report_rows)

    # ── Print summary ──────────────────────────────────────────────────────────
    print(f"Updated {summaries_updated} decision_summary.json files")
    print(f"Report written to {report_csv_path}")
    total_semantic = sum(1 for row in report_rows if row["semantic_difference"])
    total_trivial = len(report_rows) - total_semantic
    print(f"  Semantic differences found: {total_semantic}")
    print(f"  Trivial differences only:   {total_trivial}")


if __name__ == "__main__":
    main()
