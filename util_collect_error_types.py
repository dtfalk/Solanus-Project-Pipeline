"""
Utility: Collect All Error Types.

Reads every errors_*.json manifest in data/ and writes a sorted list
of all unique error_type values to data/analysis.json.

Useful for verifying that prompts.py has hints for every error type.

Usage:
    python util_collect_error_types.py
"""

import os
import json
import glob

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "data")


def main():
    # Find all error manifest files
    manifest_files = glob.glob(os.path.join(DATA_DIR, "errors_*.json"))

    if not manifest_files:
        print(f"No error manifest files (errors_*.json) found in {DATA_DIR}")
        return

    # Collect every unique error_type
    all_error_types = set()
    total_errors = 0

    for manifest_file_path in sorted(manifest_files):
        with open(manifest_file_path, "r", encoding="utf-8") as file_handle:
            manifest_data = json.load(file_handle)

        for error_record in manifest_data["errors"]:
            all_error_types.add(error_record["error_type"])
            total_errors += 1

        print(f"  {os.path.basename(manifest_file_path)}: {len(manifest_data['errors'])} errors")

    # Write sorted list to analysis.json
    output_file_path = os.path.join(DATA_DIR, "analysis.json")
    sorted_types = sorted(all_error_types)

    with open(output_file_path, "w", encoding="utf-8") as file_handle:
        json.dump(sorted_types, file_handle, indent=2)

    print(f"\nFound {len(sorted_types)} distinct error types across {total_errors} total errors")
    print(f"Written to {output_file_path}")
    print()
    for error_type in sorted_types:
        print(f"  - {error_type}")


if __name__ == "__main__":
    main()
