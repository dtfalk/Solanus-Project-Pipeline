"""
Utility: Check for Collisions Between Error Files.

Before running the pipeline, it's good to verify that no two errors
from different source files (errors_nathan.json, errors_stella.json)
map to the same output path — because that would cause one result
to silently overwrite the other.

An output path is:
    {document}/page_{page}/line_{line}/error_id_{eid}/

This script checks for:
  1. Path collisions (same output path from different source files)
  2. Error ID overlaps (same error_id in multiple source files)

Usage:
    python util_check_collisions.py
"""

import os
import json
import glob
from collections import defaultdict

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "data")


def main():
    # Find all error manifest files (errors_*.json)
    error_manifest_files = sorted(glob.glob(os.path.join(DATA_DIR, "errors_*.json")))

    if not error_manifest_files:
        print(f"No error manifest files (errors_*.json) found in {DATA_DIR}")
        return

    # ── Check 1: Output path collisions ────────────────────────────────────────
    # Map each output path to the list of (source_file, error_id, error_type)
    output_path_map = defaultdict(list)

    for manifest_file_path in error_manifest_files:
        manifest_filename = os.path.basename(manifest_file_path)
        with open(manifest_file_path, "r", encoding="utf-8") as file_handle:
            manifest_data = json.load(file_handle)

        for error_record in manifest_data["errors"]:
            error_id      = error_record["error_id"]
            document_name = error_record["source_document"].replace(".pdf", "")
            page_number   = error_record["page_number"]
            line_number   = error_record["line_number"]

            output_path_key = f"{document_name}/page_{page_number}/line_{line_number}/error_id_{error_id}"
            output_path_map[output_path_key].append(
                (manifest_filename, error_id, error_record.get("error_type", "?"))
            )

    print("=" * 70)
    print("COLLISION CHECK — same output path from different error files")
    print("=" * 70)

    all_collisions = {
        path_key: sources
        for path_key, sources in output_path_map.items()
        if len(sources) > 1
    }

    if all_collisions:
        # Separate cross-file collisions from within-file duplicates
        cross_file_collisions = {
            path_key: sources
            for path_key, sources in all_collisions.items()
            if len(set(source_file for source_file, _, _ in sources)) > 1
        }
        within_file_duplicates = {
            path_key: sources
            for path_key, sources in all_collisions.items()
            if len(set(source_file for source_file, _, _ in sources)) == 1
        }

        if cross_file_collisions:
            print(f"\n  WARNING: {len(cross_file_collisions)} CROSS-FILE path collisions found!\n")
            for path_key, sources in sorted(cross_file_collisions.items()):
                print(f"  Path: {path_key}")
                for source_file, error_id, error_type in sources:
                    print(f"    <- {source_file}  error_id={error_id}  type={error_type}")
                print()
        else:
            print("\n  OK: No cross-file path collisions.\n")

        if within_file_duplicates:
            print(f"  (Also found {len(within_file_duplicates)} WITHIN-file duplicates)")
            for path_key, sources in sorted(within_file_duplicates.items())[:5]:
                print(f"    Path: {path_key}")
                for source_file, error_id, error_type in sources:
                    print(f"      <- {source_file}  error_id={error_id}  type={error_type}")
            if len(within_file_duplicates) > 5:
                print(f"    ... and {len(within_file_duplicates) - 5} more")
            print()
    else:
        print("\n  OK: No path collisions at all.\n")

    # ── Check 2: Error ID overlaps ─────────────────────────────────────────────
    # Map each error_id to the list of source files it appears in
    error_id_map = defaultdict(list)

    for manifest_file_path in error_manifest_files:
        manifest_filename = os.path.basename(manifest_file_path)
        with open(manifest_file_path, "r", encoding="utf-8") as file_handle:
            manifest_data = json.load(file_handle)
        for error_record in manifest_data["errors"]:
            error_id_map[error_record["error_id"]].append(manifest_filename)

    print("=" * 70)
    print("OVERLAP CHECK — same error_id in multiple files")
    print("=" * 70)

    id_overlaps = {
        error_id: source_files
        for error_id, source_files in error_id_map.items()
        if len(set(source_files)) > 1
    }

    if id_overlaps:
        print(f"\n  WARNING: {len(id_overlaps)} error_ids appear in multiple files!\n")
        for error_id, source_files in sorted(id_overlaps.items())[:20]:
            print(f"  error_id {error_id:>6}  ->  {', '.join(sorted(set(source_files)))}")
        if len(id_overlaps) > 20:
            print(f"  ... and {len(id_overlaps) - 20} more")
    else:
        print(f"\n  OK: All error_ids are unique across files.")

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    for manifest_file_path in error_manifest_files:
        manifest_filename = os.path.basename(manifest_file_path)
        with open(manifest_file_path, "r", encoding="utf-8") as file_handle:
            manifest_data = json.load(file_handle)
        error_ids = [error["error_id"] for error in manifest_data["errors"]]
        print(f"  {manifest_filename}: {len(error_ids)} errors, IDs range {min(error_ids)}-{max(error_ids)}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
