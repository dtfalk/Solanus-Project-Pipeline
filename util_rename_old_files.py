"""
Utility: Rename Old-Format Filenames.

Early pipeline runs used the filename format:
    error_{eid}.json
    error_{eid}_fix_{attempt}_{model}_{pass}.json

The current format includes the source name:
    error_{name}_{eid}.json
    error_{name}_{eid}_fix_{attempt}_{model}_{pass}.json

This script renames old-format files in corrected_files/ by looking up
each error_id in the source manifests (errors_nathan.json, etc.) to
determine the correct name prefix.

Usage:
    python util_rename_old_files.py
"""

import os
import json
import re
import glob

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR          = os.path.dirname(os.path.abspath(__file__))
DATA_DIR            = os.path.join(SCRIPT_DIR, "data")
CORRECTED_FILES_DIR = os.path.join(DATA_DIR, "corrected_files")


def main():
    # ── Build lookup table: (error_id, source_document, page, line) → source_name
    error_id_to_source_name = {}

    for manifest_file_path in sorted(glob.glob(os.path.join(DATA_DIR, "errors_*.json"))):
        # Extract source name from filename: errors_nathan.json → "nathan"
        manifest_filename = os.path.basename(manifest_file_path)
        source_name = manifest_filename.removeprefix("errors_").removesuffix(".json")

        with open(manifest_file_path, "r", encoding="utf-8") as file_handle:
            manifest_data = json.load(file_handle)

        for error_record in manifest_data["errors"]:
            lookup_key = (
                error_record["error_id"],
                error_record["source_document"],
                error_record["page_number"],
                error_record["line_number"],
            )
            error_id_to_source_name[lookup_key] = source_name

        matching_count = sum(1 for key in error_id_to_source_name if error_id_to_source_name[key] == source_name)
        print(f"  Loaded {source_name} ({matching_count} errors)")

    if not error_id_to_source_name:
        print(f"No error manifest files found in {DATA_DIR}")
        return

    # ── Scan corrected_files/ for old-format metadata files ────────────────────
    # Old format: error_{digits}.json (no source name in the filename)
    OLD_FORMAT_PATTERN = re.compile(r"^error_(\d+)\.json$")

    files_renamed = 0
    files_skipped = 0
    files_not_found = 0

    for folder_root, _, folder_files in os.walk(CORRECTED_FILES_DIR):
        for filename in folder_files:
            regex_match = OLD_FORMAT_PATTERN.match(filename)
            if not regex_match or "_fix_" in filename:
                continue

            error_id_string = regex_match.group(1)

            # Load the error metadata to get the lookup key
            with open(os.path.join(folder_root, filename), "r", encoding="utf-8") as file_handle:
                error_data = json.load(file_handle)

            lookup_key = (
                error_data["error_id"],
                error_data["source_document"],
                error_data["page_number"],
                error_data["line_number"],
            )
            source_name = error_id_to_source_name.get(lookup_key)

            if not source_name:
                print(f"  NOT FOUND in source files: {filename} (eid={error_id_string})")
                files_not_found += 1
                continue

            # Rename every file in this folder that starts with the old prefix
            old_prefix = f"error_{error_id_string}"
            new_prefix = f"error_{source_name}_{error_id_string}"
            renames_in_folder = []

            for candidate_filename in folder_files:
                if not candidate_filename.startswith(old_prefix):
                    continue
                # Make sure we match exactly this ID (not e.g. error_5 matching error_59)
                remainder = candidate_filename[len(old_prefix):]
                if remainder and remainder[0] not in ("_", "."):
                    continue
                new_filename = candidate_filename.replace(old_prefix, new_prefix, 1)
                renames_in_folder.append((candidate_filename, new_filename))

            for old_name, new_name in renames_in_folder:
                os.rename(
                    os.path.join(folder_root, old_name),
                    os.path.join(folder_root, new_name),
                )
                files_renamed += 1

            if renames_in_folder:
                print(f"  [{source_name}] eid {error_id_string}: {len(renames_in_folder)} files renamed")
            else:
                files_skipped += 1

    print(f"\nDone: {files_renamed} files renamed, {files_skipped} skipped, {files_not_found} not found in source")


if __name__ == "__main__":
    main()
