"""Standalone cleaned export stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/export_cleaned_final.py
"""

from __future__ import annotations

import json
import shutil

from pipeline_clean.config import (
    CLEANED_FINAL_DIR,
    EXPORT_REMOVE_EXCLUDED_FIELDS,
    FINAL_DIR,
    ensure_workspace_dirs,
)
from pipeline_clean.contracts import EXPORT_EXCLUDED_KEYS


def load_json(file_path):
    """Load JSON from file.

    Args:
        file_path: JSON file path.

    Returns:
        Parsed dictionary.
    """
    with open(file_path, mode = "r", encoding = "utf-8") as file_handle:
        return json.load(file_handle)


def save_json(file_path, payload):
    """Write JSON to file.

    Args:
        file_path: Output JSON file path.
        payload: Serializable dictionary.

    Returns:
        None.
    """
    file_path.parent.mkdir(parents = True, exist_ok = True)
    with open(file_path, mode = "w", encoding = "utf-8") as file_handle:
        json.dump(payload, file_handle, indent = 2)
        file_handle.write("\n")


def clean_page_payload(page_payload):
    """Remove excluded annotation keys from all documents in payload.

    Args:
        page_payload: Page annotation dictionary.

    Returns:
        Cleaned page annotation dictionary.
    """
    if not EXPORT_REMOVE_EXCLUDED_FIELDS:
        return page_payload

    for _, doc_payload in page_payload.get("documents", {}).items():
        for excluded_key in EXPORT_EXCLUDED_KEYS:
            doc_payload.pop(excluded_key, None)

    return page_payload


def export_document(document_dir):
    """Export cleaned final files for one document directory.

    Args:
        document_dir: Document folder under FINAL_DIR.

    Returns:
        Tuple (json_count, pdf_count).
    """
    json_count = 0
    pdf_count = 0

    for page_dir in sorted(path for path in document_dir.iterdir() if path.is_dir()):
        page_json = page_dir / f"{page_dir.name}.json"
        page_pdf = page_dir / f"{page_dir.name}.pdf"

        target_dir = CLEANED_FINAL_DIR / document_dir.name / page_dir.name
        target_dir.mkdir(parents = True, exist_ok = True)

        if page_json.exists():
            payload = clean_page_payload(load_json(page_json))
            save_json(target_dir / f"{page_dir.name}.json", payload)
            json_count += 1

        if page_pdf.exists():
            shutil.copy2(page_pdf, target_dir / f"{page_dir.name}.pdf")
            pdf_count += 1

    return json_count, pdf_count


def main():
    """Run cleaned export from FINAL_DIR to CLEANED_FINAL_DIR.

    Args:
        None.

    Returns:
        None.
    """
    ensure_workspace_dirs()

    if not FINAL_DIR.exists():
        print(f"Final directory missing: {FINAL_DIR}", flush = True)
        return

    total_json = 0
    total_pdf = 0

    for document_dir in sorted(path for path in FINAL_DIR.iterdir() if path.is_dir()):
        json_count, pdf_count = export_document(document_dir)
        total_json += json_count
        total_pdf += pdf_count

    print(f"Exported JSON files: {total_json}", flush = True)
    print(f"Exported PDF files: {total_pdf}", flush = True)


if __name__ == "__main__":
    main()
