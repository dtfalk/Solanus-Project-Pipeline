"""Standalone output validator for pipeline_clean.

Run with:
    python pipeline_clean/stages/validate_outputs.py
"""

from __future__ import annotations

import json

from pipeline_clean.config import CLEANED_FINAL_DIR, MANIFEST_FILE_PATH
from pipeline_clean.contracts import ANNOTATION_SCHEMA_KEYS, POLYGON_KEYS


def load_json(file_path):
    """Load JSON file.

    Args:
        file_path: JSON file path.

    Returns:
        Parsed dictionary.
    """
    with open(file_path, mode = "r", encoding = "utf-8") as file_handle:
        return json.load(file_handle)


def selected_pages_from_manifest():
    """Count selected pages from current manifest.

    Args:
        None.

    Returns:
        Integer selected page count.
    """
    if not MANIFEST_FILE_PATH.exists():
        return 0

    manifest = load_json(MANIFEST_FILE_PATH)
    total = 0
    for _, payload in manifest.get("documents", {}).items():
        for page in payload.get("pages", []):
            if page.get("status", "approved") == "approved":
                total += 1
    return total


def validate_schema(doc_name, doc_payload):
    """Validate document payload key set and polygon shape.

    Args:
        doc_name: Document id label.
        doc_payload: Document annotation payload.

    Returns:
        List of issue strings.
    """
    issues = []

    expected = set(ANNOTATION_SCHEMA_KEYS)
    actual = set(doc_payload.keys())
    if expected != actual:
        issues.append(f"Schema mismatch for {doc_name}")

    for annotation_key, polygons in doc_payload.items():
        if not isinstance(polygons, list):
            issues.append(f"Non-list polygons for {doc_name}.{annotation_key}")
            continue

        for polygon in polygons:
            if not isinstance(polygon, dict):
                issues.append(f"Invalid polygon object for {doc_name}.{annotation_key}")
                continue

            polygon_keys = set(polygon.keys())
            if polygon_keys != set(POLYGON_KEYS):
                issues.append(f"Polygon keys mismatch for {doc_name}.{annotation_key}")

    return issues


def validate_connections(page_payload):
    """Validate connection doc references for all polygons.

    Args:
        page_payload: Page annotation payload.

    Returns:
        List of connection issue strings.
    """
    issues = []
    documents = page_payload.get("documents", {})
    document_keys = set(documents.keys())

    for source_doc, payload in documents.items():
        for annotation_key, polygons in payload.items():
            for polygon in polygons:
                for connection in polygon.get("connections", []):
                    target_doc = connection.get("doc")
                    if target_doc not in document_keys:
                        issues.append(
                            f"Invalid connection doc from {source_doc}.{annotation_key} -> {target_doc}",
                        )
    return issues


def validate_export_tree():
    """Validate cleaned export tree.

    Args:
        None.

    Returns:
        Tuple (page_count, issues).
    """
    issues = []
    page_count = 0

    if not CLEANED_FINAL_DIR.exists():
        return 0, [f"Missing export directory: {CLEANED_FINAL_DIR}"]

    for document_dir in sorted(path for path in CLEANED_FINAL_DIR.iterdir() if path.is_dir()):
        for page_dir in sorted(path for path in document_dir.iterdir() if path.is_dir()):
            page_count += 1
            page_json = page_dir / f"{page_dir.name}.json"
            page_pdf = page_dir / f"{page_dir.name}.pdf"

            if not page_json.exists():
                issues.append(f"Missing JSON: {page_json}")
                continue

            if not page_pdf.exists():
                issues.append(f"Missing PDF: {page_pdf}")

            payload = load_json(page_json)
            for doc_name, doc_payload in payload.get("documents", {}).items():
                issues.extend(validate_schema(doc_name, doc_payload))

            issues.extend(validate_connections(payload))

    return page_count, issues


def main():
    """Run standalone output validation and print summary.

    Args:
        None.

    Returns:
        None.
    """
    selected_count = selected_pages_from_manifest()
    exported_count, issues = validate_export_tree()

    print(f"Selected pages: {selected_count}", flush = True)
    print(f"Exported pages: {exported_count}", flush = True)

    if selected_count and selected_count != exported_count:
        issues.append("Selected page count does not match exported page count")

    if issues:
        print("Validation issues:", flush = True)
        for issue in issues:
            print(f"- {issue}", flush = True)
    else:
        print("Validation passed with zero issues.", flush = True)


if __name__ == "__main__":
    main()
