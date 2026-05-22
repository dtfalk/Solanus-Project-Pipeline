"""Standalone annotation packaging stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/package_annotation_inputs.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from pypdf import PdfReader

from pipeline_clean.config import (
    ANNOTATION_SEED_DEFAULT_DOCS,
    ANNOTATION_SEED_DIR,
    CROPPED_PAGES_DIR,
    FEW_SHOT_INCLUDE_CANDIDATES,
    FINAL_DIR,
    MANIFEST_FILE_PATH,
    POLYGONS_DIR,
    SAMPLE_PAGES_DIR,
    ensure_workspace_dirs,
)
from pipeline_clean.contracts import ANNOTATION_SCHEMA_KEYS, format_page_name


def load_manifest():
    """Load manifest JSON from configured path.

    Args:
        None.

    Returns:
        Parsed manifest dictionary.
    """
    with open(MANIFEST_FILE_PATH, mode = "r", encoding = "utf-8") as file_handle:
        return json.load(file_handle)


def selected_pages(manifest):
    """Extract selected pages from manifest using configured status policy.

    Args:
        manifest: Parsed manifest dictionary.

    Returns:
        Mapping from document name to selected page numbers.
    """
    allowed = {"approved"}
    if FEW_SHOT_INCLUDE_CANDIDATES:
        allowed.add("candidate")

    selection = {}
    documents = manifest.get("documents", {})

    for document_name, payload in documents.items():
        pages = []
        for page in payload.get("pages", []):
            status = page.get("status", "approved")
            if status in allowed:
                pages.append(int(page["page_number"]))

        if pages:
            selection[document_name] = sorted(set(pages))

    return selection


def page_dimensions(document_name, page_number):
    """Load page dimensions from polygon page size record.

    Args:
        document_name: Document folder name.
        page_number: 1-indexed page number.

    Returns:
        Tuple (width_pixels, height_pixels, render_dpi).
    """
    page_name = format_page_name(page_number)
    page_size_path = POLYGONS_DIR / document_name / "page_sizes" / f"{page_name}.json"

    with open(page_size_path, mode = "r", encoding = "utf-8") as file_handle:
        payload = json.load(file_handle)

    return (
        int(payload["width_pixels"]),
        int(payload["height_pixels"]),
        int(payload["render_dpi"]),
    )


def empty_doc_payload():
    """Build empty annotation payload for one document.

    Args:
        None.

    Returns:
        Dictionary with schema keys initialized to empty lists.
    """
    return {key: [] for key in ANNOTATION_SCHEMA_KEYS}


def build_seed_json(document_name, page_number):
    """Build default annotation seed JSON payload.

    Args:
        document_name: Document folder name.
        page_number: 1-indexed page number.

    Returns:
        Page annotation dictionary.
    """
    width_pixels, height_pixels, render_dpi = page_dimensions(document_name, page_number)

    documents = {}
    for doc_index in range(1, int(ANNOTATION_SEED_DEFAULT_DOCS) + 1):
        documents[f"doc_{doc_index}"] = empty_doc_payload()

    return {
        "page_number": page_number,
        "source_file": str(CROPPED_PAGES_DIR / document_name / f"{document_name}.pdf"),
        "page_width": width_pixels,
        "page_height": height_pixels,
        "render_dpi": render_dpi,
        "num_documents": int(ANNOTATION_SEED_DEFAULT_DOCS),
        "documents": documents,
    }


def write_json(file_path, payload):
    """Write JSON payload with stable formatting.

    Args:
        file_path: Output JSON path.
        payload: Serializable dictionary.

    Returns:
        None.
    """
    file_path.parent.mkdir(parents = True, exist_ok = True)
    with open(file_path, mode = "w", encoding = "utf-8") as file_handle:
        json.dump(payload, file_handle, indent = 2)
        file_handle.write("\n")


def package_document(document_name, page_numbers):
    """Package selected document pages into sample, seed, and final trees.

    Args:
        document_name: Document folder name.
        page_numbers: Selected page number list.

    Returns:
        Tuple (packaged_pages, missing_pages).
    """
    packaged_pages = 0
    missing_pages = 0

    for page_number in page_numbers:
        page_name = format_page_name(page_number)
        source_pdf = CROPPED_PAGES_DIR / document_name / "pages" / f"{page_name}.pdf"
        if not source_pdf.exists():
            missing_pages += 1
            continue

        sample_pdf = SAMPLE_PAGES_DIR / document_name / "pages" / f"{page_name}.pdf"
        final_pdf = FINAL_DIR / document_name / page_name / f"{page_name}.pdf"
        seed_json = ANNOTATION_SEED_DIR / document_name / page_name / f"{page_name}.json"
        final_json = FINAL_DIR / document_name / page_name / f"{page_name}.json"

        sample_pdf.parent.mkdir(parents = True, exist_ok = True)
        final_pdf.parent.mkdir(parents = True, exist_ok = True)

        shutil.copy2(source_pdf, sample_pdf)
        shutil.copy2(source_pdf, final_pdf)

        seed_payload = build_seed_json(document_name, page_number)
        write_json(seed_json, seed_payload)

        if not final_json.exists():
            write_json(final_json, seed_payload)

        packaged_pages += 1

    return packaged_pages, missing_pages


def main():
    """Run standalone annotation packaging from configured manifest.

    Args:
        None.

    Returns:
        None.
    """
    ensure_workspace_dirs()

    manifest = load_manifest()
    selection = selected_pages(manifest)

    packaged_pages = 0
    missing_pages = 0

    for document_name in sorted(selection.keys()):
        doc_packaged, doc_missing = package_document(document_name, selection[document_name])
        packaged_pages += doc_packaged
        missing_pages += doc_missing

    print(f"Packaged pages: {packaged_pages}", flush = True)
    print(f"Missing pages: {missing_pages}", flush = True)


if __name__ == "__main__":
    main()
