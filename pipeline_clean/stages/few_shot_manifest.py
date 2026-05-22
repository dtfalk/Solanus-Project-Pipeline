"""Standalone few-shot manifest stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/few_shot_manifest.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pipeline_clean.config import (
    FEW_SHOT_PAGES_TXT_PATH,
    MANIFEST_FILE_PATH,
    ensure_workspace_dirs,
)


def normalize_document_name(label):
    """Normalize document label into filesystem-safe token.

    Args:
        label: Document label from pages list.

    Returns:
        Normalized document token.
    """
    normalized = label.strip().replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def parse_pages_txt(pages_file_path):
    """Parse legacy pages list into document->page list mapping.

    Args:
        pages_file_path: Path to pages text file.

    Returns:
        Mapping of normalized document names to unique ordered page numbers.
    """
    mapping = {}
    lines = pages_file_path.read_text(encoding = "utf-8").splitlines()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or ":" not in line:
            continue

        raw_document, raw_pages = line.split(":", maxsplit = 1)
        document_name = normalize_document_name(raw_document)
        page_numbers = [int(value) for value in re.findall(r"\d+", raw_pages)]
        if not page_numbers:
            continue

        ordered_unique = []
        seen = set()
        for page_number in page_numbers:
            if page_number in seen:
                continue
            seen.add(page_number)
            ordered_unique.append(page_number)

        mapping[document_name] = ordered_unique

    return mapping


def build_manifest(page_mapping):
    """Create structured manifest dictionary.

    Args:
        page_mapping: Mapping from document names to pages.

    Returns:
        Structured manifest dictionary.
    """
    documents = {}
    for document_name in sorted(page_mapping.keys()):
        page_entries = []
        for page_number in page_mapping[document_name]:
            page_entries.append(
                {
                    "page_number": page_number,
                    "status": "approved",
                    "note": "",
                }
            )

        documents[document_name] = {"pages": page_entries}

    return {
        "version": "1.0",
        "selection_policy": "few_shot_curated",
        "documents": documents,
    }


def save_manifest(manifest):
    """Persist manifest JSON to configured path.

    Args:
        manifest: Manifest dictionary.

    Returns:
        None.
    """
    MANIFEST_FILE_PATH.parent.mkdir(parents = True, exist_ok = True)
    MANIFEST_FILE_PATH.write_text(
        json.dumps(manifest, indent = 2, sort_keys = True) + "\n",
        encoding = "utf-8",
    )


def main():
    """Build and save structured few-shot manifest from configured pages list.

    Args:
        None.

    Returns:
        None.
    """
    ensure_workspace_dirs()

    if not FEW_SHOT_PAGES_TXT_PATH.exists():
        raise FileNotFoundError(f"Pages list not found: {FEW_SHOT_PAGES_TXT_PATH}")

    page_mapping = parse_pages_txt(FEW_SHOT_PAGES_TXT_PATH)
    manifest = build_manifest(page_mapping)
    save_manifest(manifest)

    print(f"Saved manifest: {MANIFEST_FILE_PATH}", flush = True)
    print(f"Documents: {len(page_mapping)}", flush = True)
    print(f"Pages: {sum(len(pages) for pages in page_mapping.values())}", flush = True)


if __name__ == "__main__":
    main()
