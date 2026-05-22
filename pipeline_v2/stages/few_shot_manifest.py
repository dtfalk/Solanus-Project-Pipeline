"""Few-shot page selection manifest workflow for pipeline_v2.

This stage replaces direct, ad-hoc dependence on pages.txt by introducing a
structured manifest that can carry review state and notes per selected page.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def normalize_document_name(label: str) -> str:
    """Normalize a user-facing document label into a folder-safe token.

    Args:
        label: Document label from user input or legacy pages.txt.

    Returns:
        Document name suitable for filesystem paths.
    """
    normalized = label.strip().replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def parse_legacy_pages_txt(pages_file: Path) -> dict[str, list[int]]:
    """Parse legacy pages.txt content into document->pages mapping.

    Args:
        pages_file: Path to legacy pages list file.

    Returns:
        Dictionary mapping normalized document names to unique ordered pages.
    """
    document_pages: dict[str, list[int]] = {}

    for raw_line in pages_file.read_text(encoding = "utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if ":" not in line:
            continue

        raw_document_label, pages_part = line.split(":", maxsplit = 1)
        document_name = normalize_document_name(raw_document_label)
        page_numbers = [int(value) for value in re.findall(r"\d+", pages_part)]
        if not page_numbers:
            continue

        seen: set[int] = set()
        ordered_unique_pages: list[int] = []
        for page_number in page_numbers:
            if page_number in seen:
                continue
            seen.add(page_number)
            ordered_unique_pages.append(page_number)

        document_pages[document_name] = ordered_unique_pages

    return document_pages


def build_manifest_from_mapping(
    page_mapping: dict[str, list[int]],
    source_label: str,
) -> dict[str, Any]:
    """Build structured manifest JSON from document page mapping.

    Args:
        page_mapping: Mapping from document names to selected page numbers.
        source_label: Human-readable string identifying how mapping was created.

    Returns:
        Manifest dictionary with page-level review metadata.
    """
    documents: dict[str, Any] = {}

    for document_name in sorted(page_mapping.keys()):
        pages: list[dict[str, Any]] = []
        for page_number in page_mapping[document_name]:
            pages.append(
                {
                    "page_number": page_number,
                    "status": "approved",
                    "note": "",
                }
            )

        documents[document_name] = {
            "pages": pages,
        }

    manifest = {
        "version": "1.0",
        "source": source_label,
        "selection_policy": "few_shot_curated",
        "documents": documents,
    }
    return manifest


def write_manifest(manifest_path: Path, manifest: dict[str, Any]) -> None:
    """Write manifest JSON to disk with deterministic formatting.

    Args:
        manifest_path: Destination path for manifest file.
        manifest: Manifest dictionary to serialize.

    Returns:
        None. The manifest is persisted to disk.
    """
    manifest_path.parent.mkdir(parents = True, exist_ok = True)
    manifest_path.write_text(
        json.dumps(manifest, indent = 2, sort_keys = True) + "\n",
        encoding = "utf-8",
    )


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load manifest JSON from disk.

    Args:
        manifest_path: Path to existing manifest file.

    Returns:
        Parsed manifest dictionary.
    """
    return json.loads(manifest_path.read_text(encoding = "utf-8"))


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    """Validate manifest structure and return human-readable issues.

    Args:
        manifest: Parsed manifest dictionary.

    Returns:
        List of validation issue strings. Empty list means valid.
    """
    issues: list[str] = []

    if "documents" not in manifest:
        issues.append("Missing top-level key: documents")
        return issues

    documents = manifest.get("documents", {})
    if not isinstance(documents, dict):
        issues.append("Top-level documents value must be an object")
        return issues

    for document_name, payload in documents.items():
        if not isinstance(payload, dict):
            issues.append(f"Document payload must be object: {document_name}")
            continue

        pages = payload.get("pages")
        if not isinstance(pages, list):
            issues.append(f"Document pages must be list: {document_name}")
            continue

        for page_entry in pages:
            if not isinstance(page_entry, dict):
                issues.append(f"Page entry must be object: {document_name}")
                continue

            if "page_number" not in page_entry:
                issues.append(f"Missing page_number in: {document_name}")
                continue

            page_number = page_entry["page_number"]
            if not isinstance(page_number, int) or page_number < 1:
                issues.append(
                    f"Invalid page_number in {document_name}: {page_number}",
                )

            status = page_entry.get("status", "approved")
            if status not in {"approved", "candidate", "excluded"}:
                issues.append(
                    f"Invalid status in {document_name} page {page_number}: {status}",
                )

    return issues


def get_selected_pages(
    manifest: dict[str, Any],
    include_candidates: bool = False,
) -> dict[str, list[int]]:
    """Extract selected pages from manifest by review status.

    Args:
        manifest: Parsed and validated manifest dictionary.
        include_candidates: Include candidate pages in output selection.

    Returns:
        Mapping from document names to selected page numbers.
    """
    selected_statuses = {"approved"}
    if include_candidates:
        selected_statuses.add("candidate")

    selection: dict[str, list[int]] = {}
    documents = manifest.get("documents", {})

    for document_name, payload in documents.items():
        pages = payload.get("pages", [])
        chosen_pages: list[int] = []

        for page_entry in pages:
            status = page_entry.get("status", "approved")
            if status not in selected_statuses:
                continue

            chosen_pages.append(int(page_entry["page_number"]))

        if chosen_pages:
            selection[document_name] = sorted(set(chosen_pages))

    return selection
