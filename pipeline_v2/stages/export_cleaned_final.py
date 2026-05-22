"""Export cleaned final annotations for pipeline_v2.

This stage mirrors the intent of step_3/count_usages.py while being resilient to
missing keys and explicit about schema validation.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_v2.config import PipelinePaths
from pipeline_v2.contracts import (
    ANNOTATION_SCHEMA_KEYS,
    POLYGON_ENTRY_KEYS,
    REMOVED_AT_EXPORT_KEYS,
)


@dataclass(frozen = True)
class ExportStats:
    """Summary object for cleaned export stage.

    Args:
        pages_seen: Number of page folders inspected.
        json_exported: Number of JSON files exported.
        pdf_exported: Number of PDFs exported.
        schema_issues: Number of schema warnings encountered.

    Returns:
        Immutable export summary.
    """

    pages_seen: int
    json_exported: int
    pdf_exported: int
    schema_issues: int


def load_json(file_path: Path) -> dict[str, Any]:
    """Load JSON payload from disk.

    Args:
        file_path: Path to JSON input file.

    Returns:
        Parsed JSON dictionary.
    """
    with open(file_path, mode = "r", encoding = "utf-8") as file_handle:
        data = json.load(file_handle)
    return data


def save_json(file_path: Path, data: dict[str, Any]) -> None:
    """Persist JSON payload to disk with deterministic formatting.

    Args:
        file_path: Output JSON path.
        data: Dictionary to serialize.

    Returns:
        None. The file is written.
    """
    file_path.parent.mkdir(parents = True, exist_ok = True)
    with open(file_path, mode = "w", encoding = "utf-8") as file_handle:
        json.dump(data, file_handle, indent = 2)
        file_handle.write("\n")


def validate_document_schema(document_name: str, payload: dict[str, Any]) -> list[str]:
    """Validate annotation key and polygon shape assumptions.

    Args:
        document_name: Document key for error reporting.
        payload: Document-level annotation payload.

    Returns:
        List of validation issues. Empty list means no issues detected.
    """
    issues: list[str] = []

    actual_keys = set(payload.keys())
    expected_keys = set(ANNOTATION_SCHEMA_KEYS)
    if actual_keys != expected_keys:
        issues.append(
            f"{document_name}: schema key mismatch expected={sorted(expected_keys)} actual={sorted(actual_keys)}",
        )

    for annotation_key, polygon_list in payload.items():
        if not isinstance(polygon_list, list):
            issues.append(f"{document_name}:{annotation_key} should be list")
            continue

        for polygon in polygon_list:
            if not isinstance(polygon, dict):
                issues.append(f"{document_name}:{annotation_key} polygon should be object")
                continue

            polygon_keys = set(polygon.keys())
            if polygon_keys != set(POLYGON_ENTRY_KEYS):
                issues.append(
                    f"{document_name}:{annotation_key} polygon keys mismatch expected={list(POLYGON_ENTRY_KEYS)} actual={sorted(polygon_keys)}",
                )

    return issues


def remove_export_excluded_fields(page_data: dict[str, Any]) -> dict[str, Any]:
    """Remove annotation categories that are intentionally excluded at export.

    Args:
        page_data: Full page-level annotation payload.

    Returns:
        Mutated page payload with excluded keys removed.
    """
    documents = page_data.get("documents", {})
    for _, document_payload in documents.items():
        for excluded_key in REMOVED_AT_EXPORT_KEYS:
            document_payload.pop(excluded_key, None)
    return page_data


def export_cleaned_final(paths: PipelinePaths) -> ExportStats:
    """Create cleaned export output from pipeline_v2 editable final directory.

    Args:
        paths: Shared pipeline path contract.

    Returns:
        ExportStats summary.
    """
    pages_seen = 0
    json_exported = 0
    pdf_exported = 0
    schema_issues_count = 0

    if not paths.final_dir.exists():
        return ExportStats(
            pages_seen     = 0,
            json_exported  = 0,
            pdf_exported   = 0,
            schema_issues  = 0,
        )

    for document_dir in sorted(path for path in paths.final_dir.iterdir() if path.is_dir()):
        for page_dir in sorted(path for path in document_dir.iterdir() if path.is_dir()):
            pages_seen += 1

            page_json = page_dir / f"{page_dir.name}.json"
            page_pdf = page_dir / f"{page_dir.name}.pdf"

            if page_json.exists():
                page_data = load_json(page_json)

                for document_name, document_payload in page_data.get("documents", {}).items():
                    issues = validate_document_schema(document_name, document_payload)
                    if issues:
                        schema_issues_count += len(issues)

                page_data = remove_export_excluded_fields(page_data)

                target_json = paths.cleaned_final_dir / document_dir.name / page_dir.name / f"{page_dir.name}.json"
                save_json(target_json, page_data)
                json_exported += 1

            if page_pdf.exists():
                target_pdf = paths.cleaned_final_dir / document_dir.name / page_dir.name / f"{page_dir.name}.pdf"
                target_pdf.parent.mkdir(parents = True, exist_ok = True)
                shutil.copy2(page_pdf, target_pdf)
                pdf_exported += 1

    return ExportStats(
        pages_seen    = pages_seen,
        json_exported = json_exported,
        pdf_exported  = pdf_exported,
        schema_issues = schema_issues_count,
    )
