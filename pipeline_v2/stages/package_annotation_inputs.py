"""Package curated few-shot pages into annotation-ready pipeline_v2 workspace.

This stage is intentionally non-destructive. It reads legacy data from step_2,
step_3, and step_x, then writes a clean editor-ready package under pipeline_v2.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_v2.config import PipelinePaths
from pipeline_v2.contracts import format_page_name


@dataclass(frozen = True)
class PackagingStats:
    """Summary of packaged files for reporting and audit logs.

    Args:
        selected_pages: Number of selected pages from manifest.
        copied_pdfs: Number of copied page PDFs.
        copied_json: Number of copied annotation JSON files.
        missing_pdfs: Number of missing page PDFs.
        missing_json: Number of missing annotation JSON files.

    Returns:
        Immutable packaging summary object.
    """

    selected_pages: int
    copied_pdfs: int
    copied_json: int
    missing_pdfs: int
    missing_json: int


def load_json(file_path: Path) -> dict[str, Any]:
    """Load JSON file from disk.

    Args:
        file_path: Path to JSON file.

    Returns:
        Parsed JSON dictionary.
    """
    with open(file_path, mode = "r", encoding = "utf-8") as file_handle:
        data = json.load(file_handle)
    return data


def save_json(file_path: Path, data: dict[str, Any]) -> None:
    """Write dictionary to JSON file using stable formatting.

    Args:
        file_path: Destination JSON path.
        data: Dictionary content to serialize.

    Returns:
        None. The file is written to disk.
    """
    file_path.parent.mkdir(parents = True, exist_ok = True)
    with open(file_path, mode = "w", encoding = "utf-8") as file_handle:
        json.dump(data, file_handle, indent = 2)
        file_handle.write("\n")


def resolve_json_source(paths: PipelinePaths, document_name: str, page_name: str) -> Path | None:
    """Resolve page JSON source with edit-preserving precedence.

    Precedence is chosen to preserve manual annotation work first:
    1) step_3/final/{doc}/{page}/{page}.json
    2) step_3/normalized/{doc}/{page}/{page}.json
    3) step_x/label_page_data/{doc}/{page}.json

    Args:
        paths: Shared pipeline path contract.
        document_name: Document folder name.
        page_name: Canonical page token like page_001.

    Returns:
        First existing JSON path according to precedence, else None.
    """
    candidate_paths = [
        paths.source_final / document_name / page_name / f"{page_name}.json",
        paths.source_normalized / document_name / page_name / f"{page_name}.json",
        paths.source_label_data / document_name / f"{page_name}.json",
    ]

    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return candidate_path

    return None


def package_annotation_inputs(
    paths: PipelinePaths,
    selected_pages: dict[str, list[int]],
) -> PackagingStats:
    """Package PDFs and JSON into pipeline_v2 editor-ready workspace.

    Args:
        paths: Shared pipeline path contract.
        selected_pages: Mapping of document names to selected pages.

    Returns:
        PackagingStats summary with copied and missing counts.
    """
    copied_pdfs = 0
    copied_json = 0
    missing_pdfs = 0
    missing_json = 0

    selected_page_count = sum(len(page_numbers) for page_numbers in selected_pages.values())

    for document_name in sorted(selected_pages.keys()):
        for page_number in selected_pages[document_name]:
            page_name = format_page_name(page_number)

            source_pdf = paths.source_polygon_pdfs / document_name / "pages" / f"{page_name}.pdf"
            target_pdf = paths.sample_pages_dir / document_name / "pages" / f"{page_name}.pdf"

            if source_pdf.exists():
                target_pdf.parent.mkdir(parents = True, exist_ok = True)
                shutil.copy2(source_pdf, target_pdf)
                copied_pdfs += 1
            else:
                missing_pdfs += 1

            source_json = resolve_json_source(
                paths         = paths,
                document_name = document_name,
                page_name     = page_name,
            )

            if source_json is None:
                missing_json += 1
                continue

            target_seed_json = paths.annotation_seed_dir / document_name / page_name / f"{page_name}.json"
            target_final_json = paths.final_dir / document_name / page_name / f"{page_name}.json"
            target_final_pdf = paths.final_dir / document_name / page_name / f"{page_name}.pdf"

            data = load_json(source_json)
            save_json(target_seed_json, data)
            save_json(target_final_json, data)

            if source_pdf.exists():
                target_final_pdf.parent.mkdir(parents = True, exist_ok = True)
                shutil.copy2(source_pdf, target_final_pdf)

            copied_json += 1

    return PackagingStats(
        selected_pages = selected_page_count,
        copied_pdfs    = copied_pdfs,
        copied_json    = copied_json,
        missing_pdfs   = missing_pdfs,
        missing_json   = missing_json,
    )
