"""Extract selected sample pages from polygon-cropped PDFs.

Reads page selections from pages.txt and copies matching files from:
    step_2/polygon_cropped_pdfs/<Document>/pages/page_XXX.pdf

into:
    step_3/sample_pages/<Document>/pages/page_XXX.pdf

Usage:
    python3 step_3/extract_sample_pages.py
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_PAGES_FILE = ROOT_DIR / "pages.txt"
DEFAULT_SOURCE_DIR = ROOT_DIR / "step_2" / "polygon_cropped_pdfs"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "step_3" / "sample_pages"


def parse_pages_file(pages_file: Path) -> dict[str, list[int]]:
    """Parse lines like 'Appendix 1: 1, 4, 6' into a document->pages mapping."""
    document_pages: dict[str, list[int]] = {}

    for raw_line in pages_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if ":" not in line:
            continue

        document_label, pages_part = line.split(":", maxsplit=1)
        document_label = document_label.strip()
        pages_part = pages_part.strip()

        if not document_label:
            continue

        page_numbers = [int(value) for value in re.findall(r"\d+", pages_part)]
        if not page_numbers:
            continue

        # Keep order from pages.txt while removing duplicates.
        seen = set()
        ordered_unique = []
        for page_number in page_numbers:
            if page_number not in seen:
                ordered_unique.append(page_number)
                seen.add(page_number)

        document_pages[document_label] = ordered_unique

    return document_pages


def normalize_document_name(label: str) -> str:
    """Convert labels like 'Appendix 1' -> 'Appendix_1'."""
    normalized = label.strip().replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def copy_selected_pages(
    pages_map: dict[str, list[int]],
    source_root: Path,
    output_root: Path,
    dry_run: bool,
) -> tuple[int, int]:
    """Copy selected page PDFs. Returns (copied_count, missing_count)."""
    copied_count = 0
    missing_count = 0

    for document_label, page_numbers in pages_map.items():
        source_doc_name = normalize_document_name(document_label)
        source_pages_dir = source_root / source_doc_name / "pages"
        output_pages_dir = output_root / source_doc_name / "pages"

        if not dry_run:
            output_pages_dir.mkdir(parents=True, exist_ok=True)

        for page_number in page_numbers:
            source_file = source_pages_dir / f"page_{page_number:03d}.pdf"
            output_file = output_pages_dir / f"page_{page_number:03d}.pdf"

            if not source_file.exists():
                missing_count += 1
                print(f"MISSING  {source_file}")
                continue

            copied_count += 1
            if dry_run:
                print(f"DRY-RUN  {source_file} -> {output_file}")
            else:
                shutil.copy2(source_file, output_file)
                print(f"COPIED   {source_file} -> {output_file}")

    return copied_count, missing_count


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract sample pages listed in pages.txt from polygon-cropped PDFs.",
    )
    parser.add_argument(
        "--pages-file",
        type=Path,
        default=DEFAULT_PAGES_FILE,
        help="Path to pages list file (default: pages.txt in project root).",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Root folder containing polygon-cropped PDFs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output folder for extracted sample pages.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without copying files.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    pages_file = args.pages_file.resolve()
    source_dir = args.source_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not pages_file.exists():
        raise FileNotFoundError(f"Pages file not found: {pages_file}")

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    pages_map = parse_pages_file(pages_file)
    if not pages_map:
        print(f"No pages found in {pages_file}")
        return

    copied_count, missing_count = copy_selected_pages(
        pages_map=pages_map,
        source_root=source_dir,
        output_root=output_dir,
        dry_run=args.dry_run,
    )

    print("\nDone.")
    print(f"Copied files: {copied_count}")
    print(f"Missing files: {missing_count}")
    if not args.dry_run:
        print(f"Output folder: {output_dir}")


if __name__ == "__main__":
    main()