"""Populate step_3/extracted_samples from page numbers in pages.txt.

For each entry in pages.txt like:
    Appendix 1: 1, 4, 6

This script creates:
    step_3/extracted_samples/Appendix_1/page_001/

and copies:
    step_2/polygon_cropped_pdfs/Appendix_1/pages/page_001.pdf

into that folder as:
    page_001.pdf
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PAGES_FILE = ROOT / "pages.txt"
SOURCE_ROOT = ROOT / "step_2" / "polygon_cropped_pdfs"
DEST_ROOT = ROOT / "step_3" / "extracted_samples"


def parse_pages_file(path: Path) -> dict[str, list[int]]:
    pages_by_doc: dict[str, list[int]] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue

        doc_label, pages_part = line.split(":", maxsplit=1)
        doc_label = doc_label.strip()
        if not doc_label:
            continue

        page_numbers = [int(value) for value in re.findall(r"\d+", pages_part)]
        if not page_numbers:
            continue

        seen: set[int] = set()
        ordered_unique: list[int] = []
        for number in page_numbers:
            if number not in seen:
                ordered_unique.append(number)
                seen.add(number)

        pages_by_doc[doc_label.replace(" ", "_")] = ordered_unique

    return pages_by_doc


def main() -> None:
    if not PAGES_FILE.exists():
        raise FileNotFoundError(f"Missing pages file: {PAGES_FILE}")
    if not SOURCE_ROOT.exists():
        raise FileNotFoundError(f"Missing source folder: {SOURCE_ROOT}")

    pages_by_doc = parse_pages_file(PAGES_FILE)
    copied = 0
    missing = 0

    for doc_name, page_numbers in pages_by_doc.items():
        source_pages_dir = SOURCE_ROOT / doc_name / "pages"
        doc_dest_root = DEST_ROOT / doc_name

        for page_num in page_numbers:
            page_stem = f"page_{page_num:03d}"
            source_pdf = source_pages_dir / f"{page_stem}.pdf"
            dest_page_dir = doc_dest_root / page_stem
            dest_pdf = dest_page_dir / f"{page_stem}.pdf"

            if not source_pdf.exists():
                missing += 1
                print(f"MISSING  {source_pdf}")
                continue

            dest_page_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_pdf, dest_pdf)
            copied += 1
            print(f"COPIED   {source_pdf} -> {dest_pdf}")

    print("\nDone.")
    print(f"Copied: {copied}")
    print(f"Missing: {missing}")


if __name__ == "__main__":
    main()