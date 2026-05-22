"""Rasterize page PDFs while preserving folder structure.

This script walks a source directory recursively, finds PDF files, renders each
PDF page to JPEG files using pdftoppm, and writes output to the same relative
path in a second output directory.

Default flow:
    source: step_3/sample_pages
    output: step_3/sample_pages_rasterized

Usage:
    python3 step_3/rasterize_sample_pages.py
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DIR = ROOT_DIR / "step_3" / "sample_pages"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "step_3" / "sample_pages_rasterized"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a rasterized copy of page PDFs with the same folder structure.",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Source root folder containing PDFs to rasterize.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output root folder for rasterized PDFs.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=150,
        help="Render DPI for rasterization (default: 150).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files in output.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without writing files.",
    )
    return parser.parse_args()


def iter_pdf_files(source_dir: Path) -> list[Path]:
    return sorted(path for path in source_dir.rglob("*.pdf") if path.is_file())


def output_prefix_for_page(output_pdf_path: Path, page_number: int, total_pages: int) -> Path:
    """Build output prefix path for pdftoppm output files."""
    if total_pages == 1:
        return output_pdf_path.with_suffix("")
    return output_pdf_path.with_suffix("").with_name(
        f"{output_pdf_path.stem}_p{page_number:03d}"
    )


def get_pdf_page_count(pdf_path: Path) -> int:
    """Read page count using pdfinfo from Poppler."""
    command = ["pdfinfo", str(pdf_path)]
    result = subprocess.run(command, check=True, capture_output=True, text=True)

    for line in result.stdout.splitlines():
        if line.lower().startswith("pages:"):
            value = line.split(":", maxsplit=1)[1].strip()
            return int(value)

    raise RuntimeError(f"Could not determine page count for {pdf_path}")

def get_pdf_page_width_pts(pdf_path: Path) -> float:
    """Return the width of page 1 in PDF points using pdfinfo."""
    command = ["pdfinfo", str(pdf_path)]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    for line in result.stdout.splitlines():
        if line.lower().startswith("page size:"):
            # e.g. "Page size:       2550.24 x 3300 pts"
            parts = line.split(":")[1].strip().split()
            return float(parts[0])
    raise RuntimeError(f"Could not determine page width for {pdf_path}")

def rasterize_pdf_to_jpegs(pdf_path: Path, output_pdf_path: Path, dpi: int) -> None:
    """Rasterize every page in a PDF to JPEG using pdftoppm."""
    total_pages = get_pdf_page_count(pdf_path)
    if total_pages < 1:
        raise ValueError(f"No pages found in {pdf_path}")

    output_pdf_path.parent.mkdir(parents=True, exist_ok=True)

    # Get page size in points and compute effective DPI
    # so output is always based on a standard letter-size equivalent
    page_pts_width = get_pdf_page_width_pts(pdf_path)
    standard_letter_width_pts = 612.0
    scale_factor = standard_letter_width_pts / page_pts_width
    effective_dpi = max(1, round(dpi * scale_factor))

    for page_number in range(1, total_pages + 1):
        output_prefix = output_prefix_for_page(output_pdf_path, page_number, total_pages)
        command = [
            "pdftoppm",
            "-jpeg",
            "-r", str(effective_dpi),
            "-f", str(page_number),
            "-l", str(page_number),
            "-singlefile",
            str(pdf_path),
            str(output_prefix),
        ]
        subprocess.run(command, check=True)


def ensure_pdftoppm_available() -> None:
    if shutil.which("pdftoppm") is None:
        raise RuntimeError(
            "pdftoppm not found. Install poppler-utils, then rerun this script."
        )
    if shutil.which("pdfinfo") is None:
        raise RuntimeError(
            "pdfinfo not found. Install poppler-utils, then rerun this script."
        )


def main() -> None:
    args = parse_args()

    source_dir = args.source_dir.resolve()
    output_dir = args.output_dir.resolve()
    dpi = args.dpi

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    ensure_pdftoppm_available()

    pdf_files = iter_pdf_files(source_dir)
    if not pdf_files:
        print(f"No PDF files found under {source_dir}")
        return

    converted_count = 0
    skipped_count = 0

    for source_pdf in pdf_files:
        relative_path = source_pdf.relative_to(source_dir)
        output_pdf = output_dir / relative_path
        output_jpeg_check = output_pdf.with_suffix(".jpg")

        if output_jpeg_check.exists() and not args.overwrite:
            skipped_count += 1
            print(f"SKIP     {output_jpeg_check} (already exists)")
            continue

        if args.dry_run:
            print(f"DRY-RUN  {source_pdf} -> {output_pdf.with_suffix('.jpg')}")
            converted_count += 1
            continue

        rasterize_pdf_to_jpegs(source_pdf, output_pdf, dpi=dpi)
        converted_count += 1
        print(f"RASTER   {source_pdf} -> {output_pdf.with_suffix('.jpg')}")

    print("\nDone.")
    print(f"Processed: {len(pdf_files)}")
    print(f"Rasterized: {converted_count}")
    print(f"Skipped: {skipped_count}")
    if not args.dry_run:
        print(f"Output folder: {output_dir}")


if __name__ == "__main__":
    main()