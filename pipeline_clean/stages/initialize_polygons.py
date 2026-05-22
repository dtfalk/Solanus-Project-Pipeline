"""Standalone polygon initialization stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/initialize_polygons.py
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from time import time

from pypdf import PdfReader

from pipeline_clean.config import (
    CLEANED_PDFS_DIR,
    POLYGONS_DIR,
    POLYGON_INSET_BOTTOM,
    POLYGON_INSET_LEFT,
    POLYGON_INSET_RIGHT,
    POLYGON_INSET_TOP,
    POLYGON_OVERWRITE_EXISTING,
    POLYGON_RENDER_DPI,
    ensure_workspace_dirs,
)
from pipeline_clean.contracts import format_page_name


def points_to_pixels(points, dpi):
    """Convert PDF points to pixels for configured DPI.

    Args:
        points: Point value from PDF metadata.
        dpi: Target render DPI.

    Returns:
        Pixel integer.
    """
    return int(round((points / 72) * dpi))


def default_polygon(width_pixels, height_pixels):
    """Build default polygon that keeps a central clean content area.

    Args:
        width_pixels: Page width in pixels.
        height_pixels: Page height in pixels.

    Returns:
        List of polygon points.
    """
    min_x = max(0, int(POLYGON_INSET_LEFT))
    min_y = max(0, int(POLYGON_INSET_TOP))
    max_x = max(min_x + 1, int(width_pixels) - int(POLYGON_INSET_RIGHT))
    max_y = max(min_y + 1, int(height_pixels) - int(POLYGON_INSET_BOTTOM))

    return [
        {"x": min_x, "y": min_y},
        {"x": max_x, "y": min_y},
        {"x": max_x, "y": max_y},
        {"x": min_x, "y": max_y},
    ]


def collect_cleaned_pdfs():
    """Collect cleaned PDFs for polygon initialization.

    Args:
        None.

    Returns:
        Sorted list of cleaned PDF paths.
    """
    if not CLEANED_PDFS_DIR.exists():
        return []

    return sorted(
        file_path
        for file_path in CLEANED_PDFS_DIR.iterdir()
        if file_path.is_file() and file_path.suffix.lower() == ".pdf"
    )


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


def process_document(pdf_path):
    """Initialize page size and polygon JSON files for one cleaned PDF.

    Args:
        pdf_path: Cleaned PDF path.

    Returns:
        Number of pages initialized.
    """
    document_name = pdf_path.stem
    reader = PdfReader(pdf_path)

    page_sizes_dir = POLYGONS_DIR / document_name / "page_sizes"
    polygons_dir = POLYGONS_DIR / document_name / "polygons"

    initialized_pages = 0
    for page_number, page in enumerate(reader.pages, start = 1):
        page_name = format_page_name(page_number)

        width_points = float(page.mediabox.width)
        height_points = float(page.mediabox.height)
        width_pixels = points_to_pixels(width_points, POLYGON_RENDER_DPI)
        height_pixels = points_to_pixels(height_points, POLYGON_RENDER_DPI)

        page_size_payload = {
            "page_number": page_number,
            "width_points": width_points,
            "height_points": height_points,
            "width_pixels": width_pixels,
            "height_pixels": height_pixels,
            "render_dpi": POLYGON_RENDER_DPI,
            "source_pdf": str(pdf_path),
        }

        polygon_payload = {
            "page_number": page_number,
            "page_width": width_pixels,
            "page_height": height_pixels,
            "render_dpi": POLYGON_RENDER_DPI,
            "polygon": default_polygon(width_pixels, height_pixels),
            "source_pdf": str(pdf_path),
        }

        page_size_path = page_sizes_dir / f"{page_name}.json"
        polygon_path = polygons_dir / f"{page_name}.json"

        if not page_size_path.exists() or POLYGON_OVERWRITE_EXISTING:
            write_json(page_size_path, page_size_payload)

        if not polygon_path.exists() or POLYGON_OVERWRITE_EXISTING:
            write_json(polygon_path, polygon_payload)

        initialized_pages += 1

    return initialized_pages


def main():
    """Run polygon initialization for all cleaned PDFs.

    Args:
        None.

    Returns:
        None.
    """
    logging.basicConfig(level = logging.INFO, format = "%(message)s", force = True)
    ensure_workspace_dirs()

    start_time = time()
    pdf_paths = collect_cleaned_pdfs()
    if not pdf_paths:
        logging.warning(f"No cleaned PDFs found in {CLEANED_PDFS_DIR}")
        return

    total_pages = 0
    for pdf_path in pdf_paths:
        page_count = process_document(pdf_path)
        total_pages += page_count
        logging.info(f"Initialized {pdf_path.stem}: {page_count} pages")

    logging.info(f"Polygon initialization complete in {time() - start_time:.2f}s | pages: {total_pages}")


if __name__ == "__main__":
    main()
