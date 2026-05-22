"""Standalone crop stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/crop_pages.py
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from time import time

from PIL import Image, ImageDraw
from pypdf import PdfReader

from pipeline_clean.config import (
    CLEANED_PDFS_DIR,
    CROP_PAGE_CHUNK_SIZE,
    CROPPED_PAGES_DIR,
    POLYGONS_DIR,
    POLYGON_RENDER_DPI,
    ensure_workspace_dirs,
)
from pipeline_clean.contracts import format_page_name
from pipeline_clean.stages.pdf_utils import merge_pdfs, pdf_to_images, save_image_as_pdf


def collect_cleaned_pdfs():
    """Collect cleaned PDFs to crop.

    Args:
        None.

    Returns:
        Sorted list of cleaned PDFs.
    """
    if not CLEANED_PDFS_DIR.exists():
        return []

    return sorted(
        file_path
        for file_path in CLEANED_PDFS_DIR.iterdir()
        if file_path.is_file() and file_path.suffix.lower() == ".pdf"
    )


def load_polygon_record(document_name, page_number):
    """Load polygon configuration JSON for a document page.

    Args:
        document_name: Document stem.
        page_number: 1-indexed page number.

    Returns:
        Polygon record dictionary.
    """
    page_name = format_page_name(page_number)
    polygon_path = POLYGONS_DIR / document_name / "polygons" / f"{page_name}.json"

    with open(polygon_path, mode = "r", encoding = "utf-8") as file_handle:
        return json.load(file_handle)


def clamp_polygon(polygon, width, height):
    """Clamp polygon vertices to image bounds.

    Args:
        polygon: List of point dictionaries.
        width: Image width.
        height: Image height.

    Returns:
        Clamped polygon list.
    """
    clamped = []
    for point in polygon:
        x_value = max(0, min(width - 1, int(point["x"])))
        y_value = max(0, min(height - 1, int(point["y"])))
        clamped.append({"x": x_value, "y": y_value})

    return clamped


def bounding_rect(polygon, width, height):
    """Compute polygon bounding rectangle.

    Args:
        polygon: List of clamped polygon points.
        width: Image width.
        height: Image height.

    Returns:
        Tuple (left, top, right, bottom).
    """
    x_values = [point["x"] for point in polygon]
    y_values = [point["y"] for point in polygon]

    left = max(0, min(x_values))
    top = max(0, min(y_values))
    right = min(width, max(x_values) + 1)
    bottom = min(height, max(y_values) + 1)
    return left, top, right, bottom


def apply_polygon(page_image, polygon):
    """Apply polygon mask while preserving original page dimensions.

    Args:
        page_image: PIL image.
        polygon: List of polygon points.

    Returns:
        Full-size cropped PIL image.
    """
    rgb_image = page_image.convert("RGB")
    white_background = Image.new("RGB", rgb_image.size, "white")

    mask = Image.new("L", rgb_image.size, 0)
    points = [(point["x"], point["y"]) for point in polygon]
    ImageDraw.Draw(mask).polygon(points, fill = 255)

    composited = Image.composite(rgb_image, white_background, mask)
    left, top, right, bottom = bounding_rect(polygon, rgb_image.width, rgb_image.height)

    cropped = composited.crop((left, top, right, bottom))
    output = Image.new("RGB", rgb_image.size, "white")
    output.paste(cropped, (left, top))
    return output


def chunk_pages(page_numbers, chunk_size):
    """Split sorted page numbers into bounded contiguous chunks.

    Args:
        page_numbers: Sorted list of 1-indexed page numbers.
        chunk_size: Maximum chunk length.

    Returns:
        List of list chunks.
    """
    if not page_numbers:
        return []

    chunks = []
    current_chunk = [page_numbers[0]]

    for page_number in page_numbers[1:]:
        is_consecutive = page_number == current_chunk[-1] + 1
        has_capacity = len(current_chunk) < chunk_size

        if is_consecutive and has_capacity:
            current_chunk.append(page_number)
        else:
            chunks.append(current_chunk)
            current_chunk = [page_number]

    chunks.append(current_chunk)
    return chunks


def process_document(pdf_path):
    """Crop all pages for one cleaned PDF using polygon records.

    Args:
        pdf_path: Cleaned PDF path.

    Returns:
        Output merged PDF path.
    """
    document_name = pdf_path.stem
    output_doc_dir = CROPPED_PAGES_DIR / document_name
    output_pages_dir = output_doc_dir / "pages"
    output_pages_dir.mkdir(parents = True, exist_ok = True)

    page_count = len(PdfReader(pdf_path).pages)
    page_numbers = list(range(1, page_count + 1))

    saved_pages = []
    for chunk in chunk_pages(page_numbers, max(1, int(CROP_PAGE_CHUNK_SIZE))):
        first_page = chunk[0]
        last_page = chunk[-1]

        rendered_images = pdf_to_images(
            pdf_path,
            dpi = POLYGON_RENDER_DPI,
            first_page = first_page,
            last_page = last_page,
        )

        for page_number, page_image in rendered_images:
            page_name = format_page_name(page_number)
            polygon_record = load_polygon_record(document_name, page_number)
            polygon = clamp_polygon(polygon_record.get("polygon", []), page_image.width, page_image.height)
            output_image = apply_polygon(page_image, polygon)

            output_page_path = output_pages_dir / f"{page_name}.pdf"
            save_image_as_pdf(
                output_image,
                output_page_path,
                resolution = POLYGON_RENDER_DPI,
            )
            saved_pages.append(output_page_path)
            page_image.close()

    merged_pdf = output_doc_dir / f"{document_name}.pdf"
    merge_pdfs(saved_pages, merged_pdf)
    return merged_pdf


def main():
    """Run crop stage for all cleaned PDFs.

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

    for pdf_path in pdf_paths:
        merged_pdf = process_document(pdf_path)
        logging.info(f"Cropped {pdf_path.stem} -> {merged_pdf}")

    logging.info(f"Crop stage complete in {time() - start_time:.2f}s")


if __name__ == "__main__":
    main()
