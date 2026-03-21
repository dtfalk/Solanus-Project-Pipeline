"""
Step 2: Crop PDF Pages From Polygons — crop pages using per-page polygon JSON files.

For each PDF in the polygon input directory, this script:
    1. Renders each page as an image
    2. Loads the polygon JSON for that page
    3. Crops the content to the polygon bounds
    4. Pastes that crop back onto a white canvas the same size as the original page
    5. Saves each full-size edited page as a PDF
    6. Merges all edited pages into a final PDF

This script expects the polygon JSON files to already exist. Generate them first
with polygon_initializer.py, then edit them as needed.

Usage:
    python step_2/polygon_cropper.py
"""

import json
import sys
import logging
from time import time
from pathlib import Path

from PIL import Image, ImageDraw

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_2.config import (
    POLYGON_CONFIG_DIR,
    POLYGON_CROP_OUTPUT_DIR,
    POLYGON_FILES_TO_EXCLUDE,
    POLYGON_FILES_TO_RUN,
    POLYGON_INPUT_DIR,
    POLYGON_TARGET_PAGES,
    RENDER_DPI,
)
from step_2.pdf_utils import merge_pdfs, pdf_to_images, save_image_as_pdf


# ── File paths (all relative to this script's directory) ───────────────────────
SCRIPT_DIR  = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPT_DIR.parent
INPUT_DIR   = ROOT_DIR / POLYGON_INPUT_DIR
CONFIG_DIR  = ROOT_DIR / POLYGON_CONFIG_DIR
OUTPUT_DIR  = ROOT_DIR / POLYGON_CROP_OUTPUT_DIR


# ── Logging ────────────────────────────────────────────────────────────────────
log = logging.getLogger("polygon_cropper")
logging.basicConfig(level = logging.WARNING, format = "%(levelname)s %(message)s")
log.setLevel(logging.INFO)


def get_polygon_pdf_files_to_process():
    """
    Get list of PDF files to process based on the polygon workflow config.

    Applies inclusion filter first, then exclusion filter.
    Returns list of full file paths.
    """
    all_pdfs = [pdf_path for pdf_path in INPUT_DIR.iterdir() if pdf_path.suffix.lower() == ".pdf"]

    if POLYGON_FILES_TO_RUN:
        all_pdfs = [pdf_path for pdf_path in all_pdfs if pdf_path.name in POLYGON_FILES_TO_RUN]

    if POLYGON_FILES_TO_EXCLUDE:
        all_pdfs = [pdf_path for pdf_path in all_pdfs if pdf_path.name not in POLYGON_FILES_TO_EXCLUDE]

    return sorted(all_pdfs)


def get_target_pages(total_pages):
    """
    Return the set of page numbers we want to overwrite.

    If no target pages are specified, then process every page.
    """
    if not POLYGON_TARGET_PAGES:
        return set(range(1, total_pages + 1))

    valid_pages = set()
    for page_number in POLYGON_TARGET_PAGES:
        if 1 <= page_number <= total_pages:
            valid_pages.add(page_number)
        else:
            log.warning(f"Skipping invalid target page {page_number}. Document only has {total_pages} pages.")

    return valid_pages


def load_polygon_record(document_name, page_number):
    """Load the polygon JSON for one page."""
    polygon_path = CONFIG_DIR / document_name / "polygons" / f"page_{page_number:03d}.json"
    if not polygon_path.exists():
        raise FileNotFoundError(f"Polygon JSON not found: {polygon_path}")

    with open(polygon_path, "r", encoding = "utf-8") as file:
        return json.load(file)


def validate_polygon(polygon, image_width, image_height):
    """
    Validate and clamp a polygon so it fits within the image bounds.

    Returns:
        (validated_polygon, error_message) where error_message is None on success
    """
    if not isinstance(polygon, list) or len(polygon) < 3:
        return None, "Polygon must contain at least 3 points"

    validated_polygon = []
    for point in polygon:
        if "x" not in point or "y" not in point:
            return None, "Every polygon point must contain x and y"

        x_coordinate = int(point["x"])
        y_coordinate = int(point["y"])
        x_coordinate = max(0, min(image_width - 1, x_coordinate))
        y_coordinate = max(0, min(image_height - 1, y_coordinate))
        validated_polygon.append({"x": x_coordinate, "y": y_coordinate})

    unique_points = {(point["x"], point["y"]) for point in validated_polygon}
    if len(unique_points) < 3:
        return None, "Polygon must contain at least 3 unique points"

    return validated_polygon, None


def polygon_to_bounding_rectangle(polygon, image_width, image_height):
    """Convert a polygon to its bounding rectangle for intermediate cropping."""
    all_x_coordinates = [point["x"] for point in polygon]
    all_y_coordinates = [point["y"] for point in polygon]

    left = max(0, min(all_x_coordinates))
    top = max(0, min(all_y_coordinates))
    right = min(image_width, max(all_x_coordinates) + 1)
    bottom = min(image_height, max(all_y_coordinates) + 1)
    return left, top, right, bottom


def apply_polygon_to_full_page(page_image, polygon):
    """
    Keep the output page the same size as the original page.

    This crops the content to the polygon bounds and then pastes that crop back
    onto a white canvas with the exact original page dimensions.
    """
    rgb_image = page_image.convert("RGB")
    white_background = Image.new("RGB", rgb_image.size, "white")
    polygon_mask = Image.new("L", rgb_image.size, 0)

    polygon_points = [(point["x"], point["y"]) for point in polygon]
    ImageDraw.Draw(polygon_mask).polygon(polygon_points, fill = 255)

    composited_image = Image.composite(rgb_image, white_background, polygon_mask)
    left, top, right, bottom = polygon_to_bounding_rectangle(polygon, rgb_image.width, rgb_image.height)

    cropped_content = composited_image.crop((left, top, right, bottom))
    full_size_output = Image.new("RGB", rgb_image.size, "white")
    full_size_output.paste(cropped_content, (left, top))
    return full_size_output


def process_single_page(page_number, page_image, document_name, output_base_dir):
    """
    Apply the configured polygon to a single page and save it as a PDF.

    Returns:
        path to the cropped page PDF on success, None on failure
    """
    try:
        polygon_record = load_polygon_record(document_name, page_number)
    except Exception as exception:
        log.error(f"[{document_name}] page {page_number}: {exception}")
        return None

    polygon, error_message = validate_polygon(
        polygon_record.get("polygon", []),
        page_image.width,
        page_image.height,
    )
    if error_message:
        log.error(f"[{document_name}] page {page_number}: {error_message}")
        return None

    cropped_page_image = apply_polygon_to_full_page(page_image, polygon)

    pages_dir = output_base_dir / "pages"
    pages_dir.mkdir(parents = True, exist_ok = True)

    output_path = pages_dir / f"page_{page_number:03d}.pdf"
    save_image_as_pdf(cropped_page_image, output_path, resolution = RENDER_DPI)
    return output_path


def process_single_document(pdf_path):
    """
    Process an entire PDF document using its per-page polygon JSON files.

    Returns:
        path to the merged output PDF, or None on total failure
    """
    document_name = pdf_path.stem
    log.info(f"[{document_name}] starting polygon crop processing...")

    output_base_dir = OUTPUT_DIR / document_name
    output_base_dir.mkdir(parents = True, exist_ok = True)

    log.info(f"[{document_name}] converting PDF to images at {RENDER_DPI} DPI...")
    try:
        page_images = pdf_to_images(pdf_path, dpi = RENDER_DPI)
    except Exception as exception:
        log.error(f"[{document_name}] failed to convert PDF to images: {exception}")
        return None

    log.info(f"[{document_name}] found {len(page_images)} pages")

    target_pages = get_target_pages(len(page_images))
    if POLYGON_TARGET_PAGES:
        log.info(f"[{document_name}] overwriting target pages: {sorted(target_pages)}")

    pages_dir = output_base_dir / "pages"
    pages_dir.mkdir(parents = True, exist_ok = True)

    page_paths = []
    for page_number, page_image in page_images:
        existing_page_path = pages_dir / f"page_{page_number:03d}.pdf"
        should_write_page = page_number in target_pages or not existing_page_path.exists()

        if should_write_page:
            page_path = process_single_page(page_number, page_image, document_name, output_base_dir)
            if page_path is not None:
                page_paths.append(page_path)
        else:
            page_paths.append(existing_page_path)

    if not page_paths:
        log.error(f"[{document_name}] no pages were successfully processed!")
        return None

    page_paths.sort()

    final_pdf_path = output_base_dir / f"{document_name}.pdf"
    log.info(f"[{document_name}] merging pages into final PDF...")

    try:
        merge_pdfs(page_paths, final_pdf_path)
        log.info(f"[{document_name}] saved final PDF: {final_pdf_path}")
        return final_pdf_path
    except Exception as exception:
        log.error(f"[{document_name}] failed to merge PDFs: {exception}")
        return None


def main():
    """Entry point: run the polygon crop pipeline from the saved JSON files."""
    start_time = time()

    if not INPUT_DIR.exists():
        log.error(f"Input directory does not exist: {INPUT_DIR}")
        return

    pdf_files = get_polygon_pdf_files_to_process()
    if not pdf_files:
        log.warning("No PDF files found to process!")
        return

    log.info("Starting polygon crop pipeline...")
    log.info(f"Input directory:  {INPUT_DIR}")
    log.info(f"Config directory: {CONFIG_DIR}")
    log.info(f"Output directory: {OUTPUT_DIR}")
    log.info(f"Target pages: {POLYGON_TARGET_PAGES if POLYGON_TARGET_PAGES else 'all'}")
    log.info("")

    successful_documents = 0
    for pdf_path in pdf_files:
        if process_single_document(pdf_path) is not None:
            successful_documents += 1

    log.info("")
    log.info("=" * 70)
    log.info("POLYGON CROP SUMMARY")
    log.info("=" * 70)
    log.info(f"  Documents processed: {len(pdf_files)}")
    log.info(f"  Documents succeeded: {successful_documents}")
    log.info(f"  Documents failed:    {len(pdf_files) - successful_documents}")
    log.info(f"  Total runtime: {time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()