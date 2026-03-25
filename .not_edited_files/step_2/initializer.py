"""
Step 2: Initialize Polygon Crop Configs — generate per-page size and polygon JSON files.

For each PDF in the polygon input directory, this script:
  1. Reads the page size directly from the PDF metadata
  2. Converts each page size to pixels at the configured render DPI
  3. Writes one page-size JSON file per page
  4. Writes one default polygon JSON file per page

This is the first step in the manual polygon crop workflow. After these files
exist, you can inspect/edit the polygon JSON files and then run the polygon crop
pipeline to build the final cropped PDFs.

Output:
  - step_2/polygon_page_data/{document_name}/
      ├── page_sizes/
      │   ├── page_001.json
      │   ├── page_002.json
      │   └── ...
      └── polygons/
          ├── page_001.json
          ├── page_002.json
          └── ...

Usage:
    python step_2/polygon_initializer.py
"""

import json
import sys
import logging
from time import time
from pathlib import Path

from pypdf import PdfReader

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_2.config import (
    DEFAULT_CROP_POLYGON_INSET,
    POLYGON_CONFIG_DIR,
    POLYGON_FILES_TO_EXCLUDE,
    POLYGON_FILES_TO_RUN,
    POLYGON_INPUT_DIR,
    POLYGON_OVERWRITE_EXISTING,
    RENDER_DPI,
)


# ── File paths (all relative to this script's directory) ───────────────────────
SCRIPT_DIR  = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPT_DIR.parent
INPUT_DIR   = ROOT_DIR / POLYGON_INPUT_DIR
CONFIG_DIR  = ROOT_DIR / POLYGON_CONFIG_DIR


# ── Logging ────────────────────────────────────────────────────────────────────
log = logging.getLogger("polygon_initializer")
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


def convert_points_to_pixels(points, dpi):
    """Convert PDF points to pixels at the requested DPI."""
    return int(round((points / 72) * dpi))


def build_default_polygon(page_width, page_height):
    """Build an 8-point default crop polygon from the configured page insets."""
    # left_inset = max(0, int(DEFAULT_CROP_POLYGON_INSET["left"]))
    # top_inset = max(0, int(DEFAULT_CROP_POLYGON_INSET["top"]))
    # right_inset = max(0, int(DEFAULT_CROP_POLYGON_INSET["right"]))
    # bottom_inset = max(0, int(DEFAULT_CROP_POLYGON_INSET["bottom"]))

    # left = min(left_inset, max(0, page_width - 2))
    # top = min(top_inset, max(0, page_height - 2))
    # right = max(left + 1, page_width - right_inset)
    # bottom = max(top + 1, page_height - bottom_inset)

    # right = min(page_width - 1, right)
    # bottom = min(page_height - 1, bottom)

    # mid_x = (left + right) // 2
    # mid_y = (top + bottom) // 2
    return [
        {"x": 31, "y": 895},
        {"x": 2655,"y": 874},
        {"x": 5273, "y": 833},
        {"x": 5288, "y": 6001},
        {"x": 3403, "y": 6011},
        {"x": 3409, "y": 6812},
        {"x": 40, "y": 6835},
        {"x": 40, "y": 3438}
    ]



def write_page_size_json(page_sizes_dir, page_number, page_size_record):
    """Write the page-size JSON for a single page."""
    page_sizes_dir.mkdir(parents = True, exist_ok = True)
    output_path = page_sizes_dir / f"page_{page_number:03d}.json"

    with open(output_path, "w", encoding = "utf-8") as file:
        json.dump(page_size_record, file, indent = 2)


def scale_existing_polygon(existing_polygon, old_width, old_height, new_width, new_height):
    """Scale an existing polygon from one rendered page size to another."""
    width_scale = new_width / max(1, old_width)
    height_scale = new_height / max(1, old_height)

    scaled_polygon = []
    for point in existing_polygon:
        scaled_polygon.append(
            {
                "x": int(round(point["x"] * width_scale)),
                "y": int(round(point["y"] * height_scale)),
            }
        )

    return scaled_polygon


def write_polygon_json(polygons_dir, page_number, polygon_record):
    """Write the polygon JSON for a single page, respecting overwrite config."""
    polygons_dir.mkdir(parents = True, exist_ok = True)
    output_path = polygons_dir / f"page_{page_number:03d}.json"

    if output_path.exists() and not POLYGON_OVERWRITE_EXISTING:
        with open(output_path, "r", encoding = "utf-8") as file:
            existing_record = json.load(file)

        existing_polygon = existing_record.get("polygon", [])
        existing_width = int(existing_record.get("page_width", polygon_record["page_width"]))
        existing_height = int(existing_record.get("page_height", polygon_record["page_height"]))
        existing_dpi = int(existing_record.get("render_dpi", polygon_record["render_dpi"]))

        if (
            existing_width == polygon_record["page_width"]
            and existing_height == polygon_record["page_height"]
            and existing_dpi == polygon_record["render_dpi"]
        ):
            return

        polygon_record["polygon"] = scale_existing_polygon(
            existing_polygon,
            existing_width,
            existing_height,
            polygon_record["page_width"],
            polygon_record["page_height"],
        )

    with open(output_path, "w", encoding = "utf-8") as file:
        json.dump(polygon_record, file, indent = 2)


def process_single_document(pdf_path):
    """Generate the page-size JSON files and default polygon JSON files for one PDF."""
    document_name = pdf_path.stem
    log.info(f"[{document_name}] initializing page metadata...")

    output_base_dir = CONFIG_DIR / document_name
    page_sizes_dir = output_base_dir / "page_sizes"
    polygons_dir = output_base_dir / "polygons"

    try:
        pdf_reader = PdfReader(pdf_path)
    except Exception as exception:
        log.error(f"[{document_name}] failed to read PDF metadata: {exception}")
        return False

    for page_number, page in enumerate(pdf_reader.pages, start = 1):
        width_points = float(page.mediabox.width)
        height_points = float(page.mediabox.height)
        width_inches = width_points / 72
        height_inches = height_points / 72
        width_pixels = convert_points_to_pixels(width_points, RENDER_DPI)
        height_pixels = convert_points_to_pixels(height_points, RENDER_DPI)

        page_size_record = {
            "page_number": page_number,
            "width_points": width_points,
            "height_points": height_points,
            "width_inches": round(width_inches, 4),
            "height_inches": round(height_inches, 4),
            "width_pixels": width_pixels,
            "height_pixels": height_pixels,
            "render_dpi": RENDER_DPI,
            "source_pdf": str(pdf_path),
        }
        write_page_size_json(page_sizes_dir, page_number, page_size_record)

        polygon_record = {
            "page_number": page_number,
            "page_width": width_pixels,
            "page_height": height_pixels,
            "render_dpi": RENDER_DPI,
            "polygon": build_default_polygon(width_pixels, height_pixels),
            "source_pdf": str(pdf_path),
        }
        write_polygon_json(polygons_dir, page_number, polygon_record)

    log.info(f"[{document_name}] wrote {len(pdf_reader.pages)} page-size JSON files and polygon JSON files")
    return True


def main():
    """Entry point: generate the polygon page metadata and default polygon files."""
    start_time = time()

    if not INPUT_DIR.exists():
        log.error(f"Input directory does not exist: {INPUT_DIR}")
        return

    pdf_files = get_polygon_pdf_files_to_process()
    if not pdf_files:
        log.warning("No PDF files found to process!")
        return

    log.info("Initializing polygon crop workflow files...")
    log.info(f"Input directory:  {INPUT_DIR}")
    log.info(f"Config directory: {CONFIG_DIR}")
    log.info("")

    successful_documents = 0
    for pdf_path in pdf_files:
        if process_single_document(pdf_path):
            successful_documents += 1

    log.info("")
    log.info("=" * 70)
    log.info("INITIALIZATION SUMMARY")
    log.info("=" * 70)
    log.info(f"  Documents processed: {len(pdf_files)}")
    log.info(f"  Documents succeeded: {successful_documents}")
    log.info(f"  Documents failed:    {len(pdf_files) - successful_documents}")
    log.info(f"  Total runtime: {time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()