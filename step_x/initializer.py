"""
Step X: Initializer — seed label page data from step_2 polygon files.

For each PDF in the input directory, for each page:
  1. Reads the existing step_2 polygon JSON (if present).
  2. Creates a step_x label JSON with that polygon as doc_1's first label type.
  3. If no step_2 polygon exists, creates an empty label JSON with 1 document.
  4. Skips pages that already have a label JSON (preserves edits).

Usage:
    python step_x/initializer.py
"""

import json
import sys
import logging
from pathlib import Path

from pypdf import PdfReader

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR   = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_x.config import (
    LABEL_CONFIG_DIR,
    LABEL_FILES_TO_EXCLUDE,
    LABEL_FILES_TO_RUN,
    LABEL_INFO_TYPES,
    LABEL_INIT_SOURCE_DIR,
    LABEL_INPUT_DIR,
    RENDER_DPI,
)

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
INPUT_DIR  = ROOT_DIR / LABEL_INPUT_DIR
CONFIG_DIR = ROOT_DIR / LABEL_CONFIG_DIR
INIT_DIR   = ROOT_DIR / LABEL_INIT_SOURCE_DIR

log = logging.getLogger("label_initializer")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
log.setLevel(logging.INFO)


def _empty_doc_record():
    return {info_type: [] for info_type in LABEL_INFO_TYPES}


def _get_pdfs_to_process():
    all_pdfs = [p for p in INPUT_DIR.iterdir() if p.suffix.lower() == ".pdf"]
    if LABEL_FILES_TO_RUN:
        all_pdfs = [p for p in all_pdfs if p.name in LABEL_FILES_TO_RUN]
    if LABEL_FILES_TO_EXCLUDE:
        all_pdfs = [p for p in all_pdfs if p.name not in LABEL_FILES_TO_EXCLUDE]
    return sorted(all_pdfs)


def _load_step2_polygon(document_name, page_number):
    """Load a step_2 polygon for a page; return list of points or None."""
    polygon_path = INIT_DIR / document_name / "polygons" / f"page_{page_number:03d}.json"
    if not polygon_path.exists():
        return None
    with open(polygon_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    polygon = data.get("polygon", [])
    if len(polygon) < 3:
        return None
    return [{"x": int(pt["x"]), "y": int(pt["y"])} for pt in polygon]


def _load_step2_page_size(document_name, page_number):
    """Load page dimensions from step_2 page_sizes JSON if available."""
    size_path = INIT_DIR / document_name / "page_sizes" / f"page_{page_number:03d}.json"
    if not size_path.exists():
        return None, None
    with open(size_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("width_pixels"), data.get("height_pixels")


def initialize_document(pdf_path):
    document_name = pdf_path.stem
    doc_config_dir = CONFIG_DIR / document_name
    doc_config_dir.mkdir(parents=True, exist_ok=True)

    try:
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
    except Exception as e:
        log.error(f"[{document_name}] failed to read PDF: {e}")
        return

    log.info(f"[{document_name}] initializing {total_pages} pages...")
    created = skipped = 0

    for page_number in range(1, total_pages + 1):
        output_path = doc_config_dir / f"page_{page_number:03d}.json"

        if output_path.exists():
            skipped += 1
            continue

        step2_polygon = _load_step2_polygon(document_name, page_number)
        page_width, page_height = _load_step2_page_size(document_name, page_number)

        doc_1_record = _empty_doc_record()
        if step2_polygon and LABEL_INFO_TYPES:
            # Keep step_2 seed polygons in src_content when available.
            target_key = "src_content" if "src_content" in doc_1_record else LABEL_INFO_TYPES[0]
            doc_1_record[target_key] = [step2_polygon]

        page_record = {
            "page_number":   page_number,
            "source_file":   str(pdf_path),
            "page_width":    page_width,
            "page_height":   page_height,
            "render_dpi":    RENDER_DPI,
            "num_documents": 1,
            "documents": {
                "doc_1": doc_1_record,
            },
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(page_record, f, indent=2)

        created += 1

    log.info(f"[{document_name}] created {created}, skipped {skipped} existing")


def main():
    if not INPUT_DIR.exists():
        log.error(f"Input directory does not exist: {INPUT_DIR}")
        return

    pdfs = _get_pdfs_to_process()
    if not pdfs:
        log.warning("No PDFs found to initialize.")
        return

    log.info(f"Initializing label data for {len(pdfs)} PDF(s)...")
    for pdf_path in pdfs:
        initialize_document(pdf_path)
    log.info("Done.")


if __name__ == "__main__":
    main()
