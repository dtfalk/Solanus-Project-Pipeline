"""
Step X: Label Cropper — crop PDFs using per-page label JSON files.

For each page and each document defined on that page, outputs:
  - One single-page PDF per info type (polygons of that type on white background)
  - One unified single-page PDF (all types for that document combined)
  - One labels.json with the polygon data for that document

Output structure:
    label_cropped_pdfs/{source_doc}/page_{NNN}/{doc_key}/{info_type}.pdf
    label_cropped_pdfs/{source_doc}/page_{NNN}/{doc_key}/unified.pdf
    label_cropped_pdfs/{source_doc}/page_{NNN}/{doc_key}/labels.json

Documents with no polygons at all on a page are skipped.

Usage:
    python step_x/cropper.py
"""

import json
import sys
import logging
import argparse
from time import time
from pathlib import Path

from PIL import Image, ImageDraw
from pypdf import PdfReader

BOOTSTRAP_SCRIPT_DIR = Path(__file__).resolve().parent
BOOTSTRAP_ROOT_DIR   = BOOTSTRAP_SCRIPT_DIR.parent
if str(BOOTSTRAP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOOTSTRAP_ROOT_DIR))

from step_x.config import (
    LABEL_CONFIG_DIR,
    LABEL_CROP_OUTPUT_DIR,
    LABEL_FILES_TO_EXCLUDE,
    LABEL_FILES_TO_RUN,
    LABEL_INFO_TYPES,
    LABEL_INPUT_DIR,
    LABEL_PAGE_CHUNK_SIZE,
    LABEL_TARGET_PAGES,
    RENDER_DPI,
)

SCRIPT_DIR  = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPT_DIR.parent
INPUT_DIR   = ROOT_DIR / LABEL_INPUT_DIR
CONFIG_DIR  = ROOT_DIR / LABEL_CONFIG_DIR
OUTPUT_DIR  = ROOT_DIR / LABEL_CROP_OUTPUT_DIR

log = logging.getLogger("label_cropper")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
log.setLevel(logging.INFO)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_pdfs_to_process():
    all_pdfs = [p for p in INPUT_DIR.iterdir() if p.suffix.lower() == ".pdf"]
    if LABEL_FILES_TO_RUN:
        all_pdfs = [p for p in all_pdfs if p.name in LABEL_FILES_TO_RUN]
    if LABEL_FILES_TO_EXCLUDE:
        all_pdfs = [p for p in all_pdfs if p.name not in LABEL_FILES_TO_EXCLUDE]
    return sorted(all_pdfs)


def _get_pages_to_process(total_pages, page_start=None, page_end=None):
    """Return the set of page numbers to process, honouring CLI range and config target list."""
    # CLI range
    start = page_start if page_start is not None else 1
    end   = page_end   if page_end   is not None else total_pages
    start = max(1, min(start, end))
    end   = min(total_pages, max(start, end))
    cli_scope = set(range(start, end + 1))

    if LABEL_TARGET_PAGES:
        target = {p for p in LABEL_TARGET_PAGES if 1 <= p <= total_pages}
        return cli_scope & target if target else cli_scope

    return cli_scope


def _build_chunks(page_numbers, chunk_size):
    ordered = sorted(page_numbers)
    chunks, current = [], [ordered[0]]
    for p in ordered[1:]:
        if p == current[-1] + 1 and len(current) < chunk_size:
            current.append(p)
        else:
            chunks.append(current)
            current = [p]
    chunks.append(current)
    return chunks


def _pdf_to_images(pdf_path, dpi, first_page, last_page):
    from pdf2image import convert_from_path
    images = convert_from_path(pdf_path, dpi=dpi, first_page=first_page, last_page=last_page)
    return [(first_page + i, img) for i, img in enumerate(images)]


def _load_label_json(document_name, page_number):
    path = CONFIG_DIR / document_name / f"page_{page_number:03d}.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_image_as_pdf(image, output_path, resolution):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb = image.convert("RGB") if image.mode != "RGB" else image
    rgb.save(output_path, "PDF", resolution=resolution)


# ── Image compositing ──────────────────────────────────────────────────────────

def _composite_polygons_on_white(page_image, polygon_lists):
    """
    Render every polygon from every list in polygon_lists onto a white background,
    returning a full-size RGB image matching the original page dimensions.

    polygon_lists: iterable of lists of polygons, each polygon being a list of {x,y} dicts.
    """
    rgb   = page_image.convert("RGB")
    white = Image.new("RGB", rgb.size, "white")
    mask  = Image.new("L",   rgb.size, 0)
    draw  = ImageDraw.Draw(mask)

    for polygons in polygon_lists:
        for polygon in polygons:
            if len(polygon) >= 3:
                pts = [(int(p["x"]), int(p["y"])) for p in polygon]
                draw.polygon(pts, fill=255)

    return Image.composite(rgb, white, mask)


# ── Per-page / per-document processing ────────────────────────────────────────

def _process_document_on_page(page_image, page_number, doc_key, doc_record, output_page_dir):
    """
    Produce all output files for one document on one page.

    Returns True if any output was written, False if no polygons existed.
    """
    # Collect per-type polygon lists
    type_polygons = {
        info_type: doc_record.get(info_type, [])
        for info_type in LABEL_INFO_TYPES
    }

    # Skip if the document has no polygons at all on this page
    all_polygons = [p for polys in type_polygons.values() for p in polys if len(p) >= 3]
    if not all_polygons:
        return False

    doc_out_dir = output_page_dir / doc_key
    doc_out_dir.mkdir(parents=True, exist_ok=True)

    # Per-type PDFs
    for info_type, polygons in type_polygons.items():
        composited = _composite_polygons_on_white(page_image, [polygons])
        _save_image_as_pdf(composited, doc_out_dir / f"{info_type}.pdf", RENDER_DPI)

    # Unified PDF (all types combined)
    unified = _composite_polygons_on_white(page_image, list(type_polygons.values()))
    _save_image_as_pdf(unified, doc_out_dir / "unified.pdf", RENDER_DPI)

    # labels.json
    labels = {
        "page_number": page_number,
        "doc_key":     doc_key,
        "polygons":    type_polygons,
    }
    with open(doc_out_dir / "labels.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)

    return True


def _process_page(page_number, page_image, document_name):
    """Process all documents on a single page."""
    label_data = _load_label_json(document_name, page_number)
    if label_data is None:
        log.warning(f"[{document_name}] page {page_number}: no label JSON, skipping")
        return

    output_page_dir = OUTPUT_DIR / document_name / f"page_{page_number:03d}"
    num_docs        = label_data.get("num_documents", 1)
    docs            = label_data.get("documents", {})

    written = 0
    for i in range(1, num_docs + 1):
        doc_key    = f"doc_{i}"
        doc_record = docs.get(doc_key, {})
        if _process_document_on_page(page_image, page_number, doc_key, doc_record, output_page_dir):
            written += 1

    log.info(f"[{document_name}] page {page_number}: wrote output for {written}/{num_docs} document(s)")


# ── Document-level processing ──────────────────────────────────────────────────

def process_document(pdf_path, page_start=None, page_end=None, chunk_size=LABEL_PAGE_CHUNK_SIZE):
    document_name = pdf_path.stem
    log.info(f"[{document_name}] starting label crop...")

    # Check label data directory exists
    label_dir = CONFIG_DIR / document_name
    if not label_dir.exists():
        log.error(f"[{document_name}] label data directory not found: {label_dir}. Run initializer.py first.")
        return False

    try:
        total_pages = len(PdfReader(pdf_path).pages)
    except Exception as e:
        log.error(f"[{document_name}] failed to read PDF: {e}")
        return False

    pages_to_process = _get_pages_to_process(total_pages, page_start, page_end)
    if not pages_to_process:
        log.warning(f"[{document_name}] no pages in requested range.")
        return True

    log.info(f"[{document_name}] {total_pages} pages total, processing {len(pages_to_process)}")

    chunks = _build_chunks(pages_to_process, max(1, int(chunk_size)))

    for chunk in chunks:
        first_page, last_page = chunk[0], chunk[-1]
        try:
            page_images = _pdf_to_images(pdf_path, dpi=RENDER_DPI,
                                          first_page=first_page, last_page=last_page)
        except Exception as e:
            log.error(f"[{document_name}] failed to render pages {first_page}-{last_page}: {e}")
            continue

        for page_number, page_image in page_images:
            if page_number not in pages_to_process:
                page_image.close()
                continue
            try:
                _process_page(page_number, page_image, document_name)
            finally:
                page_image.close()

    log.info(f"[{document_name}] done.")
    return True


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Crop PDFs using label JSON files")
    parser.add_argument("--page-start", type=int, default=None)
    parser.add_argument("--page-end",   type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=LABEL_PAGE_CHUNK_SIZE)
    args = parser.parse_args()

    if not INPUT_DIR.exists():
        log.error(f"Input directory does not exist: {INPUT_DIR}")
        return

    pdfs = _get_pdfs_to_process()
    if not pdfs:
        log.warning("No PDFs found to process.")
        return

    log.info("Starting label crop pipeline...")
    log.info(f"Input:  {INPUT_DIR}")
    log.info(f"Config: {CONFIG_DIR}")
    log.info(f"Output: {OUTPUT_DIR}")

    t0 = time()
    succeeded = sum(
        1 for pdf_path in pdfs
        if process_document(
            pdf_path,
            page_start=args.page_start,
            page_end=args.page_end,
            chunk_size=max(1, args.chunk_size),
        )
    )

    log.info("")
    log.info("=" * 60)
    log.info("LABEL CROP SUMMARY")
    log.info("=" * 60)
    log.info(f"  Documents processed: {len(pdfs)}")
    log.info(f"  Documents succeeded: {succeeded}")
    log.info(f"  Documents failed:    {len(pdfs) - succeeded}")
    log.info(f"  Total runtime: {time() - t0:.1f}s")


if __name__ == "__main__":
    main()
