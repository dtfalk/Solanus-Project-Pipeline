"""
Utility: Test Image Crops.

Quick sanity check that renders the first N errors' PDF pages and
crops both the context line and flagged token images.

Saves the cropped images to clean/test_crops/ so you can visually
verify that bounding boxes are being interpreted correctly.

Usage:
    python util_test_image_crops.py
    python util_test_image_crops.py --count 50
    python util_test_image_crops.py --source errors_nathan.json
"""

import os
import json
import argparse

from image_helpers import render_pdf_page_as_image, crop_image_to_bounding_box

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = os.path.join(SCRIPT_DIR, "data")
PDF_PAGES_DIR = os.path.join(DATA_DIR, "pdf-pages")
OUTPUT_DIR    = os.path.join(SCRIPT_DIR, "test_crops")

# ── Settings ───────────────────────────────────────────────────────────────────
DEFAULT_DPI           = 300
DEFAULT_PADDING_INCHES = 0.02
DEFAULT_COUNT         = 20


def main():
    parser = argparse.ArgumentParser(description="Test image cropping on first N errors")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT,
                        help=f"Number of errors to test (default: {DEFAULT_COUNT})")
    parser.add_argument("--source", type=str, default="errors_stella.json",
                        help="Which error manifest file to use (default: errors_stella.json)")
    parser.add_argument("--dpi", type=int, default=DEFAULT_DPI,
                        help=f"DPI for rendering (default: {DEFAULT_DPI})")
    parser.add_argument("--padding", type=float, default=DEFAULT_PADDING_INCHES,
                        help=f"Padding in inches around crop (default: {DEFAULT_PADDING_INCHES})")
    args = parser.parse_args()

    # Load errors
    errors_file_path = os.path.join(DATA_DIR, args.source)
    if not os.path.exists(errors_file_path):
        print(f"Error manifest not found: {errors_file_path}")
        return

    with open(errors_file_path, "r", encoding="utf-8") as file_handle:
        all_errors = json.load(file_handle)["errors"]

    errors_to_test = all_errors[:args.count]
    print(f"Testing {len(errors_to_test)} errors from {args.source}")
    print(f"  DPI: {args.dpi}, Padding: {args.padding} inches")
    print(f"  Output: {OUTPUT_DIR}/\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for error_record in errors_to_test:
        error_id      = error_record["error_id"]
        document_name = error_record["source_document"].replace(".pdf", "")
        page_number   = error_record["page_number"]
        flagged_token = error_record["error"]

        pdf_file_path = os.path.join(
            PDF_PAGES_DIR, document_name,
            f"{document_name}-page-{page_number}.pdf"
        )

        print(f"  error {error_id}: {document_name} p{page_number} | \"{flagged_token}\" | {pdf_file_path}")

        # Render the full page
        full_page_image = render_pdf_page_as_image(pdf_file_path, args.dpi)

        # Crop the context line and flagged token
        context_polygon = error_record["bounding_boxes"]["context"]
        error_polygon   = error_record["bounding_boxes"]["error"]

        context_line_image = crop_image_to_bounding_box(full_page_image, context_polygon, args.dpi, args.padding)
        error_token_image  = crop_image_to_bounding_box(full_page_image, error_polygon, args.dpi, args.padding)

        # Save both crops
        context_line_image.save(os.path.join(OUTPUT_DIR, f"{error_id}_context.png"))
        error_token_image.save(os.path.join(OUTPUT_DIR, f"{error_id}_error.png"))

        print(f"    -> saved {error_id}_context.png + {error_id}_error.png")

    print(f"\nDone. Check {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
