"""Standalone preprocessing stage for pipeline_clean.

Run with:
    python pipeline_clean/stages/preprocess_pdf.py
"""

from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from time import time

import cv2
import numpy as np
from pdf2image import convert_from_path
from PIL import Image

from pipeline_clean.config import (
    CLEANED_PDFS_DIR,
    PREPROCESS_CONCURRENT,
    PREPROCESS_DESKEW,
    PREPROCESS_DPI,
    PREPROCESS_MAX_WORKERS,
    SOURCE_PDFS_DIR,
    ensure_workspace_dirs,
)


def deskew_image(
    binary_image,
    document_name,
    page_index,
):
    """Deskew thresholded page image with conservative angle checks.

    Args:
        binary_image: Thresholded uint8 image.
        document_name: Source document stem.
        page_index: Zero-based page index for logging.

    Returns:
        Deskewed binary image.
    """
    coordinates = np.column_stack(np.where(binary_image > 0))
    if len(coordinates) < 1000:
        logging.warning(f"Sparse page | {document_name} | page {page_index}")
        return binary_image

    angle = cv2.minAreaRect(coordinates)[-1]
    if angle < -45:
        angle = -(90 + angle)
    else:
        angle = -angle

    if abs(angle) > 15:
        logging.warning(f"Large rotation skipped | {document_name} | page {page_index} | angle {angle:.2f}")
        return binary_image

    if abs(angle) < 1:
        return binary_image

    height, width = binary_image.shape[:2]
    center = (width // 2, height // 2)
    rotation = cv2.getRotationMatrix2D(center, angle, 1.0)

    return cv2.warpAffine(
        binary_image,
        rotation,
        (width, height),
        flags = cv2.INTER_CUBIC,
        borderMode = cv2.BORDER_REPLICATE,
    )


def preprocess_page(
    page_image,
    document_name,
    page_index,
):
    """Apply grayscale, denoise, contrast, threshold, and optional deskew.

    Args:
        page_image: PIL page image.
        document_name: Source document stem.
        page_index: Zero-based page index.

    Returns:
        Preprocessed uint8 image.
    """
    page_array = np.array(page_image)
    grayscale = cv2.cvtColor(page_array, cv2.COLOR_BGR2GRAY)
    denoised = cv2.medianBlur(grayscale, 3)
    contrast = cv2.convertScaleAbs(denoised, alpha = 1.5, beta = -100)
    thresholded = cv2.adaptiveThreshold(
        contrast,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        10,
    )

    if PREPROCESS_DESKEW:
        return deskew_image(thresholded, document_name, page_index)

    return thresholded


def collect_source_pdfs():
    """Collect source PDFs from configured source directory.

    Args:
        None.

    Returns:
        Sorted list of source PDF paths.
    """
    if not SOURCE_PDFS_DIR.exists():
        return []

    pdf_paths = [
        file_path
        for file_path in SOURCE_PDFS_DIR.iterdir()
        if file_path.is_file() and file_path.suffix.lower() == ".pdf"
    ]
    return sorted(pdf_paths)


def clean_single_pdf(pdf_path):
    """Clean one source PDF and write output to cleaned folder.

    Args:
        pdf_path: Source PDF path.

    Returns:
        Output cleaned PDF path.
    """
    start_time = time()
    document_name = pdf_path.stem
    page_images = convert_from_path(pdf_path, dpi = PREPROCESS_DPI)

    cleaned_pages = []
    for page_index, page_image in enumerate(page_images):
        cleaned = preprocess_page(page_image, document_name, page_index)
        cleaned_pages.append(Image.fromarray(cleaned))

    output_path = CLEANED_PDFS_DIR / f"{document_name}.pdf"
    if not cleaned_pages:
        raise RuntimeError(f"No pages were produced for {pdf_path}")

    cleaned_pages[0].save(
        output_path,
        save_all = True,
        append_images = cleaned_pages[1:],
    )

    logging.info(f"Cleaned {document_name} in {time() - start_time:.2f}s -> {output_path}")
    return output_path


def main():
    """Run standalone preprocessing on all source PDFs.

    Args:
        None.

    Returns:
        None.
    """
    logging.basicConfig(level = logging.INFO, format = "%(message)s", force = True)
    ensure_workspace_dirs()

    pdf_paths = collect_source_pdfs()
    if not pdf_paths:
        logging.warning(f"No source PDFs found in {SOURCE_PDFS_DIR}")
        return

    logging.info(f"Found {len(pdf_paths)} source PDFs")

    if PREPROCESS_CONCURRENT:
        max_workers = max(1, int(PREPROCESS_MAX_WORKERS))
        with ProcessPoolExecutor(max_workers = max_workers) as executor:
            futures = [executor.submit(clean_single_pdf, pdf_path) for pdf_path in pdf_paths]
            for future in as_completed(futures):
                future.result()
    else:
        for pdf_path in pdf_paths:
            clean_single_pdf(pdf_path)


if __name__ == "__main__":
    main()
