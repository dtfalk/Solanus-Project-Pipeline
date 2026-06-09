import os
import cv2
import numpy as np
import logging
import shutil
import subprocess
import tempfile
from time import time
import platform
from pathlib import Path
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import *

# Pages rendered + cleaned in memory at once. A single 300-DPI page is ~400+ MB as an
# RGB array, so holding a whole volume (the old behavior) needed tens of GB and swapped.
# Chunking bounds peak memory to ~PAGES_PER_CHUNK pages regardless of document length;
# finished chunks are written to disk immediately and merged with poppler's pdfunite.
PAGES_PER_CHUNK = 8

def get_poppler_path():
    if platform.system() == "Windows":
        return r"C:\poppler\poppler-25.12.0\Library\bin"
    return None

def deskew(image, document_name, page_index):
    coords = np.column_stack(np.where(image > 0))

    if len(coords) < 1000:
        logging.warning(f"Sparse page | {document_name} | page {page_index}")
        return image

    angle = cv2.minAreaRect(coords)[-1]

    if angle < -45:
        angle = -(90 + angle)
    else:
        angle = -angle

    # Flag suspicious angles
    if abs(angle) > 15:
        logging.warning(f"Large rotation | {document_name} | page {page_index} | angle: {angle:.2f}")
        return image

    if abs(angle) < 1:
        return image

    (h, w) = image.shape[:2]
    center = (w // 2, h // 2)

    M = cv2.getRotationMatrix2D(center, angle, 1.0)

    rotated = cv2.warpAffine(
        image, M, (w, h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_REPLICATE
    )

    return rotated


def preprocess(page_image, document_name, page_index):
    
    # Load the image into a numpy array
    page_array = np.array(page_image)

    # 1. Cast to greyscale so easier to see where text starts and ends
    gray = cv2.cvtColor(page_array, cv2.COLOR_BGR2GRAY)

    # 2. Denoise the image
    denoised = cv2.medianBlur(gray, 3)

    # 3. Increase contrast to make easier to separate text from background
    contrast = cv2.convertScaleAbs(denoised, alpha = 1.5, beta = -100)

    # 4. Adaptive threshold
    threshold = cv2.adaptiveThreshold(
        contrast, 255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31, 10
    )

    # 5. Deskew if flag set for it
    if DESKEW_FLAG:
        cleaned = deskew(threshold, document_name, page_index)
    else:
        cleaned = threshold
    
    return cleaned, page_index


def get_pdf_paths(pdf_folder):
    """
    Returns a list of PDF paths to process, based on FILES_TO_RUN and FILES_TO_EXCLUDE.
    """

    # If FILES_TO_RUN is specified, then grab those files. Otherwise, grab all pdfs in source data dir root
    pdfs = []
    if FILES_TO_RUN:
        pdfs = [pdf_folder / f for f in FILES_TO_RUN
                if ((pdf_folder / f).exists()
                and (pdf_folder / f).is_file()
                and (pdf_folder / f).suffix.lower() == ".pdf"
                and not f in FILES_TO_EXCLUDE)]

    else:
        pdfs = [f for f in pdf_folder.iterdir()
                if (f.is_file()
                and f.suffix.lower() == ".pdf"
                and not f.name in FILES_TO_EXCLUDE)]

    return pdfs


def clean_pdf(pdf_path, output_folder, max_workers):
    """
    Converts a PDF to cleaned images and saves back as a single PDF.
    """

    # Grab start time for logging
    logging.basicConfig(level = logging.INFO, force = True, format = "%(message)s")
    start_time = time()

    # Extract document name and log it
    document_name = pdf_path.stem
    logging.info(f"\n{'-' * 70}")
    logging.info(f"Cleaning {document_name}")
    logging.info(f"{'-' * 70}\n")

    # Page count up front (no rendering) so we can stream in bounded chunks.
    info = pdfinfo_from_path(pdf_path, poppler_path = get_poppler_path())
    n_pages = int(info["Pages"])

    save_path = output_folder / f"{document_name}.pdf"
    chunk_paths = []

    # One pool reused across all chunks (when concurrency is on).
    executor = (ProcessPoolExecutor(max_workers = max_workers)
                if CONCURRENT_FLAG and max_workers > 1 else None)

    # Temp dir lives next to the output (same filesystem) and is auto-removed.
    with tempfile.TemporaryDirectory(dir = output_folder) as tmp_dir:
        try:
            for chunk_start in range(1, n_pages + 1, PAGES_PER_CHUNK):
                chunk_end = min(chunk_start + PAGES_PER_CHUNK - 1, n_pages)

                # Render ONLY this chunk's pages
                page_images = convert_from_path(
                    pdf_path,
                    dpi = TARGET_DPI,
                    first_page = chunk_start,
                    last_page = chunk_end,
                    poppler_path = get_poppler_path()
                )

                # Clean the chunk (pool if available, else serially)
                if executor is not None:
                    futures = [executor.submit(preprocess, img, document_name, chunk_start + i)
                               for i, img in enumerate(page_images)]
                    unsorted_outputs = []
                    for future in as_completed(futures):
                        cleaned, page_index = future.result()
                        unsorted_outputs.append((Image.fromarray(cleaned), page_index))
                    cleaned_images = [img for img, _ in sorted(unsorted_outputs, key = lambda x: x[1])]
                else:
                    cleaned_images = []
                    for i, page_image in enumerate(page_images):
                        cleaned_page, _ = preprocess(page_image, document_name, chunk_start + i)
                        cleaned_images.append(Image.fromarray(cleaned_page))

                # Flush the finished chunk to disk and FREE it before the next one
                chunk_path = Path(tmp_dir) / f"chunk_{chunk_start:05d}.pdf"
                cleaned_images[0].save(
                    chunk_path,
                    save_all = True,
                    append_images = cleaned_images[1:]
                )
                chunk_paths.append(chunk_path)
                del page_images, cleaned_images
                logging.info(f"  cleaned pages {chunk_start}-{chunk_end} / {n_pages}")
        finally:
            if executor is not None:
                executor.shutdown()

        if not chunk_paths:
            logging.error(f"No pages processed for {document_name}. Please examine file manually.")
            return

        # Stitch the chunk PDFs (compressed, on disk) into the single output PDF.
        if len(chunk_paths) == 1:
            shutil.move(str(chunk_paths[0]), save_path)
        else:
            subprocess.run(
                ["pdfunite", *[str(p) for p in chunk_paths], str(save_path)],
                check = True
            )

    logging.info(f"Saved cleaned PDF to {save_path}")
    logging.info(f"Time taken: {time() - start_time:.2f} seconds\n")


def main():

    # Set logging config and grab start time for logging.
    start_time = time()
    logging.basicConfig(level=logging.INFO, format="%(message)s", force = True)

    logging.info(f"\n{'=' * 70}")
    logging.info(f"Step 1: PDF Cleaner")
    logging.info(f"{'=' * 70}\n")

    # Construct the path to the data folder (project root, per config.py docs:
    # Solanus-Project-Pipeline/source_data — one level above pipeline_v2)
    pdf_folder = Path(__file__).resolve().parent.parent.parent / SOURCE_DATA_FOLDER

    # Get the paths to the pdfs
    pdf_paths = get_pdf_paths(pdf_folder)
    logging.info(f"Found {len(pdf_paths)} PDFs to process.\n")
    logging.info(f"PDFs: {[pdf_path.name for pdf_path in pdf_paths]}\n")

    max_workers = min(os.cpu_count() - 2, 8) if min(os.cpu_count() - 2, 8) > 2 else 1
    if CONCURRENT_FLAG and max_workers > 1:
        print(f"{max_workers} cores available for pdf processing")
    else:
        print(f"CONCURRENCY flag disabled or too few cores. Processing PDFs serially.")

    # Get the path to directory where we save outputs and create output folder if necessary
    output_folder = Path(__file__).parent / f"dpi_{TARGET_DPI}_cleaned_pdfs"
    os.makedirs(output_folder, exist_ok = True)

    # Clean each pdf
    for pdf_path in pdf_paths:
        clean_pdf(pdf_path, output_folder, max_workers)
    
    logging.info(f"\nAll done! Total time taken: {time() - start_time:.2f} seconds\n")
    logging.info(f"{'=' * 70}")
    logging.info(f"{'=' * 70}\n")

if __name__ == "__main__":
    main()