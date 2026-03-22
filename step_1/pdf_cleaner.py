import os
import cv2
import numpy as np
import logging
from time import time
import platform
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import SOURCE_DATA_FOLDER, FILES_TO_RUN, FILES_TO_EXCLUDE, DESKEW_FLAG, CONCURRENT_FLAG


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
    
    return cleaned


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


def clean_pdf(pdf_path, output_folder):
    """
    Converts a PDF to cleaned images and saves back as a single PDF.
    """

    # Grab start time for logging
    logging.basicConfig(level=logging.INFO, force = True, format="%(message)s")
    start_time = time()

    # Extract document name and log it
    document_name = pdf_path.stem
    logging.info(f"\n{'-' * 70}")
    logging.info(f"Cleaning {document_name}")
    logging.info(f"{'-' * 70}\n")

    # Convert PDFs to images
    page_images = convert_from_path(
        pdf_path,
        dpi = 300,
        poppler_path = get_poppler_path()
    )

    cleaned_images = []

    # Process all pages
    for page_index, page_image in enumerate(page_images):
        cleaned_page = preprocess(page_image, document_name, page_index)
        cleaned_images.append(Image.fromarray(cleaned_page))

    # Save cleaned pages back into a single PDF
    save_path = output_folder / f"{document_name}.pdf"
    if not cleaned_images:
        logging.error(f"No pages processed for {document_name}. Please examine file manually.")
        return
    
    cleaned_images[0].save(
        save_path,
        save_all = True,
        append_images = cleaned_images[1:]
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

    
    # Construct the path to the data folder
    pdf_folder = Path(__file__).resolve().parent.parent / SOURCE_DATA_FOLDER

    # Get the paths to the pdfs
    pdf_paths = get_pdf_paths(pdf_folder)
    logging.info(f"Found {len(pdf_paths)} PDFs to process.\n")
    logging.info(f"PDFs: {[pdf_path.name for pdf_path in pdf_paths]}\n")

    # Get the path to directory where we save outputs and create output folder if necessary
    output_folder = Path(__file__).parent / "cleaned_pdfs"
    os.makedirs(output_folder, exist_ok = True)

    # Use a process pool to clean multiple PDFs in parallel. We use a process pool because the cleaning is CPU intensive.
    # max_workers = min(4, os.cpu_count())
    if CONCURRENT_FLAG:
        max_workers = os.cpu_count() - 1 if os.cpu_count() > 1 else 1
        logging.info(f"Using {max_workers} workers for PDF cleaning.")
    
        # Iterate over all of the pdf files and clean them
        with ProcessPoolExecutor(max_workers = max_workers) as executor:
            futures = [executor.submit(clean_pdf, pdf_path, output_folder) for pdf_path in pdf_paths]
    
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing file: {e}")
    else:
        logging.info("Concurrent processing disabled. Processing files sequentially.")
        for pdf_path in pdf_paths:
            clean_pdf(pdf_path, output_folder)
    

    logging.info(f"\nAll done! Total time taken: {time() - start_time:.2f} seconds\n")
    logging.info(f"{'=' * 70}")
    logging.info(f"{'=' * 70}\n")

if __name__ == "__main__":
    main()