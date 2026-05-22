"""PDF utility helpers for standalone pipeline_clean stages."""

from __future__ import annotations

from pathlib import Path

from pdf2image import convert_from_path
from pypdf import PdfReader, PdfWriter


def pdf_to_images(
    pdf_path,
    dpi,
    first_page = None,
    last_page = None,
):
    """Render PDF pages to PIL images.

    Args:
        pdf_path: Input PDF path.
        dpi: Render DPI.
        first_page: Optional first page number (1-indexed).
        last_page: Optional last page number (1-indexed).

    Returns:
        List of (page_number, image) tuples.
    """
    images = convert_from_path(
        pdf_path,
        dpi = dpi,
        first_page = first_page,
        last_page = last_page,
    )

    start_page = 1 if first_page is None else first_page
    return [(page_number, image) for page_number, image in enumerate(images, start = start_page)]


def save_image_as_pdf(
    image,
    output_path,
    resolution,
):
    """Save PIL image as single-page PDF.

    Args:
        image: PIL image object.
        output_path: Destination PDF path.
        resolution: Output resolution metadata.

    Returns:
        None. File is written to disk.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents = True, exist_ok = True)

    if image.mode != "RGB":
        image = image.convert("RGB")

    image.save(output_path, "PDF", resolution = resolution)


def merge_pdfs(
    pdf_paths,
    output_path,
):
    """Merge many PDF files into one output file.

    Args:
        pdf_paths: Ordered list of PDF paths.
        output_path: Destination merged PDF path.

    Returns:
        None. File is written to disk.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents = True, exist_ok = True)

    writer = PdfWriter()
    for pdf_path in pdf_paths:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            writer.add_page(page)

    with open(output_path, mode = "wb") as file_handle:
        writer.write(file_handle)
