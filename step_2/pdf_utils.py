"""
Step 2 PDF utilities shared by the polygon workflow.

These helpers cover:
  1. Rendering PDF pages to PIL images
  2. Saving page images back to single-page PDFs
  3. Merging those single-page PDFs into a final document
"""

from pdf2image import convert_from_path
from pypdf import PdfReader, PdfWriter


def pdf_to_images(pdf_path, dpi):
    """Render a PDF into a list of 1-indexed page images."""
    pil_images = convert_from_path(pdf_path, dpi = dpi)
    return [(page_number, image) for page_number, image in enumerate(pil_images, start = 1)]


def save_image_as_pdf(image, output_path, resolution):
    """Save a PIL image as a single-page PDF."""
    output_path.parent.mkdir(parents = True, exist_ok = True)

    if image.mode != "RGB":
        image = image.convert("RGB")

    image.save(output_path, "PDF", resolution = resolution)


def merge_pdfs(pdf_paths, output_path):
    """Merge a sequence of single-page PDFs into one PDF."""
    output_path.parent.mkdir(parents = True, exist_ok = True)

    writer = PdfWriter()
    for pdf_path in pdf_paths:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            writer.add_page(page)

    with open(output_path, "wb") as output_file:
        writer.write(output_file)