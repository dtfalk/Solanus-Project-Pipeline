"""
Image Helpers — PDF rendering and image cropping utilities.

These functions are used by every script that needs to show the LLM
a cropped image of a handwritten line or flagged token from a PDF page.

The workflow is:
  1. render_pdf_page_as_image()  — opens a single-page PDF → PIL Image
  2. crop_image_to_bounding_box() — crops a region using a bounding polygon
  3. convert_image_to_base64()    — encodes the crop as base64 PNG for the API
"""

import base64
from io import BytesIO

import fitz          # PyMuPDF — used to render PDF pages as images
from PIL import Image


def bounding_polygon_to_rectangle(bounding_polygon):
    """
    Convert a bounding polygon (list of alternating x, y coordinates)
    into a simple rectangle: (min_x, min_y, max_x, max_y).

    Azure OCR returns polygons as flat lists like [x0, y0, x1, y1, x2, y2, ...].
    We just need the bounding rectangle (top-left and bottom-right corners).
    """
    all_x_coordinates = bounding_polygon[0::2]   # every even index = x
    all_y_coordinates = bounding_polygon[1::2]   # every odd index  = y
    return (
        min(all_x_coordinates),  # left
        min(all_y_coordinates),  # top
        max(all_x_coordinates),  # right
        max(all_y_coordinates),  # bottom
    )


def render_pdf_page_as_image(pdf_path, dots_per_inch=150):
    """
    Open a single-page PDF and render it as a PIL Image at the given DPI.

    Each document page is stored as its own PDF file (e.g. volume-1-page-42.pdf),
    so we always load page index 0.
    """
    pdf_document = fitz.open(pdf_path)
    first_page = pdf_document.load_page(0)

    # fitz uses a scale matrix relative to 72 DPI (the PDF standard)
    scale_factor = dots_per_inch / 72
    pixel_map = first_page.get_pixmap(
        matrix=fitz.Matrix(scale_factor, scale_factor),
        alpha=False,
    )

    page_image = Image.frombytes("RGB", [pixel_map.width, pixel_map.height], pixel_map.samples)
    pdf_document.close()
    return page_image


def crop_image_to_bounding_box(full_page_image, bounding_polygon, dots_per_inch=150, padding_inches=0.1):
    """
    Crop a region from the full page image using a bounding polygon.

    The polygon coordinates are in INCHES (as returned by Azure OCR).
    We convert them to pixels using the DPI, then add a small padding
    so the crop isn't uncomfortably tight against the text.
    """
    left_inches, top_inches, right_inches, bottom_inches = bounding_polygon_to_rectangle(bounding_polygon)

    # Convert inches → pixels, adding padding on all sides
    left_pixels  = int(max(0, left_inches  - padding_inches) * dots_per_inch)
    top_pixels   = int(max(0, top_inches   - padding_inches) * dots_per_inch)
    right_pixels = int((right_inches  + padding_inches) * dots_per_inch)
    bottom_pixels = int((bottom_inches + padding_inches) * dots_per_inch)

    return full_page_image.crop((left_pixels, top_pixels, right_pixels, bottom_pixels))


def convert_image_to_base64(pil_image):
    """
    Encode a PIL Image as a base64 PNG string.

    This is the format Azure OpenAI expects for inline images
    in chat completion requests (data:image/png;base64,...).
    """
    buffer = BytesIO()
    pil_image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode()
