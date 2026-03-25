from pathlib import Path


# =====================================================
# STEP 2 CONFIGURATION — Polygon Crop Workflow
# =====================================================


# ── Polygon Crop Workflow Settings ─────────────────────────────────────────────

# Input directory for the polygon crop workflow.
POLYGON_INPUT_DIR = Path("step_1") / "cleaned_pdfs"

# Directory where the per-page JSON files are stored.
# For each document, this workflow creates:
#   - page_sizes/page_001.json
#   - polygons/page_001.json
POLYGON_CONFIG_DIR = Path("step_2") / "polygon_page_data"

# Output directory for the cropped PDFs generated from the polygon JSON files.
POLYGON_CROP_OUTPUT_DIR = Path("step_2") / "polygon_cropped_pdfs"

# Optional: restrict the polygon workflow to a subset of PDFs.
POLYGON_FILES_TO_RUN = ["Volume_1.pdf"]

# Optional: exclude specific PDFs from the polygon workflow.
POLYGON_FILES_TO_EXCLUDE = []

# Optional: if you only want to overwrite specific pages in the crop pipeline,
# list them here. Page numbers are 1-indexed. Leave blank to process every page.
POLYGON_TARGET_PAGES = []

# Render pages in chunks to reduce memory use on large PDFs.
# Lower values use less memory but may run a bit slower.
POLYGON_PAGE_CHUNK_SIZE = 50

# Default inset for the initial polygon that is generated for each page.
# Units are PIXELS at the render DPI specified below in RENDER_DPI.
DEFAULT_CROP_POLYGON_INSET = {
	"left": 80,
	"top": 80,
	"right": 80,
	"bottom": 80,
}

# If True, the polygon initializer will overwrite existing polygon JSON files.
# If False, it will preserve any polygons you have already edited.
POLYGON_OVERWRITE_EXISTING = False

# Editor settings for the interactive polygon editor.
POLYGON_EDITOR_DOCUMENT = "Volume_2.pdf"
POLYGON_EDITOR_START_PAGE = 1
POLYGON_EDITOR_MAX_PREVIEW_DIMENSION = 1200


# ── Image Processing Settings ───────────────────────────────────────────────────

# DPI for rendering PDF pages as images.
RENDER_DPI = 150
