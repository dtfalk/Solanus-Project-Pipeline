"""Configuration for standalone pipeline_clean workflow.

This is the single control surface for all stage scripts.
Every executable in pipeline_clean reads from this file and accepts no CLI args.
"""

from __future__ import annotations

from pathlib import Path


# === Root Paths ==================================================================
THIS_FILE = Path(__file__).resolve()
ROOT_DIR  = THIS_FILE.parent

WORKSPACE_DIR      = ROOT_DIR / "workspace"
SOURCE_PDFS_DIR    = WORKSPACE_DIR / "source_pdfs"
CLEANED_PDFS_DIR   = WORKSPACE_DIR / "cleaned_pdfs"
POLYGONS_DIR       = WORKSPACE_DIR / "polygons"
CROPPED_PAGES_DIR  = WORKSPACE_DIR / "cropped_pages"
MANIFESTS_DIR      = WORKSPACE_DIR / "manifests"
SAMPLE_PAGES_DIR   = WORKSPACE_DIR / "sample_pages"
ANNOTATION_SEED_DIR = WORKSPACE_DIR / "annotation_seed"
FINAL_DIR          = WORKSPACE_DIR / "final"
CLEANED_FINAL_DIR  = WORKSPACE_DIR / "CLEANED_FINAL"


# === Files and Names ==============================================================
MANIFEST_FILE_NAME = "few_shot_manifest.json"
MANIFEST_FILE_PATH = MANIFESTS_DIR / MANIFEST_FILE_NAME


# === Preprocess ==================================================================
PREPROCESS_DPI         = 300
PREPROCESS_DESKEW      = True
PREPROCESS_CONCURRENT  = False
PREPROCESS_MAX_WORKERS = 4


# === Polygon Initialization =======================================================
POLYGON_RENDER_DPI = 150
POLYGON_INSET_LEFT = 172
POLYGON_INSET_TOP  = 234
POLYGON_INSET_RIGHT = 396
POLYGON_INSET_BOTTOM = 253
POLYGON_OVERWRITE_EXISTING = False


# === Cropper ======================================================================
CROP_PAGE_CHUNK_SIZE = 50


# === Few-Shot Manifest ============================================================
FEW_SHOT_SOURCE_MODE = "pages_txt"
FEW_SHOT_PAGES_TXT_PATH = ROOT_DIR.parent / "pages.txt"
FEW_SHOT_INCLUDE_CANDIDATES = False


# === Annotation Packaging ==========================================================
ANNOTATION_SEED_DEFAULT_DOCS = 1


# === Editor =======================================================================
EDITOR_DOCUMENT   = "Volume_4"
EDITOR_START_PAGE = None
EDITOR_HANDLE_RADIUS = 7
EDITOR_DRAW_DOT_RADIUS = 4
EDITOR_RENDER_DPI = 150


# === Export =======================================================================
EXPORT_REMOVE_EXCLUDED_FIELDS = True


# === Migration (manual, explicit script only) ====================================
MIGRATION_ENABLE = False
MIGRATION_SOURCE_POLYGONS_DIR = ROOT_DIR.parent / "step_2" / "polygon_page_data"
MIGRATION_SOURCE_ANNOTATION_DIRS = [
    ROOT_DIR.parent / "step_3" / "final",
    ROOT_DIR.parent / "step_3" / "normalized",
    ROOT_DIR.parent / "step_x" / "label_page_data",
]


# === Helpers ======================================================================
def ensure_workspace_dirs() -> None:
    """Create all pipeline_clean workspace directories.

    Args:
        None.

    Returns:
        None. Missing directories are created.
    """
    directories = [
        WORKSPACE_DIR,
        SOURCE_PDFS_DIR,
        CLEANED_PDFS_DIR,
        POLYGONS_DIR,
        CROPPED_PAGES_DIR,
        MANIFESTS_DIR,
        SAMPLE_PAGES_DIR,
        ANNOTATION_SEED_DIR,
        FINAL_DIR,
        CLEANED_FINAL_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents = True, exist_ok = True)
