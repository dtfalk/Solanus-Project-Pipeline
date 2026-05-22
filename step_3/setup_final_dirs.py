"""
Step 3: Setup Final Directories

Mirrors the directory structure of step_3/normalized/ into step_3/final/
and copies only the per-page PDFs that correspond to pages present in
normalized/.

For each document folder in normalized/ (e.g. Volume_4, Appendix_1, ...):
  1. Creates step_3/final/{doc}/page_XXX/ for every page folder in normalized
  2. Copies the matching page_XXX.pdf from
     step_2/polygon_cropped_pdfs/{doc}/pages/page_XXX.pdf
     into step_3/final/{doc}/page_XXX/page_XXX.pdf

Usage:
    python step_3/setup_final_dirs.py
"""

import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent

NORMALIZED_DIR = ROOT_DIR / "step_3" / "normalized"
FINAL_DIR      = ROOT_DIR / "step_3" / "final"
PDF_SOURCE_DIR = ROOT_DIR / "step_2" / "polygon_cropped_pdfs"


def main():
    if not NORMALIZED_DIR.exists():
        print(f"ERROR: Normalized directory not found: {NORMALIZED_DIR}")
        return

    # Iterate every document folder inside normalized/
    doc_dirs = sorted(
        d for d in NORMALIZED_DIR.iterdir() if d.is_dir()
    )

    if not doc_dirs:
        print("No document folders found in normalized/. Nothing to do.")
        return

    for doc_dir in doc_dirs:
        doc_name  = doc_dir.name
        final_doc = FINAL_DIR / doc_name
        print(f"\n{doc_name}:")

        # Create page subdirectories and copy matching per-page PDFs.
        page_dirs = sorted(
            d for d in doc_dir.iterdir() if d.is_dir() and d.name.startswith("page_")
        )
        for page_dir in page_dirs:
            page_name = page_dir.name                       # e.g. "page_006"
            target    = final_doc / page_name
            target.mkdir(parents = True, exist_ok = True)

            # Copy the single-page PDF into the page subfolder.
            src_pdf = PDF_SOURCE_DIR / doc_name / "pages" / f"{page_name}.pdf"
            dst_pdf = target / f"{page_name}.pdf"
            if dst_pdf.exists():
                print(f"  {page_name}.pdf already exists, skipping")
            elif src_pdf.exists():
                shutil.copy2(src_pdf, dst_pdf)
                print(f"  Copied {page_name}.pdf")
            else:
                print(f"  WARNING: Source PDF not found: {src_pdf}")

    print("\nDone.")


if __name__ == "__main__":
    main()
