#!/usr/bin/env bash
# Generate text-layer (searchable) PDFs for the Solanus volumes.
#
# TODO: reportlab (and/or pypdf) is not installed in the step_7 venv, so the in-pipeline
#       per-page stamping path was skipped. This script reproduces the SAME artifact with
#       ocrmypdf, the standard tool for adding an invisible OCR text layer to a PDF.
#
#   missing python deps : reportlab, pypdf
#   install either path :
#       pip install reportlab pypdf      # then re-run: python stages/index_sources.py
#       # OR use ocrmypdf (system tool): 
#       sudo apt-get install ocrmypdf tesseract-ocr   # Debian/Ubuntu
#       pip install ocrmypdf                            # (still needs the tesseract binary)

set -euo pipefail
SRC_DIR="/Workspace/Projects/Solanus-Project-Pipeline/source_data"
OUT_DIR="/Workspace/Projects/Solanus-Project-Pipeline/searchable_pdfs"
mkdir -p "$OUT_DIR"

# --redo-ocr: keep the page image, (re)build the text layer. --optimize 1: light squeeze.
ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/Appendix_2.pdf" "$OUT_DIR/Appendix_2.searchable.pdf"   # whole-volume searchable PDF
ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/Volume_1.pdf" "$OUT_DIR/Volume_1.searchable.pdf"   # whole-volume searchable PDF
ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/Volume_2.pdf" "$OUT_DIR/Volume_2.searchable.pdf"   # whole-volume searchable PDF
ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/Volume_3.pdf" "$OUT_DIR/Volume_3.searchable.pdf"   # whole-volume searchable PDF
ocrmypdf --redo-ocr --optimize 1 "$SRC_DIR/Volume_4.pdf" "$OUT_DIR/Volume_4.searchable.pdf"   # whole-volume searchable PDF

echo "Done. Searchable PDFs in $OUT_DIR"
