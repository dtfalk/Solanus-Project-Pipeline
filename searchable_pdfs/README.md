# Searchable (text-layer) PDFs

These PDFs show the original scan but carry an **invisible OCR text layer** on top, so the
text is selectable / copy-pastable / Ctrl-F-able and indexable by search engines.

## Two ways to build them
1. **In-pipeline (preferred), per page.** `stages/index_sources.py` stamps each word from
   Azure's `extract_azure.raw.json` (per-word `polygon`) onto the page's `*.masked.pdf` at
   render mode 3 (invisible), scaling image pixels -> PDF points by `72 / render_dpi`.
   Requires `reportlab` (authoring) + `pypdf` (stamping):
   ```bash
   pip install reportlab pypdf
   python stages/index_sources.py
   ```

2. **Fallback, per volume, with ocrmypdf.** If those deps are absent, run the generated
   `MAKE_SEARCHABLE_PDFS.sh` (it OCRs the whole-volume source PDFs in `source_data/`).
   ```bash
   ./MAKE_SEARCHABLE_PDFS.sh
   ```

TODO: install `reportlab`+`pypdf` (or `ocrmypdf`) to actually produce the PDFs — until then
only this recipe is written (no PDFs), by design (non-destructive, no heavy install here).
