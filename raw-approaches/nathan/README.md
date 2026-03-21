# Research Apex Raw Data: What We Built

This workspace contains two connected workstreams:

1. OCR quality analysis and review tooling (`text-extraction-data/`)
2. Standardized error export for downstream cleaning (`solanus-data-cleaning-repo/raw-approaches/nathan/`)

## Workstream 1: OCR Extraction QA + Review

Location: `text-extraction-data/`

What we did:
- Organized source scans in `raw-documents/` (volumes + appendices).
- Stored OCR outputs in `output/<volume>/page-*/read.json` and `reconstructed.html`.
- Built scripts to find and review low-confidence OCR tokens.

Key scripts:
- `text-extraction-data/scripts/generate_low_confidence_report.py`
- `text-extraction-data/scripts/serve_report.py`
- `text-extraction-data/scripts/page_viewer.html`
- `text-extraction-data/scripts/generate_error_clusters_report.py`

Generated artifacts:
- `text-extraction-data/scripts/low_confidence_report.html`
- `text-extraction-data/scripts/error_clusters_report.html`
- `text-extraction-data/scripts/error_clusters_summary.html`
- `text-extraction-data/scripts/error_clusters_summary.csv`
- `text-extraction-data/reviews/reviewed.json` (manual review tracking)

Current snapshot metrics:
- 1,410 OCR pages scanned (`read.json` files)
- 7,300 words below confidence 0.85
- 16,864 words below confidence 0.95
- 2,426 clustered OCR error groups in `error_clusters_summary.csv`

## Workstream 2: Standardized Error Dataset for Cleaning

Location: `solanus-data-cleaning-repo/raw-approaches/nathan/`

What we did:
- Created `generate_low_confidence_errors.py` to convert low-confidence OCR words into a standard error schema.
- Produced `errors.json` with one structured entry per suspected OCR error.
- Added document/page/line/word context and bounding boxes for each error.

Key files:
- `solanus-data-cleaning-repo/raw-approaches/nathan/generate_low_confidence_errors.py`
- `solanus-data-cleaning-repo/raw-approaches/nathan/errors.json`

Current snapshot metrics (`errors.json`):
- 7,300 total error entries
- volume-1: 1,103
- volume-2: 1,990
- volume-3: 1,696
- volume-4: 1,552
- appendix-1: 157
- appendix-2: 482
- appendix-3: 320

## How the Two Workstreams Connect

- Workstream 1 identifies likely OCR mistakes and supports human review.
- Workstream 2 packages those same low-confidence findings into a normalized JSON format for data-cleaning workflows.

In short: we moved from **OCR outputs -> review/analysis reports -> structured error dataset**.

## Walkthrough Talk Track

1. We ingested volumes/appendices and generated page-level OCR outputs.
2. We built low-confidence and clustering reports to find systematic OCR issues quickly.
3. We tracked manual review progress in `reviews/reviewed.json`.
4. We exported 7,300 standardized error records for downstream cleaning in the Solanus repo.
