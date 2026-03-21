# OCR Error Flagging Pipeline – Developer Documentation

This workspace implements a large-scale OCR error detection and export pipeline for the Solanus corpus.

It transforms page-level OCR outputs into a structured, reviewable, image-backed error dataset suitable for cleaning and AI-assisted correction.

---

## Location

solanus-data-cleaning-repo/raw-approaches/Stella/

---

## What We Built

We created a full-document OCR flagging system that:

- Scans every OCR page in `text-extraction-data/output/`
- Maps each page to its original PDF scan in `pdf-pages/`
- Applies multiple heuristic error detectors
- Generates cropped visual evidence for each issue
- Exports all findings into a normalized JSON schema

---

## Data Inputs

Expected upstream structure:

text-extraction-data/
├── output/<doc>/page-<n>/read.json
├── pdf-pages/<doc>/<doc>-page-<n>.pdf


The pipeline uses:
- `read.json` for OCR words, geometry, confidence, spans
- per-page PDFs for rendering and cropping

---

## Error Detection Rules Implemented

### Word-Level Flags
- Low OCR confidence (< 0.90)
- Hyphenated line-wrap tokens (word ends with `-`)
- Small tokens (height < 65% of median word height)
- Known problematic tokens (abbreviations, numbers, all-caps)

### Text-Level Pattern Flags
- Ellipses (`...`)
- Double periods (`..`)
- Straight quotes (`"`)
- Equals sign (`=`)

Each flagged instance becomes a structured error entry.

---

## Output Artifacts

### 1. Structured Error Dataset

Written to:
reviews/errors.json


Each entry contains:
- `error_id`
- `error_type`
- `source_document`
- `page_number`
- `line_number`
- `word_number`
- `character_number` (span offset)
- full line context (`context.line_text`)
- bounding boxes (`context` + `error`)
- relative image paths (`images.context`, `images.error`)

Schema (simplified):

```json
{
  "error_id": 0,
  "error_type": "low_confidence",
  "source_document": "appendix-1.pdf",
  "page_number": 10,
  "line_number": 8,
  "word_number": 2,
  "character_number": 13469,
  "context": { "line_text": "Portland, O." },
  "error": "O.",
  "bounding_boxes": {
    "context": [ ... ],
    "error": [ ... ]
  },
  "images": {
    "context": "reviews/imgs_SC/0_images/context.png",
    "error": "reviews/imgs_SC/0_images/error.png"
  }
}

2. Cropped Image Evidence

Stored under:

reviews/imgs_SC/<error_id>_images/
  context.png
  error.png


context.png shows the entire line
error.png zooms in on the exact token

All paths in JSON are relative for portability.



Current Snapshot Metrics

Latest run produced:
- 38,564 flagged error entries
- Full corpus coverage across volumes and appendices
- Two cropped images per error



How to Run

From project root:
python run_flagging.py


The script:
- auto-discovers documents in output/
- processes all pages
- writes reviews/errors.json
- writes image crops to reviews/imgs_SC/


Performance Characteristics
- Runtime is dominated by PDF rendering and image writes.

If output becomes too large:
- Reduce DPI
- Cap errors per page
- Disable text-level patterns
- Batch or compress image outputs


Design Goals
- Deterministic and reproducible
- Fully traceable to source scan
- Self-contained error entries (reviewable independently)
- Compatible with LLM correction workflows
- Compatible with downstream cleaning system

Relationship to Prior Work:
Earlier approaches focused mainly on low-confidence tokens.
This pipeline expands detection to include:
- token-size anomalies
- domain-specific hotwords
- punctuation artifacts
- hyphenated line-wrap indicators