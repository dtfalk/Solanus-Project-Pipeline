# PIPELINE_FLOW.md — how step_4 works, file by file

_The concrete code/data flow of this folder: from `polygon_cropped_pdfs/` input to the final
output. Companion to `MASTER_GUIDE.md` (the generalized why) and `PLAYBOOK.md` (the reusable
template) — this doc is the map of THIS directory. Written 2026-06-05._

## The one-line answer

**The final output is `reviewed/<Volume>/page_NNN/page_NNN.json`** — human-verified ("gold")
structured labels per page: document polygons by category, plus cross-polygon connections.
Everything else in this folder is machinery to produce that ever more cheaply.

## The flow

```
polygon_cropped_pdfs/<Vol>/pages/page_NNN.pdf     ← INPUT (1-page scans from steps 1–3)
        │
        ▼  STAGE 1 · auto_labeler.py  (~$0.05/page, defaults: gemini-3.5-flash + page-type routing)
        │   1. discover_target_pages(): globs the pages dir; skips pages whose output
        │      JSON already exists (unless --overwrite); --start/--end filter by number
        │   2. classify_page_type(): cheap flash-lite call types the page
        │      (notebook/letter/mass_card/other) → cached in page_type_cache/
        │   3. select_few_shot(): picks 12 same-TYPE demo pages from labeled_examples/
        │      (the pool, minus EXCLUDED_EXAMPLES; demo images pre-uploaded to the
        │      Gemini Files API → file_uris.json, 48 h TTL → refresh: upload_examples.py)
        │   4. PASS 1 call_gemini(): demos + target image (1024 px) → documents with
        │      category polygons in [0,1000] normalized space → scaled back to full-res
        │      pixel space (page_width/height = this PDF rendered at 150 DPI)
        │   5. Post-process: _normalize_quad (clean 4-vertex quads) → snap_polygon_to_ink
        │      (Otsu ink tightening) → resolve_overlaps → coverage_backstop (detects
        │      uncovered ink, re-prompts the model on missed regions, merges added boxes)
        │   6. PASS 2 call_gemini_connections(): infers connection edges between polygons
        │      (e.g. document continuations) → apply_edges()
        ▼
auto_labeled/<Vol>/page_NNN/{page_NNN.json + page_NNN.pdf copy}      + cost row in usage.csv
        │
        ▼  STAGE 2 · QA & triage (gold-free)
        │   qa_report.py <Vol>   → qa_output/<Vol>/qa_report.json + overlays/*.png
        │                          (uncovered-ink / clipped / cross-role-overlap flags)
        │   triage.py <Vol>      → re-labels the volume with a cheap shadow model
        │                          (shadow_labels/), ranks pages by model disagreement
        │                          → qa_output/<Vol>/triage.txt (review this top-down)
        ▼
        ▼  STAGE 3 · Human review — THE GOLD GATE
        │   EDITOR_DOCUMENT=<Vol> ./venv/bin/python normalized_editor.py
        │   Loads auto_labeled JSON+PDF; qa_report flags become a one-click Review Queue
        │   (Extend / Add box / Dismiss — dismissals persist in qa_output/<Vol>/dismissed.json);
        │   saves your edits to reviewed/ — NEVER machine-written.
        ▼
reviewed/<Vol>/page_NNN/page_NNN.json             ← FINAL OUTPUT (gold, git-tracked)
        │
        ▼  STAGE 4 · Flywheel (gold feeds back)
            ├─ promote good pages → labeled_examples/ (+ upload_examples.py re-uploads)
            │  → better few-shot for the next volume
            ├─ review_diff.py / panoptic_eval.py → measure model vs gold
            │  (ink-IoU + Panoptic Quality — the honest metric; see DEEP_REVIEW.md)
            └─ export_tuning_data.py → tuning_data/*.jsonl → finetune.py (Vertex SFT:
               prepare→tune→status→eval→park/redeploy/teardown) → rerun_compare/tuned_eval.py
               A/B — GATE-1: a tuned model must beat few-shot-3.5-flash before any swap
```

## Per-page JSON schema (same shape in auto_labeled/ and reviewed/)

```json
{
  "page_number": 12,
  "source_file": ".../polygon_cropped_pdfs/<Vol>/<Vol>.pdf",
  "page_width": 5496, "page_height": 6934,      // render-pixel space at 150 DPI
  "render_dpi": 150,
  "num_documents": 2,
  "documents": {
    "document_1": {
      "src_content":   [ { "id": "<uuid>", "vertices": [{"x":..,"y":..} ×4],
                           "connections": [ ... ] } ],
      "src_greeting":  [ ... ], "src_signature": [ ... ], "archv_date": [ ... ], ...
    }
  }
}
```
Coordinates are in the page's own render-pixel space — **always interpret them relative to the
file's stored `page_width`/`page_height`**, never assume they match a fresh render.

## File inventory

### Active scripts
| File | Role |
|---|---|
| `auto_labeler.py` | The labeler (Stage 1). Defaults: `--model gemini-3.5-flash`, `--page-type-fewshot` ON, `--concurrency 4`. |
| `qa_report.py` | Ink-based QA flags per volume → `qa_output/`. |
| `triage.py` | Gold-free review ordering via shadow-model disagreement → `triage.txt`. |
| `normalized_editor.py` | Tkinter review editor (`EDITOR_DOCUMENT=<Vol>`); writes gold. |
| `normalized_viewer.py` | Read-only viewer (`VIEWER_DIR`/`VIEWER_DOCUMENT`). |
| `review_diff.py` | auto_labeled vs reviewed correction-pattern report. |
| `panoptic_eval.py` | Ink-IoU Panoptic Quality (RQ/SQ/PQ) of any pred dir vs gold. |
| `export_tuning_data.py` | reviewed/ → `tuning_data/{train,val}.jsonl` (base64 images + [0,1000] targets). |
| `finetune.py` | Vertex SFT lifecycle. Endpoints bill hourly — always `teardown`; verify 0 deployed. |
| `rerun_compare/tuned_eval.py` | Tuned-model eval + A/B vs production few-shot. |
| `upload_examples.py` | Re-upload few-shot images to Gemini Files API → `file_uris.json` (48 h TTL). |
| `compare_overlay.py` / `labeled_overlay.py` | Overlay renderers for visual diffing. |
| `clean_gold_foreign_keys.py` | Opt-in gold scrubber (dry-run default). |
| `run_all_tests.py` | 33-check test suite (`--no-api` for offline subset). Run after any change. |
| `apply_relabels.py` / `pricing.py` | Batch relabel helper / model pricing table. |

### Data directories
| Dir | Contents | Regenerable? |
|---|---|---|
| `polygon_cropped_pdfs/` | INPUT: per-page single-page PDFs per volume. **Not in git** (3.9 GB) — sync between machines manually. | From step 1–3 |
| `auto_labeled/` | Stage-1 model output (JSON + PDF copy per page). | Yes (API $) |
| `reviewed/` | **GOLD. The product. Never machine-written.** | NO — human work |
| `labeled_examples/` | Few-shot pool (gold-quality example pages, typed, per volume). | Curated from gold |
| `page_type_cache/` | Cached page-type classifications. | Yes (pennies) |
| `qa_output/` | QA reports, overlays, triage worklists, editor dismissals. | Yes (free) |
| `shadow_labels/` | Cheap-model labels used by triage. | Yes (API $) |
| `tuning_data/` | SFT JSONLs + `tuning_job.json` (Vertex job state). | Yes from gold |
| `rerun_compare/` | Frozen A/B experiment artifacts (evidence behind DEEP_REVIEW.md) + `tuned_eval.py`. | Mostly frozen history |
| `checkpoints/` | Numbered working-state snapshots (01–19). | Frozen history |
| `label_review/` | Label-agreement review artifacts (manifest + overlays). | Frozen history |

### Setup (new machine)
```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt   # needs apt: poppler-utils
echo 'GEMINI_API_KEY=<key>' > .env                                   # labeling (AI Studio)
gcloud auth application-default login                                # only for fine-tuning (Vertex)
./venv/bin/python run_all_tests.py --no-api                          # verify healthy
```
`venv/`, `.env`, and `polygon_cropped_pdfs/` do not travel via git. **Caution:** if a machine's
`polygon_cropped_pdfs/` differs (re-crop, renumbering), every page-numbered artifact silently
misaligns — `run_all_tests.py` check B7 (gold-vs-gold panoptic) is the canary; run it after any sync.

## The standard per-volume loop

```bash
./venv/bin/python auto_labeler.py --volume <Vol>        # label (skips already-done pages)
./venv/bin/python qa_report.py <Vol>                    # QA flags
./venv/bin/python triage.py <Vol>                       # review order (optional, API $)
EDITOR_DOCUMENT=<Vol> ./venv/bin/python normalized_editor.py    # human review → gold
./venv/bin/python review_diff.py <Vol>                  # how much did the human change?
./venv/bin/python panoptic_eval.py <Vol>                # model quality vs the new gold
```
