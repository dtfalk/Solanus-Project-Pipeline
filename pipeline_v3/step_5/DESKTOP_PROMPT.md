# Desktop pickup — finish step_5 page masking

Paste the block below to your desktop Claude Code instance (run from the repo root,
`~/projects/solanus-project-pipeline`). It resumes a job that was ~60% done on the laptop.

---

You're picking up step_5 of the Solanus pipeline mid-task. Working dir:
`pipeline_v3/step_5`. Read `pipeline_v3/step_5/HANDOFF_2026-06-18.md` first — it has the
full context and the plan we agreed on. Then continue the page-masking run that was
interrupted when I left for the airport.

## Immediate task: finish masking
A batch was masking every labeled source page down to its polygons (whiting out
unlabeled text), writing `page_NNN.masked.pdf` + `page_NNN.masked.png` into each page
folder. It was killed at **853 / 1408 pages**; ~555 remain.

`mask_pages.py` is **resumable and self-healing**: it skips pages whose masked PDF+PNG
already exist, and re-links any missing source PDFs from
`pipeline_v3/step_4/polygon_cropped_pdfs`. So just run it again:

```bash
cd pipeline_v3/step_5
../step_4/venv/bin/python mask_pages.py --workers 16 --pad 4 2>&1 | tee -a /tmp/mask_full_run.log
```

- Expect ~10–12 min for the remaining pages at 16 workers (~1 page/s).
- Live progress (buffering-proof):
  `watch -n 5 'find pipeline_v3/step_5/2_src_organized -name "*.masked.pdf" | wc -l'`
  (climbs toward 1406 — see "skipped" below).
- When it prints `nothing to do — all pages already masked`, it's complete.

## Verify when done
1. `find pipeline_v3/step_5/2_src_organized -name "*.masked.pdf" | wc -l` → **1406**
   (1408 labeled pages minus the 2 zero-polygon pages below).
2. Spot-check a masked PNG vs its source (render at low dpi and eyeball): the labeled
   content must be intact with no clipping; unlabeled margin marks gone.
3. The run prints a `SKIPPED (...)` line listing the 2 expected zero-polygon pages.

## Two zero-polygon pages (expected skips — flag to David, don't mask blank)
- `Volume_1/1_source_pages/page_234`
- `Volume_2/2_post_pages/page_372`
These have no labeled polygons, so masking would produce a blank page. Confirm whether
they're genuinely blank scans or just never labeled.

## Then: next step (don't start without David's go-ahead on granularity)
After masking, the agreed pipeline is **dual text extraction** → `id`-keyed text sidecar
→ per-document JSON → RAG/NER/graph-RAG. See HANDOFF §"Plan" for the one open decision
(blanket per-polygon zoom vs. full-page-structured + targeted zoom), which a small
calibration pass should settle. Do NOT mutate the gold JSONs — text is a new sidecar
keyed to polygon `id`.

## Gotchas
- **Folder/file offset is intentional**: e.g. `Volume_2/1_source_pages/page_001/`
  contains `page_011.json`/`.pdf` (folder = renumbered "true" page; inner files keep the
  original step_4 number = `page_number`). Always key the PDF off the inner JSON number.
- Masked artifacts are ~8 GB and **untracked** in git. Consider a `.gitignore` for
  `*.masked.*` if you don't want them committed. They must reach the desktop via whatever
  syncs the working tree (not git), along with `2_src_organized/` and
  `step_4/polygon_cropped_pdfs/`.
- venv: use `pipeline_v3/step_4/venv/bin/python` (has PIL + numpy). Rendering uses the
  system `pdftoppm` (poppler), same renderer as the editor, at `render_dpi`=150.
