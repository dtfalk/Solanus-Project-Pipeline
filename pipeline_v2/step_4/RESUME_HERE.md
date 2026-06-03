# RESUME HERE — current state (updated 2026-06-03)

_The 2026-06-02 mid-shutdown version of this file is superseded; its in-progress item
(the overlap auto-resolver) was finished and **dropped by decision rule** (safely resolved
only 1/6 cases; the 6 cross-role overlaps are precisely flagged for the editor instead —
see PLAYBOOK §3.6)._

## Read these first
- **`PLAYBOOK.md`** — the distilled, reusable template (issue log, pre-flight checklist,
  VLM label-audit flow, acceptable bounds, fast-human-pass design). The lasting artifact.
- **`LABEL_REVIEW.md`** — this corpus's full iteration log (Iter 1–6) + acceptable-range table.

## State
- **Appendix_1**: leak-free labeled (52/52), `uncovered_ink` 0 at old floors; recalibrated floors
  surfaced 5 short-word misses (pages 12, 26, 33, 34, 43) + 6 cross-role overlaps — all queued in
  the editor for one-click fixes. Gold in `reviewed/` (never machine-written). NOTE: auto_labeled
  pages 001–004 were re-labeled by a concurrency smoke test (different few-shot draw than the full
  run — cosmetic provenance mix only).
- **Pipeline**: per-page few-shot holdout (no leakage) · extend-aware coverage backstop ·
  ink-aware cross-role overlap flags · calibrated QA floors · `--concurrency` (default 4) with
  jittered exponential backoff · full-res render-cache OOM fix (big volumes safe).
- **Editor/viewer**: wheel zoom + pan + fit, flag auto-zoom, ghost preview, Extend/Add box,
  persisted dismissals, doc Merge ▲ / Split ▼.
- **Few-shot pool**: 66 clean examples; page_031 synced to the 2-docs-per-card convention.
- **Checkpoints `01`–`09`** on disk; nothing committed to git yet (exclude `checkpoints/` if committing).

## Next
1. **Fire Appendix_3 — from YOUR terminal, in `pipeline_v2/step_4/`** (uploads refreshed 2026-06-03;
   all defaults are already right: concurrency 4, backstop on, per-page holdout on, seed 42):
   ```bash
   ./venv/bin/python auto_labeler.py --volume Appendix_3      # label: ~3-4 min, ~60-70¢
   ./venv/bin/python qa_report.py Appendix_3                  # coverage/overlap flags + overlays
   EDITOR_DOCUMENT=Appendix_3 ./venv/bin/python normalized_editor.py   # human sweep (saves to reviewed/)
   ./venv/bin/python review_diff.py Appendix_3                # after review: learn from your edits
   ```
   (`--overwrite` only if you want to REDO already-labeled pages; first run doesn't need it.)
2. Editor sweep of the 11 queued Appendix_1 flags; `reviewed/page_031` salutation still
   `src_recipient` (one click) — few-shot copy already fixed.
3. Deferred batch in `LABEL_REVIEW.md` (letterhead splits, page_028 docs under the new card
   convention, 2 prompt tie-breaks + held-out re-test).
