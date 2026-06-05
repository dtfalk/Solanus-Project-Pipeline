# RESUME HERE — current state (updated 2026-06-03)

_The 2026-06-02 mid-shutdown version of this file is superseded; its in-progress item
(the overlap auto-resolver) was finished and **dropped by decision rule** (safely resolved
only 1/6 cases; the 6 cross-role overlaps are precisely flagged for the editor instead —
see PLAYBOOK §3.6)._

## Read these first
- **`MASTER_GUIDE.md`** — THE deep, research-grounded, generalized guide: the 3-gear data-flywheel that
  drives human labeling → 0 as gold accumulates (for a continuous document stream), measurement done
  right (ink-IoU/Panoptic Quality, NOT area-IoU), the fine-tune/distill loop, cost engineering,
  reproducibility. Start here for the why and the where-we're-going.
- **`RESEARCH_AND_PLAN.md`** — the error-ID redesign + clustering verdict + fine-tune roadmap + ranked
  error-reduction menu (with the honest re-analysis of the metric).
- **`PLAYBOOK.md`** — the distilled, reusable template (issue log, pre-flight checklist,
  VLM label-audit flow, acceptable bounds, fast-human-pass design). The lasting artifact.
- **`LABEL_REVIEW.md`** — this corpus's full iteration log (Iter 1–7) + acceptable-range table.
- **`DEEP_REVIEW.md`** — the 2026-06-03 Appendix_3 deep review: the model/page-type/fine-tune A/B
  evidence and the recommended config. Read this for "why the defaults changed."

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
- **Few-shot pool**: 66 clean examples; page_031 synced to the 2-docs-per-card convention. Now also
  TYPED (notebook 36 / letter 21 / mass_card 4 / other 5) and routed by page type.
- **Appendix_3**: labeled (flash-lite) + fully hand-reviewed into `reviewed/Appendix_3/` (43 pages, gold).
- **NEW defaults (Iter 7, evidence in `DEEP_REVIEW.md`)**: `--model gemini-3.5-flash` (was flash-lite;
  −77% hard errors on dense pages) and `--page-type-fewshot` ON (types each page, routes same-type demos;
  rescues the cheap model −62%, regression-safe `other→same-volume` fallback). Tuning data exporter ready.
- **Checkpoints `01`–`12`** on disk; nothing committed to git yet (exclude `checkpoints/` if committing).

## Next
1. **Confirm the lift on a full volume**, then continue the corpus. From `pipeline_v2/step_4/`
   (new defaults already correct — 3.5-flash + page-type routing; uploads may need refreshing if >48h):
   ```bash
   # OPTIONAL re-run of Appendix_3 to measure the new defaults end-to-end (overwrites auto_labeled/
   # Appendix_3 ONLY — your reviewed/ gold is untouched; ~$4, ~5 min):
   ./venv/bin/python auto_labeler.py --volume Appendix_3 --overwrite
   ./venv/bin/python review_diff.py Appendix_3        # expect far fewer ADD/REMOVE than before
   # then the next real volume (e.g. Appendix_2 — also mixed, biggest page-type payoff):
   ./venv/bin/python auto_labeler.py --volume Appendix_2
   ./venv/bin/python qa_report.py Appendix_2
   EDITOR_DOCUMENT=Appendix_2 ./venv/bin/python normalized_editor.py
   ./venv/bin/python review_diff.py Appendix_2
   ```
   Cheap mode if cost matters: add `--model gemini-3.1-flash-lite` (keep page-type ON).
2. Editor sweep of the 11 queued Appendix_1 flags; `reviewed/page_031` salutation still
   `src_recipient` (one click) — few-shot copy already fixed.
3. **Fine-tuning when corpus ~300–500 pages**: `./venv/bin/python export_tuning_data.py` already writes
   `tuning_data/` (95 examples today). Train a flash model on Vertex SFT, then benchmark tuned-flash vs
   few-shot-3.5-flash on a held-out volume (goal: drop the 12 few-shot images + shrink the prompt).
4. Deferred batch in `LABEL_REVIEW.md` (letterhead splits, page_028 docs under the new card convention).

## 2026-06-04 — machine-move reconciliation (back on Desktop machine)
The other-machine session's work is fully synced here (checkpoints 11–14, new defaults, all docs/tools).
**Fine-tune loop status:** job `5447888…` = SUCCEEDED (training proven, ~4 min). BUT the serving rung
was never closed: the checkpoint lists endpoint `…/endpoints/1098148783113371648`, yet that endpoint
AND the model registry entry both 404 — **nothing is deployed, nothing billing** (directive satisfied).
GCP holds only: 2 tuning-job records (free) + the 13 MB dataset bucket
`gs://solanus-project-vertex-tuning-389262253193` (keep — it accumulates). To fully prove the eval rung:
re-run a small tune → explicitly deploy the checkpoint → call → teardown (~20 min, ~$1) — OR defer until
the real tune at ~150–250 gold pages. `tuning_data/tuning_job.json` now records the (dead) endpoint name.
