# RESUME HERE — session recovery (Solanus step_4 labeling work)

_Written 2026-06-02 mid-session because the laptop is shutting down. This captures
the EXACT state so a fresh Claude Code session can continue. Read this + `LABEL_REVIEW.md`
(full iteration log) + the memory files first._

## How to resume
1. Reopen Claude Code in `pipeline_v2/step_4`. Try `claude --resume` (or `--continue`) to
   reload this exact conversation; if that's unavailable, start fresh and paste this file.
2. Tell the new session: "Read `step_4/RESUME_HERE.md` and `step_4/LABEL_REVIEW.md`, then
   continue from the IN-PROGRESS task below."
3. Nothing is committed to git — all work is in the working tree + `checkpoints/01`–`06`.
   `git status` will show modified `auto_labeler.py`, `qa_report.py`, `normalized_editor.py`
   and many new files. Do NOT discard. (Commit if you want a durable snapshot.)

## ⏳ IN-PROGRESS (resume exactly here)
**Building a SAFE cross-role overlap-resolution pass.** Just rewrote (v3, UNTESTED):
`rerun_compare/overlap_resolve_proto.py` — trims a box's padding back to its own text using
the blank strip between two text blocks, tried both vertical (stacked) and horizontal
(side-by-side) axes, with a **revert-net** (undo any trim that raises `uncovered_ink`).

**NEXT COMMAND (run first on resume):**
```
./venv/bin/python rerun_compare/overlap_resolve_proto.py 2>/dev/null
```
Expected: `TOTAL overlap 6->?  resolved ?/6  NEW uncovered ink: 0 (MUST be 0)`.
- v1 (gap-in-overlap-span) resolved 0/6. v2 (vertical padding-trim) resolved 1/6 (page_050).
- v3 should catch more: page_013/045 = small-but-real vertical gap; page_031/034 = side-by-side
  (date beside content) → horizontal cut.
**Decision rule:** if v3 resolves most (≥4/6) with NEW uncovered = 0 → port `resolve()` into
`auto_labeler.py` as a pass in `process_page` AFTER the coverage backstop (add a `--no-overlap-fix`
flag like the backstop), apply to current `auto_labeled/Appendix_1` output, re-run `qa_report`.
If still low-yield → DROP it (don't add pipeline complexity for a rare issue); the 6 are precisely
flagged for the editor. The 6 overlaps are on pages 013(×2), 031, 034, 045, 050.

## ✅ DONE this session (all verified)
- **Leak-free re-run COMPLETE**: `auto_labeler.py --volume Appendix_1 --overwrite` → 52/52, 0 fail.
  Backstop: **16 extended, 12 new on 11 pages**. `auto_labeled/Appendix_1` = this output.
- **Coverage backstop** (Iter 1) + **category-safe extend-vs-new** (Iter 3) in `auto_labeler.py`:
  after pass-1+snap, re-detects uncovered ink on the model's own output, re-prompts, and either
  EXTENDS an adjacent same-category box or adds a NEW box. `uncovered_ink` = 0 volume-wide.
- **Label-agreement review** (29-agent workflow): findings, 9-axis acceptable-range table, 8
  generalization principles in `LABEL_REVIEW.md`; full JSON in `rerun_compare/workflow_result.json`.
- **8 gold relabels + 1 doc-merge applied** (`apply_relabels.py`); **4 pages rehabilitated** →
  `EXCLUDED_EXAMPLES` 13→9, clean few-shot pool 62→66.
- **Editor UX overhaul** (Iter 4, `normalized_editor.py`): Extend/Add-box with per-flag suggestion,
  keyboard nav (↑↓/Enter/e/a/d/f), auto-center-zoom, ghost preview, persisted dismissals
  (`qa_output/<doc>/dismissed.json`). Compiles + headless-verified. **NEEDS interactive click-test.**
- **Overlap detector made ink-aware** (Iter 5, `qa_report.py check_overlap_loose`): flags only
  cross-role (diff category/doc) FULL-WORD overlaps → **30→6**. Severity tool:
  `rerun_compare/overlap_severity.py` (of 30: 0 whitespace, 19 partial/benign, 11 full-word, 6 cross-role).
- **LEAKAGE FIXED** (`auto_labeler.py`): `select_few_shot` + `select_pass2_fewshot` take `target_page`
  and hold out the page being labeled. Verified page_001 excluded from its own few-shot. (The leak-free
  re-run already used this — all 52 pages clean. Earlier runs leaked on 9 pages: 001/004/006/015/019/031/035/045/047.)

## Checkpoints (revertible, on disk under `checkpoints/`)
01_pre-label-review · 02_pre-coverage-backstop · 03_pre-relabels · 04_pre-extend-logic ·
05_pre-queue-ux · 06_pre-overlap-leakage. Each has a README + GIT_HEAD.txt. `reviewed/` is the GOLD,
never written by any tool.

## Pending / deferred batch (after the overlap pass decision)
1. Decide + finish the overlap-resolution pass (the IN-PROGRESS item).
2. **Interactive test of the editor** (the user runs `./venv/bin/python normalized_editor.py`).
3. Geometric letterhead splits (Appendix_1 page_004 `225 Jerome St`, page_028 `HUNTINGTON`),
   page_028 `num_documents` 1→3 — precision edits, best done in the editor.
4. 6 medium-confidence relabels (hold for second look) — see `LABEL_REVIEW.md` Iter 2.
5. Two confirmed prompt tie-breaks (opening-doxology dateline-gate; non-US `archv_possessor` address)
   + re-run the held-out reproduction test to confirm ~91 alignment held.
6. **Appendix_3 run** (the user's next volume): re-upload few-shot first (`upload_examples.py`, the
   uploads 403-expire), then `auto_labeler.py --volume Appendix_3 --overwrite`.

## Key facts / gotchas
- Few-shot selection is rng-stateful (seed 42), so single-page runs (`--start N --end N`) get
  different few-shot than full runs → different output. Compare full-run to full-run.
- Box ids are fresh `uuid4()` every run, so you can't id-diff runs; compare spatially.
- `auto_labeled/` is the model output (regenerable); `reviewed/` is gold; `labeled_examples/` is the
  few-shot pool (corrected this session). Editor reads `reviewed/` if present (stable during re-runs).
- The user's criteria: **content COVERED + correctly LABELLED**. Over/under-boxing and whitespace/
  partial-char overlap do NOT matter to them (a downstream extraction prompt that "weeds out
  half-finished words/stray marks" handles partials). Only flag full foreign words.
