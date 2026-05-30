# Step 4 — Auto-labeling quality session (2026-05-29)

## TL;DR
Built a measurement + review toolchain around `auto_labeler.py`, made three high-confidence
prompt fixes driven by data, and verified them on Appendix_1. **Labeling is reliable;** the
remaining hand-work is mostly box padding (cosmetic, low value) plus a now-fixed letterhead bug.

The pipeline is now: **`auto_labeler.py`** (label) → **`qa_report.py`** (flag problems) →
**`normalized_editor.py`** (review / accept / fix) → **`review_diff.py`** (learn from your edits).

---

## The tools (all in step_4/)

| Tool | What it does | Run |
|---|---|---|
| `auto_labeler.py` | Gemini 2-pass labeler + grow-only snap-to-ink. **Prompts improved this session.** | `./venv/bin/python auto_labeler.py --volume Appendix_1 --overwrite` (~70¢) |
| `qa_report.py` | Flags missing boxes / overlaps / clips per page; writes `qa_output/` + overlay PNGs. | `./venv/bin/python qa_report.py Appendix_1` |
| `normalized_editor.py` | Polygon editor on `auto_labeled/`, saves to `reviewed/`, with a per-page accept/reject queue from qa_report. | `./venv/bin/python normalized_editor.py` |
| `review_diff.py` | Compares `auto_labeled/` vs your `reviewed/` to surface systematic corrections. | `./venv/bin/python review_diff.py Appendix_1` |

Key safety design: the editor **reads `auto_labeled/` but saves to `reviewed/`**, so re-running the
labeler with `--overwrite` can never clobber your manual edits.

---

## What we changed in the prompt (3 fixes)

1. **Date splitting** — "Yonkers N.Y. 16/7/18" was one box (day-first date, no comma). Generalized
   the rule → splits correctly, no regression on normal datelines.
2. **Completeness/recall** — added a "capture every line of writing" instruction targeting wrapped
   PS lines, standalone blessings, handwritten additions.
3. **Letterhead split (from your edits)** — the prompt contradicted itself: rule 3 said split a
   stacked letterhead into NAME→`src_origin` + ADDRESS→`src_location_sender`, but rule 4 said keep
   the whole letterhead in `src_origin`. Gemini followed rule 4 and **blended origin + location**.
   Rewrote rule 4 to agree with rule 3. (This matters: origin = institution, location = sender place
   — different entities for your downstream tools.)
   **Verified:** re-ran Appendix_1 → 6/7 of the pages you'd hand-split now match your review
   automatically, and the model now splits letterheads on pages your review left merged (it is now
   *more* consistent than the manual pass). QA flags went 54 → 51 (no regression).

## What we deliberately did NOT change (and why)
- **Box "oversize"/overlap (p8):** humans overlap location/date up to 22% in the ground truth;
  p8 is ~15% = normal. The existing >50% auto-trim threshold is correct.
- **Snap clip tuning:** 0/378 Appendix_1 boxes are slanted, so snap's skew-skip never fires on
  typed letters. Re-examine only for handwritten notebook volumes.
- **Box padding:** your 75 resizes were measured to be **mostly padding into blank space**, not real
  clipped text (e.g. left edges: 29/29 moves into blank). Both tight and padded boxes enclose the
  same text, so this is cosmetic; changing snap would diverge from the 71 training examples.
- **p15 (card with printed verse):** ambiguous one-off → relabel by hand, not a prompt rule.

---

## Stats

**Recall fix — controlled A/B (same seed, same 52 pages, only the instruction differing):**

| Metric | Without | With recall | Change |
|---|---|---|---|
| `uncovered_ink` (missing content) | 36 | 25 | **−31%** |
| `overlap` | 26 | 25 | flat |
| total flags | 65 | 54 | −17% |

**Your Appendix_1 review (39/52 pages changed), via `review_diff.py`:**
- Recategorizations: **3** (one-offs) · Doc-count errors: **0** → labeling is reliable
- Resizes: 75 (mostly padding, see above)
- Adds: 20 (6 were `src_location_sender` = the letterhead-blending bug, now fixed)
- Removes: 8

**Scale/cost:** Appendix_1 = 52 pages ≈ 70¢. Full corpus ~1,150 pages ≈ **$15–20** per pass.

---

## What to do next
1. **Your reviewed Appendix_1 is safe in `reviewed/Appendix_1/`** and untouched by re-runs.
2. **Run the held-out reproduction test** before scaling — the gate that confirms these 3 prompt
   edits didn't dent the locked 91/100 alignment on the other volumes. (Heavy/rate-limit-prone.)
3. **Then label a real notebook volume** (e.g. Volume_1) as the next test bed — handwriting +
   connections are untested by Appendix_1 (letters have ~0 connections by design; notebooks fan
   page-numbers/dates into body text). Expect more flags and more hand-editing there.
4. **Re-run `review_diff.py` after each volume's review** to keep learning systematic fixes.

## Honest caveats
- Appendix_1 is **typed letters — the easy case.** Handwritten notebooks will be harder.
- **Connections (Pass 2) are untested** — Appendix_1 letters don't link boxes; notebooks do.
- The recall + date + header fixes are validated on Appendix_1 but **not yet** against the full
  held-out test.
