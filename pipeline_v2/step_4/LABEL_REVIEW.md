# Label-Agreement Review & Generalization Study

_Started 2026-06-02. Living document — methodology first, findings appended as they land.
Goal stated by the user: tailor this labeling scheme to the Solanus corpus **and** distill a
generalizable prompting + few-shot + labeling workflow for future projects._

## What we are trying to answer

1. **Label agreement / acceptable ranges.** Across the ~75 human-labeled example pages, how
   *internally consistent* are the labels? Where the same kind of content is labeled differently
   on different pages, that boundary is **fuzzy** — and the model should not be expected (or
   forced) to be more consistent than the ground truth. We want, per confusable category axis:
   the *canonical* rule the labels mostly follow, the genuine exceptions, and the **tolerance
   band** (when is each label defensible?). This protects the model from "freaking out" on
   contradictory few-shot demonstrations and stops us from chasing phantom errors that are really
   label noise.
2. **Coverage to ~zero, without overfitting.** Drive down `uncovered_ink` (missed content) using
   *general* mechanisms (prompt completeness rules, a coverage backstop pass) — never page-specific
   memorization or stuffing the answer into few-shot.
3. **Generalization.** How much of this flow is corpus-specific vs portable? What is the reusable
   recipe (scheme design → few-shot curation → prompt → snap → QA → review loop)?

## Why this method (and not just "compare to gold")

The label JSONs carry **geometry only, no text** (by design — every "missing text" issue becomes a
pure coverage problem detectable from ink). So you cannot audit *label* correctness from JSON alone;
you must see the box drawn on the page. Hence step 1 below renders **category-annotated overlays**.

Single-annotator corpus ⇒ this is an **internal-consistency** audit (not inter-annotator κ). The
unit of analysis is the *confusable category axis*, reviewed across every page where it appears.

## Method

- **S1. Annotated overlays + manifest.** `labeled_overlay.py` draws every example box labeled with
  its category on the page image, and emits `label_review/manifest.json` (per page: categories
  present, counts, num_documents, edge count) to target the review.
- **S2. Confusable-axis fan-out (workflow).** One agent per axis (see list below) reads the
  overlays for pages carrying that axis and reports: canonical rule, inconsistencies (page-cited),
  proposed tolerance band. Each finding is **adversarially verified** by an independent agent
  before it is trusted (kills plausible-but-wrong claims).
- **S3. Coverage-miss fan-out (workflow).** One agent per `uncovered_ink` spot classifies it:
  real missed content the model should box / ambiguous content the gold *also* skips / detector
  noise — and whether a *general* fix exists.
- **S4. Generalization synthesis + web research.** Distill portable principles; cross-check against
  published practice on layout-annotation schemes, inter-annotator agreement, few-shot consistency.
- **S5. Apply non-overfit changes → re-run → re-measure → iterate.** Each change measured by the
  before/after deltas in coverage (`qa_report`) and agreement (this review). Checkpoint before each
  mutation.

## Confusable category axes (initial — refined by S1 manifest)

| Axis | The hard call |
|---|---|
| `archv_date` vs `src_date` | archivist's top-corner year-first date vs the source's own dateline |
| `struct_id` vs `struct_doc` | centered piece-title vs page/document structural marker |
| `src_origin` vs `src_location_sender` | institution name vs its street/city address |
| `src_origin` vs `src_signature` | letterhead identity vs the author's sign-off |
| `src_other` vs `src_greeting` vs `src_farewell` | mottos / blessings / doxologies by position |
| `src_recipient` vs `src_greeting` | who it's addressed to vs the opening salutation |
| `other` (role-neutral) usage | when is the catch-all justified vs a real category |
| connections / page-type | which pages carry edges (notebook) vs none (formal letter) |

## Safety / checkpoints

On-disk, revertible, under `step_4/checkpoints/NN_<desc>/` (copies of mutable code + labels + gold +
the git HEAD at checkpoint time). **`reviewed/` is the gold and is never written by any tool here.**

---

## Findings — workflow `w8dxot79q` (29 agents, adversarially verified, 2026-06-02)

Full machine-readable result: `rerun_compare/workflow_result.json` (acceptable_ranges, per-axis
reviewer+verifier verdicts, coverage classifications, two research dossiers). Highlights below.

### Acceptable ranges (the "don't freak out" tolerance bands)

The corpus follows its own rules on ~73/75 pages. Per confusable axis, the rule + where **both
readings are defensible** (a model matching one valid reading must NOT be scored wrong):

| Axis | Canonical rule | Defensible-either-way (tolerance) |
|---|---|---|
| **archv_date vs src_date** | FORM IS DISPOSITIVE: year-first `YYYY, Month` / `c.YYYY`, top-corner or mid-page divider, never connects → `archv_date`; natural/numeric/feast dateline or notebook margin date → `src_date` | Mass-card per-card lower year-first dates (rule-549 → src_date, OR archv_date); year-LEADING source datelines; feast/date embedded *inside* a body line stays `src_content` |
| **struct markers** | `struct_id`=centered top piece-title/notebook title (y<0.30H, no edge); `struct_doc`=far-left page/section anchor that fans out | reprinted title running-header as `struct_other` (pref.) or `struct_id`; fused-line outline markers |
| **origin vs location_sender** | institution NAME kept whole → `src_origin`; street/city address below → separate `src_location_sender` | street+city in one box vs two; a fused place-token line lacking "Monastery/Church" either way |
| **origin vs signature** | institution identity vs author's personal sign-off; clean corpus (0 swaps) | only the adjacent origin/location cut; never swap a personal `Fr. Sol.` sign-off ↔ letterhead |
| **mottos/blessings** | by POSITION: on/above sig=`src_farewell`; below sig name=`src_other`; opening reader-addressed=`src_greeting`; leading verse on single-prose page stays in `src_content` | motto sharing the sign-off line (farewell OR other); opening doxology above its own dateline (greeting OR other, gated on an intervening dateline) |
| **recipient axis** | name line=`src_recipient`; city/convent below=`src_location_recipient`; salutation=`src_greeting` | `Dear <name>:-` with no separate name line → `src_greeting` canonical (`src_recipient` = error); name fused into salutation may stay in greeting |
| **archv_meta** | `archv_possessor`=provenance "in possession of:" + owner+modern address; `archv_format_note`=transcription/format note; `archv_commentary`/`archv_other` | depicts-vs-provenance note (format_note OR commentary); insertion/continuation note (archv_* OR struct_commentary) |
| **catch-all `other`** | the SPECIFIC category almost always beats the catch-all | Mass-card intention tag = `src_content` canonical (OR src_other/greeting per layout) — but **`other` is NOT defensible** for source text |
| **connections / page-type** | PAGE TYPE governs edges: notebook page-markers + margin dates fan into content (5 allowed pairs); letters/Mass-cards = ZERO edges | **edge PRESENCE is strict, 0 inconsistencies** — penalize any edge on a letter/continuation-header/Mass-card, any non-allowed pair, any cross-doc edge |

**Load-bearing lesson:** the ~91/100 alignment ceiling is **human-label noise**, not prompt
deficiency. Past the plateau → relabel gold; do NOT keep tightening prompts (medium-confidence
prompt fixes overcorrected twice before and were reverted).

### Confirmed gold relabels (verifier-backed)

**High-confidence (13)** — apply to `labeled_examples/`:
- `Volume_2/page_002`: possessor block `archv_other`→`archv_possessor`; letterhead `St. Michaels Monastery` `src_content`→`src_origin`
- `Volume_3/page_001`: `Feast of the Presentation.` `struct_doc`→`src_date`
- `Appendix_1/page_004`: split `225 Jerome Street.` out of `src_origin`→`src_location_sender`
- `Appendix_1/page_028`: split `HUNTINGTON, IND.`→`src_location_sender`; `num_documents` 1→3 (must match page_031)
- `Appendix_1/page_031`: merge the two `SERAPHIC`/`ST.FELIX` origin boxes into one; `Dear Margaret and Frank:-` `src_recipient`→`src_greeting`
- `Appendix_1/page_006`: `num_documents` 2→1 (no signature at the in-body timestamp)
- `Volume_2/page_075`: intention tag `src_greeting`→`src_content`
- `Appendix_3/page_006`: running header `other`→`struct_other`
- `Appendix_3/page_019`: `- for speedy recovery -` `other`→`src_content`
- `Volume_3/page_218`: `WANTED` `other`→`src_content` (kills a degenerate edge)

**Model-output fix (not gold):** `Appendix_1/page_047` `1953, April 6` `src_date`→`archv_date` (fix in `auto_labeled/` or via re-run, NOT in `labeled_examples`).

**Medium (6) — hold for a second look:** `Appendix_1/page_001` (Shonnard Place split), `Volume_1/page_245` (`C.I`→struct_doc), `Appendix_1/page_042` (intention tag), `Appendix_1/page_045` (God-bless-you→greeting; closing doxology→farewell).

**Refuted (leave alone — in-band):** `Appendix_3/page_019` date axis, `Volume_3/page_310` (already archv_other), several doc-counts within the 2-or-3 / 4-or-5 tolerance.

### Exclude / rehabilitate

Keep `EXCLUDED_EXAMPLES` as the quality gate. **Rehabilitate only AFTER the relabel lands**, and only the single-defect pages: `Volume_3/page_001`, `Volume_3/page_218`, `Volume_2/page_002`, `Appendix_1/page_006`, `Appendix_1/page_028`. Keep the multi-defect/Mass-card-pathology pages excluded.

### Coverage plan (drive `uncovered_ink`→0 WITHOUT overfitting)

1. **Coverage-backstop second pass** *(overfit risk: none)* — after pass-1+snap, run the uncovered-ink detector on the model's OWN output; re-prompt on each uncovered region to add the missing box. Content-driven, fires only on real uncovered ink, portable verbatim. **Primary fix.**
2. **One general completeness rule** in `SYSTEM_PROMPT` *(low)* — every line of source text in exactly one `src_*` box: short standalone lines, the topmost line, short wrapped tails; box boundaries only at true blank breaks.
3. Narrow multi-entry-letter rule *(low)* — new-entry date + same-line greeting: box the rest of the line.
4. `qa_report` clipped_edge de-noise *(diagnostic only)* — suppress a clip flag coincident with an uncovered_ink region.

### Generalization principles (portable to future corpora)

1. **Corpus FACTS vs portable MACHINERY.** The ontology, the 5 edge-pairs, the carve-outs, `labeled_examples/` are facts a new corpus redefines; the two-pass engine, snap-to-ink, `qa_report`, `review_diff`, few-shot selection, held-out reproduction test are reusable machinery.
2. **Write down irreducible ambiguity + a tie-break** (the "case-D" zones) rather than over-tightening the prompt to force one reading.
3. **The alignment ceiling is label noise** → relabel gold past the plateau; prefer high-confidence, high-frequency prompt changes only.
4. **Few-shot: PRUNE noisy examples; eval set: CORRECT them.** Exemplar correctness materially moves extraction outputs (Yoo 2022 — the "labels don't matter for ICL" result does NOT transfer here); the few-shot pool must be internally non-contradictory. `EXCLUDED_EXAMPLES` is the gate.
5. **Right agreement metric:** mAP@[.5:.95] (field standard + the detector's own metric) + per-class κ/α, computed per-class × per-doc-type; concentrate effort on the low cells. Heavy cross-class confusion = a SCHEME problem, not a data problem.
6. **Coverage fixes content-driven & general**, never page-specific (geometry-only JSON makes every miss a pure-ink problem detectable without ground truth).
7. **Two-pass high-recall→high-precision:** geometry+class (pass 1, permissive), relations (pass 2, from layout + allowed-pair whitelist); page type governs whether edges exist; report precision & recall separately.
8. **Onboarding a new corpus = data+prompts, not an engine rewrite:** pick ontology → edit the 4 coupled spots that must agree → hand-label ~50–75 seed pages across styles → mine allowed edge-pairs → rewrite the two prompts keeping the scaffolding → re-run the four built-in loops.

### Prioritized actions (from the synthesis)

1. Apply the 13 high-confidence gold relabels (then re-run `upload_examples.py`).
2. Fix `Appendix_1/page_047` in model output (not gold).
3. Implement the coverage-backstop pass + the one completeness rule (+ qa de-noise). **Highest leverage, lowest risk, portable.**
4. Apply the 6 medium relabels only after a second look.
5. Make NO change for refuted items.
6. Re-examine `EXCLUDED_EXAMPLES` for rehabilitation after step 1.
7. Encode the two confirmed prompt-gaps (opening-doxology gated on intervening dateline; non-US modern addresses in `archv_possessor`).
8. Re-run held-out test + `qa_report` after steps 1–3; expect alignment to stay near ~91 (residual is now-reduced label noise), `uncovered_ink` to drop.

---

## Iteration log

### Iter 1 — coverage backstop (2026-06-02) · checkpoint `02_pre-coverage-backstop`
**Change (code only, no prompt/label edits):** added a `coverage_backstop()` second pass to
`auto_labeler.py` (+ `--no-backstop` flag). After pass-1+snap it re-detects uncovered ink on the
model's OWN output via `qa_report.check_uncovered_ink`, re-prompts the model to box only those
regions, snaps just the new boxes, and merges them into the nearest existing document. **Add-only**
— existing labels and `num_documents` can never change. Overfit risk: none (fires only on real
uncovered ink, zero page-specific knowledge; portable verbatim to any corpus).
**Smoke test (page_022):** `uncovered_ink 4 → 0`, all flags → 0, `num_documents` 1→1, 8 boxes added
(src_content/farewell/signature — all sane); visually the missed "But after all…", the verse block,
and closing lines are now boxed. ✅
**Full-volume A/B (52 pages, same prompt/few-shot, only the backstop differs):**

| metric | no backstop | with backstop |
|---|---|---|
| **uncovered_ink (missed content)** | 16 | **0** |
| clipped_edge | 1 | 0 |
| overlap (cosmetic) | 15 | 15 |
| total flags | 32 | 15 |
| doc-count errors vs gold | 0 | **0** |

Backstop fired on exactly the 6 pages that had misses, added 13 targeted boxes (categories:
`src_content`×11, `src_signature`×2, `archv_other`×1 — all sane), and **drove missed content to
zero volume-wide** with no change to doc-count and no schema flags. The only residual is cosmetic
`overlap` (the box-swallow geometry the user doesn't care about). **Content coverage: complete.**

### Iter 2 — high-confidence gold relabels + rehabilitation (2026-06-02) · checkpoint `03_pre-relabels`
**Applied (id-matched, dry-run-checked, then verified from JSON — see `apply_relabels.py`):**
8 category swaps, geometry untouched:
`Volume_2/page_002` archv_other→archv_possessor & src_content→src_origin · `Volume_3/page_001`
struct_doc→src_date · `Volume_2/page_075` src_greeting→src_content · `Appendix_3/page_006`
other→struct_other · `Appendix_3/page_019` other→src_content · `Volume_3/page_218` other→src_content
(+ dropped its degenerate WANTED↔Page1 edge) · `Appendix_1/page_031` src_recipient→src_greeting.
Plus `Appendix_1/page_006` `num_documents` 2→1 (clean structural merge of doc_2 into doc_1: one
farewell + one signature → one document; no geometry/category change).

**Rehabilitated** (single defect now fixed, re-admitted to the few-shot pool):
`Volume_2/page_002`, `Volume_3/page_001`, `Volume_3/page_218`, `Appendix_1/page_006`.
→ `EXCLUDED_EXAMPLES` **13 → 9**, clean few-shot pool **62 → 66** (11 multi-doc).
All 8 swaps + the merge + the edge-drop verified programmatically; overlays refreshed. ✅

### Iter 3 — extend-vs-new uncovered-ink handling (2026-06-02) · checkpoint `04_pre-extend-logic`
**The gap (user-raised):** measured on the 16 real misses, **14 were "box too short/narrow" (should
EXTEND an existing box) and only 2 were genuine new regions** — yet both the backstop and the editor
handled everything by ADDING a box.
**Recognition (category-safe).** First instinct (extend the geometrically-nearest box) was validated
against gold and **failed 7/16** — on page_033 it inherited a wrong-category neighbour (src_date/
archv_commentary) for what is one `src_greeting` block. Fix: **category comes from the model re-prompt**
(judged fresh with full-page context); **geometry only decides topology** — a returned box that abuts
an existing SAME-category box EXTENDS it, else it's a new box; adjacent same-category additions
coalesce. This can never mislabel (category is always the model's). Re-validated vs gold: **16/16
labels correct**; page_022's 4 continuations extend the body (0 new boxes), page_033's missing greeting
block becomes new `src_greeting` boxes.
**Backstop:** `_merge_added_boxes` rewritten category-safely (`abuts` + same-category union); logs
`extended X, added Y`. Deterministic simulation + end-to-end page_033 re-run both clean (uncovered 6→0,
doc-count unchanged).
**Editor (`normalized_editor.py`):** new **Extend** button + per-flag suggestion in the queue
(`abuts src_content → Extend` vs `orphan → Add box`) — one click grows the nearest box for the dominant
"too short" case; **Add box** stays for orphans. Compiles + extend-geometry unit-tested (self-contained,
no new import). Full interactive UX still to be exercised by the user.
**Net:** the two cases are recognized and handled distinctly — automatically in the backstop,
one-click in review — which is what "severely minimize human review" needs.

### Iter 4 — review-queue UX overhaul (2026-06-02) · checkpoint `05_pre-queue-ux`
Built all four upgrades to `normalized_editor.py`'s Review Queue:
- **Extend vs Add box** — per-flag suggestion (`abuts src_content → Extend` / `orphan → Add box`) +
  one-click Extend (grows the nearest box) vs Add box (new region).
- **Keyboard-driven** — ↑/↓ navigate (auto-select + zoom), Enter = the suggested action (Extend if it
  abuts a box, else Add), `e`/`a`/`d` = extend/add/dismiss, `f` = fit. Bound on the queue list, no
  global-shortcut conflicts.
- **Auto-center-zoom** — selecting a flag zooms ~4× and centers it (render capped at 4000px to bound
  memory; flag verified to map to canvas centre); `f` or a page change restores fit.
- **Ghost preview** — a cyan dashed box shows exactly what Extend/Add would produce, labelled
  `Extend → <cat>` or `New box`, before you commit.
- **Persisted dismissals** — written to `qa_output/<doc>/dismissed.json` and filtered on load, so a
  dismissed false-alarm stays gone across sessions.
Verified headless: compiles, constructs, and zoom/center math + dismiss roundtrip + ghost-draw + nav
all exercised cleanly. Interactive click-through is the user's to confirm.

### Iter 5 — overlap-flag refinement + leakage holdout (2026-06-02) · checkpoint `06_pre-overlap-leakage`
**Overlap severity measured** (`rerun_compare/overlap_severity.py`, by ink in the shared region):
of 30 overlaps — **0 whitespace, 19 partial** (thin ink band = clipped character tops/bottoms; a
text-extraction prompt that "weeds out half-finished words/stray marks" handles these), **11
full-word**, of which only **6 cross-role**.
**Overlap detector now ink-aware** (`check_overlap_loose`): flags ONLY when the shared region holds
≥½ a text line of ink (a real word) that belongs to a **different category or document** box — i.e.
cropping this box would capture text that isn't its own. Whitespace, partial-character overlaps, and
a box's own sub-line inside a same-role/same-doc neighbour ("reasonable" containment) are no longer
flagged. **Result: overlap flags 30 → 6** (the genuine cross-role swallows: pages 6, 13×2, 31, 34, 50).
The editor queue reads qa flags, so it inherits this automatically.
**Leakage fixed**: `select_few_shot` + `select_pass2_fewshot` take a `target_page` and hold out the
exact page being labeled — a page can never see its own answer. Verified (page_001 excluded from its
own few-shot). NOTE: the earlier clean re-run still used the old per-page leak on the 9 Appendix_1
few-shot pages (001/004/006/015/019/031/035/045/047); the other 43 were already clean. A fresh run
would now be fully leak-free on all 52.

### Iter 6 — QA floors, doc Merge/Split, zoom, playbook (2026-06-03) · checkpoints `07`–`08`
- **QA floor calibration** (user caught it): page_012's unboxed `etc.` (62px tall, ~2.5k ink px) fell
  under `UNCOVERED_MIN_H/INK` — QA said 0, and the backstop only fixes what QA flags. Floors lowered
  to sit just above speck noise → **5 hidden short-word misses surfaced** (pages 12, 26, 33, 34, 43),
  now in the editor queue. Lesson → PLAYBOOK §3.4.
- **Doc Merge ▲ / Split ▼** in the editor (user-designed, adjacent-only): merge folds doc_N into
  doc_N−1; split duplicates doc_N into a new doc_N+1 (fresh ids, no edges) for keep/delete
  partitioning; both renumber later docs and remap connection `{doc,…}` refs. Headless-verified;
  **Merge ▲ field-validated** — the user merged page_031's front/back card 3→2 docs in production.
- **Zoom everywhere**: editor + viewer now have cursor-anchored wheel zoom, middle-drag pan, `f`=fit
  (render capped ~16MP); editor keeps flag-auto-zoom.
- **Convention sync**: few-shot `page_031` copy synced to the 2-doc gold *while preserving* the
  verifier-confirmed recipient→greeting relabel (backup `.bak-3docs`). NOTE: `reviewed/page_031`
  itself still has the salutation as `src_recipient` — one editor click if the user wants gold to match.
- **`PLAYBOOK.md` written** — the full distilled template (issue log §3 with 10 issue classes,
  pre-flight checklist, the VLM audit flow, acceptable-bounds method, fast-human-pass design,
  extraction/RAG rationale). Living doc: append new issues as they arise.
- **Concurrency lever** (checkpoint `09`): `MAX_CONCURRENCY = 4` global + `--concurrency` flag;
  pages labeled in a thread pool with `--delay` as a submission stagger; retries strengthened to
  6 attempts with jittered exponential backoff (to 60s) for rate-limit bursts. Few-shot selection
  stays precomputed sequentially → seed-42 determinism survives concurrency. Smoke: 4 pages in
  22s vs ~56s serial, 0 failures.

### Deferred — next batch (precision edits / re-validation; not "stuck", just out of scope for a blind script)
- **Geometric letterhead splits** (need a within-box y-cut → best done in `normalized_editor.py`):
  `Appendix_1/page_004` split `225 Jerome Street.`→src_location_sender; `Appendix_1/page_028` split
  `HUNTINGTON, IND.`→src_location_sender.
- **`Appendix_1/page_028` `num_documents` 1→3** — needs content-aware reassignment of boxes to 3 docs
  (which signed block → which doc); a human/editor call, not a safe script. (Keeps page_028 excluded.)
- **`Appendix_1/page_047`** model-output date `src_date`→`archv_date` — a single box in `auto_labeled/`
  on a *leaked* page; fix on next labeling run, low priority.
- **6 medium-confidence relabels** (Shonnard Place split, C.I→struct_doc, two page_042/045 tags) —
  per action #4, hold for a second look; do NOT bundle with the decisive batch.
- **2 confirmed prompt-gaps** (opening-doxology gated on intervening dateline; non-US addresses in
  archv_possessor) — small prompt edits, but per the "re-verify after each batch" lesson they warrant
  re-running the held-out reproduction test, so grouped with that.
- **Re-upload few-shot images** (`upload_examples.py`) + **Appendix_3 run** + **held-out re-test** to
  quantify the agreement lift from the relabels and confirm the locked ~91 didn't move.

### State at end of session (2026-06-02)
**Coverage:** `uncovered_ink` **0** volume-wide. The extend-aware backstop **extended 16 regions /
added 11 new across 9 pages** (more extends than new boxes — the "box too short" case dominates, as
predicted); doc-count vs gold **0**. **Labels:** 8 confirmed swaps + 1 doc-merge; few-shot pool
cleaned **62→66** (`EXCLUDED` 13→9). **Tooling:** category-safe extend-vs-new in *both* the backstop
and the editor; full review-queue UX (keyboard nav, auto-zoom, ghost preview, persisted dismissals).
Insight + acceptable-range table + 8 generalization principles captured above. Checkpoints `01`–`05`
on disk; `auto_labeler.py` / `qa_report.py` / `normalized_editor.py` surgical + import/compile-clean.
**Overlap (post Iter 5):** the ink-aware detector flags **6** genuine cross-role swallows (down from
30 raw); the rest were whitespace/clipped-character overlaps. **Leakage:** now held out per page going
forward (the measured run still had the old leak on 9 of 52). Checkpoints `01`–`06` on disk.

### Iter 7 — page-type few-shot + model lever (2026-06-03) · checkpoints `11`/`12`
**Trigger:** you hand-corrected all 43 Appendix_3 pages (~1 h). `review_diff` (model vs your gold):
RECAT **4** / DOC-COUNT **1** (categories & doc-count ~right) but ADD **36** / REMOVE **90**, and the
damage was *concentrated on pages 023–037* — the dense **notebook/ledger** pages. Visual overlays
(`rerun_compare/overlays/Appendix_3/`, green=gold/red=model) confirmed: boxes broadly **concentric**;
the pain is per-entry **segmentation granularity/geometry** on dense pages (IoU<0.5 inflates those into
add+remove). Full write-up: `DEEP_REVIEW.md`.

**Root cause (measured offline):** few-shot was selected by **volume**, not page type. A dense
Appendix_3 notebook page drew **3/12** notebook demos (7/12 were *letters*) — and the `min_multi_doc=4`
quota made it worse, spending 4 slots on multi-doc letters/cards (notebooks are single-doc). The page
that most needed notebook demonstrations saw the fewest.

**Shipped (`auto_labeler.py`):**
- **Page-type classification.** `infer_page_type_from_labels` (types the pool offline from gold:
  notebook/letter/mass_card/other) + `classify_page_type` (one cheap cached VLM call types the target).
  Validated vs gold types: **notebook 22/22 perfect**; front-half letter/preamble confusions all land on
  the safe side of the fallback.
- **Type-routed `select_few_shot`** (`target_type`/`type_of`): same-TYPE demos first (across all
  volumes), multi-doc quota **relaxed for single-doc types**. Notebook target now gets **12/12** notebook
  demos (was 3/12). **Regression-safe:** `target_type∈{letter,mass_card,notebook}` routes by type;
  `None`/`other` falls back to the original same-volume selection (proved identical) — a misclassified
  front-half page can never drop below its old behavior. `--page-type-fewshot` (default ON).
- **Default model flash-lite → `gemini-3.5-flash`** (`--page-type-model` stays flash-lite for the cheap
  classify).

**A/B (7 hard pages, identical few-shot per cell, hard=recat+add+rem+doc):**

| few-shot / model | flash-lite | 3.5-flash | pro |
|---|---|---|---|
| same-volume (old) | 138 | **32** | 36 |
| page-type (new) | **52** | 34 | — |

→ **Better model is the biggest lever** (−77%, and fixed the p034/p037 doc-count splits); **page-type
few-shot rescues the cheap model** (−62%) and is ~neutral for the strong one (substitute levers). 3.5-flash
**beat** pro (newer gen, cheaper) → pro rejected. Cost ~$4 vs ~$0.66 per ~45-pg volume — trivial vs the
review hour. Prompting held (demos+model fixed granularity without new rules; §3.8 overcorrection risk).

**Fine-tuning:** researched (Vertex SFT takes image input, rec ≥100 examples, tuned-endpoint ~1.5× base).
Built `export_tuning_data.py` → `tuning_data/` (95 gold pages, 86 train/9 val). **Defer the train** until
~300–500 pages & stable conventions; the payoff is dropping the 12 few-shot images + shrinking the
~50k-tok/page prompt → cheaper and more consistent. Then benchmark tuned-flash vs few-shot-3.5-flash.

### Iter 8 — non-training integrations + held-out protocol (2026-06-04) · checkpoint `15_pre-integrations`
**Holdout protocol (user directive: "completely held out, no training leakage"):** volume-level, not
page-level. Training = Appendix_1 + Appendix_3 gold ONLY (pinned in `export_tuning_data.py` —
`TRAIN_VOLUMES` — so a re-export after Appendix_2 review can't silently pull the test volume in).
**Appendix_2 = the untouchable test volume**: not in training, not in the few-shot pool until after
the tuned-model eval.
**Integrated:**
- **Promoted 6 hand-corrected hard ledger pages** (A3 023/025/031/033/034/037) into the few-shot pool
  (72 clean: notebook 42 / letter 21 / mass_card 4 / other 5) — the hour-of-pain pages are now the
  demonstrations. Mass-card gap can't be filled from A3 (only 2 exist; both already in). Schema-foreign
  editor keys stripped on promote (§3.11). Uploads refreshed (72 @ 1024).
- **`triage.py`** — production gold-free review triage: shadow-labels a volume with flash-lite,
  ranks pages by model disagreement (top-half catches ~79% of real errors), writes
  `qa_output/<vol>/triage.txt` with zero-disagreement auto-accept candidates. Verified offline on A3
  (reproduces the known hard-page ranking; 12/43 zero-disagreement).
- **Self-consistency A/B** (`rerun_compare/sc_ab.py`, running): 3-seed flash-lite ensemble (diversity
  via few-shot draws at temp-0) + ≥2/3-vote box fusion vs single 3.5-flash, scored with panoptic ink-IoU.
  LEAKAGE CONTROL: selection replays against the checkpoint-15 (pre-promotion) pool so the promoted
  hard-page siblings can't serve as demos for their own test.
- **Cost decisions:** Batch API integration SKIPPED (real complexity; bottleneck is review time, not
  ~$4-7/volume API cost). NOTED: per-page few-shot variation defeats Gemini implicit prefix-caching;
  fixed per-type example sets would unlock ~90%-off input pricing but change selection semantics —
  deferred as a future A/B, not a silent change.
**Phase B (running):** Appendix_2 labeled with the new defaults (3.5-flash + page-type routing +
promoted pool) → qa_report → triage. **Phase C (next):** pinned-volume tune → deploy checkpoint →
eval on val + the fully-held-out Appendix_2 → teardown same-session.

**Iter 8 addendum — self-consistency A/B result: REJECTED (negative result, kept honest).**
7 hard pages, panoptic ink-IoU: single lite PQ 0.538 / ensemble-3×lite PQ **0.272** / single
3.5-flash PQ 0.582. The ≥2/3-vote box fusion halved recall (FN 66→128): flash-lite's variance on
dense pages is segmentation GRANULARITY, so same-content boxes don't align across samples at
IoU≥0.5 and get dropped. Box-vote self-consistency suits presence-variance, not boundary-variance.
Not integrated; 3.5-flash stays the quality path. (Possible future variant, not pursued: fuse by
ink-union / pick-best-sample-by-self-agreement.) Also: the honest hard-page gap lite→3.5-flash is
0.538→0.582 PQ — real, but area-IoU's "138 vs 32" exaggerated it (§3.14 again).

**Iter 8 — fine-tune v1 result: UNDERFIT (instructive failure).** Job SUCCEEDED (86 ex × 3 epochs,
default LoRA) and the serving rung now works (checkpoint endpoints serve; v1 mystery solved — they're
real dedicated deployments, `minReplicaCount: 1`, NOT serverless: never leave them up). But the tuned
model exhibited **partial transfer**: it learned our ONTOLOGY (emits `struct_id`, `src_content`...)
while keeping the base model's native `box_2d` detection FORMAT — default adapter/LR on 86 examples
cannot override a strong RLHF'd format prior. Tuned-vs-base outputs differ only slightly (adapter
loaded but too weak). **v2 (running):** epochs 8, `learning_rate_multiplier` 5, `adapter_size` 8,
`export_last_checkpoint_only` (no endpoint sprawl). v1 model + all 3 endpoints torn down (0 left).

### Iter 9 — Phase C closed: v2 evaluated + torn down, tuning-prep bug fixed (2026-06-04, machine B)
**Picked up the interrupted v2 tune** (8 epochs, LR×5, adapter_size 8, last-ckpt-only) on the SUCCEEDED
endpoint and finished the staged sequence: format-probe → val → panoptic score → teardown.
- **Format: FIXED.** Unlike v1 (kept the base model's native `box_2d` format), v2 emits our exact
  nested schema *natively* (`{"num_documents":1,"documents":{"doc_1":{"src_content":[{"vertices":…}]}}}`),
  no schema-constraint needed. So stronger adapter+LR+epochs *can* override the RLHF'd format prior on
  ~95 examples. The format question is settled.
- **Labels: POOR at 95 pages.** Held-out val (9 never-trained A1/A3 pages), panoptic ink-IoU:
  tuned-v2 **PQ 0.159** (RQ 0.182, SQ 0.874, TP6/FP30/FN24) vs few-shot-3.5-flash **PQ 0.940**. SQ shows
  the boxes it *does* get are tight, but RQ is low — it can't yet localize/recall regions from 95 examples.
  **Verdict: a tuned flash-lite is NOT production-ready at 95 pages** — confirms the MASTER_GUIDE GATE-1
  threshold (~150–250+). Few-shot-3.5-flash remains the labeler.
- **Torn down immediately** (val score was decisive; not worth keeping a billing endpoint up for the
  32-min full-A2 sweep): undeploy → delete endpoint → delete model; **verified 0 endpoints / 0 models.**

**Two real bugs fixed for the GATE-1 tune (validated, not re-run to avoid an unattended billing endpoint):**
1. **Coordinate-space inconsistency (the likely quality drag).** Gold stores vertices in SOURCE PIXELS
   (x up to 5340) but the system prompt says output [0,1000]; the unnormalized training target therefore
   contradicted the prompt and made the space resolution-dependent. `_canonical_target` now **normalizes
   vertices to [0,1000]** (matching the prompt + production's `_scale_response_to_original`) and **strips
   the random UUID ids + empty connections** (pure training noise). `tuned_eval._label_page` now scales
   the model's [0,1000] output back to source pixels for the panoptic comparison. Round-trip verified
   (≤3px on a 5340px page).
2. **`finetune.py teardown` was the broken SDK version** (`client.endpoints` doesn't exist) AND wrongly
   commented "serverless." Rewritten to the validated gcloud order (undeploy → delete endpoint → delete
   model) with the correct warning: checkpoint endpoints are **dedicated `minReplicaCount=1` deployments
   that bill hourly — never leave up.**

**GATE-1 is now de-risked and one command-sequence away** (RESUME_HERE): re-`prepare` (now emits
[0,1000] targets) → `tune` with the v2 hyperparameters on ~150–250 gold → `tuned_eval val`/`score` vs
few-shot-3.5-flash on a held-out volume → `teardown`. Nothing deployed; ~$1–2 spent this session.

### Iter 10 — Vertex tuned-model COST LIFECYCLE, mapped (2026-06-04)
Tested "tear down restorably" on a throwaway tune (subset 20, 1 epoch). Findings:
- **Only deployed ENDPOINTS bill** (dedicated `minReplicaCount=1` replicas); the registered tuned
  **Model artifact has no idle cost.** A no-`--last-ckpt-only` tune deployed **2 checkpoint endpoints**
  (sprawl) — always pass `--last-ckpt-only`, and teardown/park must sweep ALL endpoints, not just the
  state-recorded one.
- **`park` works** (new): undeploy + delete endpoint(s), keep the Model → **0 billing, model retained.**
- **BUT managed Gemini tuned models are NOT manually redeployable:** `gcloud ai endpoints deploy-model`
  crashes (`KeyError 'supportedDeploymentResourcesTypes'` — managed models carry no deployment resource
  types), the genai SDK has no deploy, and the model 404s when called without an endpoint. They
  auto-deploy ONLY at tune time.
- **So the restorable path = RE-TUNE** (dataset + tuning job persist → `prepare`+`tune` is one cheap
  ~15-min command), not park+redeploy. `park` is still useful to stop billing while keeping the artifact
  for the Vertex console / reference, but don't rely on API redeploy.
- **`finetune.py`:** added `park` + `redeploy` (redeploy self-cleans its empty endpoint on the expected
  failure and points to re-tune); teardown sweeps endpoint+model. Final GCP state: **0 endpoints / 0
  models / 0 jobs.** Lifecycle-test spend ~$0.50.

### Iter 11 — full test suite (2026-06-05, overnight) · checkpoint `19`
Built `run_all_tests.py` (project had none): **33 checks PASS / 0 FAIL** — static (compile, ontology
consistency, JSON validity), logic (leakage/holdout gate, determinism, page-type routing, panoptic
gold-vs-gold=1.0 sanity), edge cases, cheap-API integration (classifier, full process_page, export), and
integrity (0 Vertex resources, gold unmodified). Caught + handled 2 real issues: 206 empty schema-foreign
gold stamps (editor §3.11 — opt-in `clean_gold_foreign_keys.py`, gold untouched) and 10 non-quad L-shaped
pool polygons (fixed the tuning target to square them). Full detail in `TEST_REPORT.md`. No tune run;
nothing deployed/billing. `reviewed/` gold pristine.

### Iter 12 — Appendix_2 gold + the archivist-catalog fix (2026-06-06, autonomous block)
**Appendix_2 reviewed by David** (76 pages → corpus 174 gold; "brutal", edits concentrated in the
first ~20 notebook pages). `review_diff`: 53 recat / 118 add / 17 rm / 120 resize. Production
(few-shot 3.5-flash + page-type) scored vs this fresh gold: **PQ 0.909 / PQ_strict 0.852** — the
0.057 strict-gap is almost entirely ONE error: archivist contents-inventory descriptions labeled
`src_content` (39 of 53 recats; David's "archivist describing page contents instead of content
written by Solanus").

**Root causes found and fixed (validated by 6-page ablation on the worst recat pages):**
1. **The prompt itself mandated the wrong convention** — `archv_other`'s scope-limit rule said a
   contents/index page's per-page descriptions are `src_content`. Codified in an earlier iteration;
   directly contradicts the new gold. Flipped to `archv_commentary` (+ `struct_doc` connections,
   matching gold) and amended `archv_commentary` with the catalog-entry pattern + third-person
   decisive test.
2. **Few-shot pool drift** — all 10 `labeled_examples/Appendix_2` entries predated David's review
   and disagreed with his gold (incl. the catalog pages themselves) → demos were TEACHING the
   mislabel. Pool synced to gold.
   Ablation (recats vs gold on pages 019/007/006/020/018/021): production **40** → prompt-fix alone
   **40** (demos dominate prompt text!) → prompt-fix + synced pool **6** (src→archv 36 → **1**).
   Lesson for the playbook: a wrong DEMO beats a right PROMPT — convention changes must always
   propagate to the pool, not just the text.

**Demos' value measured** (`experiments/run_arms.py base-bare`, all 76 A2 pages): 3.5-flash with
ZERO demos = PQ 0.870 / strict 0.806 vs production's 0.909 / 0.852 — the 12 routed demos buy
~+0.04 PQ. The bare model is far stronger than assumed; the pool's main value is conventions, not
capability.

**Gold-consistency audit (multi-agent, 55 agents):** all 98 A1/A3 gold pages audited against the
A2 conventions (spec distilled from David's edits; every finding adversarially verified).
**21 candidate one-click fixes** → `label_review/GOLD_CONSISTENCY_AUDIT_2026-06-06.md` (+13
findings excluded as stale-A3-PDF artifacts, not label errors). Gold NOT modified.

**GATE-1 tune** launched on the corrected 98-page A1+A3 set (8ep/LR×5/adapter8, A2 fully held
out as the cold test volume); arms base±demos × tuned±demos → `experiments/gate1_results.csv`.

### Iter 13 — GENTLE_TUNING_PLAN executed end-to-end (2026-06-06, unattended) · checkpoint `22`
**Machine-vintage repair first (Phase 0):** this machine's local A1/A2/A3 crops were a stale vintage
(A2 gold-vs-gold RQ 0.709, 275 dead boxes). Swapped in the git-tracked desktop-vintage page PDFs from
`auto_labeled/` → RQ **1.000**, B7 green, full suite 30/30. All scores below are desktop-comparable.

**Phase 1 — gentle sweep (cold-20 A2, PQ_strict; val-selected checkpoints):**
| arm | config | best ckpt (val PQs) | cold-20 PQ | cold-20 PQ_strict |
|---|---|---|---|---|
| **sweep-G1** | epochs **default→40**, LR default, adapter 4 | ep37 (0.793) | **0.782** | **0.669** |
| sweep-G4 (control) | 8 / LR×5 / adapter 8 | ep8 (0.655) | 0.510 | 0.362 |
| sweep-G2† | 6 / LR×2 / adapter 8 | ep6 (0.435) | 0.429 | 0.329 |
| sweep-G3 | 6 / LR×2 / adapter 4 | ep6 (0.453) | 0.412 | 0.313 |

†G2 redefined from the plan's default-epochs spec to the affordable mid-pole (budget: default
epochs = 40 ≈ 32.8M tokens/job). **OVER-TUNING VERDICT: CONFIRMED** — G1 ≥ G4 by nearly 2× on cold
strict-PQ. Nuance: "gentle" wins through default LR + small adapter + *many* epochs + **val-loss
checkpoint selection** (G1's curve rises to ep37 then dips at 40); epochs 1–4 can't even emit the
schema; adapter size is a minor factor (G2≈G3). **Phase 2 (flash base): SKIPPED per R3** — needs
~30–40 epochs to be useful ⇒ $35–98 at unverifiable rates.

**Phase 3 — continuous tuning works (the "fewer errors every round" loop):** `tune --from-model G1
--from-ckpt 9` on an 85-line targeted top-up (struct_doc/archv_commentary pages + David's 21
consistency-fix pages ×3). Val: +1ep **0.838** / +2ep 0.812 / +3ep 0.796 (val-selection caught the
overfit knee). **Cold-20: PQ 0.807 / PQ_strict 0.692 = +0.023 over its base** — real, narrowly under
the +0.03 bar (PARTIAL). Each future gold volume = one cheap continuation round (~$1–2).

**Phase 4 — adoption rung 1 PASSED:** tuned shadow ranks David's actual edit pages better than the
flash-lite shadow (Spearman **0.739 vs 0.644**). Shipped gated `triage.py --shadow-pred /
--shadow-tuned-endpoint / --out-name` (defaults unchanged; endpoints can't persist so the tuned
shadow runs from precomputed preds or a temporarily-live endpoint). Production untouched.

**State:** 0 endpoints; parked models: `solanus-gentle-G1` (sweep winner) + `solanus-cont-r1`
(current best, ckpt1 = the future continuation base). Gap to production (0.852 strict, full-A2):
0.692 vs 0.852 on cold-20 — tuned is now within ~0.16 and climbing ~+0.02/round. Spend this run:
~$17–22 (empirical basis; 2.5-tier tuning prices unpublished — David: verify in console).
Full ledger + curves: `experiments/RUN_LOG_gentle.md`; all rows in `experiments/gate1_results.csv`.
