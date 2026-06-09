# Deep Review — Appendix_3 model-vs-gold, and the path to "near-zero human correction"

_Started 2026-06-03 (autonomous session). This is the living deliverable: findings, the
A/B evidence, and the three-lever plan (prompting · better model · fine-tuning). Companion to
`LABEL_REVIEW.md` (iteration log) and `PLAYBOOK.md` (reusable template)._

## The trigger
You hand-corrected all 43 pages of **Appendix_3** and it took ~an hour — "broad strokes correct
but a ton of errors." You asked for: (1) page-type identification to route better few-shot
examples, (2) a deep visual before/after review of every page, (3) options to fix via prompting /
a better model / light fine-tuning as the corrected corpus grows. Iterate; checkpoint on disk.

## What the numbers said (review_diff Appendix_3, model `auto_labeled/` vs your `reviewed/` gold)
36 of 43 pages changed. **RECATEGORIZE: 4 · ADD: 36 · REMOVE: 90 · RESIZE: 194 · DOC-COUNT: 1.**

- Categories are ~right (4 recats / 43 pages) and doc-count is ~right (1 / 43). The model is
  **not** mislabeling or miscounting documents.
- The pain is **REMOVE 90 / ADD 36**, and it is *concentrated*: pages 001–014 are clean
  (cosmetic resizes only); the damage is pages **023–037** — the dense, multi-entry
  **notebook / ledger** pages ("FATHER SOLANUS NOTEBOOK NO. 12": date column + content column +
  "Page N" markers + a donation ledger).

## What the pictures said (rendered overlays, green=your gold, red=model — `rerun_compare/overlays/Appendix_3/`)
Looked at 023, 024, 025, 031, 033, 034, 037, 015, 021 in full.

- On **every** page type — letter, Mass card, dense notebook, ledger — the model's boxes are
  **largely concentric with yours**. It finds the right regions and gives them the right roles.
- The divergence is **per-entry segmentation granularity and box geometry on the dense pages**:
  where you draw one entry box, the model sometimes draws two (or splits a 2-line entry, or
  double-boxes a date cell, or segments the money column differently). review_diff matches boxes
  by IoU≥0.5; when granularity differs the same ink shows up as remove+add instead of resize —
  so **"90 removes" overstates true error; most are granularity/geometry, the cosmetic class the
  PLAYBOOK (§2) explicitly says is NOT the quality bar.** But there are enough of them, on dense
  pages, that correcting by hand is genuinely an hour of work.
- **page_021** is a dense notebook page that is itself in the few-shot pool, and it labels well —
  proof that dense-notebook *demonstrations* work; we just aren't routing enough of them to dense
  pages.

## Root causes (ranked)
1. **The model is the cheapest tier — `gemini-3.1-flash-lite`.** Precise polygon geometry on
   faint, dense archival scans is exactly where a small VLM is weakest. Geometry precision is
   capability-bound.
2. **Few-shot is selected by VOLUME, not PAGE TYPE** (`select_few_shot`, auto_labeler.py:487 —
   `p.parent.name == target_doc`). Appendix_3's 9 pool examples are all front-half
   (≤page_021, mostly letters/Mass cards). When the model labels a dense back-half ledger page,
   same-volume bias prefers those letter examples over the *excellent* notebook demonstrations
   that live in Volume_1 / Volume_3 / Volume_4. The granularity convention for the page type at
   hand is under-demonstrated.
3. **Granularity convention for dense list/ledger rows is under-specified** — the prompt's
   "one src_content per anchor span" rule is good for prose-y notebook entries but says little
   about tabular name-lists ("Points Gained by Servers…") and money ledgers, where you and the
   model legitimately diverge.

> ⚠️ **METRIC CAVEAT (added 2026-06-04):** the absolute "hard error" counts below use **area-IoU vs a
> single annotator's gold**, which over-states real errors ~1.8× (box-jitter double-counting + gold
> noise — see `RESEARCH_AND_PLAN.md` §0 and `MASTER_GUIDE.md` §3). The **relative** model/page-type
> comparisons are robust (they hold at every IoU threshold), so the conclusions stand — but the right
> yardstick going forward is **ink/token-IoU + Panoptic Quality** calibrated to the inter-annotator
> ceiling, which the next-built eval harness will use.

## THE EVIDENCE — A/B on the 7 hardest pages (023,024,025,031,033,034,037)
Harness `rerun_compare/ab_model.py`: every cell relabels the page with the SAME few-shot set,
the SAME snap+backstop+pass2 pipeline — only the named lever varies — then diffs vs your gold.
**"hard" = recat + add + remove + doc-count errors** (cosmetic geometry "resizes" excluded — they
are the PLAYBOOK-§2 non-bar). Lower is better. Sum over the 7 pages:

| few-shot ↓  /  model → | gemini-3.1-flash-lite (current) | gemini-3.5-flash | gemini-3.1-pro |
|---|---|---|---|
| **same-volume (current)** | **138** | **32**  (−77%) | 36 (−74%) |
| **page-type-aware (new)** | **52**  (−62%) | 34 | — |

Reading it:
- **Better model is the single biggest lever.** flash-lite→**3.5-flash** cuts hard errors **77%**
  (138→32) and even fixed the doc-count splits on p034/p037. 3.5-flash slightly **beat** pro here
  (newer generation) *and* is cheaper — so **3.5-flash is the pick**, not pro.
- **Page-type few-shot rescues the CHEAP model:** flash-lite 138→**52** (−62%) from notebook demos
  alone. The two levers are **substitutes** — a strong model already "knows" notebook granularity,
  so routing is ~neutral for it (32↔34, within run noise) but decisive for flash-lite.
- **Why it works (offline-measured):** a dense notebook page used to get **3/12** notebook demos
  (and **7/12 letters**!) under same-volume selection; page-type routing gives it **12/12**.

### Confirmed on the FULL volume (all 43 pages re-labeled with the new defaults)
Fair comparison — OLD baseline and NEW run BOTH id-stripped and scored vs gold under identical
pure-spatial matching (`rerun_compare/fair_diff.py`; the raw `review_diff` numbers are NOT comparable
because the gold descended from the OLD run and shares its box ids — PLAYBOOK §3.9):

| full-volume config | HARD | recat | add | **rem** | doc |
|---|---|---|---|---|---|
| OLD flash-lite + same-volume | 213 | 10 | 74 | **128** | 1 |
| NEW 3.5-flash + page-type | **113** | 16 | 58 | **39** | 0 |

**−47% hard errors over all 43 pages**, driven by **−70% over-production (REMOVE 128→39)** — the
spurious/over-segmented boxes that WERE the review hour — and doc-count 1→0. (Less than the −77% on the
7 hardest pages because the front half was already clean; recat ticked 10→16 as 3.5-flash makes a few
different category calls, some within the §6 tolerance bands.) Re-run cost: ~$3.87 (2.05M in / 88k out).
Old baseline preserved at `rerun_compare/old_auto_Appendix_3/`; new overlays in
`rerun_compare/overlays/Appendix_3/` (old ones kept at `..._OLD_flashlite/`).

## The three levers — VERDICT
| Lever | Verdict | What I shipped |
|---|---|---|
| **Better model** | ✅ **Adopt `gemini-3.5-flash`.** −77% hard errors; ~$4 vs ~$0.66 per ~45-pg volume (~$130 vs ~$22 for the whole 1,400-pg corpus) — trivial vs the review hour it saves. | Default model changed flash-lite→3.5-flash (one-flag revert). |
| **Page-type few-shot** (your ask) | ✅ **Adopt, ON by default.** Free insurance: rescues the cheap path (−62%), neutral for the strong model, biggest upside on mixed volumes (Appendix_2/3). | New classifier + type-routed selection + regression-safe "other→same-volume" fallback + per-page cache. |
| **Prompting** | ⚖️ **Hold.** The prompt is already ~30k chars and PLAYBOOK §3.8 shows tightening overcorrects. Demonstrations (page-type few-shot) + a better model fixed the granularity *without* new rules. | No prompt-rule changes. Only the page-type *classifier* prompt was tuned. |
| **Fine-tuning** | ⏳ **Prep now, train later.** ~95 gold pages today (Google rec ≥100). The real win: a tuned flash BAKES IN the schema+conventions so you can drop the 12 few-shot images & shrink the ~50k-tok/page prompt → cheaper AND more consistent. | `export_tuning_data.py` → `tuning_data/` (Vertex SFT JSONL, 86 train/9 val). Train when corpus ~300–500 pages & conventions stable; benchmark tuned-flash vs few-shot-3.5-flash on a held-out volume. |

## RECOMMENDED CONFIG (now the defaults)
```bash
./venv/bin/python auto_labeler.py --volume <V>      # = --model gemini-3.5-flash --page-type-fewshot
```
Cheap fallback if cost matters more than the last few corrections:
```bash
./venv/bin/python auto_labeler.py --volume <V> --model gemini-3.1-flash-lite   # keep page-type ON
```

## Safety / cost
- Checkpoints **`11_pre-pagetype-fewshot`** (before) and **`12_post-pagetype-model`** (after) on disk
  (code + git HEAD). `reviewed/` gold never written by any tool. A/B output in throwaway `rerun_compare/ab/`.
- This session's API spend (A/B matrices + smokes): **~$3** total. Not in `usage.csv` except the one
  Appendix_2 production-smoke row (harness calls bypass the cost logger).

---
## STATUS LOG
- [x] Read & internalize prior chat history (SOLANUS_V2_LABELING_REVIEW.txt, 6939 lines)
- [x] review_diff + per-page ranking of Appendix_3; visual before/after overlays reviewed (9 pages)
- [x] A/B harness: model × few-shot-strategy on the 7 hard pages — full 2×3 matrix above
- [x] Implement page-type-aware few-shot (classifier + routing + safe fallback + cache); validated
      (notebook 22/22; routing 3→12 demos; flash-lite −62%); ON by default
- [x] Adopt gemini-3.5-flash as default (−77%); pro tested and rejected (worse + pricier)
- [x] Fine-tuning: researched (Vertex SFT, image input, ≥100 ex) + built exporter (95 examples ready)
- [x] Docs updated (this file, LABEL_REVIEW Iter 7, PLAYBOOK §3.13); checkpoints 11 & 12
- [x] Re-ran ALL 43 Appendix_3 pages with the new defaults → −47% hard errors full-volume, −70% over-
      production (fair spatial diff above); new overlays rendered; visually concentric on the dense pages.
      Your `reviewed/` gold was NOT touched; old flash-lite baseline preserved in rerun_compare/.
- [ ] YOUR CALL when back: (a) eyeball a few new overlays in `rerun_compare/overlays/Appendix_3/` vs
      `..._OLD_flashlite/`; (b) run the next real volume (Appendix_2 is mixed → biggest page-type payoff);
      (c) decide cost posture (3.5-flash default vs flash-lite+page-type). Nothing committed to git yet.
