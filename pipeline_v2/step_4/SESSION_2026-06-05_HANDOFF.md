# Session Handoff — 2026-06-05 (Desktop machine) — A3 renumbering, metric upgrade, GATE-1 design

_Written for pulling onto the other machine. Read top-to-bottom; the **"ON THE OTHER MACHINE"**
section is the one action item that can only be done there. Companion docs: `RESUME_HERE.md`
(running state), `PIPELINE_FLOW.md` (file-by-file map of step_4, written today)._

---

## TL;DR

- **Gold corpus: 98 pages** (Appendix_1 52, Appendix_3 46 — A3 now includes the 3 front-matter
  pages, labeled today and human-approved).
- **Appendix_3 was renumbered +2 on the Desktop machine** to match its 46-page PDF set (title page,
  ToC, back title included — David wants ALL pages kept). Every page-numbered artifact shifted.
- **Known gap:** Desktop's A3 PDFs are an older crop vintage with the archivists' top-margin pencil
  annotations whited out → ~96 gold boxes (archv_date/struct_id/archv_commentary…) sit on blank
  paper there. **The OTHER machine has the annotation-bearing PDFs.** Sync recipe below. Test B7
  stays deliberately red on Desktop as the canary until synced.
- **Test suite: 32 PASS / 1 FAIL (B7 = the canary, documented)** — including all live-API
  integration tests with the renumbered few-shot pool.
- **`panoptic_eval.py` upgraded:** per-side coordinate scaling (kills the cross-machine bug class),
  new **PQ_strict** (match must agree on category), and `--csv/--arm/--notes` appending to
  **`experiments/gate1_results.csv`** (baselines recorded).
- **`tuning_data/` regenerated:** new numbering, corrected [0,1000] targets, 98 examples (89/9).
- **Next human action: review Appendix_2** (76 pages, triage worklist ready) → ~174 gold →
  run the GATE-1 experiment matrix (4 arms, defined below).
- Vertex verified repeatedly: **0 endpoints, 0 models — nothing billing.**

---

## What happened this session (chronological)

1. **Machine pickup + deep review.** Resumed from `RESUME_HERE.md` / `NEW_MACHINE_HANDOFF.md`.
   venv/.env already present; suite ran 29/31 with **B7 failing** (gold-vs-gold panoptic should be
   perfect; got RQ 0.701, 230 unmatched boxes).
2. **Root-caused B7 → page misnumbering.** Desktop's `polygon_cropped_pdfs/Appendix_3/pages/` has
   **46 pages**; the labels were made against a **43-page set = local pages 003–045**. Proven by
   exact render-dimension matching for all 43 (unique, in sequence) + a pixel-tight overlay of gold
   boxes on the shifted page. The 3 extras: title page (001), table of contents (002), back title (046).
3. **David's call: keep all 46 pages.** So instead of renaming PDFs down to 43, ALL label artifacts
   were shifted **+2**: `reviewed/`, `auto_labeled/`, `labeled_examples/` (15 pool entries),
   `shadow_labels/`, `file_uris.json` keys, `qa_output/Appendix_3/dismissed.json` keys,
   `EXCLUDED_EXAMPLES` in auto_labeler.py (page_019→page_021), 5 hardcoded test refs.
   `page_type_cache/Appendix_3` quarantined as `.stale-pre-renumber` (regenerates for pennies).
4. **3 front-matter pages auto-labeled** (3.5-flash + page-type; 43 skipped by skip-logic), QA-flagged
   clean, **David reviewed and approved them in the editor** → gold 46/46.
5. **Residual B7 failure diagnosed as a SECOND issue:** 96 remaining unmatched boxes are almost all
   archival-annotation categories; full-res crop of one region = pure white (extrema 255,255).
   Desktop's A3 PDFs (pages AND volume PDF) are an **older crop vintage with the top-margin pencil
   annotations whited out**. Appendix_1 verifies clean; the other machine's B7 passed → its A3 PDFs
   contain that ink. Upstream `step_2/polygon_page_data` is stale in git → can't regenerate locally.
6. **Committed + pushed** (`641aee2`): the migration, the 3 new gold pages (per-page PDFs + JSONs),
   `PIPELINE_FLOW.md` (new doc: the concrete file-by-file flow of step_4), `.gitignore`
   (`.env`, `venv/`, `__pycache__/`, `polygon_cropped_pdfs/`, the zip), removal of 2 raw
   chat-transcript dumps (David-approved). On-disk checkpoint `checkpoints/20_a3-renumbered/`.
7. **Autonomous block (~1 h, David away), committed + pushed (`f796f9d`):**
   - Full **API** test suite: 32 PASS / 1 FAIL (B7 canary only). C1 (page-type), C2 (full
     process_page), C3 (tuning export) all green with the renumbered pool.
   - `panoptic_eval.py`: each side now scales by its **own stored page_width** (never assumes the
     live render matches the JSON's pixel space — the exact bug class behind the misalignment;
     gold-vs-gold A1 verifies exactly 1.000). Added **PQ_strict** + CSV row appending.
   - `experiments/gate1_results.csv` started with auto-vs-gold baselines (see numbers below).
   - `tuning_data/` regenerated in new numbering with corrected targets (98 examples).

Also cleaned up earlier in the session: cleanup REPORT delivered (nothing deleted except the two
chat-transcript dumps David approved); `RESUME_HERE.md` updated with both session notes.

---

## ⭐ ON THE OTHER MACHINE (after `git pull`)

Its `polygon_cropped_pdfs/Appendix_3/pages/` holds the **43 annotation-bearing PDFs in OLD
numbering**. Convert it to the canonical 46-page set (descending rename to avoid collisions, then
restore the 3 front-matter PDFs, which ARE in git under `auto_labeled/`):

```bash
cd pipeline_v2/step_4/polygon_cropped_pdfs/Appendix_3/pages
for n in $(seq 43 -1 1); do
  mv "page_$(printf '%03d' $n).pdf" "page_$(printf '%03d' $((n+2))).pdf"
done
cd ../../..   # back to step_4
cp auto_labeled/Appendix_3/page_001/page_001.pdf polygon_cropped_pdfs/Appendix_3/pages/page_001.pdf
cp auto_labeled/Appendix_3/page_002/page_002.pdf polygon_cropped_pdfs/Appendix_3/pages/page_002.pdf
cp auto_labeled/Appendix_3/page_046/page_046.pdf polygon_cropped_pdfs/Appendix_3/pages/page_046.pdf
./venv/bin/python run_all_tests.py --no-api   # B7 should now PASS there (expect 30/30 offline)
```

Then **copy that machine's now-46-page A3 `pages/` dir (and ideally its volume `Appendix_3.pdf`)
back to the Desktop** (drive/scp — PDFs don't travel via git). That clears B7 on Desktop → 33/33.

⚠️ Until Desktop has the synced PDFs: do NOT run `upload_examples.py` from Desktop (it would bake
annotation-less renders into the few-shot demo images). Current uploads expire 2026-06-06 ~12:20.

---

## Where we stand on performance (plain English)

| What | PQ (0–1) | Meaning |
|---|---|---|
| Current production (few-shot 3.5-flash + page-type) on held-out | **0.940** | finds nearly everything, tight boxes |
| Historical auto-labels vs gold, Appendix_1 | 0.786 / **0.695 strict** | the old flash-lite era; categories hurt |
| Historical auto-labels vs gold, Appendix_3 | 0.831 / 0.810 strict | depressed by the archv gap on Desktop |
| The one fine-tune attempt (95 gold pages) | **0.159** | failed — not enough data; format DID bake in |

PQ = Panoptic Quality = (did you find each region) × (how tight on the ink). **PQ_strict** (new
today) additionally requires the category to match — A1's drop 0.786→0.695 shows category
confusions were invisible in the headline number. The metric to trust for big gaps, not ±0.02;
the REAL metric is minutes-of-David's-review per page (Appendix_2 gives the next data point).

Model ladder reminder: **flash-lite** (smallest, original labeler) → **flash** (current default;
−77% hard errors vs lite; ~$4/volume vs ~$0.66) → **pro** (tested, REJECTED: slightly worse than
3.5-flash and pricier).

---

## GATE-1 experiment design (agreed today, runs after Appendix_2 review)

David's two pushes, both incorporated:

1. *"Use flash inference to train a better flash-lite (or a trained flash)"* → that IS the current
   plan (gold = flash output + human corrections = the best distillation data). Raw-flash
   distillation (no human check) is a legit **booster arm** if pure gold isn't enough — train on
   gold + a few hundred raw flash labels (optionally agreement-filtered). Tuned **flash** (not just
   flash-lite) is also on the menu: even mid-tier weights with baked-in conventions drop the 12
   demo images and the ~50k-token prompt.
2. *"Can we really drop the few-shot? Training bakes in conventions but may lose specifics"* →
   correct instinct, and there's a counter-subtlety: the tuned model trains on BARE prompts, so
   12 demos at inference are out-of-distribution for it and could even hurt. Empirical question →
   it's the matrix:

| Arm | Weights | Demos | Answers |
|---|---|---|---|
| base + few-shot | vanilla | 12 | today's baseline (0.940 to beat) |
| base, bare | vanilla | 0 | floor — how much do demos actually do? |
| tuned, bare | trained | 0 | did training bake in enough? |
| tuned + few-shot | trained | 12 | do specifics stack — or interfere? |

(+ optional half-step: tuned + 4 demos; + optional distillation-boosted training set.)

All arms scored on the same held-out gold via `panoptic_eval.py` (standard + strict), every row
appended to `experiments/gate1_results.csv`. Sequence: `finetune.py prepare` → `tune --epochs 8
--lr-mult 5 --adapter 8` → `tuned_eval` per arm → **`teardown` (NON-NEGOTIABLE: endpoints bill
hourly; verify 0 deployed)**. Decision: switch production to any tuned arm that beats 0.940 at
acceptable cost; otherwise keep prompting, grow gold, retry at ~300–500 pages.

---

## Condensed transcript — the closing discussion (for continuity)

**David:** what is this A3 B7 terminology?
**Claude:** A3 = Appendix_3. B7 = test ID in `run_all_tests.py` (section B check 7): "evaluate A3
gold against itself; must be perfect." Comparing a thing to itself only fails if something
underneath is broken — it caught both the +2 misnumbering (fixed) and the missing-annotations crop
vintage (pending PDF sync). It stays red deliberately until the sync.

**David:** ok so put this all in english — where do we stand?
**Claude:** ~1,400 pages to label; AI draws boxes, you correct in the editor; corrected pages are
the product AND the training fuel. 98 gold pages done. Machinery solid (83–85% right before you
touch a page). Fixed today: your two computers had drifted — this machine's A3 scans had 3 extra
pages so labels were attached to wrong numbers; realigned, labeled the 3, you approved. Loose end:
this machine's A3 scans are an older version with archivists' pencil notes whited out — copy the
A3 PDFs from the laptop. Next: you review Appendix_2 (the only thing blocked on a human; also
answers "is reviewing getting faster?" — A3 took a painful hour). Then the big experiment at ~174
gold. If the custom model wins it becomes the labeler; if not, nothing breaks.

**David:** gemini flash is the smallest one right? and didn't we try the next level up and found it
was quite good?
**Claude:** One rung off: flash-LITE is the smallest (original labeler); FLASH is the next level up
— that's the one you remember, −77% hard errors, now the default; PRO (biggest) was tested and
rejected (3.5-flash slightly beat it, cheaper). Page-type routing mostly rescues the SMALL model
(−62% for lite, ~neutral for flash) — which is why the fine-tune targets flash-lite: tuned tiny
model matching prompted flash = top quality at bottom-tier cost.

**David:** so where do we stand performance-wise? Why not do inference with flash and use that to
train a better flash-lite? or even a trained flash?
**Claude:** [table above]. Your idea is distillation — and the current plan already IS its best
version: gold = flash inference + your corrections, strictly better training data than raw flash
output (which would bake the teacher's ~6–15% errors into the student). Raw distillation still has
a place as a data booster (label all 1,400 pages for ~$130, filter to high-confidence, mix with
gold). And yes, tuning flash itself (not just lite) is on the menu — same prompt-cost win even if
the tier stays mid. Both became experiment arms.

**David:** but can we really drop the few-shot? my impression was training bakes them in but loses
the specifics… curious to compare training + few shot vs training vs few shot vs none.
**Claude:** Right to be skeptical — that's the hypothesis to TEST. Evidence so far: the failed tune
DID bake in the exact output schema at 95 pages (format: settled). Uncertain: whether demos carry
page-specific anchoring weights can't. Counter-subtlety: the tuned model trains on bare prompts, so
demos at inference are out-of-distribution and could even interfere. Hence exactly your 4 arms
(base±demos × tuned±demos) — "base, bare" is a nice floor that prices what the demos are worth at
all. If close, add tuned+4-demos. All to the CSV; fires after your Appendix_2 review.

---

## Next steps (priority order)

1. **Other machine:** run the rename recipe above; sync the annotation-bearing 46-page A3 set back
   to Desktop. (Clears B7 → 33/33.)
2. **David: review Appendix_2** — `EDITOR_DOCUMENT=Appendix_2 ./venv/bin/python normalized_editor.py`,
   worklist `qa_output/Appendix_2/triage.txt` top-down. Success measure: ≪ the A3 "painful hour."
3. **GATE-1 matrix** (agent-runnable, ~$1–5, teardown enforced) → CSV rows → switch decision.
4. Promote corrected A2 pages into the few-shot pool (mass_card thinnest at 3 in tuning set).
5. Scale to Volume_1/3/4 with whichever labeler wins.
