# The Archival Document Labeling Playbook

_Distilled from labeling the Solanus Casey corpus (typed letters, Mass cards, handwritten
notebooks — ~1,400 pages, 7 volumes). Written 2026-06-03 as a reusable template: VLM-assisted
region labeling → QA → human review → clean role-typed regions ready for extraction, training,
and RAG — for **any** document corpus. The hard part of every ingestion pipeline is here.
This is a **living document**: every new issue gets logged in §3 as we hit it._

Companion docs: **`MASTER_GUIDE.md`** (the deep, research-grounded, generalized data-flywheel guide —
read it for the big picture: the 3-gear maturity model, measurement done right, the fine-tune/distill
loop, cost engineering, reproducibility, streaming generalization), `LABEL_REVIEW.md` (this corpus's
iteration log + acceptable-range table), `DEEP_REVIEW.md` / `RESEARCH_AND_PLAN.md` (the measured A/B
evidence + the error-reduction plan), `rerun_compare/workflow_result.json` (audit + research dossiers).

---

## 1. The pipeline shape

```
hand-label seed set (~50–75 pages, all visual styles)
        │
        ▼
┌─ AUTOMATED PASSES (cheap, repeatable, each shrinks the issue list) ──────────┐
│  pass 1  VLM few-shot labeling (count docs FIRST, then regions)              │
│  pass 1b snap-to-ink (grow-only geometric tightening)                        │
│  pass 1c coverage backstop: re-detect uncovered ink on the model's OWN       │
│          output → re-prompt only those regions → EXTEND same-category        │
│          neighbour or ADD new box (category always from the model)           │
│  pass 2  connections/relations (page-type-driven, allowed-pair whitelist)    │
└──────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
QA harness (read-only): coverage (uncovered ink), cross-role overlap, schema sanity
        │
        ▼
HUMAN REVIEW (always; the goal is to make it FAST, not to skip it):
editor with a precise flag queue → one-click Extend / Add / Dismiss(persisted),
zoom, doc Merge ▲ / Split ▼ — saves to a SEPARATE dir so reruns never clobber gold
        │
        ▼
LEARN FROM THE EDITS (review_diff): mine systematic corrections → fix prompts/data
→ promote corrected pages into the few-shot pool → repeat on the next volume
```

**Portable machinery** (reuse as-is): the two-pass engine, snap, coverage backstop,
QA harness, the editor + queue, review_diff learn-loop, few-shot selection with
multi-doc quota + per-page holdout, the held-out reproduction test.
**Corpus facts** (redefine per corpus): the category ontology, allowed edge-pairs,
page-type carve-outs (letter vs notebook vs card), the seed gold set, doc-count rules.

## 2. The quality bar (decide this FIRST, in writing)

Ours: **every piece of content is covered by a box, and every box has the correct
category.** Explicitly NOT the bar: box padding/tightness, over/under-segmentation of
same-category regions, whitespace or clipped-character overlap. Writing this down early
prevents weeks of chasing cosmetic diffs — most human "corrections" turn out to be
padding taste (75 of 106 edits on our first reviewed volume were padding, not errors).

## 3. The issue log — every issue, root cause, fix, and the general rule

### 3.1 Few-shot leakage: the page was its own example  ⚠ check this FIRST
- **Symptom:** suspiciously good pages; metrics partly fake. 9/52 pages of our test volume
  were in the few-shot pool and saw their own answer.
- **Root cause:** the selector filtered examples by *volume* but never excluded the *page*
  being labeled.
- **Fix:** pass `target_page` into selection and hold the exact page out (both passes).
- **Rule:** *any* time training/few-shot data and inference data come from the same pool,
  prove per-item holdout exists before believing any metric. It's one line of code and
  the single most common silent evaluator bug.

### 3.2 Humans overestimate their own labeling consistency
- **Symptom:** model "errors" that wouldn't go away with better prompts; alignment ceiling
  (~91/100) that prompt-tightening couldn't move.
- **Root cause:** ~⅓ of residual divergence was the **gold contradicting itself** —
  the same content type labeled differently across pages (Mass-card greetings, letterhead
  splits, doc-count at in-body timestamps, mottos by position).
- **Fix:** the automated VLM label audit (§5) found 13 verifier-confirmed gold errors,
  tolerance bands for genuinely-ambiguous zones, and an exclude/rehabilitate list.
- **Rule:** **run a VLM consistency audit over the gold BEFORE scaling.** Past the
  alignment plateau, relabel data — do not keep tightening prompts. Budget for it:
  single-annotator corpora are *internally* inconsistent, always.

### 3.3 Contradictory demonstrations poison few-shot
- **Symptom:** the model "freaks out" — flips between readings that the examples themselves
  disagree on.
- **Fix/rule:** maintain an **exclusion gate** (`EXCLUDED_EXAMPLES`) for internally
  inconsistent pages: **prune** them from the few-shot pool, **correct** them in the eval
  set (different treatments!). Shrink the gate as gold gets fixed (we went 13→9 after
  relabeling). Exemplar label correctness materially changes structured-extraction output —
  the "labels don't matter for in-context learning" folklore does NOT transfer here.

### 3.4 Your QA detector has its own thresholds — and they hide misses
- **Symptom:** `uncovered_ink = 0` while a human spots an unboxed `etc.` in seconds.
- **Root cause:** detector minimum-size floors (height ≥1.2% of page, ink ≥1e-4 of page)
  were set to suppress specks but silently dropped *short words*. Downstream the coverage
  backstop only fixes what QA flags — so the blind spot propagated.
- **Fix:** calibrate the floors against a REAL missed short word (ours: 62px tall,
  ~2.5k ink px) so they sit just above speck/punctuation noise. Re-measuring surfaced 5
  hidden misses across the volume.
- **Rule:** every automated check is itself a model with thresholds. **Calibrate floors on
  a real minimal miss, not intuition, and periodically eyeball a "clean" page** — a page
  with zero flags is a test of the *detector*, not just the labels.

### 3.5 Uncovered ink is TWO different problems — recognize which
- **Measurement:** of 16 real coverage misses, **14 were "box too short/narrow"** (the ink
  abuts an existing box — fix = EXTEND it) and only 2 were genuine new regions (fix = NEW box).
- **Trap:** geometry-only "extend the nearest box" failed 7/16 on validation — it inherited a
  wrong-category neighbour (six missed greeting lines got date/commentary/content labels).
- **Fix (category-safe rule):** **the model re-prompt supplies the category** (judged fresh,
  full-page context); **geometry only decides topology** — merge into an abutting
  *same-category* box, else add a new box; adjacent same-category additions coalesce.
  Re-validated 16/16 correct labels; a wrong extend mislabels silently, a wrong add only
  fragments (recoverable), so bias accordingly.
- **Rule:** **never let geometry assign semantics.** Geometry chooses shape; a model (or
  human) chooses meaning.

### 3.6 Most overlap is harmless — flag only what corrupts a crop
- **Measurement:** of 30 raw geometric overlaps (>50% of smaller box): 19 were thin-band
  partial-character clips, 5 were same-role duplication, only **6** put a *full word of a
  different role* inside the box (content box swallowing the farewell/signature/date).
- **Fix:** the overlap check now measures **ink in the shared region** and flags only
  cross-role (different category/document) regions holding ≥½ a text line. 30→6 flags.
- **Rule:** define overlap severity by **downstream harm** (would cropping this box capture
  a full foreign word?) not geometry. Partial characters are cheap to handle at extraction
  time with one prompt line: *"if you notice a half-finished word or stray mark that is not
  part of the intended text, weed it out."* Same-role containment is fine if reasonable.
- **Anti-lesson:** we prototyped an automatic overlap-trimmer (snap boxes to the blank strip
  between blocks, with an undo-if-it-uncovers-ink safety net). Provably safe but resolved
  only 1/6 — the rest were genuinely ambiguous. **Dropped it**: when an auto-fix can only
  decline-or-guess, precise flags + a fast editor beat pipeline complexity.

### 3.7 Document-segmentation conventions DRIFT during review
- **Symptom:** mid-project the curator decides front/back of one card = ONE document —
  contradicting earlier gold (and a few-shot example teaching the old convention).
- **Fix:** cheap structural editor ops — **Merge ▲** (fold doc N into N−1, renumber later
  docs, remap connection `{doc,…}` refs) and **Split ▼** (duplicate doc N's boxes into a new
  doc N+1 with fresh ids/no edges, then delete from each side to partition). Adjacent-only
  keeps the logic trivial.
- **Rule:** conventions WILL evolve once a human sees real model output at volume. Make
  convention changes cheap to apply, and **sync all three places**: reviewed gold, the
  few-shot copies, and (only if the model keeps misbehaving) a one-line prompt tie-break.
  Don't add prompt rules pre-emptively — demonstrations beat rules.

### 3.8 Prompt-tightening overcorrects
- Two medium-confidence prompt "fixes" inverted behavior elsewhere and had to be reverted.
- **Rule:** only high-confidence, high-frequency prompt changes; ONE batch at a time;
  re-verify after each batch (held-out reproduction test); resolve contradictions with a
  single tie-break **default**, never by pasting tolerance ranges into the prompt
  ("either is fine" raises output variance — tolerance belongs in *scoring*, §6).

### 3.9 Reproducibility gotchas (cost us real debugging time)
- **Few-shot selection is RNG-stateful**: same seed but a `--start N` partial run gives page
  N a different example set than a full run → different output. Compare full-run to full-run.
- **Box ids are fresh UUIDs every run** → you can never id-diff two runs; match spatially
  (IoU), and treat "resize" counts under spatial matching as noise.
- **temperature=0 ≠ deterministic** across pool changes: adding one example reshuffles every
  page's selection.
- **Uploaded few-shot file caches expire** (403) → graceful inline fallback hides it; watch
  for the cost/latency drift and re-upload before big runs.
- **A/B discipline:** one change per run, same seed, full volume, and an on-disk checkpoint
  (`checkpoints/NN_desc/` + git hash) before every mutation of code, prompts, or labels.

### 3.10 Pilot-scale resource habits detonate at production scale
- **Symptom (caught in review, not in production):** the page-render cache stored every
  full-resolution render (~110 MB each), unbounded. The 52-page pilot volume quietly held
  ~5 GB and "worked"; the 373-page volume would have needed ~40 GB → OOM mid-run.
- **Fix:** cache only what is REUSED (the small few-shot example images, fetched on every
  page); things used once per item (full-res renders) are recomputed, not cached.
- **Rule:** before scaling 5–10×, audit everything that grows with item count (caches,
  in-memory accumulators, log lists, temp dirs). "It worked on the pilot" is a statement
  about the pilot's size, not the design. Same applies when adding concurrency: N workers
  × per-item memory must fit too.
- **Second instance, same week (step_1 PDF cleaner — the user FELT this one):** rendered the
  ENTIRE volume into memory (`convert_from_path` with no page range; ~437 MB/page at 300 DPI)
  *and* accumulated every cleaned page until one `save(append_images=…)` at the end — a 273-page
  volume needed >100 GB and swap-thrashed. Fix: stream in bounded chunks (render chunk → clean
  chunk → write chunk-PDF → free), then stitch the compressed chunk-PDFs on disk (`pdfunite`,
  already a poppler dependency — no new packages). 52 pages: peak RSS **363 MB**, 4.6 s.
  The pattern to grep for in ANY ingestion script: `convert/load(<whole file>)` + `results.append(…)`
  + single save at the end.
- **Bonus find while testing:** the cleaner's source-folder path pointed one directory too
  shallow, so with the current layout it found **0 PDFs and exited "successfully."** Rule:
  an empty work list is almost never success — log it loudly (or fail) instead of no-opping.
- **The user's actual crash signature, decoded (measured, not theorized):** multi-volume runs
  crashed *late*; one-volume-per-process runs never did. Reproduced: in one long-lived process the
  old code's RSS **ratchets and never returns to baseline between volumes** (measured at 150 DPI:
  54→375→891→1,451→2,263 MB across 7 volumes — allocator/fragmentation retention). The crash math
  only closes at the configured 300 DPI, where pages are 4× bigger: one volume's working set is
  ~13–16 GB (fits 30 GB alone ✓) but the *n-th* volume rides on top of the retained ratchet →
  late-run OOM ✗. **Rules:** (1) per-item memory freed logically ≠ memory returned to the OS —
  long batch processes ratchet; (2) "crashes only late / only in combination" is the ratchet's
  fingerprint — bisecting to one-item-per-process is both the diagnostic and the workaround;
  (3) the streamed rewrite caps RSS by *chunk* size (measured flat ~500 MB across all 7 volumes),
  making memory independent of item size, item count, and DPI.

### 3.11 The EDITOR's category list is a coupled schema spot too
- **Found in final audit:** the editor carried 22 categories, the labeler's schema 20
  (`src_insertion`, `src_margin_note` existed only in the editor). Editor saves stamp empty
  lists for every editor category into the JSON, and promotion copied those schema-foreign
  keys into the few-shot pool — so demonstrations contained fields the model's response
  schema cannot emit, and worse, the editor would happily accept a real box in a category
  the entire downstream pipeline can't represent.
- **Fix:** stripped the (verified-empty) foreign keys from the few-shot files; the 22-vs-20
  decision (adopt the two categories everywhere, or drop them from the editor) is the
  curator's ontology call and is flagged, not made unilaterally.
- **Rule:** the "spots that must agree" on the ontology include every TOOL with its own
  category list — model schema, prompts, descriptions, manifest, **and the human editor**.
  Diff them mechanically (set comparison) as a pre-flight check; never let an annotation UI
  offer a class the pipeline can't carry.

### 3.12 "No flags" is not "correct" — the QA blind-spot taxonomy
Geometric QA sees coverage + overlap + schema. It can NEVER see: wrong category on a
well-drawn box, granularity convention violations (5 paragraph boxes where the scheme says
1 body box), doc-count judgment calls, or connection semantics. Those need (a) the VLM
audit (§5), (b) the held-out reproduction test, (c) human eyes. Maintain the list of what
each instrument can and cannot detect, and never report a number an instrument can't support.

### 3.13 Route few-shot by PAGE TYPE, not by volume — and spend the model lever where geometry is hard
- **Symptom:** on a mixed volume, the dense multi-entry pages (notebook/ledger) cost almost all of the
  human-review hour — over-segmentation + per-entry geometry — while letters/cards were near-clean.
  `review_diff` read as ADD/REMOVE-heavy, but the rendered overlays showed model boxes broadly
  **concentric** with gold: the errors were segmentation *granularity*, the §2 non-bar — just enough of
  it, on the dense pages, to be an hour of clicking.
- **Root cause (two compounding):** (1) few-shot was selected by **volume**; a mixed volume's dense
  page drew mostly *letter* demos (measured: **3/12** same-type) — and (2) a `min_multi_doc` quota
  spent slots on multi-doc letters/cards even though notebook pages are single-document, starving the
  page that most needed same-type demonstrations. (3) The base model was the cheapest tier, and dense
  geometry on faint scans is exactly where small VLMs are weakest.
- **Fix (both levers, measured by a one-change-per-cell A/B vs gold on the hardest pages):**
  - **Page-type routing** — type each page (examples offline from their gold structure; the target by
    one cheap cached VLM call), then bias few-shot to the same TYPE across ALL volumes and relax the
    multi-doc quota for single-doc types. Same-type demos went 3/12 → **12/12**; cheap-model hard
    errors **−62%**. Make it **regression-safe**: only STRONG types route; an unsure/"other" type falls
    back to the previous (same-volume) selection, so a misclassification can never regress a page.
  - **Model capability** — the same A/B showed a stronger model cut hard errors **−77%** (and fixed the
    doc-count judgment calls). The two levers are **substitutes**: a capable model already knows the
    granularity (routing ~neutral for it); a cheap model is rescued by the demos. Newer-smaller beat
    older-bigger (a 3.x-flash edged a 3.x-pro) — **always A/B the actual models, don't assume the
    biggest wins.** For a corpus labeled ONCE, the per-page price delta is noise against review hours.
- **Rules:** (a) when one corpus mixes visual styles, *page type is the unit that should drive few-shot
  selection and any per-type quotas* — volume is a proxy that breaks on mixed volumes. (b) A coverage/
  granularity gap that won't close by prompting is often a *demonstration* gap (wrong-type examples) or a
  *capability* gap (model too small) — diagnose with a clean one-lever-at-a-time A/B that scores **hard
  errors only** (exclude cosmetic geometry) against gold, summed over the *hardest* pages. (c) Bias any
  classifier-driven routing so its confident cases (here: notebook 22/22) carry the win and its unsure
  cases fall back to the prior safe behavior.

### 3.14 Area-IoU vs a single human's gold is the WRONG yardstick — and it lied to us
- **Symptom:** a "hard error" count (recat+add+remove via IoU≥0.5 vs gold) that swung wildly and
  over-stated real errors ~1.8×. A visually-perfect notebook page scored "6 errors."
- **Root cause:** those 6 were 3 tiny ditto-mark date boxes whose slight shift dropped them below
  IoU 0.5, so each was double-counted as remove+add. Area-IoU-vs-one-annotator conflates FOUR things:
  real errors, granularity choices, **box-jitter matching artifacts**, and gold noise. Sweeping the IoU
  threshold collapsed "113 hard" → ~62 (the ~45% that was artifact melted away); the model-improvement
  conclusion survived every threshold (good), but the absolute number was not trustworthy.
- **Fix:** (1) match boxes by **ink/token-IoU** (shared ink), not bbox-area IoU — documents are Manhattan
  layouts, so containment-of-ink is the right notion and it is robust to boundary jitter. (2) Report
  **Panoptic Quality = Recognition × Segmentation** so "did you find the region" and "is it tight" never
  collapse into one figure. (3) Calibrate against the **inter-annotator ceiling** (DocLayNet: models
  trail human agreement ~10%), not 100%. (4) Lead with **gold-free** signals (ink coverage, committee
  disagreement, VLM-judge) and audit the gold itself for label errors (Confident Learning / cleanlab).
- **Rule:** **fix the measurement before chasing the model.** A metric that can't separate a jitter
  artifact from a real miss will send you chasing ghosts (and make any "improvement" unfalsifiable).
  Full treatment in `MASTER_GUIDE.md` §3.

## 4. Pre-flight checklist — new corpus or next volume

1. **Ontology** (new corpus only): pick region classes on DocLayNet's axes — distinct,
   recognizable from a single page, coverage-complete; ~20 classes max; write the
   confusable-pair disambiguations into the category descriptions from day one.
2. **Seed gold**: hand-label 50–75 pages spanning every visual style; include several
   multi-document and relation-carrying pages; mine allowed edge-pairs from it.
3. **Audit the seed gold with the VLM flow (§5)** — fix/exclude before it teaches the model.
4. **Prove per-item holdout** in few-shot selection (§3.1).
5. **Calibrate QA floors** on a real minimal miss (§3.4).
6. Re-upload few-shot media if cached uploads expire.
7. Run a 5-page dry run; eyeball one "clean" page deliberately (§3.10).
8. Full run → QA → editor sweep → `review_diff` → promote corrected pages → log new issues here.

## 5. The automated VLM label audit (find inconsistencies BEFORE they bite)

The single highest-leverage step, because labels carry no text — you must *see* them:
1. **Render category-annotated overlays** for every gold page (box + category abbreviation
   on the image) + a manifest (page → categories present, doc count, edge count).
2. **Fan out one reviewer agent per confusable axis** (date-vs-date, name-vs-address,
   motto-by-position, recipient-vs-greeting, catch-all use, doc segmentation, relations…).
   Each reads the intended rules + views every relevant page, and reports: the canonical
   rule actually followed, every page-cited inconsistency (classified: data-relabel /
   prompt-gap / inherently-ambiguous), and a proposed tolerance band.
3. **Adversarially verify**: an independent skeptic re-views every claim (default: refute)
   and flags what the reviewer missed. ~Half of plausible findings die here — keep only
   confirmed ones. (Ours: 19 relabels claimed → 13 confirmed high-confidence.)
4. **Synthesize**: relabel list (apply high-confidence by stable box-id, dry-run first),
   exclude list (gate from few-shot), rehabilitate list (re-admit after the fix lands),
   tolerance bands (§6), prompt-gaps (encode as tie-break defaults only).
5. **Resolution flow:** apply high-confidence → re-verify → hold medium for a human look →
   leave refuted alone → rehabilitate single-defect pages → re-run the held-out test to
   confirm the ceiling moved for the right reason (less noise, not overfit).

## 6. Acceptable bounds (so nobody "freaks out" over ambiguity)

For each genuinely ambiguous zone, write three things: the **canonical rule**, the
**tolerance band** (which alternative readings are defensible — used by *scoring*, so the
model is never penalized for a valid reading), and a **tie-break default** (which goes in
the prompt, so output stays consistent). Example from this corpus: an opening doxology
above its own dateline may be greeting OR other (band), default greeting when no dateline
intervenes (tie-break). Edge *presence* by page type, by contrast, had ZERO defensible
variation — bands are earned by evidence, not granted by default.

## 7. Designing the human pass to be fast (it always exists)

Precision beats recall in the queue: every flag shown must be worth a human's glance
(we cut 30 overlap flags to the 6 real ones — that's the difference between a queue that
gets used and one that gets ignored). One-click application of the *suggested* fix
(Extend grows the right box; Add creates one with a sensible category), auto-zoom to the
flag, ghost preview before committing, **persisted** dismissals, structural ops (Merge ▲ /
Split ▼), keyboard everything. And the iron rule: the editor reads model output but
**saves to a separate directory** — a rerun must never be able to destroy human work.

## 8. Toward extraction / training / RAG (why these labels are shaped this way)

- **Geometry-only labels** (no text field) make every "missing text" issue detectable from
  ink alone — coverage is provable without ground truth, which is what lets the backstop
  and QA run unsupervised on new volumes.
- **Crop-per-box extraction** then pairs each region's image with its role; the extraction
  prompt carries the half-word weed-out line (§3.6) and inherits clean role types
  (content vs date vs signature vs archivist-note) — exactly the metadata RAG chunking and
  fine-tuning need (provenance, addressee, date, document boundaries for chunk boundaries).
- **Relations** (date↔entry, page-marker↔block) give you reading order and entry-level
  chunking in notebooks/ledgers for free.
- The same loop (seed gold → audit → few-shot → backstopped passes → fast human sweep →
  learn from edits) is the template for any corpus: letters, ledgers, forms, registers,
  marginalia — only the ontology and the carve-outs change.

---
_Append new issues to §3 with: symptom → root cause → fix → general rule._

### 3.15 Box-vote self-consistency fails under boundary variance (measured negative result)
- **Tried:** 3-sample cheap-model ensemble (diverse few-shot draws), same-category IoU≥0.5
  clustering, keep ≥2/3-vote clusters, average coords — the literature's recipe for "small model
  + self-consistency rivals big model."
- **Result:** PQ collapsed 0.538 → 0.272; FN doubled. The cheap model's instability on dense pages
  is segmentation *granularity* (where boundaries fall), so the same content rarely aligns across
  samples at the vote threshold and gets dropped — the fusion deletes exactly the hard content.
- **Rule:** before reaching for ensembles, identify WHAT varies across samples. Presence-variance →
  voting helps. Boundary/granularity-variance → voting destroys recall; spend the lever on a more
  capable model or same-type demonstrations instead. Publish negative results in the log — a
  measured "no" prevents the next person from re-spending the same tokens.
