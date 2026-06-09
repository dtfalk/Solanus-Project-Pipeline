# HITL_BOOTSTRAP.md — the per-volume human-in-the-loop bootstrap protocol

_2026-06-09. Written in response to: "build a flow that reads a document, picks representative
pages, labels a few of each type, the user corrects them, and the rest are labeled with those
corrected, gold-standard, maximally relevant examples." Companion to `PIPELINE_FLOW.md` (the
machinery map), `RESEARCH_AND_PLAN.md` (the clustering verdict this builds on), and
`PLAYBOOK.md` (the generic template). Lives in **pipeline_v3** — pipeline_v2 stays canonical
for data until cutover._

---

## 0. The one-line answer

**Label ~12 maximally-diverse representative pages first, hand-correct only those, promote them
into the few-shot pool (plus a one-paragraph volume convention note), then label the remaining
~360 pages against your own corrections** — so the expensive human pass reviews a model that has
already seen this volume's conventions, instead of discovering them page by page.

## 1. Why this exact flow — the evidence already in this repo

This is not a new architecture; it is the **automation of what already worked on Volume_4**
(commit `8a9f3df6`): David corrected pages 1–5, they were promoted + pinned, a per-volume
convention note (`VOLUME_PROMPT_NOTES["Volume_4"]`) was added, and the remaining 267 pages were
re-labeled against them — QA flagged only 10/272 pages. The protocol below replaces each manual
step with a tool, and replaces "pages 1–5" (arbitrary) with a measured diverse sample.

The supporting evidence, all local:

| Finding | Where | Consequence for this protocol |
|---|---|---|
| "A wrong DEMO beats a right PROMPT" (−85% on A2's worst pages came from fixing demos+prompt together) | LABEL_REVIEW Iter 12 | The highest-leverage artifact is *correct same-volume demos*; get them in front of the model before bulk labeling. |
| Demos' main value is **conventions, not capability** (12 demos buy base only ~+0.04 PQ, but convention errors dominate review time) | GATE-1, RESUME_HERE 2026-06-06 | A small number of *corrected, convention-bearing* demos is the right target — not more demos. |
| More shots HURT (c20 PQ 0.609 < c12 0.639); layout-similarity ranking of the same 12 wins (PQ 0.681) | `experiments/fewshot_ab.py`, commit `d8491382` | Keep 12 demos; spend effort on *which* 12, not how many. |
| Pure-layout clustering: 65–73% purity, can't split letter/mass_card/preamble | RESEARCH_AND_PLAN §1, `rerun_compare/cluster_pages.py` | Layout features are NOT good enough to *route*: but picking representatives is a **coverage** problem, where a muddled cluster only costs one redundant rep. Stratify by the VLM page type (the proven router), diversify by layout within type. |
| QBC triage: top-half disagreement pages held 79% of real errors | `triage.py`, A3 measurement | The *review* phase already has a literature-correct uncertainty signal; the *seed* phase needs the diversity signal. |
| Same-volume + same-type demos already sort to bucket 0 in `select_few_shot` | `auto_labeler.py:712` | Once promoted, this volume's corrected reps dominate every prompt for the rest of the volume **automatically** — pinning is only a backstop for "other"-typed pages. |

## 2. Research grounding (web, 2026-06-09)

- **Cold-start active learning is a two-stage problem**: first a *representative + diverse* seed
  set chosen without model signal, then *uncertainty-guided* expansion once a model exists
  ([cold-start AL for segmentation](https://arxiv.org/html/2601.18532),
  [Cold PAWS](https://arxiv.org/pdf/2305.10071), [low-budget AL](https://arxiv.org/pdf/2201.07200)).
  Our protocol maps 1:1: farthest-point seed → label → human gold → QBC-triage-ordered review.
- **Farthest-point / k-center greedy** is the standard seed selector with a 2× approximation
  guarantee on the max-distance objective
  ([coreset survey](https://arxiv.org/html/2505.17799v1)). We run it per page-type stratum on the
  existing 192-dim ink-grid descriptors (`_layout_descriptor`) — zero new dependencies.
- **ICL demo selection**: the similarity-vs-diversity question is task-dependent; the emerging
  consensus is *both* — affinity to the target plus coverage of the task's modes
  ([survey](https://arxiv.org/pdf/2401.11624), [coverage-based selection](https://arxiv.org/pdf/2305.14907),
  [active-learning view of ICL](https://arxiv.org/pdf/2305.14264)). We already do affinity at
  inference (layout-sim ranking); this protocol adds coverage at *seed-selection* time — the two
  are complementary, not competing.
- **Contrastive ICL** (positive + near-miss negative demos) measurably helps information
  extraction ([C-ICL](https://arxiv.org/pdf/2402.11254),
  [implicit learning from mistakes](https://arxiv.org/html/2502.08550v3),
  [negative-sample few-shot](https://arxiv.org/pdf/2507.23211)) — David's "send the model its own
  diff" instinct has literature support. BUT our local evidence (more shots hurt; demos at
  inference hurt the tuned model) says don't ship it untested → **A/B-gated experiment**, §6.
- **Guideline refinement from corrections** is an active research line — iteratively folding
  reviewer disagreements back into the annotation instructions
  ([Refining & reusing annotation guidelines, ACL 2026](https://arxiv.org/html/2605.20809),
  [human-centered automated annotation](https://arxiv.org/pdf/2409.09467),
  [Label Studio's loop](https://labelstud.io/guide/ml)). That is exactly
  `review_diff.py --draft-note`: corrections → draft convention note → human edits → prompt.

## 3. The protocol — phase by phase

New tools in **bold** (all in this folder). Everything else already existed.

```
PHASE 0  Seed audit (once per volume, BEFORE anything)
         The pool already holds old seed examples for V2 (12) and V3 (10).
         Bucket-0 priority puts them at the TOP of every prompt for their volume —
         a stale-convention seed here is the A2 archivist-catalog failure mode waiting
         to happen. David eyeballs them in the editor/viewer against current conventions;
         remove stale ones from the pool (or fix + keep). Agents do NOT judge conventions
         (David's standing rule) — this is a human pass over ~12 pages.

PHASE 1  Pick representatives                    **pick_representatives.py <Vol> --k 12**
         VLM page-type per page (cached, pennies) → stratify k across the types present
         (largest-remainder, min 1) → within each stratum, farthest-point sampling on the
         192-dim layout descriptors, seeded at the stratum medoid (deterministic, no RNG).
         → qa_output/<Vol>/representatives.{txt,json} + a paste-ready --pages string
         + coverage stats (every page's distance to its nearest rep).

PHASE 2  Label only the representatives          auto_labeler.py --volume <Vol> **--pages 3,17,42,...**
         Model sees the existing cross-volume pool (best available demos).

PHASE 3  Human corrects the representatives      EDITOR_DOCUMENT=<Vol> normalized_editor.py
         ~12 pages ≈ well under an hour. This is THE convention-setting moment for the
         volume — granularity decisions made here propagate to every other page.

PHASE 4  Promote + capture conventions
         **promote_examples.py <Vol> --pages <reps> --upload**
            reviewed/ JSON + polygon_cropped_pdfs/ PDF → labeled_examples/<Vol>/page_NNN/,
            refuses overwrites (--force to override), warns on EXCLUDED_EXAMPLES,
            incrementally uploads ONLY the new pages and merges into file_uris.json
            (no full 400-page re-upload; respects the map's image_width).
         **review_diff.py <Vol> --draft-note**
            corrections on the reps → qa_output/<Vol>/volume_note_draft.txt: a DRAFT
            VOLUME_PROMPT_NOTES entry listing the systematic patterns (top recats, missed
            categories, over-production, resize direction). David rewrites it into the
            actual convention ("one box per person") — the tool surfaces WHAT changed,
            the human supplies WHY. Paste into VOLUME_PROMPT_NOTES[<Vol>] in auto_labeler.py.

PHASE 5  Label the rest                          auto_labeler.py --volume <Vol>
         (already-labeled reps are skipped without --overwrite; bucket-0 + layout-sim put
         the corrected reps at the front of every prompt; --pin-examples '<reps>' optionally
         guarantees them even on "other"-typed pages — recommended, it's what V4 did.)

PHASE 6  Triaged human review                    qa_report.py <Vol> · triage.py <Vol> ·
         EDITOR_DOCUMENT=<Vol> normalized_editor.py
         (worst-first via triage.txt; zero-disagreement pages are spot-check candidates.)

PHASE 7  Measure + close the flywheel            review_diff.py <Vol> · panoptic_eval.py <Vol> --csv
         Success metric vs the V4 baseline: corrections-per-page in review_diff and
         PQ/PQ_strict of auto_labeled vs the volume's finished gold. Then promote the
         best reviewed pages (PHASE 4 tool again) and export_tuning_data.py — at ~1,100
         gold pages after V2+V3, the GATE-1 fine-tune retry becomes live.
```

### Why representatives-first beats sequential-first (what V4 did by accident)

V4's pages 1–5 worked because Volume_4 is homogeneous (one ledger convention throughout). A
mixed volume (the Appendices; possibly V2/V3) violates that: the first 5 pages are front matter
and letters, and the notebook convention wouldn't be set until the human hits page 60 in
PHASE 6 — after 300 pages were already labeled the old way. Farthest-point + type stratification
guarantees every layout/semantic mode of the volume gets a gold convention exemplar **before**
bulk labeling. That is the difference between "review = verify" and "review = relabel".

### Cost (V2, 372 pages, current defaults)

- PHASE 1: ~372 flash-lite type calls (mostly cache misses) ≈ **well under $1**, ~3 min of renders.
- PHASE 2: 12 pages × ~$0.09 ≈ **$1.1**.
- PHASE 5: ~360 pages ≈ **$33**.
- Human: ~1 h (PHASE 3) + the PHASE 6 review that this whole protocol exists to shrink.

## 4. What this changes in the code (v3 vs v2)

| Piece | File | Nature |
|---|---|---|
| Representative picker | `pick_representatives.py` (NEW) | type-stratified FPS over `_layout_descriptor` vectors; deterministic; `--no-api` fallback = single stratum |
| Arbitrary page lists | `auto_labeler.py --pages` | parse "3,17,42-45"; requires `--volume`; composes with --start/--end |
| Audit-gated promotion | `promote_examples.py` (NEW) | reviewed→pool copy + incremental Files-API upload merged into file_uris.json; never deletes, never overwrites silently |
| Convention-note drafting | `review_diff.py --draft-note` | deterministic template over the existing aggregates → qa_output/<Vol>/volume_note_draft.txt |
| Offline tests | `run_all_tests.py` | new checks: FPS determinism+coverage, --pages parsing, promote dry-run safety, draft-note generation |

Explicitly NOT built (decision + rationale):

- **No new clustering router.** RESEARCH_AND_PLAN §1's verdict stands; the VLM classifier routes,
  layout features diversify. DINOv2 stays gated on a measured need.
- **No orchestrator script.** The loop has a human in the middle (PHASE 3) and spans days
  (Files-API TTL is handled by promote's incremental upload). A protocol doc + 4 small tools
  beats a stateful runner — same reasoning as PLAYBOOK's per-volume loop.
- **No auto-promotion.** `reviewed/` → pool stays an explicit human-triggered act (the gold gate).

## 5. Measurement plan (success criteria, falsifiable)

1. **Primary: corrections-per-page** (`review_diff.py <Vol>`, total RECAT+ADD+REMOVE+RESIZE /
   pages reviewed) on V2 with this protocol vs Volume_4's number (compute V4's once its review
   finishes — the in-flight re-run + David's review make it the natural baseline).
2. **Secondary: PQ / PQ_strict** of `auto_labeled` vs final gold (`panoptic_eval.py --csv --arm
   bootstrap-v2`) — append to `experiments/gate1_results.csv` alongside the existing
   A1 0.786 / A3 0.831 / A2-production 0.909 rows.
3. **Tertiary: wall-clock review time** for PHASE 6 (David's subjective report is fine — that is
   the quantity this protocol optimizes).
If V2-with-protocol does not beat V4's corrections-per-page, the added complexity is not paying
and PHASE 1 should be simplified back to "first K pages + pin" (the V4 recipe).

## 6. A/B-gated follow-ups (designed, deliberately not shipped)

1. **Contrastive demo pairs** (David's diff idea, C-ICL-style): for 2–3 reps with the most
   instructive corrections, include in the prompt the model's ORIGINAL attempt labeled
   "INCORRECT (common mistake on this volume)" next to the gold "CORRECT" version, with a
   one-line contrast. Test exactly like d8491382: `experiments/fewshot_ab.py` arms
   `c12_sim` (control) vs `c12_sim+contrast2` on 12 held-out gold pages, judged by
   `panoptic_eval.py`. Ship only on a PQ win — the c20 result and GATE-1's "demos hurt the
   tuned model" both warn that extra prompt mass can subtract.
2. **Semantic sub-typing**: widen `classify_page_type` to also emit a one-line content gist
   (cached alongside the type) and route/diversify on it — pennies, no new deps; A/B the same way.
3. **Tuned-model triage shadow** for PHASE 6 (already supported via `triage.py
   --shadow-tuned-endpoint`) once the next gentle tune lands.

## 7. Provenance

pipeline_v3 was copied from pipeline_v2 on 2026-06-09 while a Volume_4 re-label run was live in
v2 — see `../README_V3.md` for exactly what that means for this copy's data directories.
Gold here matches git `01cbcc54`.

---

## 8. STAGED-FLOW EXTENSION (2026-06-09 evening, desktop — DESKTOP_PROMPT TASK B)

The protocol above (PHASES 0–7) is extended into David's staged flow: *"classify within a doc,
pick sample pages per cluster, label those, human corrects, run a solid chunk, review + correct
the prompt, then the rest."* New tools (all offline-tested, suite F7–F9):

```
STAGE 0  cluster_pages.py <Vol>            geometric page-architecture clusters
         → qa_output/<Vol>/clusters.{json,txt}
         → label_review/contact_sheets/<Vol>/cluster_N.png   ← DAVID EYEBALLS THESE
         Membership = measurable layout/ink features ONLY (192-dim grid + 12
         structural signals, k by silhouette). --vlm-names adds display-only
         names; they are never load-bearing. David confirms/renames/merges
         clusters from the contact sheets BEFORE anything is labeled.
STAGE 1  pick_representatives.py <Vol> --clusters     (k≈12 across clusters, FPS)
         auto_labeler.py --volume <Vol> --pages <reps> → David corrects in editor
STAGE 2  promote_examples.py <Vol> --pages <reps> --upload
         review_diff.py <Vol> --draft-note → David rewrites the note and saves it
         to volume_notes/<Vol>.md  ← NEW: notes are David-editable FILES now
         (file replaces the python constant; optional per-cluster addenda in
         volume_notes/<Vol>.cluster_<N>.md apply only to that cluster's pages)
STAGE 3  pick_chunk.py <Vol> --frac 0.25   cluster-stratified, excludes gold+reps
         auto_labeler.py --volume <Vol> --pages <chunk> --pin-examples <reps>
         → David reviews a SAMPLE of the chunk. Convention errors persist?
           fix volume_notes/<Vol>.md / pool, re-run THE CHUNK ONLY (~$8), iterate.
STAGE 4  auto_labeler.py --volume <Vol> --pin-examples <reps>   (labels the rest —
         already-labeled pages skip without --overwrite) → qa_report → triage →
         PHASE 6/7 as above.
```

**Honest validation notes (Volume_1, 274 gold pages, 2026-06-09):**
- End-to-end run works: k=3 chosen by silhouette (0.136 — weak but real structure), contact
  sheets render correctly, clusters are visually coherent density/architecture bands.
- Cluster-vs-gold-type purity is only **42.7%** — geometric clusters are NOT page types and
  must never be treated as semantic groups. Their job here is *coverage* (every architecture
  band gets gold demos + David's eyes), not routing. Demo routing at labeling time still uses
  the VLM page type + layout similarity (the validated path).
- `pick_chunk.py` exclusion logic verified: on all-gold Volume_1 it correctly finds zero
  eligible pages; on Volume_4 it stratifies and excludes the 13 gold pages.
- Per DESKTOP_PROMPT §3.1, no semantic features were used for membership; the numbered-list
  signal is a measurable proxy only (line-start alignment spread).

**⚠ Volume_2 / Volume_3 are BLOCKED on this machine (desktop), 2026-06-09:** the local crops
are a stale vintage (V2 360 pages vs the laptop's 372; V3 310 vs 312; V4 was 270 vs 272,
shifted −1 — repaired from committed auto_labeled PDFs). The laptop's 372/312-page sets came
from upstream polygon data that was NEVER COMMITTED (git's step_2/polygon_page_data has only
360/310 entries). The committed V2/V3 representatives (`qa_output/Volume_2/representatives.json`
references page_372!) use LAPTOP numbering. **Do not run any V2/V3 stage on this machine until
the laptop's `polygon_cropped_pdfs/Volume_2 + Volume_3` (and ideally its step_2 polygon data)
are synced over — or David decides the canonical crop set.** Verify with pixel correlation,
not filenames (the drift was invisible to checksum-by-name).
