# EPISTEMIC_AUDIT.md — where assistant judgment got hard-coded as fact

_2026-06-09, home-desktop session, per DESKTOP_PROMPT.md TASK A. Report only — **no code,
prompt, pool, or gold was changed by this audit.** Each finding: location, what is asserted,
what evidence actually exists, and a proposed disposition for David to accept/reject._

## Evidence classes used below

- **[D]** — traces to a David statement or to gold David explicitly reviewed (his verbatim words
  are in `session_backups/CHAT_HISTORY_DIGEST.md`; page-level gold = `reviewed/`).
- **[M]** — measured: an A/B, harness, or diff with numbers in `experiments/` or LABEL_REVIEW.
- **[W]** — a multi-agent workflow verdict (29-agent review 2026-06-02, 55-agent audit
  2026-06-06). NOTE: David's 2026-06-07 standing verdict — *"agents are not reliable judges of
  the labeling scheme; gold is ground truth; never audit/second-guess conventions"* — applies
  retroactively to this entire class. A [W] is NOT evidence of a convention; at best it is a
  hypothesis David has not vetoed.
- **[A]** — assistant-assumed: plausible-sounding text with no David statement and no
  measurement behind it.

Dispositions: **KEEP** (evidence-backed) · **SOFTEN** (right direction, overclaimed) ·
**GATE** (needs David's explicit decision before it is load-bearing again) · **FIX-DOC**
(stale/contradictory comment, safe textual fix) · **DELETE** (no support).

---

## TOP FINDINGS (the ones that can cost money or another wasted volume)

### 1. The general notebook merge-rule contradicts David's stated general principle — GATE

- **Where:** `auto_labeler.py:83` (`CATEGORY_DESCRIPTIONS["src_content"]`, "NOTEBOOK / JOURNAL /
  LEDGER GRANULARITY: emit exactly ONE generous src_content polygon per GOVERNING-ANCHOR SPAN …
  Do NOT emit one polygon per entry, per person …") and `auto_labeler.py:809-810`
  (SYSTEM_PROMPT: "draw ONE src_content polygon per page-marker/date span, never one per entry
  or per line").
- **Asserted:** merging by date-span is THE notebook convention.
- **Evidence:** consistent with A1/A2/A3/V1 notebook gold as labeled at the time [D-derived for
  those volumes]. BUT David, 2026-06-09 (digest #104): *"general principles are SPLITTING UP
  INDIVIDUAL JOURNAL ENTRIES and NOT SPLITTING JUST BY DATES. this requires a semantic
  understanding of the docs"* — and #109: the granularity he wants *"persists outside of just
  this person-to-person database."* The Volume_4 failure was precisely this rule winning over
  his actual convention; it is currently neutralized for V4 only by the volume-gated override.
- **Risk:** any V2/V3 notebook page labeled under the current global rule repeats the V4
  mistake at volume scale.
- **Disposition: GATE.** Before any V2/V3 run, David states the granularity rule per volume (or
  per cluster — the TASK B flow is built for exactly this). Concretely: does the per-entry
  principle apply to V2/V3 notebooks, and should the existing V1/appendix notebook gold be
  treated as its own (older) convention rather than the general rule? Recommend migrating ALL
  granularity language out of the global prompt into per-volume/per-cluster notes that David
  approves (mechanism exists: `VOLUME_PROMPT_NOTES`, extended by TASK B).

### 2. EXCLUDED_EXAMPLES declares 7 of David's gold pages defective — machine judgment of gold — GATE

- **Where:** `auto_labeler.py:413-427`.
- **Asserted:** Appendix_1/page_028, Appendix_1/page_042, Appendix_3/page_021, Volume_2/page_075,
  Volume_1/page_274, Appendix_2/page_020, Volume_1/page_245 carry "internally inconsistent or
  contradictory labels."
- **Evidence:** all seven verdicts originate from the 2026-06-02 29-agent workflow and assistant
  follow-ups [W], i.e. machines judging David's labels. The stated reasons were never put to
  David as a list. One entry literally calls a David gold page "Idiosyncratic" (V1/page_245 —
  an agent's aesthetic judgment). David's 2026-06-07 verdict makes this class untrustworthy.
- **Mitigating:** exclusion only shrinks the demo pool — a conservative, low-blast-radius error;
  and 3 previously excluded pages were rehabilitated after relabels, so the list does shrink
  with evidence.
- **Disposition: GATE.** 5-minute David pass: for each of the 7, keep-excluded or rehabilitate
  (open in editor/viewer). Until then the list stands (cheap), but no NEW entry may ever be
  added by an agent — additions require David.

### 3. The committed Volume_2/Volume_3 pool seeds are stale-convention AND partly machine-edited — GATE (already flagged by HITL PHASE 0, confirmed here)

- **Where:** `labeled_examples/Volume_2/` (12 pages), `labeled_examples/Volume_3/` (10 pages).
- **Asserted (implicitly, by being in the pool):** these are demo-grade gold.
- **Evidence:** they predate every convention David established since 2026-06-02 (archivist-
  catalog fix, per-entry granularity, mass-card role split refinements). Worse: at least 3 were
  **edited by a script on workflow verdicts**, not by David (`apply_relabels.py`, Iter 2:
  V2/page_002 ×2 swaps, V3/page_001, V3/page_218) [W]. Same-volume seeds sort to bucket 0 — the
  TOP of every prompt for their volume — so a stale seed here is the single highest-leverage
  wrong-demo position in the system (Iter 12: "a wrong DEMO beats a right PROMPT").
- **Additional hazard found this session:** the desktop's V2/V3 input-page sets are a DIFFERENT
  VINTAGE than the laptop's (360 vs 372 pages; 310 vs 312 — see RESUME_HERE 2026-06-09 note),
  so these pool pages' NUMBERING may not match whichever crop set is eventually used. Verify
  page-content identity (pixel-correlate pool PDF vs crop PDF), not just filenames, before any
  V2/V3 run.
- **Disposition: GATE** (this is HITL_BOOTSTRAP PHASE 0, unchanged): David eyeballs ~22 pages in
  the viewer against current conventions; remove or re-fix stale ones. No V2/V3 labeling before
  this pass.

### 4. "DECISIVE TEST: third-person description ⇒ archivist" overgeneralizes a David correction — SOFTEN

- **Where:** `auto_labeler.py:93` (`archv_commentary`): "a line that names or describes a
  document/artifact in the THIRD PERSON … is the archivist cataloguing, not the author writing."
- **Evidence:** the underlying convention — contents-page catalog descriptions are
  `archv_commentary`, never `src_content` — is David's, from his Appendix_2 gold [D], and the
  fix was ablation-validated (recats 40→6) [M]. The "decisive test" WORDING is assistant-
  invented [A], and it is demonstrably not universal: Volume_4 is an entire volume of
  *Solanus himself* writing third-person descriptions of people ("Thomas Kauffman – 63 –
  stroke…" = `src_content` per David's own gold). The voice-tiebreaker in `archv_other`
  (line 97) already had to patch this.
- **Disposition: SOFTEN.** Reword to the evidenced scope: on a CONTENTS/INDEX page, per-page
  catalog descriptions are archv_commentary (cite: contents-inventory context), dropping the
  universal third-person voice claim. (Wording change only; do not change behavior before a
  5-page ablation re-check like Iter 12's.)

### 5. Stale comment claims V4 pages 001/007 are "now valid demos" — contradicts David's explicit list — FIX-DOC

- **Where:** `auto_labeler.py:421-422` (comment inside EXCLUDED_EXAMPLES).
- **Asserted:** "(Volume_4/page_001 & page_007 were excluded for entry-merging; David re-labeled
  them to the per-person convention 2026-06-08, so they are now valid demos.)"
- **Evidence:** David's explicit final list of demo-grade V4 gold (2026-06-09, digest #104) is
  pages 2,3,4,5,6,8,9,10,11,12,13 — pages 1 and 7 deliberately NOT included ("the correct 11
  pages to use are…"). DESKTOP_PROMPT §0 repeats: "pages 1 & 7 are his but NOT demo-grade." The
  pool correctly holds exactly the 11; only the comment is wrong — but it is a loaded gun for a
  future session that "helpfully" adds 1 & 7 back.
- **Disposition: FIX-DOC** (one comment line; zero behavior change). Left unfixed by this audit
  per the change-nothing rule — apply on David's nod or alongside the next code commit.

---

## SECONDARY FINDINGS

### 6. Mass-card "decisive role split" hard-codes a tolerance-banded call — SOFTEN
`auto_labeler.py:83` (the MASS-ENROLLMENT block) + `:89` ("normally NO src_greeting").
The 2026-06-02 review's own acceptable-range table says the intention tag is `src_content`
*canonical* with `src_other`/`src_greeting` "defensible per layout" [W]; the prompt promotes the
canonical reading to "decisive." Direction fine, certainty overstated. Also V2/page_075 (the page
this rule was derived from) is itself in EXCLUDED_EXAMPLES — the rule's anchor page is excluded
as pathological. SOFTEN wording when next touched; verify against David's V1 card gold (5 cards).

### 7. The src_farewell / src_other / ditto-mark rule chains: provenance untraced — INVENTORY
`auto_labeler.py:88` (partial-ditto exception), `:90` ("MANDATORY: … ALWAYS its own src_farewell
polygon" + 2 carve-outs), `:92` (position-overrides-motto chain). These accreted across Iters
1–11; each clause likely traces to a specific review_diff finding on a specific page, but the
page citations were not carried into the code. They are exactly what David flagged 2026-06-09:
*"i worry you may have made the rules too specific."* No specific contradiction found with
current gold — but they are unfalsifiable as written. Disposition: when any of these zones next
produces review pain, do NOT add another clause — trace the existing clause to its gold pages
and simplify. (No change now: the offline suite is green and run-3 demos embody current gold.)

### 8. infer_page_type_from_labels thresholds are guesses; the routing they feed is measured — KEEP
`auto_labeler.py:517-530`: `content>=3 or sdate>=3 ⇒ notebook` etc. [A thresholds], but the
system they serve was measured end-to-end (page-type routing: −62% flash-lite errors, ≈neutral
for 3.5-flash; Iter 7 [M]) and misroutes only demo SELECTION (regression-safe "other" fallback
[M]). KEEP. Caveat for TASK B: the 4-type taxonomy is a demo-routing vocabulary, NOT a
clustering vocabulary — V2 types as 314/372 "letter," so it provides almost no within-volume
discrimination there (representatives.json). That is the measured argument for TASK B's
finer clustering.

### 9. Snap/geometry constants: core validated, periphery inherited — KEEP (with notes)
`auto_labeler.py:1353-1368`. Validated [M]: `margin_frac_h=0.45` + per-line x-extent (snap_lab
harness: clip residual −35%, no new text loss; real re-snap: −45% clips; Iter 14);
`vmargin_scale=1.0` (reducing it clipped text on 85% of boxes — measured and REJECTED);
keeping `margin_frac_v=0.22` (the 2026-06-09 lever sweep showed every vertical-tightening lever
trades ~1:1 against clip risk — leaving it is a measured decision, documented in digest #56).
`text_margin_frac=0.008` ≈ 54px at 150dpi vs measured gold padding median ~49px [M-adjacent].
Inherited-untested [A]: `h_gap_frac=0.016`, `v_gap_frac=0.009` (measured CONSEQUENCE: ~62px
bridge merges between-block gaps — known neighbor-capture cause, accepted trade-off),
`min_ink_frac=0.035`, `min_run_frac=0.004`, `fence_margin=6`, `skew_tol=0.12`, and the
`src_origin` override (`margin_frac_v=0.75, v_gap_frac=0.022` — fixed a real letterhead-capture
failure but was never harness-swept). KEEP all; they are an ensemble that passes B7/B9 and the
gold-grounded harness. Any future tuning goes through `experiments/snap_lab.py`, never by hand.

### 10. qa_report floors: calibrated once, by David's catch — KEEP
`qa_report.py:38-55`. The uncovered-ink floors were recalibrated after David caught a hidden
miss (page_012 "etc.", Iter 6 [D→M]); the others (clip bands, overlap=0.5, loose-slack 0.030)
are assistant-chosen but only produce review FLAGS (cheap to dismiss, the editor queue is
human-gated). KEEP.

### 11. HITL_BOOTSTRAP / pick_representatives: honest about most things; two overclaims — SOFTEN
- `HITL_BOOTSTRAP.md §3` "Farthest-point + type stratification guarantees every layout/semantic
  mode of the volume gets a gold convention exemplar" — overclaim [A]: FPS covers only what the
  192-dim ink-grid descriptor can SEE (layout). Semantic modes invisible to the grid (David's
  numbered-list vs numeral-outline distinction) are NOT guaranteed a representative. The doc's
  own §1 admits layout features can't split letter/mass_card/preamble (65–73% purity [M]).
  TASK B's David-eyeballed contact sheets are the correct mitigation — rely on those, not the
  guarantee.
- `--k 12` default justified as "one full same-volume few-shot set" [plausible A — untested as
  a coverage budget]. Fine as a default; §5's falsifiable success criteria (corrections-per-page
  vs the V4 baseline) is the right way to find out, KEEP that.
- The layout descriptor itself was validated for SIMILARITY ranking (PQ 0.639→0.681 [M]) —
  validity for DIVERSITY sampling is assumed by analogy [A]. Acceptable risk given the
  human gate, but say so. (This file says it now.)

### 12. VOLUME_PROMPT_NOTES["Volume_4"]: David-derived, correctly gated — KEEP, measure
`auto_labeler.py:824-845`. Nearly every sentence traces to digest #104/#85 [D] ("casebook of
individual people," "one box per person," "not every entry follows the exact same surface
pattern — segment by MEANING," "never assume dates"). Volume-gated (`doc_name=="Volume_4"`), so
it cannot leak. Within-V4 specificity risk on atypical pages (covers/indexes): partially
mitigated by "struct_doc … and archivist material are labeled per the normal rules"; the real
measurement is David's review of the run-3 output — track corrections-per-page on non-person
pages vs person pages. The closing claim "The few-shot examples are all from this volume" is
TRUE only under run-3's invocation (`--num-fewshot 11 --pin-examples <the 11>`) — it is an
invocation-dependent statement living in an invocation-independent constant. FIX-DOC candidate:
condition it ("when this volume's gold is pinned…").

### 13. ALLOWED_EDGE_PAIRS contradicted David's gold — REAL BUG, CONFIRMED AND FIXED
`auto_labeler.py:109-122` + the pass-2 prompt. Verified by mining `reviewed/` for actual
connection pairs [M]:

| pair in David's gold | count | in whitelist? | in pass-2 prompt? |
|---|---|---|---|
| src_content↔struct_doc | 1053 | yes | yes |
| src_content↔src_date | 564 | yes | yes |
| **archv_commentary↔struct_doc** | **88** (A2 74 / V1 10 / A3 4) | **NO** | **explicitly forbidden** |
| src_content↔src_content | 29 (A2 25 / V1 4) | NO | explicitly forbidden |
| archv_other↔struct_doc | 18 | yes | yes |
| src_content↔struct_id | 5 | NO | no |
| (singletons ≤3) | 9 | NO | no |

Iter 12 (2026-06-06) flipped the pass-1 category text to David's contents-page convention
("archv_commentary … connected to struct_doc") but **never propagated it to the pass-2 prompt's
EDGE INVENTORY / NEVER-CONNECT list or to ALLOWED_EDGE_PAIRS** — so `apply_edges()` silently
dropped every such edge the model proposed, and the pass-2 prompt told it not to propose them
at all. Every contents/index page in V2/V3 would have needed David to hand-draw those links.
**Fixed this session** (whitelist + pass-2 inventory edge 3 + never-connect carve-out; offline
suite 35/0 after). The remaining non-whitelisted pairs are GATE items for David:
`src_content↔src_content` (29 — is the content-to-content continuation link a convention the
model should propose, or David-only?), `src_content↔struct_id` (5), and four singletons —
left dropped for now (conservative; the model is also told not to emit them).

### 14. Pricing table — REFERENCE ONLY
`pricing.py:15-20` hard-codes mid-2026 list prices [A/reference]. usage.csv costs are estimates,
stated as such in the docs. KEEP with the existing caveat.

---

## What this audit did NOT find
No case where a measured number in `experiments/gate1_results.csv` / LABEL_REVIEW was
misreported; the negative results (continuation-r2 regression, c20 demos hurting, tuned-fs
collapse) are recorded honestly. The browse-pollution and demo-poisoning postmortems (Iter 15)
are accurate against the raw transcript. The standing operating principle (machines measure,
David decides) is correctly encoded in HITL_BOOTSTRAP PHASES 0/3/4 and GENTLE_TUNING_PLAN
Phase 0.5.

## Recommended David checklist (15 minutes total)
1. **Granularity ruling (Finding 1)** — per-entry vs per-date-span, per volume; one sentence each
   for V2 and V3 when their time comes (the staged flow will ask at the right moment).
2. **EXCLUDED_EXAMPLES pass (Finding 2)** — 7 pages, keep or rehab.
3. **V2/V3 seed audit (Finding 3 = HITL PHASE 0)** — ~22 pool pages, eyeball vs current
   conventions.
4. Nod (or veto) the two FIX-DOC items (Findings 5, 12) and the SOFTEN rewordings (4, 6) — all
   textual, none change behavior.
