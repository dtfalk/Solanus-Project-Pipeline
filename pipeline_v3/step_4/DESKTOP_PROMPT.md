# DESKTOP_PROMPT.md — instructions for the next Claude session (home desktop)

_Written 2026-06-09 on the work laptop, at David's request, by the session whose full transcript
is archived in `session_backups/session_7631bd81-*.jsonl.gz` (THIS file's author; read it for
100% context — it contains every decision, mistake, and David correction of the last 5 days).
The paste-in prompt for David is at the bottom._

---

## 0. Current state (verified at handoff)

- **Working copy: pipeline_v3/step_4 — CUTOVER DONE.** pipeline_v2 is frozen as of the handoff
  commit; all data (gold, pool, auto labels, qa, experiments, docs) and all tool fixes are synced
  here. v3 additionally has the HITL bootstrap tooling (`HITL_BOOTSTRAP.md`,
  `pick_representatives.py`, `promote_examples.py`, `auto_labeler.py --pages`,
  `review_diff.py --draft-note`) built by a parallel session.
- **Volume_4**: pages 1–189 of `auto_labeled/` carry run-3 labels (correct per-person demos);
  **pages 190–272 still carry run-2 vintage** — the run was killed mid-flight for this handoff.
  `resume_on_desktop.sh` finishes them (+qa+triage), unattended. Gold = `reviewed/Volume_4`
  pages 2–6, 8–13 (David's explicit list; pages 1 & 7 are his but NOT demo-grade).
- Corpus gold: A1 52, A2 76, A3 46, V1 273, V4 11 → ~458 pages.
- Remaining unlabeled: Volume_2 (372 pages), Volume_3 (312 pages). **These are the target of the
  new flow below — do NOT label them with the old whole-volume approach.**
- Cloud: 0 Vertex endpoints (verified); parked models solanus-cont-r1 (best tuned), gentle-G1.
  AI Studio balance ~$140 remaining of David's $220.
- Standing rules (non-negotiable): no AI attribution in commits; `reviewed/` is David-only;
  0 endpoints at every stop; push first, checkpoint on disk, then work; CSV for experiment
  results; killed processes/monitors must be filesystem-verified (a tail-piped log buffers!).

## 1. The lesson that triggered this handoff (read carefully — it's the design constraint)

The Volume_4 labeling failed TWICE in ways traceable to **the assistant's overconfidence**, not
the model's capability:
1. It promoted all of `reviewed/Volume_4` to the few-shot pool, assuming presence = human-reviewed.
   Most were auto-saves from David merely BROWSING pages (the editor saved on navigation — now
   fixed with a dirty-check). The pool taught the model the wrong convention.
2. The editor shows `reviewed/` over `auto_labeled/`, so David was LOOKING AT his own stale browse
   copies while judging the new run — the assistant told him it was better; what he saw wasn't.
3. Earlier: a 55-agent "gold consistency audit" produced findings David checked and dismissed
   wholesale; prompt rules the assistant wrote ("contents pages are src_content") directly
   contradicted conventions David later established.

**Operating principle going forward: machine judgment about WHAT a region means or HOW granular a
convention should be is never load-bearing. Machines measure (pixels, ink, layout, counts,
diffs); David decides (conventions, granularity, gold).** Every piece of the flow below is
structured so that a wrong machine guess is caught by a cheap human checkpoint before it
multiplies across hundreds of pages or dollars.

## 2. TASK A — Epistemic-humility audit of the codebase

Sweep the code/prompts for places where assistant judgment got hard-coded as fact. For each
finding: file:line, what is asserted, what EVIDENCE exists for it (David statement? measured A/B?
or just plausible-sounding assistant text?), and a proposed disposition (keep / soften / move
behind a David-approval gate / delete). Write the report to `EPISTEMIC_AUDIT.md`; change NOTHING
without separating "evidence-backed" from "assistant-assumed". Known suspects (start here, be
broader):
- `auto_labeler.py` CATEGORY_DESCRIPTIONS — long rules accreted across iterations; some encode
  David-corrected conventions (evidence-backed), others are assistant-invented "decisive tests"
  (e.g. the archv_commentary third-person test, the contents-page rules rewritten 2026-06-06).
  Which sentences trace to a David correction vs an assistant inference?
- `VOLUME_PROMPT_NOTES["Volume_4"]` — David worries it is TOO specific ("rules too specific...
  persists outside this person-to-person database"). Note it is volume-gated (only fires for
  doc_name=="Volume_4") so it cannot leak to other volumes — but audit whether its specificity
  could hurt WITHIN V4 (atypical pages: covers, indexes, non-person pages) and whether the
  general/default prompt's notebook merge-rule is itself an over-assertion for future volumes.
- `EXCLUDED_EXAMPLES` — each entry's stated reason: still true? Evidence?
- Snap/geometry constants (NUDGE/margins/gap fractions in auto_labeler + experiments/snap_lab.py)
  — which were validated (snap_lab harness, geom_diagnose) vs guessed?
- `infer_page_type_from_labels` + PAGE_TYPES — the 4-type taxonomy is itself an assumption; is it
  the right clustering vocabulary for V2/V3 (see Task B)?
- HITL_BOOTSTRAP.md + pick_representatives.py (built by another session, same hubris risk):
  representative-selection criteria, cluster counts, any "this works" claims without measurement.

## 3. TASK B — The staged per-volume flow (build/extend, then run on Volume_2 or 3)

Goal (David's words): "classify within a doc, pick sample pages for each cluster, run those via
API, human reviews/corrects, run a solid chunk, review + correct the prompt, then run the rest —
iterate until completion. A real flow for not wasting money, classifying so examples are properly
sorted and the few-shots are maximally effective."

Foundation exists: `HITL_BOOTSTRAP.md` + `pick_representatives.py` (diversity sampling via the
layout fingerprint already validated by the A/B: PQ 0.639→0.681). EXTEND it with:
1. **Within-volume clustering by page architecture** — cluster the layout fingerprints (plus
   cheap measurable features: ink density profile, line-count estimate, marginalia presence,
   has-numbered-list vs prose blocks). Use ONLY measurable features for the clusters; a cheap VLM
   pass MAY propose semantic cluster NAMES/descriptions, but cluster MEMBERSHIP stays geometric,
   and names are display-only (never load-bearing). Honesty note from the outgoing session: the
   assistant's track record on FINE semantic distinctions (e.g. numbered-list vs numeral-outline
   notebook pages as David means them) is poor; coarse visual clustering is measurable and
   trustworthy. Validate clusters by rendering a contact sheet per cluster (grid of page thumbs)
   for David to eyeball — HE confirms/renames/merges clusters, not the model.
2. **Stage 1 — samples:** pick K diverse pages PER CLUSTER (k≈2-3 small clusters, more for big
   ones; ~12-18 pages total), label via API with current best config, David hand-corrects in the
   editor → per-cluster gold.
3. **Stage 2 — promote + note:** promote his corrected pages (promote_examples.py), draft a
   per-volume/per-cluster convention note FROM HIS DIFFS (review_diff.py --draft-note) — David
   approves the note text before it enters any prompt.
4. **Stage 3 — chunk:** label a SOLID CHUNK (~20-30%, cluster-stratified, pinned per-cluster gold
   demos + approved note). David reviews a sample; if convention errors persist → fix note/pool,
   re-run THE CHUNK ONLY (cheap), iterate.
5. **Stage 4 — rest:** only after the chunk passes David's check, label the remainder. qa_report +
   triage as usual.
Cost math to respect: full V2 ≈ $35 at 3.5-flash; a wasted whole-volume run = $35 + David's hour.
The staging caps any convention mistake at chunk size. Keep per-stage spend visible in usage.csv
and report it.
Implementation notes: `--pages` exists for arbitrary page sets; pinning exists (--pin-examples);
per-volume notes exist (VOLUME_PROMPT_NOTES — consider generalizing to per-cluster notes loaded
from a file David can edit rather than python constants).

## 4. Validation gates (apply to BOTH tasks)

- Before any >$10 run: label ≤5 test pages, RENDER them (label_review/audit_render.py), and
  actually READ the boxes against the image. Counts are necessary, not sufficient.
- After any pool/prompt change: offline suite (`run_all_tests.py --no-api`) must stay green.
- Anything touching `reviewed/`: STOP — David only. Quarantine pattern (don't delete):
  see reviewed_quarantine/.
- Report honestly: if something regressed, say so with numbers (the gate1_results.csv pattern).

---

## PASTE-IN PROMPT FOR DAVID (desktop session — start claude in pipeline_v3/step_4)

```
You are picking up a 6-day project mid-stream on a new machine. Build FULL
context in this exact order before doing anything:
1. Read session_backups/CHAT_HISTORY_DIGEST.md COMPLETELY (312KB) — the
   entire prior conversation; David's messages are verbatim and define every
   labeling convention, correction, and rule. Read every David message.
   (Deep reference if you need exact details: zgrep the raw transcript
   session_backups/session_7631bd81-*.jsonl.gz.)
2. Read LABEL_REVIEW.md, iterations 12-15 (the recent history: the
   archivist-catalog fix, GATE-1 runs, the Volume_4 per-person convention,
   and the browse-pollution postmortem).
3. Read HITL_BOOTSTRAP.md (the staged per-volume protocol foundation) and
   skim pick_representatives.py + promote_examples.py.
4. Read RESUME_HERE.md's top section for current state.
5. FINALLY read DESKTOP_PROMPT.md in full and treat its TASK A and TASK B
   as your prompt, under its §0 standing rules and §4 validation gates.
First action after reading: check whether resume_on_desktop.sh has been
started (resume_run.log exists?); if not, launch it with
nohup bash resume_on_desktop.sh & — it finishes Volume_4 pages 190-272 +
qa + triage in the background (~2h) while you do TASK A. Commit and push at
every safe milestone (no AI attribution, ever). When done: summary + costs.
```
