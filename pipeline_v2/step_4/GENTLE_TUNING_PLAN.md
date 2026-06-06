# GENTLE_TUNING_PLAN.md — incremental fine-tuning, fewer errors every round

_2026-06-06. Written per David's directive: **"gentle!!! fewer and fewer errors... we're not trying
to create some full perfect thing, but as best as we can, slowly slowly, step by step make this
process less painful. Go for broke is a lot to deal with."** Research: 103-agent deep-research
sweep (adversarially verified, sources cited in Appendix A) + this project's own measured evidence.
This plan is designed to be EXECUTED UNATTENDED by an agent on the desktop — exact phases, decision
rules, budget caps, and teardown discipline are spelled out in §4._

---

## 1. Where we actually stand (measured, 2026-06-06)

All numbers = ink-IoU Panoptic Quality on the **cold, fully-held-out Appendix_2** (76 fresh gold
pages), from `experiments/gate1_results.csv`:

| arm | PQ | strict-PQ | note |
|---|---|---|---|
| production (few-shot 3.5-flash + page-type) | **0.909** | **0.852** | the bar |
| base-bare (0 demos) | 0.870 | 0.806 | demos worth only ~+0.04 |
| tuned flash-lite bare (8ep/LR×5/adapter8, 89 pages) | 0.561 | 0.403 | up from 0.159 |
| tuned + 12 demos | 0.320 | 0.227 | demos HURT tuned models |

**Where production's remaining errors live** (per-category, A2): essentially THREE recall gaps —
`src_content` FN 63 (notebook span granularity), `struct_doc` FN 25 (page markers), and
`archv_commentary` FN 22. Eleven other categories sit at RQ 0.94–1.00. "Fewer and fewer errors"
means attacking those three, not re-learning the whole task.

**How the tuned model fails**: broad structural hallucination — archv_other 42 FP (a category
production never confuses), archv_date 39 FP, struct_doc 76 FP/101 FN. It over-stamps small
structural boxes everywhere: the signature of aggressive fitting on tiny data, and David's
"are you over-tuning?" question is the right one.

**Already fixed this round (cheapest error reduction so far, $2)**: the archivist-catalog prompt
contradiction + few-shot pool drift — recats on the worst pages 40 → 6. Lesson: a wrong DEMO beats
a right PROMPT; convention changes must propagate to the pool.

---

## 2. What the research changes (full findings + sources in Appendix A)

1. **Continuous tuning exists and is the centerpiece of the gentle strategy.** Vertex supports
   tune-from-tuned: "continue tuning an already tuned model or model checkpoint by adding more
   epochs or training examples." Requirements we meet: Gen AI SDK (we use it), base tuned after
   2025-07-11. **Protocol change: PARK (delete endpoint, keep model — $0) instead of full teardown**
   for any round we may build on. Each new volume of gold = a cheap +epochs/+data continuation, not
   a from-scratch retrain. This IS "slowly slowly, step by step," natively.
2. **We've been throwing away the anti-overfit machinery.** Vertex saves per-epoch intermediate
   checkpoints (one/epoch under 10 epochs), exposes validation-loss metrics (`eval_total_loss`)
   when you supply a validation file (up to 256 examples), and lets you pick the best
   pre-overfitting checkpoint as default. Our `--last-ckpt-only` flag discarded exactly that.
   **Protocol change: always supply the val set, keep all checkpoints, select by val loss.**
3. **Gentle hyperparameters are the documented default.** Epochs/LR-multiplier left UNSET =
   Google's recommended values; LR multiplier default 1.0, documented range ~0.1–2.0. Our LR×5 was
   far outside it. (Counterpoint kept honest: Google's older 1.5-gen IMAGE recipes did use LR=5
   and epochs≥15 — aggressive settings aren't insane for vision, but they're the far pole, not the
   start. Smaller adapter = gentler: forgetting research shows damage grows with LoRA rank.)
4. **The bigger model is tunable — David's ask is supported.** SFT is GA for Gemini 2.5 Flash
   (2.5 Pro / 2.5 Flash-Lite / 3.1 Flash-Lite also tunable, all with continuous tuning). Tuning the
   tier that already works best (flash) is a one-flag change. Caveat: 2.5-tier tuning PRICES are
   not published — measure cost on the first job and respect the budget cap.
5. **Dataset reality check:** Google recommends ≥100–500 examples; we have 98 trainable (174 minus
   held-out A2). We're at the bottom of the band — expectations should be "useful model," not
   "production parity." Quality-over-quantity is explicitly endorsed ("a smaller, refined and
   representative dataset often outperforms a large, noisy one") — which is why the gold-consistency
   audit findings (21 queued fixes) and pool sync feed directly into tuning quality.
6. **Cross-domain transfer of document-layout models degrades sharply** (avg −33% in the
   literature). Our corpus mixes letters/cards/notebooks — the tuned model needs balanced per-type
   data; don't expect notebook competence from letter examples.
7. **Don't mix demos into tuned-model inference** (we measured it: 0.561 → 0.320), and the
   literature pattern for under-performing tuned models is **cascades / disagreement routing**,
   not replacement — which maps perfectly onto our existing triage.py shadow-model design.

---

## 3. The strategy in one paragraph

Stop trying to leapfrog production. Make the tuned model **earn its way up a ladder of low-risk
jobs**, getting a little better each round through continuous tuning on each new volume of gold:
first beat flash-lite as the **triage shadow** (better review ordering — saves David minutes
immediately, zero risk), then serve as a **disagreement check** on production output, and only if
it ever beats production on a cold volume does a swap even come up for discussion. Each tuning
round is gentle (default LR, small adapter, val-loss checkpoint selection), each adds the newest
human-corrected volume to training, and each is measured on the same cold test before/after.
Production (few-shot 3.5-flash + the now-fixed prompt/pool) keeps labeling volumes meanwhile.

---

## 4. EXECUTION PLAN (for the desktop agent — run top to bottom)

**Hard rules (override everything):**
- R1. NEVER more than one Vertex endpoint live; after every eval, enumerate ALL endpoints
  (`gcloud ai endpoints list`) and delete every one. Checkpoint endpoints count — each saved
  checkpoint can carry its own endpoint.
- R2. End state of every phase: **0 endpoints**. Tuned MODELS may be kept ("parked") — they bill
  nothing — but list each kept model by name in the run log with a reason.
- R3. Budget cap: **$40 total** for the whole plan (tuning price for 2.5 tiers is unpublished —
  measure the first job's cost in the console/billing before launching the next; abort the phase
  if a single job exceeds $15).
- R4. Every eval result appends a row to `experiments/gate1_results.csv` (use
  `panoptic_eval.py --csv --arm --notes`). No silent results.
- R5. `reviewed/` gold is read-only. No production default changes (model/prompt/pool) — the only
  production-adjacent change permitted is swapping triage.py's SHADOW model (Phase 4, gated).
- R6. If anything anomalous happens (job stuck >2h, endpoint that won't delete, eval crashes),
  capture state to the run log, teardown, stop the plan, and write a handoff note in RESUME_HERE.

### Phase 0 — Preflight (~10 min, $0)
1. `git pull` (expect this plan + current state; HEAD at/after `5062741`).
2. `./venv/bin/python run_all_tests.py --no-api` — expect 30 PASS / 0 FAIL on the desktop
   (B7 passes there; if B7 FAILS you are not on the machine with the annotation-bearing A3 PDFs —
   note it and proceed, it doesn't block tuning, but record which machine ran this).
3. `gcloud ai endpoints list --region=us-central1 --project=solanus-project` → must be empty.
4. Refresh few-shot uploads if labeling is planned: `./venv/bin/python upload_examples.py`.
5. **Code changes required before Phase 1** (small, test each):
   a. `finetune.py tune`: add `--base` passthrough (exists), add `--val` to attach
      `vertex_val.jsonl` as the tuning job's validation dataset, REMOVE the default use of
      `--last-ckpt-only` (keep flag available but off), and surface `learning_rate_multiplier`
      unset-means-default behavior (only send the param when explicitly given).
   b. `finetune.py`: new subcommand `checkpoints` — list a finished job's checkpoints with their
      val metrics; new flag `tune --from-model <tuned-model-or-checkpoint>` for continuous tuning
      (Gen AI SDK `tunings.tune` accepts a tuned-model base).
   c. `finetune.py status`: also print `eval_total_loss` per checkpoint when available.
   d. Enlarge the val split: `prepare --val 17` (stratified across page types; cap 256 is far away).
   e. Run `run_all_tests.py --no-api` again after the edits — must stay green.

### Phase 0.5 — Label agreement (the data-quality gate; David-dependent, $0)
The training volumes (A1+A3) carry pre-A2 conventions while evaluation is against A2-convention
gold — part of what any tune "learns" is that inconsistency. Two worklists reconcile it:
1. `label_review/GOLD_CONSISTENCY_AUDIT_2026-06-06.md` — 21 verified one-click fixes (DAVID
   approves/applies in the editor; the agent NEVER edits gold).
2. The notebook-convention audit (A2's notebook handling as reference, all three appendices) —
   if its report exists in `label_review/`, same treatment.
**Agent behavior:** proceed with whatever gold state exists at run time — do NOT wait — but
record in the run log which worklists were applied (count `git log reviewed/` since this plan's
commit). If gold changed since the last tune, re-`prepare` picks it up automatically. ALSO:
exclude from the tuning export any page with an UNRESOLVED audit finding (add a skip-list to
`export_tuning_data.py` from the audit JSONs — the tuning-set analog of EXCLUDED_EXAMPLES) and
log how many pages were skipped. **Quantify the cleanup (Phase 3 hook):** when David later applies
a worklist, the next continuous-tuning round re-runs the SAME config on the cleaned gold — the
before/after delta on the cold subset is the measured value of label agreement (arm
`tuned-cont-cleangold`).

### Phase 1 — Gentle sweep on flash-lite (~3–4 h wall, ~$8–12)
Four configs, SEQUENTIALLY (R1), each: `prepare` (same data) → `tune` → wait → score → record → clean.
| # | epochs | LR mult | adapter | rationale |
|---|---|---|---|---|
| G1 | unset (Google default) | unset (=1.0) | 4 | the documented gentle baseline |
| G2 | unset | unset | 8 | adapter-size ablation |
| G3 | 6 | 2 | 4 | mid-pole (top of documented LR range) |
| G4 | 8 | 5 | 8 | current aggressive config = control |
Per config: score with `tuned_eval val` + `score`, AND cold subset
`experiments/run_arms.py`-style on the SAME 20 stratified A2 pages (arm name `sweep-G<N>`),
using the **best checkpoint by val loss** (not the last). After scoring: delete ALL endpoints;
keep (park) ONLY the single best model of the phase, delete the rest.
**Decision rule:** best = highest cold-subset PQ_strict. If G1/G3 ≥ G4, David's over-tuning
hypothesis is CONFIRMED — record prominently. If everything plateaus ≈0.55, data is the binding
constraint — skip Phase 2 cost and go to Phase 3.

### Phase 2 — The bigger base (only if Phase 1 shows headroom; ~1–2 h, ~$5–15)
Tune **gemini-2.5-flash** (`--base gemini-2.5-flash`) with the winning gentle config from Phase 1.
Same eval, arm `tuned-flash-G<N>`. Measure actual job cost FIRST (R3) — if the first flash job
costs >$15, record and skip. Park the model only if it beats the Phase-1 winner.

### Phase 3 — Data-centric continuation (the "fewer errors" round; ~1–2 h, ~$5)
On the parked winner, via **continuous tuning** (`tune --from-model ...`):
1. Build a targeted top-up set aimed at the three production error concentrations:
   - the David-corrected notebook/catalog pages (oversample ×2–3),
   - every gold page with ≥1 `struct_doc` or `archv_commentary` box (balanced per type).
2. Continue-tune +2–4 epochs, default LR, with val set.
3. Same eval; arm `tuned-cont-r1`. Success = cold PQ_strict improves ≥0.03 over its own base
   row (NOT vs production). That's the per-round "fewer errors" loop that repeats every time a
   new volume of gold lands — each future round: add new volume's gold → continue-tune → eval →
   park. (Optional, only if budget remains: a distillation booster — label Volume_1 with
   production, keep pages where production and the tuned model AGREE (low disagreement), add as
   silver data, continue-tune. Record as `tuned-cont-distill`.)

### Phase 4 — The adoption ladder (gentle integration, ~30 min, ~$1)
Rung 1 (this run): compare the best tuned model vs the current triage shadow (flash-lite few-shot)
as a DISAGREEMENT SIGNAL: run `triage.py`-style disagreement using the tuned model as shadow on
A2's auto labels, and check rank-correlation of its worklist against David's ACTUAL edit counts
per page (we have them via review_diff). If the tuned shadow ranks David's actual problem pages
higher than the current shadow does → **swap the shadow** (`triage.py --shadow-model` route or a
small code change) and record before/after in the run log. Production labeling stays untouched.
Rungs 2–3 are FUTURE gates, not this run: (2) tuned model as post-label disagreement checker on
new volumes; (3) candidate production swap ONLY when a tuned arm beats production PQ AND
strict-PQ on a full cold volume.
**End of Phase 4: enumerate endpoints → 0 (R1/R2).**

### Phase 5 — Wrap up (~20 min, $0)
LABEL_REVIEW Iter 13 entry (sweep table, over-tuning verdict, continuation delta, shadow decision),
RESUME_HERE update, `run_all_tests.py --no-api` green, commit + push (NO Claude attribution —
verify `git log` trailer-free), on-disk checkpoint `22_gentle-tuning/`, final summary for David
including the parked-model inventory and total spend.

---

## Appendix A — Research findings (103-agent deep-research, adversarially verified)

1. **Tunable models (HIGH, 3-0):** SFT GA for Gemini 2.5 Flash (June 2025); 2.5 Pro / 2.5
   Flash-Lite / 3.1 Flash-Lite tunable; image+document tuning are first-class documented paths.
   Caveat: docs name "image classification / document analysis," not bounding-box region labeling
   specifically. [cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning;
   cloud.google.com blog 2025-06-17]
2. **Continuous tuning (HIGH, 3-0):** "Continue tuning an already tuned model or model checkpoint
   by adding more epochs or training examples." Gen AI SDK only (not legacy Vertex SDK); base must
   be tuned ≥2025-07-11; checkpoint-as-base supported, default checkpoint used if unspecified.
   [docs.cloud.google.com/...gemini-use-continuous-tuning]
3. **Dataset size (HIGH, 3-0):** "At least 100 to 500 examples" recommended; hard minimum 16;
   validation dataset up to 256 examples generates periodic metrics. "A smaller, refined and
   representative dataset often outperforms a large, noisy one."
   [gemini-use-supervised-tuning; "Master Gemini SFT" blog]
4. **Hyperparameters (HIGH, 3-0):** epochs & learning_rate_multiplier optional — unset = Google's
   recommended values; LR multiplier default 1.0 (range ≈0.1–2.0); larger adapter needs more data;
   small adapter suits small datasets; overfitting controlled primarily via epoch count.
5. **Vendor recipes are generation/modality-specific (HIGH, 3-0):** published aggressive recipes
   (epochs 15–20, LR×5–10, adapter 4–16) are for Gemini **1.5** text/image — do not copy them
   into 2.5 vision tuning; start from defaults instead.
6. **Checkpoints & early stopping (HIGH, 3-0):** per-epoch intermediate checkpoints (<10 epochs),
   each servable; "Export last checkpoint only" toggle; compare checkpoints and set the best
   pre-overfitting one as default; `eval_total_loss` validation curves.
7. **Economics (HIGH, 3-0):** tuning price rows published only for 2.0 tiers ($3.00/1M training
   tokens Flash, $1.00 Flash-Lite); **no published tuning price for 2.5 tiers**; "tuned endpoint
   bills same per-token as base" was REFUTED — do not assume; measure. (Project experience:
   tuned-model serving = dedicated endpoint billing hourly while deployed; models parked = $0.)
8. **Forgetting vs rank (MEDIUM):** catastrophic forgetting occurs even with LoRA and worsens with
   higher rank (zero-shot drops up to 30–40% in CLIP studies); selective/low-rank adaptation
   reduces it. Mechanism transfers: gentler = lower rank, fewer epochs. [arXiv 2501.15377]
9. **Cross-domain fragility (MEDIUM, 3-0):** document-layout models transfer badly across domains
   (avg −32.6%, worst −65.6%) — keep per-type balance in training data; expect domain gaps.
   [arXiv 2503.18742 + corroborating DLA literature]
