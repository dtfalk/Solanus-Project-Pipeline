# Research & Plan — error identification, page-ID clustering, fine-tuning, and driving errors to ~0

_2026-06-04. Written in response to: "page identifier via content-agnostic clustering?", "small
fine-tuning as gold grows?", "drive errors further down — you've not been great at identifying
errors", "do a large research + planning phase." Grounded in built prototypes + web research, not
assertions. Companion to `DEEP_REVIEW.md` (the model/page-type A/B), `LABEL_REVIEW.md`, `PLAYBOOK.md`._

---
## 0. You were right — my error identification was flawed. Here's the honest version.

My headline "113 hard errors" (and the −47%) leaned on `review_diff`, which matches boxes by **IoU≥0.5
vs your gold** and lumps four very different things into one number:
1. **real errors** (wrong category; genuinely missed or hallucinated content),
2. **granularity** (model splits/merges a region differently — explicitly NOT the quality bar, PLAYBOOK §2),
3. **spatial-matching artifacts** (a small box shifted a few px drops below IoU 0.5 → counted as
   *remove + add*, double-counting one box as two errors),
4. **gold noise** (your single-annotator gold is ~internally inconsistent on the fuzzy axes — PLAYBOOK §3.2).

**Proof it mattered (measured this session):**
- `page_035` scored "6 hard errors" yet is visually perfect and both independent models AGREE on it.
  The 6 = **3 `src_date` "added" + the same 3 `src_date` "removed"** — three ditto-mark date cells
  (`" 18`, `" 19`, `" 20`) whose tiny boxes shifted under IoU 0.5. Zero real errors.
- Sweeping the IoU match threshold (absorbing small shifts into "resize"): NEW hard
  **113 → 77 → 62** at IoU 0.5 → 0.35 → 0.2 (stable ~62 below that). **~45% of "113" was artifact.**
  The real floor is **~62 / 43 pages ≈ 1.4 discrepancies/page**: ~31 recall-misses, ~19 category-calls
  (several defensible per your §6 tolerance bands), ~12 truly-spurious. Over-production — the thing that
  cost you the hour — is now basically solved; the residue is **recall + category judgment**.
- The model/page-type improvement holds at EVERY threshold (213→113, 156→77, 133→62, ~50% throughout),
  so that conclusion was safe even though the absolute number was inflated.

**Better error-identification, going forward (two built, one researched):**
- **(BUILT) Model-disagreement triage** (`rerun_compare/disagreement.py`, Query-by-Committee). Two
  independent labelers (flash-lite + 3.5-flash) on the same page; where they *disagree* is a
  gold-free likely-error signal. Result on Appendix_3: **reviewing the top-21 disagreement pages
  catches 79% of real errors** (89/113), and **13 pages have zero disagreement → auto-accept
  candidates**. This is how you cut review time without gold.
- **(BUILT) Threshold-aware / artifact-separated metric** — always report hard errors at IoU 0.5 AND
  ~0.2; the gap is cosmetic. Treat add+remove of the SAME category in the SAME spot as one resize.
- **(RESEARCH) VLM-as-judge** (ClipGrader 91% on COCO; arXiv 2510.03376) — render the boxes on the page
  + attach the per-box text tag, ask a strong model "is each box tight, correctly-classed, and is
  anything unboxed?". CRUCIAL caveat from the literature: VLMs judging *raw coordinates* have ~0.32
  recall — you MUST give them the **rendered overlay + text tags**. This catches *semantic* errors
  (wrong category on a well-drawn box) that geometry-vs-gold never can, and needs no gold.
- **Consensus-vs-gold mismatch** (e.g. `page_035`) → route to the gold-audit (PLAYBOOK §5), because
  "both models agree but differ from gold" is usually *gold noise*, not model error.

---
## 1. Page identifier: content-agnostic clustering — worth it, but as a *complement*, not a replacement

**Your idea, tested.** I built `rerun_compare/cluster_pages.py`: ~44 pure-layout features per page
(6×6 ink-density grid, horizontal projection → text-line count, vertical projection → left-margin
date-column signal, ink center-of-mass, gap structure) — **zero text reading** — then k-means.

**Result (91 gold pages):** purity ~**65–73%**. It cleanly isolates **notebook vs not** (one cluster
is 21 notebook / 5 other) — the split that drives the whole error story — but **cannot separate
letter / mass_card / transcription-preamble**, because that distinction lives in the *content*, not the
geometry. This exactly mirrors the VLM classifier (perfect on notebook, fuzzy on the front half).

**Research.** The modern content-agnostic route is **DINOv2 (or SigLIP/CLIP) image embeddings +
HDBSCAN/k-means** — richer visual features than hand-rolled projections, the standard for
label-free image clustering. It would lift purity, but needs `torch` + a ~300MB–1GB model (against
this repo's deliberately-lean, dependency-light design).

**Recommendation (hybrid, ranked):**
1. **Keep the VLM classifier as the primary router** (it's cheap, cached, and the regression-safe
   `other→same-volume` fallback means a wrong call never hurts). It already wins where it counts.
2. **Add the cheap layout-feature clustering as a free, deterministic CROSS-CHECK and DISCOVERY tool**,
   not a router: (a) it flags pages where geometry and the VLM disagree (audit candidates); (b) running
   it at k=8 already hints at **sub-types** (single vs multi-doc letters, ledger vs prose notebook) you
   haven't named — useful for curating few-shot coverage and spotting a new style before it bites.
3. **Only reach for DINOv2 embeddings if you later need finer automatic routing** (e.g. ledger-vs-prose
   notebook demos). At ~1,400 pages the dependency is justified *if* it measurably cuts review; gate it
   on an A/B like we did for the model.
**Net:** clustering is worth having, but its job is *discovery + cross-check*, while a content-aware
classifier does the routing. Don't replace the working router with a 70%-purity unsupervised one.

---
## 2. Fine-tuning, iteratively, as gold grows — YES, this is the real long-game. Roadmap below.

**Why it's the right end-state (researched).** Today every page ships ~12 few-shot images + a ~30k-char
prompt (~50–57k input tokens/page). Vertex SFT explicitly lets you **drop the few-shot examples after
tuning** (Google's own guidance) and "internalize the recurrent prompt" (PromptIntern, arXiv 2407.02211)
— so a tuned model is **both cheaper and faster per page AND more consistent** (conventions baked in,
not re-derived from 12 examples each call). Tuned-endpoint inference ≈ 1.5× base; train cost = dataset
tokens × epochs (3–5 typical). Catastrophic forgetting (the usual SFT risk) is irrelevant here — this is
a single-purpose model, we WANT it over-specialized.

**The flywheel (this is the "keep iterating, faster and better" you described):**
```
label a volume (few-shot 3.5-flash)  ──►  disagreement-triaged human review (fast)
        ▲                                              │
        │                                              ▼
   re-tune flash-lite on the         gold grows  ◄── corrected gold added to tuning_data/
   bigger, cleaner gold set                           │
        ▲                                              ▼
        └──── benchmark tuned vs few-shot ◄──  active-learning: the pages where the
              on a held-out volume               committee disagreed are the most
              (drop few-shot if it wins)         informative ones to label next
```
Each turn the model gets cheaper (drop few-shot), faster, and better (more/cleaner data), and
active-learning (disagreement = uncertainty) spends your review minutes where they teach the model most.

**Concrete roadmap with decision gates:**
- **NOW (done):** `export_tuning_data.py` writes `tuning_data/` every time; 95 examples today (Google
  rec ≥100 — you cross it after the next reviewed volume). Keep labeling with few-shot-3.5-flash.
- **GATE 1 — first tune at ~150–250 gold pages** (≈ +2 volumes). Tune **gemini-flash-lite** (cheapest
  base) on Vertex SFT, 3 epochs, images at 1024px, thinking budget 0. Then **A/B on a held-out volume**:
  *tuned-flash-lite with NO few-shot* vs *few-shot-3.5-flash*, scored with the artifact-separated metric.
  Win condition: tuned matches/beats on hard errors at materially lower $ and latency.
- **GATE 2 — at ~400–600 pages**, re-tune on the cleaner/bigger set; consider tuning a stronger base if
  flash-lite plateaus. Re-run the held-out A/B. Lock the convention set before tuning (re-tuning on
  drifting conventions wastes train cost — PLAYBOOK §3.7).
- **Caveats to verify at GATE 1:** confirm current Vertex SFT supports the exact base model + image
  inputs for the 3.x family (docs lag the API; 2.5-flash/-lite/-pro are confirmed, 3.x to verify);
  needs a GCP project (not just the AI-Studio key); validate the JSONL envelope `export_tuning_data.py`
  emits against the live `gemini-supervised-tuning-prepare` page before submitting.

**Verdict:** worth it, but **sequenced** — don't tune at 95 noisy pages; tune at ~150–250 once a held-out
A/B can prove it beats few-shot-3.5-flash. The data pipeline is already in place so nothing is lost by
waiting.

---
## 3. Driving errors further down — ranked menu (highest leverage first)

The remaining real errors are **recall-misses + category-calls**, not over-production. Matched tools:

1. **Self-consistency / ensemble (Weighted Boxes Fusion).** *Highest-leverage next experiment.* Research
   is emphatic: sampling a model N× (temp>0) and fusing boxes "consistently improves recall, calibration,
   robustness, especially for rare/ambiguous categories," and **"smaller models with self-consistency
   match or surpass substantially stronger models."** Two concrete bets: (a) **flash-lite ×3 + WBF** to
   rival 3.5-flash at ~½ the cost; (b) **3.5-flash ×3 + WBF** to push the recall-misses toward zero.
   Targets exactly our residual (ADD). Ready-to-run design: relabel the 7 hard pages N=3 at temp 0.3,
   union-then-vote (keep boxes appearing in ≥2 runs; union of high-agreement), score vs gold. ~$2, ~10 min.
2. **Disagreement-triaged review (BUILT).** Ship `disagreement.py` into the QA step: auto-accept the
   zero-disagreement pages, send the human only the top-disagreement ones (79% of errors in ~½ the pages).
   Pure efficiency; no model change.
3. **Close the few-shot coverage gaps.** The pool has only **4 mass_card** and **5 other** examples — so
   mass-card targets max out at 4/12 same-type demos. Promote a few corrected mass-card/ledger pages from
   `reviewed/Appendix_3` into `labeled_examples/` (you have them now). Cheap, directly lifts the weak types.
4. **Audit the ~19 category disagreements (recat) — separate gold noise from real.** Many fall on the
   fuzzy axes (`src_other↔src_content`, `archv_date↔src_date`) your §6 tolerance bands already cover. Run
   the PLAYBOOK §5 VLM audit on just these; fix gold where it's wrong, add ONE tie-break only for a
   high-frequency real gap. Do NOT broadly re-tighten the prompt (§3.8 overcorrection).
5. **VLM-as-judge refinement pass (label → critique → fix).** After labeling, a strong model views the
   rendered overlay + text tags and proposes only fixes (missed line, wrong class). Add-only/■precision,
   gold-free. Pairs with #2 — judge only the flagged pages to control cost.
6. **Tighten snap-to-ink geometry.** A large share of "resize" churn (and some IoU artifacts) is the
   model's boxes being looser/tighter than yours. Better snap reduces both the cosmetic noise and the
   borderline IoU misses — improving the *metric's* signal even where labels are fine.
7. **Fine-tuning (§2)** — the durable error-reducer once data is sufficient.

---
## 4. Recommended sequence (what I'd actually do, in order)
1. **Ship disagreement triage** into `qa_report.py` (auto-accept agreers, prioritize disagreers). Free, immediate.
2. **Run the self-consistency A/B** (flash-lite×3+WBF and 3.5-flash×3+WBF on the 7 hard pages). Decide if
   ensemble beats single-shot enough to adopt (and whether cheap-ensemble can replace 3.5-flash).
3. **Promote a few mass_card / ledger gold pages** into the few-shot pool (fixes the coverage gap).
4. **Keep labeling volumes** with few-shot-3.5-flash + page-type; `export_tuning_data.py` accumulates.
5. **At ~150–250 gold pages: GATE 1 fine-tune A/B.** If tuned-flash-lite (no few-shot) wins, switch — big
   cost/latency drop. Else re-evaluate at GATE 2.
6. Keep the **layout clustering** as a periodic discovery/cross-check; reach for **DINOv2** only if finer
   auto-routing proves necessary (gate on an A/B).

---
## Sources
- Fine-tuning VLMs for localization / in-context grounding: [arXiv 2411.13317](https://arxiv.org/pdf/2411.13317) · [Vertex SFT overview](https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning?hl=en) · [SFT data prep](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare) · prompt internalization [PromptIntern arXiv 2407.02211](https://arxiv.org/pdf/2407.02211)
- Content-agnostic clustering: [DINOv2](https://arxiv.org/html/2304.07193v2) · [DINOv2 + HDBSCAN guide](https://medium.com/@EnginDenizTangut/%EF%B8%8F-image-clustering-with-dinov2-and-hdbscan-a-hands-on-guide-35c6e29036f2)
- VLM-as-judge / annotation QA: [VLM-as-Judge for detection, arXiv 2510.03376](https://arxiv.org/html/2510.03376v1) · [ClipGrader, arXiv 2503.02897](https://arxiv.org/pdf/2503.02897)
- Self-consistency / ensemble detection (WBF): [multi-model consensus for detection, MDPI 2026](https://www.mdpi.com/2076-3417/15/24/12961) · [self-consistency voting](https://www.kinde.com/learn/ai-for-software-engineering/workflows/llm-fan-out-101-self-consistency-consensus-and-voting-patterns/)
- Active learning / query-by-committee: [Encord active-learning guide](https://encord.com/blog/active-learning-machine-learning-guide/)
