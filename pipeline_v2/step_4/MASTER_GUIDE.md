# The VLM Document-Labeling Data-Flywheel — A Deep, Reproducible Guide

_2026-06-04. The definitive, research-grounded guide to building a VLM-assisted document-region
labeling system whose **human effort shrinks toward zero as gold-tier labels accumulate**, designed
for the general case of a **continuous stream of incoming documents**. Distilled from the Solanus
corpus build-out (typed letters, Mass cards, dense notebooks/ledgers) but written to be **portable to
any corpus**. Companions: `PLAYBOOK.md` (the issue log + checklists), `DEEP_REVIEW.md` /
`RESEARCH_AND_PLAN.md` (the measured A/B evidence), `LABEL_REVIEW.md` (per-iteration log)._

> **Core thesis.** Don't think of this as "a labeler." Think of it as a **data engine / flywheel**:
> a sequence of ever-cheaper labelers, each trained on the verified output of the last, where the
> human's role degrades from *drawing boxes* → *correcting boxes* → *spot-checking flagged boxes* →
> *auditing samples*. Every accumulated gold page makes the next model better AND the next human pass
> shorter. The engineering job is to make that loop **measurable, reproducible, and self-shrinking.**

---
## 1. The reference architecture — a 3-gear data engine

Meta's Segment Anything (SA-1B) built 1.1B masks with a **three-gear data engine**, and it is the
right mental model for *any* growing-corpus annotation problem ([SAM](https://arxiv.org/pdf/2402.11413)):

| Gear | Who does what | When you're in it | Human cost/page |
|---|---|---|---|
| **G1 assisted** | Model proposes, human corrects most pages | cold start, new page types, < ~150 gold | high (minutes) |
| **G2 semi-auto** | Model labels; human only reviews **flagged** pages | conventions stable, model decent, 150–500 gold | medium→low |
| **G3 auto** | Model labels; human **audits a sample**; exceptions escalate | model ≥ human-noise ceiling on common types | ~0 (sampling) |

The whole guide is about **what moves you down the gears** (better model, page-type routing, ensembles,
fine-tuning) and **how you prove you've earned the next gear** (measurement + gates). The Solanus
project today sits at the **G1→G2 boundary**: page-type routing + 3.5-flash cut hard errors ~50%, and
the disagreement triage (below) is the lever that unlocks G2.

```
            ┌──────────────────────────────────────────────────────────────┐
            │  incoming documents (a STREAM, not a fixed set)                │
            └───────────────┬──────────────────────────────────────────────┘
                            ▼
   ┌─ LABEL (cheap, repeatable) ──────────────────────────────────────────────┐
   │  page-type route → few-shot VLM (count docs → regions) → snap → coverage  │
   │  backstop → relations.  Optionally: self-consistency / ensemble fusion.   │
   └───────────────┬──────────────────────────────────────────────────────────┘
                   ▼
   ┌─ MEASURE (gold-free first) ──────────────────────────────────────────────┐
   │  ink-coverage (no gold) · committee DISAGREEMENT (no gold) · VLM-judge ·  │
   │  vs-gold with ink/token-IoU + Panoptic Quality (NOT area-IoU)             │
   └───────────────┬──────────────────────────────────────────────────────────┘
                   ▼
   ┌─ HUMAN, MINIMIZED (active learning) ─────────────────────────────────────┐
   │  auto-accept agreers · review only flagged/uncertain · structural ops ·   │
   │  saves to gold dir (rerun-safe).  Effort ∝ uncertainty, not page count.   │
   └───────────────┬──────────────────────────────────────────────────────────┘
                   ▼
   ┌─ LEARN (the flywheel) ───────────────────────────────────────────────────┐
   │  audit gold (Confident Learning) → promote clean pages to few-shot →      │
   │  when gold ≥ threshold: fine-tune/distill a cheaper student → DROP         │
   │  few-shot → re-benchmark → if it wins, it becomes the labeler. Repeat.     │
   └───────────────────────────────────────────────────────────────────────────┘
```

---
## 2. Component A — the labeling engine

**Two-pass, count-documents-first, region-then-relations** is the right shape (high-recall pass 1 →
high-precision relations pass 2; PLAYBOOK §1). The levers that move quality, in measured order of impact:

1. **Model capability dominates geometry-bound tasks.** Precise polygons on faint/dense scans is
   capability-bound; the cheapest tier underperforms badly. Measured here: flash-lite→3.5-flash cut hard
   errors ~77% on hard pages. **Always A/B the actual models** — newer-smaller beat older-bigger
   (3.5-flash ≥ 3.1-pro) and was cheaper. Don't assume the biggest model wins.
2. **Page-type routing beats volume routing on mixed corpora.** Select few-shot by *page type*, not by
   source volume; relax any multi-doc quota for single-doc types. Measured: a notebook page went from
   3/12 → 12/12 same-type demos; the cheap model's errors dropped ~62%. The two levers (model, routing)
   are **substitutes** — a strong model already "knows" the conventions, so routing mainly rescues the
   cheap path and mixed/streamed volumes. Keep routing ON with a **regression-safe fallback**: only
   confident "strong" types route; unsure → previous (same-volume) behavior, so it can never hurt.
3. **Demonstrations beat prompt rules.** Exemplar correctness materially moves structured-extraction
   output (the "labels don't matter for ICL" folklore does NOT transfer to structured/grounding tasks).
   Curate a clean, internally-consistent, type-balanced few-shot pool; gate inconsistent pages out.
   Prefer fixing/adding demonstrations over piling on prompt rules (which overcorrect — PLAYBOOK §3.8).
4. **Self-consistency / ensembles** (next experiment): sample N× (temp>0) and fuse boxes with
   **Weighted Boxes Fusion**. Research: consensus "consistently improves recall, calibration, robustness,
   especially for rare/ambiguous categories," and **a small model + self-consistency can match a
   substantially stronger one** ([WBF/consensus](https://www.mdpi.com/2076-3417/15/24/12961)). Two bets:
   cheap-model×N+WBF to rival the strong model at lower cost; strong-model×N+WBF to push recall→1.
5. **Geometry post-processing**: snap-to-ink (grow-only), coverage backstop (re-detect uncovered ink on
   the model's OWN output → re-prompt → extend same-category neighbour or add). These are content-driven,
   portable, and remove whole error classes without touching the model.

---
## 3. Component B — measurement done right (where most projects, and my first pass, go wrong)

**The single biggest methodological lesson of this project:** *area-IoU vs a single human's gold is the
wrong yardstick.* It conflates (i) real errors, (ii) granularity choices, (iii) **box-jitter artifacts**
(a tiny box shifted a few px drops below IoU 0.5 and is double-counted as remove+add), and (iv) gold
noise. Measured here: ~45% of a naive "hard error" count was artifact (`page_035`'s "6 errors" were 3
ditto-date boxes counted twice). Fix the measurement before chasing the model.

- **Use ink/token-IoU + Panoptic Quality, not area-IoU.** Document objects rarely overlap (Manhattan
  layout), so the right match metric is **IoU over the tokens/ink contained**, not bbox area
  ([doc panoptic](https://link.springer.com/chapter/10.1007/978-3-030-86331-9_1)). **Panoptic Quality =
  Recognition Quality × Segmentation Quality** cleanly separates the two questions I conflated: *did you
  find the region* (RQ) vs *how tight is it* (SQ). Report both; never collapse to one number.
- **Calibrate against the inter-annotator ceiling, not 100%.** DocLayNet measures the human ceiling as
  pairwise inter-annotator mAP@[.5:.95] and notes **models trail human agreement by ~10%**
  ([DocLayNet](https://arxiv.org/pdf/2206.01062)). A single-annotator corpus is internally inconsistent
  (~⅓ of residual "errors" are gold contradictions — PLAYBOOK §3.2). **Target the ceiling, then stop
  tightening and start relabeling.**
- **Gold-free signals first (they need no labels and scale to a stream):**
  - **Ink coverage** — every ink pixel covered exactly once; over/under-coverage detectable from the
    image alone (this is why the schema is geometry-only).
  - **Committee disagreement (Query-by-Committee)** — two independent labelers (e.g. flash-lite +
    3.5-flash, or N temp>0 samples) disagreeing = likely error/ambiguity. Measured: top-half-disagreement
    pages held **79% of real errors**; 13/43 pages had zero disagreement (auto-accept). This is the
    backbone of both triage (Component C) and the auto-accept gate (Gear G2/G3).
  - **VLM-as-judge** — a strong model views the **rendered overlay + per-box text tags** and flags loose/
    mis-classed/missing boxes ([ClipGrader 91%](https://arxiv.org/pdf/2503.02897);
    [VLM-judge](https://arxiv.org/html/2510.03376v1)). **Critical:** judging *raw coordinates* gives ~0.32
    recall — you MUST render the boxes and attach text tags. Catches *semantic* errors geometry can't.
- **Label-error detection on the GOLD itself.** As gold accumulates it must be audited, not trusted.
  **Confident Learning / cleanlab's object-detection module** finds missing boxes, wrong classes, and
  poorly-drawn boxes; it flagged ~80% of injected errors and cleaning lifted mAP 16–46%
  ([cleanlab OD](https://docs.cleanlab.ai/v2.6.6/tutorials/object_detection.html),
  [CLOD](https://arxiv.org/pdf/2211.13993)). "Both models agree but differ from gold" (e.g. `page_035`)
  is the highest-yield gold-audit signal.

---
## 4. Component C — the human-in-the-loop, minimized

The human pass never disappears; the goal is to make it **proportional to uncertainty, not to page
count** (this is literally what moves you down the gears). Mechanisms, in order:

1. **Auto-accept the confident pages.** Where the committee agrees AND ink coverage is complete AND no
   schema/overlap flags fire → accept without human eyes. (Validate the gate on held-out gold first;
   watch for the "both wrong, both agree" miss — cover it with periodic sampling, Gear G3.)
2. **Triage the rest by informativeness** (active learning). Surface highest-disagreement / lowest-
   confidence pages first; a fixed review budget then catches the most errors. Verbalized model
   confidence is overconfident — prefer **sample-consistency / disagreement** as the uncertainty signal
   ([UQ survey](https://arxiv.org/pdf/2510.20460)).
3. **Make each correction one click** and **structural ops cheap** (Merge/Split docs, Extend/Add box,
   persisted dismissals, auto-zoom-to-flag). Precision in the queue matters more than recall — every
   shown flag must be worth a glance, or the queue gets ignored.
4. **The iron rule:** the editor reads model output but **saves to a separate gold directory** — a rerun
   can never destroy human work. (Verified here: reruns write `auto_labeled/`, never `reviewed/`.)
5. **Active learning doubles as data selection.** The pages the human *does* correct are, by construction,
   the most informative ones — exactly what most improves the next model. Human effort and training value
   are the same currency.

---
## 5. Component D — the gold flywheel & model improvement (the heart of "reduce labeling over time")

This is the self-training / **Noisy-Student** loop ([Noisy Student](https://sh-tsang.medium.com/review-noisy-student-self-training-with-noisy-student-improves-imagenet-classification-2e4e0acb7358))
specialized to grounding, with a human verifier in the loop:

```
teacher (few-shot strong VLM) ─labels→ candidate ─human verify (triaged)→ GOLD↑
        ▲                                                                    │
        │ becomes new teacher if it wins                                     │ accumulates + audited
   STUDENT (tuned cheaper model, few-shot DROPPED) ◄── fine-tune/distill ────┘
```

- **Stage 1 — promote, don't tune (now → ~150 pages).** Every verified page is a candidate few-shot
  example. Audit (Confident Learning), then promote clean, type-balanced pages into the pool; prune
  contradictory ones. This improves the *few-shot* teacher with zero training cost. (Solanus today: only
  4 mass_card / 5 "other" examples — promote corrected ones to close the gap.)
- **Stage 2 — fine-tune / distill a student (~150–500 pages).** Two equivalent framings that converge:
  *supervised fine-tuning* on verified gold, and *knowledge distillation* of the strong few-shot teacher
  into a cheap student via its pseudo-labels ([VLM-KD](https://arxiv.org/abs/2408.16930)). The payoff is
  not only accuracy — **Vertex SFT explicitly lets you drop the few-shot examples after tuning**, and
  prompt internalization ([PromptIntern](https://arxiv.org/pdf/2407.02211)) shrinks the ~30k-char prompt,
  so the student is **cheaper + faster + more consistent** (conventions baked in, not re-derived from 12
  images each call). Inference ≈ 1.5× base; train = dataset-tokens × epochs (3–5).
- **Stage 3 — iterate with replay & drift triggers.** Re-tune on the bigger, cleaner set, but **mix in
  old gold (replay)** to avoid forgetting earlier page types ([continual FT](https://arxiv.org/html/2603.09892v1)).
  **Trigger retraining by signal, not calendar:** rising human-correction rate or disagreement rate on
  new documents = distribution drift = time to re-tune (adaptive > fixed interval). Confidence-filter
  pseudo-labels (Noisy Student drops teacher predictions below ~0.3 as out-of-domain).
- **Decision gates (prove the student before promoting it):** at each stage, **A/B the student (no
  few-shot) vs the few-shot teacher on a held-out volume**, scored with the §3 metric. Promote only on a
  win at materially lower cost/latency. Lock conventions before tuning (re-tuning on drifting conventions
  burns train cost — PLAYBOOK §3.7).

**Net trajectory:** few-shot-strong (now) → audited few-shot-strong + promoted examples → tuned-cheap
(few-shot dropped) → periodically-re-tuned-cheap. Cost/page and human-min/page both fall each turn.

---
## 6. Cost engineering (so "use the better model" and "label a stream" stay cheap)

Output tokens cost ~6× input; the repeated prompt+examples dominate input. Levers (Gemini-specific,
[pricing 2026](https://ai.google.dev/gemini-api/docs/pricing)):
- **Implicit context caching — ~90% off repeated input, ON by default for paid projects** (cache hits
  ~$0.15/M vs $1.50/M for 3.5-flash). Our ~30k-char system prompt + 12 few-shot images are identical
  across pages → put the *stable* content first so it caches; this **largely dissolves the 3.5-flash cost
  premium** ([caching](https://findskill.ai/blog/gemini-api-pricing-guide/)).
- **Batch API — 50% off, ≤24h latency.** Labeling a volume is not latency-critical → batch it.
- **Constrain output** (structured schema, no chain-of-thought in output; thinking budget 0 on tuned
  tasks). **Model-tier by difficulty:** cheap classifier for page-type; strong model only for labeling;
  later, the tuned student for everything.
- Combined, caching + batch can take 3.5-flash from ~$4/volume toward well under $1 — verify empirically.

---
## 7. Reproducibility (so results are trustworthy and portable across scenarios)

- **Version everything that affects output:** code (git), prompts, the few-shot pool, the model name,
  the seed, and each run's config — and the **gold sets + tuned models** (large files → DVC-style
  pointers, not git) ([DVC](https://doc.dvc.org/user-guide)). Emit a per-run **manifest** (model, prompt
  hash, few-shot ids, seed, cost) next to the output.
- **A determinism contract, with its limits stated:** seed the few-shot RNG; know that temp=0 is *not*
  bit-reproducible across pool changes and that box-ids are fresh per run (never id-diff two runs — match
  spatially with ink-IoU). Compare full-run to full-run (a `--start N` partial run reshuffles selection).
- **A frozen held-out eval volume** that is NEVER in any few-shot pool or training set — the only honest
  signal. Report held-out separately from in-pool (here: the win was −52% on held-out, proving it wasn't
  leakage). **Prove per-item holdout** in few-shot selection (one line; the most common silent eval bug).
- **One change per A/B, checkpoint on disk before every mutation** (code/prompts/labels), and re-measure
  after each. Keep the eval harness itself under test (calibrate detector floors on a real minimal miss).

---
## 8. Generalizing to a new corpus / a live document stream

- **Cold start (new corpus):** design the ontology on DocLayNet's axes — distinct, single-page-
  recognizable, coverage-complete, ~≤20 classes, with confusable-pair disambiguations written into the
  category descriptions from day one. Hand-label ~50–75 seed pages spanning every visual style; audit the
  seed gold (§3) *before* it teaches the model; mine allowed relation-pairs from it. You are in Gear G1.
- **Page-type discovery (don't hand-enumerate types you don't know):** cluster pages by **content-
  agnostic** features. Cheap/no-deps: layout features (projection profiles, ink-density grid, margin-
  column detection) — enough to find the dominant split (here: notebook vs rest, ~70% purity). Stronger:
  **DINOv2/SigLIP image embeddings + HDBSCAN** ([DINOv2+HDBSCAN](https://medium.com/@EnginDenizTangut/%EF%B8%8F-image-clustering-with-dinov2-and-hdbscan-a-hands-on-guide-35c6e29036f2)).
  Use clustering for **discovery + a cross-check on the router**, not as the router itself.
- **Streaming (the general case the user is targeting):** new documents arrive continuously. Run the
  current labeler; **monitor disagreement/correction rate as a drift detector**; route low-confidence /
  novel-cluster pages to the human and high-confidence ones to auto-accept. When drift or a new cluster
  appears, you've effectively re-entered G1 for that *type* only — seed a few examples, and it rejoins the
  auto path. The flywheel (§5) absorbs the stream: more documents → more gold → cheaper better student →
  less human per document. The system gets *less* expensive per page over time, not more.

---
## 9. The maturity ladder — how human effort provably falls, with gates

| Stage | Trigger to enter | Labeler | Human role | Exit gate (measured) |
|---|---|---|---|---|
| **G1 assisted** | new corpus/type | few-shot strong VLM | correct most pages | conventions stable; vs-gold (ink-IoU/PQ) at ~ceiling on common type |
| **G2 semi-auto** | ~150 audited gold, triage live | few-shot strong VLM + ensemble | review only flagged (disagreement/coverage) pages | auto-accept gate validated on held-out (≤X missed err / 100 pages) |
| **G3 auto** | tuned student wins held-out A/B | tuned cheap student, few-shot dropped | audit a random sample + escalations | sampled error ≤ inter-annotator noise; drift monitor green |

Effort/page: G1 minutes → G2 seconds-on-a-subset → G3 ~sampling-only. Cost/page falls in parallel
(strong+caching+batch → tuned-cheap). **You never skip the human; you shrink them.**

---
## 10. Application to Solanus — where we are and the next moves
- **Now:** G1→G2 boundary. Shipped: page-type routing (regression-safe), 3.5-flash default (−50%
  full-volume, −77% hard pages), tuning-data exporter (95 gold pages), disagreement/clustering/fair-diff
  prototypes. Over-production is solved; residual = recall-misses + category-calls (some gold noise).
- **Immediate (this session):** (1) ship disagreement triage + auto-accept into QA → unlock G2;
  (2) self-consistency A/B (drive recall-misses down / test cheap-ensemble); (3) promote mass_card/ledger
  gold into few-shot; (4) re-score with ink-IoU/PQ (replace area-IoU); (5) **stand up the fine-tuning
  plumbing and test it end-to-end (do NOT leave a tuned endpoint deployed).**
- **Soon:** Confident-Learning audit of the growing gold; at ~150–250 pages, GATE-1 fine-tune A/B
  (tuned-flash-lite, few-shot dropped, vs few-shot-3.5-flash on a held-out volume).

---
## 11. Annotated references
- **Data engine / flywheel:** SAM 3-gear engine ([MATT/SAM](https://arxiv.org/pdf/2402.11413)); HITL
  scaling ([O'Reilly HITL-ML](https://www.oreilly.com/library/view/human-in-the-loop-machine-learning/9781617296741/OEBPS/Text/08.htm)).
- **Self-training / distillation:** [Noisy Student](https://sh-tsang.medium.com/review-noisy-student-self-training-with-noisy-student-improves-imagenet-classification-2e4e0acb7358); [VLM-KD](https://arxiv.org/abs/2408.16930); [KD-for-detection survey](https://pmc.ncbi.nlm.nih.gov/articles/PMC12788226/).
- **Fine-tuning Gemini:** [Vertex SFT](https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning?hl=en); [data prep](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare); [PromptIntern](https://arxiv.org/pdf/2407.02211); [continual FT / replay](https://arxiv.org/html/2603.09892v1).
- **Measurement:** [DocLayNet](https://arxiv.org/pdf/2206.01062); [doc panoptic / token-IoU](https://link.springer.com/chapter/10.1007/978-3-030-86331-9_1); [Panoptic Quality](https://iq.opengenus.org/pq-sq-rq/); label errors — [cleanlab OD](https://docs.cleanlab.ai/v2.6.6/tutorials/object_detection.html), [CLOD](https://arxiv.org/pdf/2211.13993); VLM-judge — [ClipGrader](https://arxiv.org/pdf/2503.02897), [VLM-as-Judge](https://arxiv.org/html/2510.03376v1).
- **Ensembles / uncertainty / active learning:** [WBF/consensus](https://www.mdpi.com/2076-3417/15/24/12961); [UQ survey](https://arxiv.org/pdf/2510.20460); [active learning](https://encord.com/blog/active-learning-machine-learning-guide/).
- **Page clustering:** [DINOv2](https://arxiv.org/html/2304.07193v2); [DINOv2+HDBSCAN](https://medium.com/@EnginDenizTangut/%EF%B8%8F-image-clustering-with-dinov2-and-hdbscan-a-hands-on-guide-35c6e29036f2).
- **Cost / reproducibility:** [Gemini pricing/caching/batch](https://ai.google.dev/gemini-api/docs/pricing), [batch 50%](https://apidog.com/blog/gemini-api-batch-mode/); [DVC](https://doc.dvc.org/user-guide).
