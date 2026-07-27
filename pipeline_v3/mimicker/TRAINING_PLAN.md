# The top-of-the-line run

The recommended build for the Solanus voice mimicker, grounded in
`/Style-Mimickry-Research/style-mimickry-report.md` and adapted to our corpus (~124k words) and your GPUs.
The report's verdict: **a unified LoRA/QLoRA fine-tune on bootstrapped synthetic pairs + a
reconstruction/continuation objective, with RAG over his writings to ground stance at inference.** We add
general-instruct **replay** to limit forgetting, and stage an optional research layer.

## Why this shape (not the alternatives)
- **Not a pure prompt/frozen model:** prompting does style "okay," fine-tuning is the ceiling (report
  §1–2). We keep a frozen baseline (`generate.py`) only as the *floor* to measure against.
- **Not a two-translator pipeline:** it washes out exactly the fine-grained idiosyncrasy we want and
  *cannot reproduce opinions* (report §4, Approach 1).
- **Unified model, de-risked:** catastrophic forgetting → replay + LoRA (forgets less on small domain
  shifts); confabulated opinions → RAG grounding + demurral; drift → optional persona-vector monitoring.

## Stage 0 — data (do regardless of model)
1. `extract_corpus.py` → his 511 letters.
2. `data_prep.py` → segments (40–200w), whole-letter causal set, situation→letter instruction set,
   stylometry profile.
3. `data_prep.py --embed` → RAG index (`mimic::gemini-embedding-001@1536`) + **style centroid** (the
   neural target for the gate and for evaluation).
4. `data_prep.py --synth` → STRAP neutral→Solanus pairs, **filtered** by centroid cosine ≥
   `PAIR_STYLE_GATE` (copies the 2026 controllable-intensity quality gate from the report).

**Corpus is small (124k words).** That is ample for *surface style* but thin for *stance coverage* — which
is exactly why stance is grounded by retrieval at inference, not trusted to the weights.

## Stage 1 — baseline (hours)
`generate.py "<prompt>"` (frozen model + retrieved exemplars) → run `evaluate.py` on the outputs to set the
floor on centroid cosine / stylometry. Expect mediocre idiosyncrasy; this calibrates the gap.

## Stage 2 — the core run (the "top" profile)
`python train_qlora.py --profile top --replay tatsu-lab/alpaca`

- **Base model:** `meta-llama/Llama-3.3-70B-Instruct`, **QLoRA (4-bit nf4)**, LoRA r=32/α=64 on all
  attention+MLP projections. Fits on a multi-GPU box (≈2×48GB or 1×80GB with `device_map="auto"`); drop to
  `--profile fast` (Llama-3.1-8B) on a single 24GB card to validate first.
  - *Swap-friendly:* Qwen3-32B / Mistral-Small are fine substitutes — change `base_model` in `config.py`.
- **Mixture:** whole letters (rhythm/lexicon) + situation→letter pairs (first-person voice) + synthetic
  neutral→styled pairs (controllable restyling) + ~15% general-instruct **replay** (`REPLAY_FRACTION`).
- **Schedule:** 4 epochs, cosine LR 1e-4, effective batch 16 (bs1 × grad-accum16), seq 2048, gradient
  checkpointing, bf16.
- **Watch:** train loss + a few held-out letters; checkpoint per epoch; pick the checkpoint that best
  trades centroid cosine against `evaluate.py` mean|z| (don't over-train into parroting).

## Stage 3 — serve, grounded
`generate.py "<prompt>" --adapter solanus-top` — base+adapter, RAG over his letters, persona that
**demurs** where passages are silent. This is the honest artifact: style from weights, stance from retrieval.

## Stage 4 — research upside (optional, after a solid Stage 2/3)
- **Steering:** a StyleVector-style direction (authentic − style-agnostic activations) or an
  Anthropic-style persona vector to monitor/resist drift under sycophantic pressure. Add-on, modest
  magnitude (steering vectors are documented to be uneven).
- **Horikawa-style refinement:** evolve a draft with masked-LM edits scored by cosine to the style
  centroid (+ a base-LM fluency term). The report flags this exact loop toward a *style/belief embedding*
  as an unexplored, novel contribution — pilot it as a final polish, not the backbone.

## Evaluation (build before scaling)
- **Style:** `evaluate.py` (centroid + nearest-exemplar cosine; stylometry z-scores). Harden the AA
  discriminator with scikit-learn (real classifier: can it separate clone vs. held-out real letters?).
- **Stance faithfulness:** a held-out opinion-QA set where ground truth is his attested position; measure
  grounding to retrieved passages, a sycophancy stress test, and **abstention** accuracy on topics he never
  addressed (penalize confabulation).
- **Capability retention:** spot-check a reasoning/instruction benchmark before/after to quantify the
  alignment tax; if it drops sharply, lower LoRA rank / raise replay / consider proxy-tuning.
- **Human judgement is required** — no single automatic metric is reliable; have someone who knows his
  letters rate voice + faithfulness.

## Decision thresholds (from the report)
- AA classifier trivially separates clone from real → strengthen synthetic-pair SFT, add Stage 4 refiner.
- Reasoning benchmark drops sharply → move toward proxy-tuning / decoding-time steering (keep base frozen).
- Confabulation/sycophancy high → tighten RAG grounding + abstention, add persona-vector steering.

## Cost / hardware notes
- Data-prep API spend is small (a few hundred Gemini calls for `--synth`; `--embed` is one pass over ~1–2k
  segments). Everything heavy is on your GPUs.
- `--profile fast --max-steps 30` first — it shakes out tokenizer/template/OOM issues in minutes before
  you commit a multi-hour 70B run.
