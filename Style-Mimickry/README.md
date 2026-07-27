# Style-Mimickry — writing in David's voice, **without fine-tuning**

A small toolkit that makes a *frozen* large language model write in David's voice, and then
**measures** whether it actually did. David forbade fine-tuning, so there is no training anywhere in
here: every drop of "style" is carried in the prompt (retrieved real examples of his writing) and
judged from the outside (embedding distance + interpretable stylometry).

It reuses the `step_7` libraries wholesale — the same `config`, cost logging, embedding/LLM
adapters, and vector store that power the Solanus archival RAG tool — so a style index is the exact
same shape as a step_7 retrieval index, and swapping the model is a one-line `config.py` change.

> **TL;DR.** `data_prep.py` cuts David's writings into bite-sized **exemplars**, embeds them into a
> **RAG index**, and computes a **style centroid** (the center of his voice in embedding space).
> `generate.py` retrieves the exemplars nearest a new prompt, shows them to a frozen LLM, and asks
> it to answer in that voice (with two more advanced, *gated* layers behind it). `evaluate.py` scores
> the result three ways: neural cosine to the centroid, an interpretable stylometry dashboard, and an
> authorship-attribution discriminator. **Nothing is billed until you pass `--embed` / `--run`.**

---

## Why no fine-tuning changes the whole design

The research report's strongest bet is a fine-tuned hybrid. David ruled that out, so we keep only the
parts that work on a **frozen** model:

| Report idea | Needs training? | In this toolkit? |
|---|---|---|
| Synthetic neutral→styled pairs + SFT / QLoRA | **yes** | ❌ omitted (forbidden) |
| Few-shot + **retrieved exemplar** prompting | no | ✅ `generate.py` Layer 1 |
| RAG over the person's own writing | no | ✅ `data_prep.py` index |
| Style **centroid** as a target (LUAR/StyleDistance) | no | ✅ `data_prep.py` + `evaluate.py` |
| **StyleVector** activation steering | no (but needs hidden-state access) | ◐ scaffolded, prompt-proxy now (`generate.py` Layer 2) |
| **Horikawa** iterative MLM refinement toward the centroid | no | ◐ scaffolded + gated (`generate.py` Layer 3) |
| Interpretable LIWC/stylometry audit | no | ✅ `evaluate.py` |
| AA discriminator | yes (small classifier) | ◐ stub + free fallback (`evaluate.py`) |

With a frozen model you cannot bake style into the weights, so you must **carry it in the prompt**
(exemplars) and **measure it from outside** (centroid + stylometry). That is exactly this layout.

---

## The three files

### `data_prep.py` — Stage 0: build the raw materials (free unless `--embed`)
1. Reads David's writings (the `.tex` explainers in `examples/writing_examples`, with the LaTeX
   scaffolding stripped so the embedder sees prose, not `\usepackage` noise).
2. Segments them into **exemplars** of ~40–180 words — the band where style features are stable
   (the LICW report warns count features are noise below ~100 words and diluted above).
3. With `--embed`: embeds every exemplar into a `step_7` vector-store partition (`style::model@dim`)
   and saves the **style centroid** (mean unit vector) + per-axis spread.

```bash
python data_prep.py            # FREE: segment + write exemplars.jsonl + style_profile.json
python data_prep.py --embed    # PAID: also embed -> RAG index + style_centroid.npy
```

### `generate.py` — write in David's voice with a frozen LLM (dry-run unless `--run`)
- **Layer 1 (runs now):** retrieve the `k` exemplars nearest the prompt, few-shot them to a frozen
  LLM (`lib.providers.llm.generate`), ask for the answer in David's voice.
- **Layer 2 (`--steer`, prompt-proxy now):** a StyleVector-style "style direction," described in
  words and injected into the prompt. The true activation-space version needs an open/local model
  and is a documented TODO.
- **Layer 3 (`--refine`, gated stub):** the Horikawa mask→MLM→select loop that evolves the draft
  toward the centroid. The *fitness function* (cosine-to-centroid × length penalty) is real; the
  RoBERTa-large proposer is a stub and the loop is `enabled=False` by default.

```bash
python generate.py "Explain entropy to a beginner."            # DRY RUN (no API calls)
python generate.py "Explain entropy to a beginner." --run      # PAID: actually generate
python generate.py "..." --run --steer --refine                # add Layer 2 + (gated) Layer 3
```

### `evaluate.py` — does it sound like David? (stylometry/discriminator free; `--neural` to embed)
1. **Neural cosine** (`--neural`, primary): cosine of the candidate's embedding to David's centroid,
   plus cosine to the nearest individual exemplar (so a bland output can't hide near the average).
2. **Stylometry dashboard** (free): per-feature **z-scores** vs. David's profile — function-word
   rates, sentence-length mean/variance, punctuation, TTR, word length. Reads like
   "sentences too long (z=+1.2), too few 'I' (z=−2.4)."
3. **AA discriminator** (free fallback; real one is a TODO): can a judge tell clone from David? The
   honest classifier needs scikit-learn; until then a transparent nearest-centroid heuristic returns
   a "clone-probability."

```bash
python evaluate.py                       # FREE: sanity-check on a real exemplar (should score David-like)
python evaluate.py "some generated text" # FREE: stylometry + discriminator
python evaluate.py "..." --neural        # PAID: also embed for the cosine metric
```

---

## Install / run

Use the **step_7 venv** (it already has `numpy`, `fastembed`, `google.genai`, `dotenv`, `tenacity`):

```bash
/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/step_7/venv/bin/python data_prep.py
```

Optional, for the fuller (still-free) features later:
- `spacy` → POS/dependency-n-gram stylometry (currently a TODO; we use dependency-free features).
- `scikit-learn` → the real AA discriminator (currently a stub + heuristic fallback).
- `transformers` + RoBERTa-large (~1.4 GB) → the Horikawa Layer-3 MLM proposer (gated; not run).

---

## Cost & safety

- **Every** model call routes through `lib.costlog`, so embeddings/generations land in
  `step_7/costs/usage.csv` exactly like an archival run.
- **Free by default:** `data_prep.py` (no `--embed`), `generate.py` (no `--run`), and
  `evaluate.py` (no `--neural`) make **zero** API calls.
- **Non-destructive:** everything is written under `Style-Mimickry/data/`; David's source writings
  are only ever read.
- **No training, no fine-tuning, no large downloads** happen anywhere in this toolkit.

---

## What's deferred (and why)

| Deferred piece | Where | Why it's gated |
|---|---|---|
| Embedding the exemplars | `data_prep.py --embed` | first **paid** step; David hasn't said "go" |
| Live generation | `generate.py --run` | **paid** frozen-LLM call |
| True StyleVector steering | `generate.py` Layer 2 TODO | needs an open model with hidden-state access |
| RoBERTa-large MLM proposer | `generate.py` `_mlm_propose` | ~1.4 GB download, not approved |
| Trained AA discriminator | `evaluate.py` TODO | needs `scikit-learn` (not in venv) |
| spaCy syntactic stylometry | `evaluate.py` TODO | needs `spacy` (not in venv) |

See `docs/mimicker.md` (and the LaTeX/PDF `docs/mimicker.tex`) for the full, voice-matched
explanation of how and why it works.

---

## Sources

Built from `Style-Mimickry-Research/style-mimickry-report.md` (the architecture report) and
`Style-Mimickry-Research/LICW-incorporation.md` (the interpretable-stylometry report). Key methods:
STRAP (Krishna et al. 2020), LUAR (Rivera-Soto et al. 2021), StyleDistance (Patel et al. 2025),
StyleVector (Zhang et al. 2025), ASTRAPOP (Liu et al. 2024), Horikawa "mind captioning" (2025),
LIWC-22 (Boyd et al. 2022), Writeprints (Abbasi & Chen 2008), MTLD (McCarthy & Jarvis 2010).
