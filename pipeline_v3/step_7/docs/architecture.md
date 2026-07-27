# step_7 architecture — the archival tool, end to end

**TL;DR.** step_7 turns the cleaned, segmented corpus from step_6 (letters in `documents.json`, notebook pages in `notebooks.json`, gold page JSON + masked images under `3_enriched/`) into a queryable archival research tool: it runs **NER → resolve → enrich → graph** to build a temporal knowledge graph, and **chunk → embed/index** to build the retrieval surfaces (a partitioned vector store, a SQLite full-text index, IIIF manifests, and text-layer searchable PDFs), and then an app serves a citing, tool-using RAG agent on top of all of it. Two design ideas hold the whole thing together. First, every expensive choice — *which* generation LLM, *which* embedding model and dimension, *which* reranker — is a **variable in `config.py`**, not hard-coded, so you can A/B two models without touching pipeline code. Second, the build is a **diff-and-rerun DAG** (`lib/pipeline.py`): each stage is content-hashed over its code, the config, its inputs, and its upstream stages, so editing one early artifact automatically marks everything downstream stale and reruns *exactly* what changed and nothing else. Nothing runs on import and nothing is billed until you explicitly run a stage; every model call is cost-logged to `costs/usage.csv`.

---

## Skip-if-you-know-this: what a "diff-and-rerun pipeline" is

If you've used `make`, this will feel familiar — skim and move on.

Imagine you have a chain of expensive steps: extract names from 9,000 documents, cluster them, build a graph, embed everything. Step 3 depends on step 2, which depends on step 1. Now you fix a bug in step 1. You want step 1 to rerun, and *because its output changed*, you want 2 and 3 to rerun too — but you do **not** want to re-embed the corpus if you only edited the graph builder. Re-running everything every time is slow and, when the steps call paid APIs, expensive. Re-running by hand is error-prone: you forget that the graph depended on the dates file you just regenerated.

The fix is to make the pipeline figure this out for you. You give each step a **fingerprint** — a hash that summarizes everything the step's output depends on. If the fingerprint matches what's on disk from last time, the step is "fresh" and gets skipped. If anything it depends on changed, the fingerprint changes, the step is "stale," and it reruns — and so does everything downstream of it. That's the entire idea. The rest of this doc is the specific way step_7 implements it and the specific stages it wires together.

---

## The big picture: how the stages fit together

The corpus flows through step_7 along two tracks that meet in the app.

```
                 step_6 outputs
        documents.json  notebooks.json  3_enriched/**
                          │
        ┌─────────────────┴──────────────────────────────────┐
        │                                                      │
   STRUCTURE / GRAPH TRACK                            RETRIEVAL TRACK
        │                                                      │
  stitch_notebooks  (A1: cross-page entries)          chunk (lib/chunks.py)
  normalize_dates   (A2: EDTF + bitemporal)                    │
        │                                          embed_corpus → data/vectors/<space>/
  extract_entities  (NER: free harvest + opt LLM)   index_sources → fts.sqlite + IIIF
        │                                                          + searchable PDFs
  resolve_entities  (A3: normalize/block/cluster)            │
        │                                          contextualize_chunks  (Tier-0, PAID)
  enrich_entities   (grounded context, PAID)        colpali_index         (Tier-3, heavy)
        │                                          raptor                 (Tier-3, PAID)
  build_graph       (RiC-O temporal KG: graph.json + graph.ttl)
        │                                                      │
        └─────────────────────┬───────────────────────────────┘
                               │
                       app/ (FastAPI + SPA)
        lib/retrieval.py — hybrid BM25 + dense + RRF + rerank + grade
        tool-using RAG agent → cited answers (doc/rid/page) + graph + timeline
```

The **graph track** is about *facts and structure*: who is mentioned, where they are, when things happened, and how the entities relate. The **retrieval track** is about *finding the right passage to read*. They are complementary — the graph answers "show me Solanus's correspondents" by traversing edges; retrieval answers "what did the Panyard family write about" by finding and reading the actual letters. The app exposes both, and the RAG agent can lean on whichever fits the question.

Everything downstream of step_6 is **non-destructive**: stages write *new* artifacts into `data/` (gitignored), and the runner never deletes anything. If a stage's output looks wrong, you fix the stage and rerun; the new run overwrites that stage's own artifacts, but no source data is ever mutated in place.

---

## The diff-and-rerun engine (`lib/pipeline.py`)

This is the ~165-line core that makes reruns cheap and correct. Two dataclasses do all the work.

A **`Stage`** declares four things:

- `name` — a unique id (e.g. `"build_graph"`).
- `fn` — a zero-arg callable that does the work.
- `deps` — names of upstream stages it depends on.
- `inputs` — path **globs** it reads (e.g. `documents.json`, and — importantly — *its own source file*).
- `outputs` — paths it writes (used both for reporting and for a "is the output even there?" check).

A **`Pipeline`** holds the stages and computes a **fingerprint** per stage. The fingerprint is a SHA-256 over four things (see `Pipeline.fingerprints()`):

1. **The source code of `fn`** (`inspect.getsource`). Edit the stage's logic → it goes stale. This is why each stage lists `ST("stage_name")` — its own `.py` file — in its `inputs`: a code edit changes both the source hash *and* an input hash.
2. **The config fingerprint** (`config.fingerprint()`). Swap a model or change the embedding matrix → dependent stages go stale.
3. **The contents of every input file** (`_inputs_fp` hashes each matched file). New data → stale.
4. **The fingerprints of all upstream stages.** This is the key recursive bit: if an upstream stage's fingerprint changes, this stage's fingerprint changes too, *transitively*, all the way down the chain.

A stage is **stale** when (a) its fingerprint differs from the one stored in `_pipeline_state.json`, OR (b) one of its declared outputs is missing, OR (c) an upstream stage actually re-ran this pass. That third condition matters: `run()` recomputes fingerprints *as it goes* (`fp_now = self.fingerprints()[name]`), so when an early stage writes a new output, the downstream stages see the changed input and rerun in the same pass. Refining an early artifact propagates all the way up — exactly the behavior you want.

`status()` is read-only: it prints the diff (what's stale and *why* — "never run", "inputs/code/config changed", "output missing"). `run()` executes stale stages in **topological order** (`topo()`, a depth-first sort that also detects dependency cycles and unknown deps), and after each successful run records `{fingerprint, ran_at, secs}` to the state file. Flags: `--only` restricts to a subset (still respecting order), `--dry` reports what *would* run without executing, `--force` reruns regardless of freshness.

It's **stdlib only** — no build-system dependency to install. The state lives in one JSON file you can read or delete.

### The CLI (`run.py`)

`run.py` is where the DAG is *declared* and driven:

```
python run.py status     # the diff — what's stale and why (read-only)
python run.py graph      # print the dependency DAG + each stage's note
python run.py run        # run all stale stages + everything downstream
python run.py run --only embed_corpus
python run.py run --dry  # report what WOULD run, execute nothing
```

`build_pipeline()` adds every stage with its deps, inputs, outputs, and a one-line `note`. Crucially, **`run.py` wires each stage to its FREE/SAFE default path** so a blanket `python run.py run` never spends money or downloads heavy models. The expensive passes are opt-in. The pattern shows up three ways in the DAG declaration:

- `extract_entities.run` defaults to `execute=False` — the **free structured-field harvest** only; the paid LLM NER pass is `run(execute=True)`.
- `enrich_entities` and `contextualize_chunks` are wired as `lambda: ...run(execute=False)` / `run(dry_run=True)` — the DAG node runs the **free dry-run that previews scope + projected cost**; the real paid pass is run by hand.
- `colpali_index` and `raptor` are wired as `do_build=False` / `execute=False` — **plan-only scaffolds**; the heavy model download / paid summary build is opt-in via the flag.

Nothing runs on import. Stage bodies live in `stages/*.py` and are real, implemented modules.

---

## The model-agnostic axes (`config.py` + `lib/providers/`)

The expensive part of any RAG/graph system is the model choices, and the *right* choice is empirical — you don't know whether `text-embedding-3-large` beats `gemini-embedding-001` on *this* corpus until you measure. So step_7 treats each model as a **swappable variable**. There are three axes, each with a config table and a provider adapter:

| Axis | config table | adapter | entry shape |
|---|---|---|---|
| **Generation LLM** | `config.LLMS` | `lib/providers/llm.py` `generate()` | `{provider, in, out}` ($/1M tok) |
| **Embedding** | `config.EMBEDDINGS` | `lib/providers/embed.py` `embed_texts()` | `{provider, price, dims}` |
| **Reranker** | `config.RERANKERS` | `lib/providers/rerank.py` `rerank()` | `{provider, price}` |

The principle is identical across all three: the caller names a model string; the adapter looks up `config.<TABLE>[model]["provider"]` and dispatches to the matching provider function; every call is **cost-logged**; and providers without an API key present raise a *clear* error rather than failing mysteriously, so the free path never needs a paid key. Concretely:

- **LLMs.** Gemini is wired now (`gemini-2.5-flash` is the default). Claude (via GCP Model Garden) and OpenAI are commented placeholders in the table — adding a row + its adapter branch is the whole change.
- **Embeddings.** Gemini and a local `fastembed` adapter (BGE models, `$0`, offline) work today; OpenAI and Voyage adapters are real but key-guarded. Note the **Matryoshka dimensions**: `gemini-embedding-001` can emit 768/1536/3072-dim vectors from the *same* model, so each `(model, dim)` pair is its own selectable space.
- **Rerankers.** A reranker is a second-stage judge: first-stage retrieval casts a wide net cheaply (retrieve ~50), then a cross-encoder re-reads each (query, passage) pair *together* for a sharper final order (rerank to ~5). Local BGE cross-encoder is free; Voyage/Cohere are paid + key-guarded. The app falls back to the free local reranker when a paid key is missing (logged), so the model-agnostic promise holds without keys.

The runtime selections live in `config.DEFAULTS` (`llm: gemini-2.5-flash`, `embedding: [gemini-embedding-001, 1536]`, `reranker: rerank-2.5`). Want to A/B? Change a default, or pass a different space string to the retriever.

### One clever trick: alias spaces

Look at `gemini-embedding-001-ctx` in `config.EMBEDDINGS`. It carries an `"api": "gemini-embedding-001"` override. This is an **alias**: a *distinct config key* (so it gets its own vector-store partition and its own cost line) backed by the *same real model* (so the embed adapter calls `gemini-embedding-001` under the hood, honoring the `api` override in `_gemini()`). It exists to hold **Contextual Retrieval** embeddings — chunks with an LLM-written situating blurb prepended before embedding — so the contextualized space sits *side by side* with the base space and you can A/B them directly. (And we did: contextualization lifted answer faithfulness from 0.63 to 0.88 on 12 golden questions.)

### The fingerprint connection

`config.fingerprint()` hashes `LLMS`, `EMBEDDINGS`, `RERANKERS`, `EMBEDDING_MATRIX`, and `DEFAULTS`. That hash feeds every stage's fingerprint. So the model-agnostic axes and the diff-and-rerun engine are tied together: **editing a model price, adding an embedding space, or changing a default marks the dependent stages stale automatically.** Add a tuple to `EMBEDDING_MATRIX` and only that new space (re)builds.

---

## Cost logging (`lib/costlog.py`)

Cost tracking is first-class, not an afterthought — every paid call routes through `costlog.log(...)`, which appends a timestamped row to `costs/usage.csv` (`ts, provider, model, op, input_tokens, output_tokens, items, usd, meta`). If you don't pass `usd`, it's `estimate()`d from the config price tables (per-1M-token math). The module is thread-safe (a lock around the append), so the parallelized bulk loops — NER, contextualization — can all write to the same ledger from 16 workers without corrupting it.

Two query-time helpers matter for the app: `snapshot()` returns *running totals* across all models, and `delta(before, after)` subtracts two snapshots to give **one operation's own cost**. That's what a per-query UI should show — "this answer cost $0.011" — rather than the cumulative ledger from `summary()`.

> ⚠️ The prices in `config.py` are **placeholders**. The header says so and so does this doc: confirm against each provider's current pricing page before any billed run. The whole point of logging is that you can see what you spent — but the dollar figure is only as good as the price table.

---

## The stages, in pipeline order

Each stage below is a real module in `stages/`. The DAG note (from `run.py`) is paraphrased; "tier" labels come from the research plan. Free/paid is called out because it determines whether a blanket `run` touches it.

### Graph track

**`stitch_notebooks`** (Tier-1 A1, free) — Notebook entries sometimes run across a page break ("... Cont." on the next page). This stage detects those continuations and merges them into **logical multi-page entries**, so one real entry becomes one unit instead of several fragments. Output: `data/stitched_notebooks.json`. Free rule-based path with a gated LLM tie-breaker for the ambiguous band. (This is *also* why retrieval chunking prefers the stitched entries — one source spanning its pages, fixing the "baked beans → many sources" fragmentation.)

**`normalize_dates`** (Tier-1 A2, free) — Reconstructs real dates from the messy structural truth: an entry's month/day, the page's archival year, and carry-forward logic, producing **EDTF** date strings, plus **bitemporal** enrolled/reported pairs. Output: `data/dates.json`. Free rule path with a gated LLM normalizer. These dates are the spine of the graph timeline.

**`extract_entities`** (Tier-1 NER) — Pulls entity mentions (PERSON, PLACE, CONDITION, RELIGIOUS_TERM, FAVOR, RELATION, …) with a rich nested schema. **Defaults to the free structured-field harvest**; the paid LLM extraction pass is opt-in (`execute=True`) and is parallelized (16 workers, append-only + resumable). Every mention cites `doc/rid/page/vertices` so it can be traced back to the exact source region. Output: `data/entities_raw.jsonl`.

**`resolve_entities`** (Tier-1 A3) — Entity *resolution*: normalize surface forms, **block** candidates, then **cluster** mentions of the same real entity into canonical entities (a calibrated cautious-merge: veto → auto-merge → LLM-gate → abstain, with hard vetoes like "mass ≠ massachusetts" and dates merged only on exact match). Free path; opt-in embedding/LLM clustering + authority reconciliation (Wikidata/VIAF/GeoNames/Getty TGN). Output: `data/entities.json`.

**`enrich_entities`** (Tier-1.5, **PAID**) — One LLM context call per significant entity, producing a grounded blurb: clean name + description + relation-to-Solanus + location. The DAG runs the **free dry-run** that previews scope and projected cost; the real pass is opt-in. Output: `data/entities_enriched.json`. (A sibling `geocode_places` stage attaches coordinates.)

**`build_graph`** (Tier-2) — Assembles the **temporal knowledge graph aligned to RiC-O** (Records in Contexts). It folds in resolved entities, normalized dates, and the enrichment blurbs (as node descriptions), and emits both `data/graph.json` and `data/graph.ttl` (RDF/Turtle). Nodes are people/places/conditions/favors/dates/orgs/records; edges are `mentioned_in`, `has_condition`, `enrolled`, `wrote_to`, `located_at`, `continues_on`, `dated_in` — each carrying provenance record ids and valid-time. Depends on `resolve_entities`, `normalize_dates`, and `enrich_entities`.

### Retrieval track

**`embed_corpus`** (M1/M2) — Chunks the corpus (`lib/chunks.py`: one chunk per letter, one per *logical* notebook entry, with full provenance metadata) and embeds it into **one vector partition per space** (`data/vectors/<model@dim>/` via `lib/vectorstore.py`). Default spaces come from `config.EMBEDDING_MATRIX`. A `--contextualized` mode embeds the Contextual-Retrieval chunks into the `…-ctx` alias space. The vector store is a simple partitioned local store — `vectors.npy` + `ids.json` + `metas.jsonl` per space, brute-force cosine over pre-normalized vectors (instant at ~9k chunks; swappable for LanceDB later without changing callers).

**`index_sources`** (Tier-2/4) — Builds the *non-vector* retrieval surfaces: a **SQLite FTS5** full-text index over page text, **IIIF manifests** (with region annotations for deep-zoom viewers), and **text-layer searchable PDFs** in `searchable_pdfs/`. Output: `data/fts.sqlite` + the PDFs/manifests. This is what lets BM25 (lexical) search and human-facing source viewing work alongside the dense vectors.

**`contextualize_chunks`** (Tier-0, **PAID**) — Anthropic-style Contextual Retrieval: an LLM writes a short situating blurb for each chunk and prepends it before embedding, so a fragment that just says "she was cured" carries enough context ("In a 1933 letter from Mrs. Panyard reporting…") to be retrievable. The DAG runs the **free dry-run plan** (counts + cost estimate, no calls); the real pass is opt-in, parallelized, incremental, and resumable. Output: `data/contextualized_chunks.jsonl`.

**`colpali_index`** (Tier-3, scaffold) — Multimodal *visual* retrieval over page images via ColPali (find a page by what it *looks like*, not just its OCR text). Scaffold wired plan-only; the real index build is opt-in (`do_build=True`, heavy model download). Output dir: `data/colpali`.

**`raptor`** (Tier-3, **PAID** scaffold) — RAPTOR: recursively cluster-then-summarize chunks into a tree, so multi-hop "summarize everything about X across the corpus" questions can retrieve a high-level summary node instead of 50 leaf passages. Depends on `embed_corpus`. Scaffold wired plan-only; real build is opt-in (`execute=True`, paid summaries). Output: `data/raptor_tree.json`.

---

## How retrieval and the app fit on top

The stages above build *artifacts*. `lib/retrieval.py` is the **retrieval brain** that reads those artifacts at query time. Every piece is a pure function returning ranked hits *with provenance*, and every stage is toggleable. A query flows:

1. **Router** — a cheap heuristic decides how hard the question is ("When was Solanus born?" is simple; "List every cancer favor reported in 1933" wants multi-query decomposition). An LLM classifier is available but gated.
2. **Query transforms** — optional rewrites: **HyDE** (ask an LLM to hallucinate a plausible answer, then embed *that* — a fake answer lives closer to the real passages than a terse question does) and **multi-query / RAG-Fusion** (paraphrase, retrieve for each, fuse). Paid → gated.
3. **Hybrid retrieval** — run BOTH BM25 lexical search (nails exact names/dates like "Panyard" or "Nov. 8th") AND dense semantic search (nails paraphrase), then fuse with **Reciprocal Rank Fusion (RRF, k=60)** — no score calibration needed between the two.
4. **Rerank** — optionally hand the fused top-N to a cross-encoder (the reranker axis; local free, paid gated).
5. **Grade / abstain** — Self-RAG / CRAG style: score how trustworthy the retrieved set is from real signals we already have (rank scores, OCR `min_conf` on the source regions, lexical overlap). If it's weak, abstain rather than confabulate — vital for an *archive*, where a confident wrong answer is worse than "not found".

The **FREE path** alone — router-heuristic → BM25 + local dense → RRF → grade — is already a solid hybrid retriever; the LLM/rerank stages are opt-in upside.

The **app** (`app/`, FastAPI backend + a themed SPA in the Capuchin Province palette) puts a face on all of it: a tool-using RAG agent (`/api/query`) that returns **cited** answers (each claim tagged with doc/rid/page), a single-shot RAG endpoint (`/api/ask`) with CRAG abstention, a graph view (`/api/graph`, defaulting to legible *semantic* relations and opening on Solanus's ego-network), a `/api/timeline`, plus walkthrough / how-it-works / explanations tabs. Per-query cost is shown via the `costlog.delta` trick. Model selection — the three axes — is exposed in a settings modal, which is the whole payoff of making them variables: you can flip embedding spaces or rerankers from the UI and watch the answers (and the cost) change.

---

## Putting it together: the rerun story

Here's the architecture in one concrete scenario. Suppose the full NER pass finishes and writes a new `entities_raw.jsonl`. You run `python run.py status`. The engine recomputes fingerprints: `extract_entities` is now fresh (you ran it), but its *output changed*, so `resolve_entities` (which lists that file as an input) is **stale**; `enrich_entities` depends on `resolve_entities`, so it's stale; and `build_graph` depends on both, so it's stale too. `embed_corpus` and `index_sources` are *untouched* — they don't depend on the entity chain — so they stay fresh and skip. You run `python run.py run`, and only the graph track from resolve onward reruns, in topological order, picking up each upstream output as it lands. That's the entire value proposition: **change one thing, rerun exactly what depends on it, and spend money only where you chose to.**
