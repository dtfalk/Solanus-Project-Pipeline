# step_7 — Archival tool (NER · Knowledge Graph · RAG/GraphRAG · dev tool)

Turns the segmented corpus (`step_6/documents.json`, `step_6/notebooks.json`, `step_6/3_enriched/`)
into an **archival research tool** + a **developer/eval platform**. Plan:
`~/.claude/plans/i-owuld-like-you-vast-rossum.md`.

> ⚠️ **PAID stages never run automatically.** The DAG previews their scope/cost; David runs the real
> pass explicitly (`entity_engine.run(execute=True)`, etc.). Stages M1–M3 are implemented and have been
> run (NER → resolve → **entity_engine** → geocode → RiC-O graph → embeddings → indexes); the app is live.
> The pieces below (pipeline engine, cost logging, config) are the stable foundation everything plugs into.

## Design commitments
1. **Diff-and-rerun reproducibility.** Every unit of work is a `Stage` in a content-hashed DAG
   (`lib/pipeline.py`). A stage is *stale* if its inputs, its code, the config, or any upstream
   stage changed. `run()` re-executes stale stages **and everything downstream** — so refining,
   say, the cleaned text re-runs entity extraction → graph → embeddings → indexes and "populates
   all the way up." `status()` shows the diff first. State in `_pipeline_state.json`. Stages are
   **non-destructive** (write new artifacts; the runner never deletes).
2. **Always log prices.** Every model/API call goes through `lib/costlog.py` →
   `costs/usage.csv` (provider, model, op, tokens, $). For test/dev cost tracking.
3. **Model-agnostic, three swappable axes** (all in `config.py`): generation **LLM**, **embedding**
   model (vector store partitioned by `provider/model/dim` → RAG sources from the selected space),
   and **reranker**. A generic local adapter lets any open model plug in.
4. **Provenance-first + archival standards.** Every fact/answer cites `doc_id+rid+page+vertices`
   → IIIF deep-zoom region view + source-PDF download. KG aligned to **RiC-O**; entities reconciled
   to Wikidata/VIAF/GeoNames/Getty TGN; observability via OpenTelemetry/Phoenix.
5. **Expandable.** Add a stage (register in `run.py`), an embedding space (`config.EMBEDDING_MATRIX`),
   a model (`config.LLMS`/`EMBEDDINGS`/`RERANKERS`), or an agent tool (`app/`) without touching the rest.

## Layout
```
step_7/
  config.py            # paths, model registries + prices, embedding matrix, defaults  (edit me to extend)
  run.py               # the stage DAG + CLI:  python run.py status | run [--only NAME] [--dry]
  lib/
    pipeline.py        # content-hashed Stage/DAG engine (diff-and-rerun)
    costlog.py         # always-on cost logging -> costs/usage.csv
    providers/         # (M2) llm / embedding / reranker adapters (model-agnostic)
  stages/              # extract_entities, resolve_entities, entity_engine, geocode_places, normalize_dates,
                       #   stitch_notebooks, build_graph, embed_corpus, index_sources, ingest_book, raptor, ...
  data/                # step_7 outputs (entities, graph, vectors, indexes)  [generated]
  costs/usage.csv      # cost log  [generated]
../../searchable_pdfs/ # text-layer source PDFs + IIIF manifests  [generated]
app/                   # FastAPI dev tool + frontend (graph viz, IIIF citation modal, tool toggles)
```

## Milestones (dev tool first; each capability = a toggleable tool)
- **M1** thin slice: embed (1 space) → `vector_search` tool → FastAPI + UI (toggles, chat, citation modal + PDF download, trace).
- **M2** model-agnostic retrieval: full embedding matrix + reranker + hybrid/RRF + `pdf_fulltext_search` + IIIF viewer.
- **M3** foundation: NER → resolve (seed clusters) → **entity_engine** → RiC-O temporal graph; entity/graph/temporal tools; graph viz.
- **M4** GraphRAG: community summaries + hybrid graph+vector.
- **M5** observability + hosting (Phoenix/OTel; GCP/Azure).

## The entity engine (`stages/entity_engine.py`)
The canonical entity stage — a **context-aware, iterative** engine that REPLACES resolve's final
clustering + the old `enrich_entities`. Per entity it reads the actual source excerpts and an LLM decides
what it *is* (`is_real_entity`, `entity_kind`, `place_type`), researches/disambiguates it (e.g. "Colorado"
in a Detroit address → a *street*, not the state), and emits a canonical `disambiguated_identity`; a
cheap O(n) group-by on that identity then merges duplicates (His Eminence → Cardinal Farley; cancer's 340
spellings → one node). Writes the central store `data/entity_store.json` (the new source of truth;
`build_graph` + `geocode_places` read it and drop `is_real_entity=false` noise) and a gated
`entities_enriched.json` view (streets never map as states; friaries roll up to their city).
- **Reproducible + incremental:** a content hash per entity → a re-run (cleaner OCR / more docs) only
  re-thinks what changed; unchanged entities reuse their stored understanding (no spend).
- **Non-destructive + rollback:** timestamped backups in `data/.backups/`; pre-merge thought saved to
  `data/entity_thought.json` so merge rules re-tune for free via `--remerge`.
- **Loop-capable, cost-capped:** `run(execute=True, passes=N, max_cost_usd=M)`; bulk model
  `gemini-2.5-flash-lite` (~$6 for the full corpus). Robust to provider stalls (shared client + warming +
  no-progress watchdog + hard-exit). `--emit-only` re-derives the enriched view without spend.

## How to extend
- **New stage:** add a function in `stages/`, register a `Stage(name, fn, deps, inputs, outputs)` in `run.py`. The DAG handles staleness/ordering.
- **New embedding space:** add `(model, dim)` to `config.EMBEDDING_MATRIX` (+ the model to `config.EMBEDDINGS`). The embed stage goes stale and re-embeds only the new space.
- **New model:** add to `config.LLMS`/`EMBEDDINGS`/`RERANKERS` with its price (for cost logging).
- **New agent tool:** drop a tool module in `app/tools/`; it shows up as a toggle in the UI.
