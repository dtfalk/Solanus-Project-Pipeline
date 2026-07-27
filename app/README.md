# app — Solanus archival dev tool (web)

The served product + developer/eval harness. The **backend (`server.py`) and the toggleable tools
(`tools/`) are implemented**; the front-end (`static/`) is a vanilla-JS SPA that drives them.

## Run it

```bash
# one-time: add the two web deps to the step_7 venv (everything else is already there)
pipeline_v3/step_7/venv/bin/pip install "fastapi>=0.110" "uvicorn[standard]>=0.29"
# optional observability (else a lightweight JSON tracer is used automatically):
#   pip install opentelemetry-sdk opentelemetry-api openinference-semantic-conventions arize-phoenix
# then, from the repo root:
pipeline_v3/step_7/venv/bin/uvicorn app.server:app --reload --port 8000
```

`python app/server.py` runs a **free, offline self-check** (prints the wiring; starts nothing, spends
nothing).

## Backend (`server.py`)

A FastAPI service that serves the SPA and the API. **Every model call routes through
`lib.providers.*` → `lib.costlog`** — the server never calls a provider directly, so the cost ledger
is complete and the free path stays free (BM25 + local embeddings + local reranker = $0; the LLM
turns and any hosted embedding/rerank are billable and logged).

Endpoints (the task's canonical names + the front-end's dialect, both onto one backend):

| Endpoint | What it does |
|---|---|
| `POST /api/query` | **Tool-using agent loop** (Gemini function-calling via `lib.providers.llm`) over the **toggleable tools**. Params: `llm`, `embedding_space`, `reranker`, `enabled_tools[]`. Returns `answer` + `citations` (doc_id/rid/page/vertices) + the full tool-call **trace** + `cost`. |
| `POST /api/ask` | Single-shot, **toggle-driven** retrieval (`lib.retrieval` RetrievalConfig) + cited synthesis — what the SPA's left rail models. |
| `GET /api/graph` | `data/graph.json` (built on demand if `networkx` is installed). |
| `GET /api/search_pdf` | Full-text **FTS5** page search → page + source PDF + snippet. |
| `GET /api/source` · `GET /api/region` | Citation-modal payload: text + gold polygon `vertices` + OCR `min_conf` + page image/PDF (+ IIIF deep-zoom hooks for `/api/region`). |
| `GET /api/cost` | `costlog.summary()` — the running $ ledger. |
| `GET /api/tools` · `GET /api/config` | Registry manifest / self-describing config the UI builds its rail from. |
| `GET /api/image/<section>/<page>` · `GET /api/pdf/<section>?page=` · `GET /api/manifest/<section>` | Page PNG / source PDF / IIIF manifest for the viewer. |
| `GET /api/health`, `/static/*`, `/` | liveness; the themed SPA. |

**Observability.** If OpenTelemetry + OpenInference import, spans are set up for export to Arize
Phoenix (configure an OTLP endpoint via the standard `OTEL_*` env vars). If they don't, a
**lightweight JSON tracer** writes one record per request to `app/traces/` *and* returns the trace
inline — so the "under the hood" panel works with zero extra deps.

## Tools (`tools/`) — one module per toggleable capability

Each module declares a `TOOL_SPEC` (name/description/JSON-Schema) + a `run(args)` and registers via
`base.tool_from_module(...)`; the server only offers the LLM the tools left ON. Adding a capability =
dropping a file in here (it auto-appears as a toggle + a function declaration).

- `vector_search` — hybrid BM25 + dense (RRF, optional rerank, CRAG grade) over `lib.retrieval`. *free; hosted embed/rerank paid.*
- `pdf_fulltext_search` — SQLite FTS5 page search → page PDF + snippet. *free, local.*
- `entity_lookup` — resolve a name to a canonical entity (`entities.json`) + cited mentions. *free, local.*
- `graph_query` — 1–2-hop walk of the knowledge graph (`graph.json`) with edge provenance + dates. *free, local.*
- `temporal_query` — records in a year window (EDTF dates, `dates.json`), chronologically. *free, local.*
- `community_summary` — GraphRAG community detection (free); optional per-cluster LLM report (`summarize=true`, paid).
- `book_search` — semantic search over the Crosby biography (*Thank God Ahead of Time*, `book-tgat@1536`), cited by book page. **Off by default** (secondary literature; the archive is primary). *free local index; small paid query embed.*

> Entity/graph data now comes from the **entity engine** (`stages/entity_engine.py` → `entity_store.json` +
> a gated `entities_enriched.json`); `graph.json` is rebuilt from it (noise dropped, places disambiguated).
> The graph viz adds: clusters/importance layouts, a clickable color+shape legend, hover degree-of-interest
> fade, a timeline **date-brush**, an asserted-vs-inferred **confidence slider**, semantic-zoom labels, and
> **Street View** links on map pins + place dossiers.

## Deps to add to the venv

- **Required:** `fastapi`, `uvicorn[standard]`.
- **Optional:** `opentelemetry-sdk`/`-api` + `openinference-*` + `arize-phoenix` (else JSON tracer);
  `networkx` (for `graph_query`/`community_summary`/`/api/graph`); `reportlab`+`pypdf` (searchable PDFs).

Theme: `static/theme.css` (Capuchin Province palette + Raleway/Open Sans; matches solanuscasey.org).
