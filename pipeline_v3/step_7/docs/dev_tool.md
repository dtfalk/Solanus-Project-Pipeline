# The Dev Tool — a glass-box archival agent

*A companion to the four files that make the web app run: `app/server.py` (the FastAPI backend), `app/static/index.html` (the page shell), `app/static/app.js` (all the behavior), and `app/serve.py` (the launcher). The long-form, diagrammed version is `dev_tool.tex` → `dev_tool.pdf`.*

---

## TL;DR

The dev tool is a single-page web app that sits on top of Father Solanus Casey's papers — ~570 letters and ~716 notebook pages — and lets you **ask the archive a question and get a grounded, *cited* answer back, while watching exactly how the machine got there.** It is a glass box, not a black box. A FastAPI server (`app/server.py`) exposes two ways to answer (a tool-using **agent** at `/api/query`, and a simpler single-shot **RAG** at `/api/ask` that the shipped UI uses), plus endpoints for a knowledge graph, a map, a year-by-year timeline, and the scanned source pages. The browser side (`app/static/`) is a tabbed app — **Ask & trace, Knowledge graph, Map, Walkthrough, How it works, Tools & data, Explanations** — with a **Settings** gear that holds the three model choices (LLM, embedding space, reranker) and the **toggleable retrieval techniques**. Every answer cites the exact handwritten region, and clicking a `[1]` opens a deep-zoom viewer over the scan. Every paid call is cost-logged, and each question shows its own price. You launch it with `app/serve.py`, which makes the whole thing work **from any directory**.

---

## A 60-second primer (skip if you know RAG)

**RAG** ("retrieval-augmented generation") is the pattern behind almost every "chat with your documents" tool. The trick: a language model has read a lot of the internet but has *not* read Solanus Casey's mail, and if you just ask it, it will confidently make things up. So instead of asking the model to *remember*, you first **retrieve** the handful of passages from the corpus that are most relevant to the question, then hand *only those passages* to the model and ask it to answer **using just them**, with citations. Retrieval decides *what the model gets to see*; generation *writes the prose*. This tool makes both halves visible and swappable — and that is the whole point of calling it a "dev tool" rather than a chatbot.

Two more terms you'll see. An **embedding** turns a passage into a list of numbers (a vector) so passages with similar *meaning* land near each other, which lets you search by meaning instead of by exact keyword. A **reranker** is a second, slower model that looks at the question and a candidate passage *together* and scores how well they actually match — used to re-sort the top few hits.

---

## How it's wired (the shape of the thing)

There are exactly two moving parts, and they talk over HTTP:

- **The backend** — `app/server.py`, a FastAPI app. It owns the corpus, the retrieval library, the tools, the cost ledger, and the source scans. It never calls a model provider directly; every model touch goes through `lib/providers/*`, which cost-logs through `lib/costlog.py`. That discipline is what keeps the free path genuinely free and the running dollar total honest.
- **The frontend** — `app/static/index.html` (structure + brand chrome) and `app/static/app.js` (~940 lines of vanilla JavaScript, no framework, no build step). Color and type come from `theme.css` (the Capuchin Province palette: deep browns like `#3C1605`, Raleway + Open Sans). Three third-party libraries load from CDNs so there's nothing to compile: **OpenSeadragon** (deep-zoom image viewer), **Cytoscape.js** (the graph), and **Leaflet** (the map).

The frontend's mental model is one mutable `STATE` object that mirrors a single `RetrievalConfig`. Every model picker and every toggle just edits a field on `STATE`; when you Ask, the UI serializes `STATE` and POSTs it. One source of truth — which is why every control can be a pure "set a field" action.

### `app/serve.py` — why paths don't depend on where you stand

A subtle but important detail. `uvicorn app.server:app` only resolves if the directory that *contains* `app/` happens to be on Python's import path — and whether it is depends on where you launched the command. Run it from the wrong folder and you get `ModuleNotFoundError: No module named 'app'`. `app/serve.py` fixes this by resolving everything **relative to itself**: it computes the repo root as `Path(__file__).resolve().parent.parent`, puts that on `sys.path`, and hands the same root to uvicorn as `app_dir` (which uvicorn re-injects into the reload worker, so `--reload` keeps working). The net effect:

```
pipeline_v3/step_7/venv/bin/python app/serve.py --port 8000      # works from anywhere
pipeline_v3/step_7/venv/bin/python /abs/path/to/app/serve.py     # also works
```

All server-side data and asset paths are already anchored to `__file__` (see `_APP` in `server.py` and everything in `config.py`), so once the import resolves, **nothing else depends on the working directory.**

### Two answer paths, both real

The task brief names one canonical endpoint, `/api/query` (the agent); the shipped UI is built around a second shape, `/api/ask` (single-shot RAG). The server supports **both**, and the canonical routes are declared first so nothing shadows them.

- **`POST /api/query` — the agent.** A tool-using function-calling loop. You give it a question + the model variables + which tools are enabled; it asks the model for its next move, the model either *calls a tool* or *answers*, and the loop repeats until it answers or hits `max_steps` (default 6 — a safety cap so a confused model can't loop forever and run up cost). Each tool is a plugin under `app/tools/`; only the *enabled* ones are offered to the model on a given turn. Returns `{answer, citations[], trace[], grade, cost}`.
- **`POST /api/ask` — single-shot RAG (what the UI calls).** No agent loop. The UI's toggles map straight onto a `RetrievalConfig`, retrieval runs **once** via `lib.retrieval.route_and_retrieve`, the hits are numbered `[1..N]`, and one LLM call synthesizes a cited answer over exactly those passages. Returns the same citation/trace shape `app.js` renders.

The reason for two paths is honesty about cost and complexity. The single-shot path is cheaper and easier to reason about; the agent path is more capable but spends more. Both share the same retrieval library, the same tools, the same cost ledger, and the same provenance contract.

---

## The Ask tab (the headline feature)

The "Ask & trace" tab is two stacked panels: the conversation on top, the **agent trace** ("under the hood") below it.

**What happens when you ask.** You type a question and hit Ask (or Enter). The UI reads the current settings into the request body — `{query, llm, embedding, reranker, kinds, toggles}` — and POSTs it to `/api/ask`. While it waits it shows a "retrieving + grading…" spinner. When the answer comes back it renders three things:

1. **Grounded prose with clickable `[n]` markers.** The answer string contains inline markers like `[1]`, `[2]`. The UI splits the text on those tokens and turns each one into a small footnote chip bound to citation *n*. Crucially, the model's text is **never** injected as HTML — it's built as DOM text nodes, because the model output is untrusted and `innerHTML` would be an XSS hole.
2. **A grade badge.** A Self-RAG / CRAG-style verdict — `answer`, `caveat`, or `abstain` — with a confidence percent. This tells you whether the system trusted its own retrieved context. In an archive, "I could not find this" is a correct and valuable answer; if retrieval came back weak, the synthesizer is instructed to abstain rather than fabricate, and the badge turns the warning color.
3. **A Sources list** mirroring the inline markers, each row clickable to open the citation modal.

**Per-query cost, not the whole ledger.** A subtlety learned the hard way: an early version showed the *running* total, so a one-cent question read as the entire project's spend — alarming and wrong. Now both paths compute the **delta** for *this* query (snapshot the ledger before, snapshot after, subtract) and report it prominently, with the cumulative session total shown smaller for context. The trace footer reads like `this query: $0.000412 · in 1843 tok · out 206 tok (session total $0.1937)`.

### The citation modal — provenance you can click through to

Click any `[1]` (in the prose, in the Sources list, or even a record node in the graph) and a modal opens with an **OpenSeadragon** deep-zoom viewer over the scanned page. The UI calls `GET /api/region?doc_id=&rid=` to get the canonical image URL, the page's exact pixel size, the cited region's polygon vertices, its OCR confidence, and a PDF link. It then:

- Loads the page PNG (`/api/image/<section>/<pdf_page>`) as a single tiled image source.
- Converts the region's pixel vertices into OpenSeadragon's normalized `[0..1]` viewport coordinates (knowing the exact pixel size up front makes this overlay math correct on the very first frame), draws a translucent **brand-brown highlight box** with a dimming shadow over everything else, and zooms so the cited region fills most of the viewport.
- Shows the region's transcribed text, section, page, category, and OCR min-confidence in a sidebar, plus a **"Download source PDF"** button pointed at the searchable page PDF.

If the backend is down or a region is unknown, it degrades gracefully: it reconstructs what it can from the citation alone and shows a friendly note. An archive answer you can't click through to is just a rumor with footnotes — so this modal is not a nice-to-have, it's the point.

### The trace panel — the toggleable harness, made visible

Below the conversation, "Under the hood — agent trace" prints, in order: the **route** line (the adaptive router's decision: complexity → strategy, with its reason), a **queries** line if multiple phrasings were fused (RAG-Fusion), one line per **tool/step** that fired (with item counts and milliseconds), and the **cost** footer described above. This is where the toggles stop being abstract: flip on rerank or HyDE and you see extra steps appear and the price move.

---

## The Knowledge Graph tab (legible by design)

The corpus, turned into a network: nodes are letters, notebook pages/entries, and **resolved entities** (people, places, organizations, conditions, favors, outcomes, roles, events); edges are asserted relations (`WROTE_TO`, `LOCATED_AT`, `CONTINUES_ON`, `HAS_CONDITION`, …). It's drawn with Cytoscape.js, fed by `GET /api/graph`.

The full graph is roughly **47k nodes / 65k edges (~42 MB)**. Shipping all of that to the browser would hang Cytoscape and re-parsing 42 MB per request would be wasteful, so the server is careful in three ways — and all three are why this graph is *readable* where most knowledge-graph views are a hairball:

**1. Semantic relations by default; structural edges hidden.** About 80% of the edges are structural plumbing — every entity points at every record it was *mentioned in* (`MENTIONED_IN`), and every record points at its *year* (`DATED_IN`). Those turn the picture into one giant blob. So by default the server returns *only the interpretable semantic relations* ("who wrote to whom", "X enrolled for Y", "located at"), and ranks nodes by their degree *in that relation graph*. The `structural=true` flag re-includes mentions and dates (the focus view turns it on automatically when you center on a year, since a year is only interesting *through* its date edges); a `rels` parameter can restrict to specific edge kinds.

**2. A concentric layout, not a force-directed spring.** The most-connected node sits in the center and everything fans out in rings (`concentric: n => n.degree() + 1`), with a large `minNodeSpacing` (100) and a generous `spacingFactor`. For a focused view this naturally puts the focus entity in the middle with its relations around it ("Solanus → the people he wrote to"). It's predictable and overlap-free — far more legible than a force-directed tangle. There's lots of space; you pan and zoom to read. Every edge always shows its relation verb as a label, because a graph without the relation is meaningless.

**3. Focus search instead of "show everything."** A free-text box up top resolves a label to a center node and shows its neighborhood. The server's `q` resolver is deliberate: it prefers an **entity** node over a record or year that merely mentions the term, then breaks ties by degree. When you first open the tab it doesn't dump the backbone blob on you — it defaults the focus box to **"Solanus"**, so you land on his own network, an immediately sensible view. The toolbar also has Fit, Reset, and an "entities only" filter (drop record/year nodes). Node size scales with mention count, so the hubs read at a glance.

**Clicking nodes.** Click a **record** node (a letter or notebook page) and the citation modal opens on its scan — the record ids are minted from `doc_id` in graph-building, so they route straight to the viewer. Click an **entity** node and the trace panel fills with a rich inspector: name, description, role, relation to Solanus, location, mention count, and known name variants ("aka …"). The `meta` block also reports shown-vs-total, so the toolbar can say "showing 70 of 46,931 nodes."

There's also a **career timeline** strip above the graph (`GET /api/timeline`): a records-per-year histogram built from the `DATED_IN` edges, split by letters vs. notebook entries, so you can see the arc of Solanus's career. Click a bar and it focuses the graph on that year.

---

## The Map tab

`GET /api/map` returns the **place** entities that carry coordinates (geocoded upstream by `geocode_places` → `build_graph`), and Leaflet draws them as circle markers sized by how often each place appears — the geography of his correspondence and petitions. The default view centers on the upper Midwest (Detroit was his home friary). One nice touch in the server: generic single-word place names like "Hospital," "Monastery," or "Church" geocode to arbitrary spots, so they're filtered out — the map shows only meaningful, specific locations. Click a marker for the place name, its role, mention count, and description.

---

## The Walkthrough tab (editable, no restart)

A plain-English, step-by-step tour of the whole pipeline plus a "what still needs doing" list. The content lives in **`app/content/walkthrough.json`** and is served verbatim by `GET /api/walkthrough`, read **live** on every request — so David edits the JSON file, reloads the tab, and the page updates with no server restart. Steps render as cards with an optional status tag (done / in-progress) and a "Next:" pointer; to-dos render as their own cards.

---

## The Tools & data tab

Two halves. The top half lists each **retrieval tool** the agent can use (from `GET /api/tools`, the registry manifest): name, description, and whether it's free/local or paid. The bottom half shows corpus and cost stats — including the live **session API cost** pulled from `GET /api/cost` (the running ledger across every model call to date) — alongside fixed counts (570 letters, etc.). This is the "what tools exist and what do they cost" reference.

(There's also a sibling **"How it works"** tab that draws the pipeline as a flow — scanned pages → OCR → segmented documents → NER → resolve → knowledge graph → embed+index → ask — and fills a grid of live counts read from the graph meta and cost ledger.)

---

## The Explanations tab

The long-form write-ups, rendered **in-app**. `GET /api/explain` reads the markdown files in `pipeline_v3/step_7/docs/` — `architecture`, `retrieval`, `ner_and_resolution`, `graph_and_dates`, `dev_tool` (this file), and `ENRICHMENT_AND_DEDUP_PLAN` — and returns each as `{name, title, markdown}`. The frontend runs a small, safe-enough markdown-to-HTML pass (headings, bold, inline + fenced code, lists, paragraphs — escaping along the way) and shows a topic picker. So the full architecture / retrieval / graph documentation lives right next to the thing it describes.

---

## The Settings modal

The three model choices and the technique toggles used to live in a left rail; they now live behind the header's **⚙ Settings** button so each tab gets the full width. The controls kept their IDs, so the API wiring is unchanged. Inside:

- **Models** — three dropdowns: **LLM** (generation), **Embedding space** (retrieval), **Reranker**. These are the dev tool's central idea: the three model "variables" are things you pick from a menu, not constants baked into code. Swap any one and re-ask. The dropdowns are populated from `GET /api/config`, which lists only the embedding spaces that actually have a built vector partition on disk — so you can't pick a space that would silently fail.
- **Tool toggles** — the retrieval techniques (next section).
- **Scope** — restrict to all sources / letters only / notebook entries only.

Changes apply to your next question. If `/api/config` is unreachable (e.g. the backend isn't up yet), the UI falls back to a static config mirrored from `config.py` so the menus are never empty during development.

---

## Toggleable retrieval techniques

Retrieval is not one algorithm — it's a stack of techniques, each with a different strength and a different cost. The dev tool makes each one a switch so you can *see* its effect on the answer and the trace. The server advertises them via `/api/config`, flags the paid ones, and maps each toggle key directly onto a `RetrievalConfig` field:

| Toggle | What it does | Default | Cost |
|---|---|---|---|
| **BM25 (lexical)** | Sparse keyword search. Matches exact words, names, and dates literally. | on | free / local |
| **Dense (semantic)** | Embedding-similarity search in the chosen space — finds passages by *meaning*, not shared words. | on | free / local |
| **Rerank** | Cross-encoder re-orders the top candidates for sharper relevance. | off | local free / hosted paid |
| **HyDE** | The LLM drafts a *hypothetical* answer, then retrieves passages similar to *that* (closer to the real wording than a terse question). | off | paid (1 LLM call) |
| **Multi-query (RAG-Fusion)** | Rephrases the question several ways, retrieves each, and fuses the results with Reciprocal Rank Fusion for better recall. | off | paid (1 call per rephrase) |
| **LLM router** | An LLM first judges the question's complexity and recommends a retrieval strategy. | off | paid |
| **Auto-escalate route** | Lets the router actually *turn on* the paid transforms it recommends, not just suggest them. | off | paid |

The honest default is the **free** stack: BM25 + local dense cost **$0**, and so does the heuristic grading. The paid toggles spend only when you flip them on, and every one is cost-logged. Two safety nets in the server worth knowing: if you pick a dense space that has no built partition, the server quietly drops to BM25-only (and says so in the trace, rather than pretending dense ran); and the synthesizer skips the LLM call entirely — returning a plain "not found" — when retrieval grades the evidence as *abstain*, which is both cheaper and the right archival default.

In the **agent** path the same idea appears one level up as **toggleable tools** rather than retrieval flags: `vector_search` (meaning), `pdf_fulltext_search` (exact strings, FTS5), `entity_lookup` (resolve a name), `graph_query` (relationships), `temporal_query` (dates/timelines), and `community_summary` (whole-corpus themes). You choose which tools the agent is *allowed* to use; the *model* chooses which to call and with what arguments. The human-picked variables (embedding space, reranker, LLM) are injected into the calls that consume them — not into the model's schema — so the model only ever decides the *semantic* parts of a tool call.

---

## Observability and the trace, end to end

The dev tool's whole pitch is "see under the hood," so tracing is built to work with **zero extra dependencies** and to get *better* if you install more. On startup the server probes for **OpenTelemetry + OpenInference** (the stack that flows LLM spans to Arize Phoenix). If they're present, it registers a tracer provider so a Phoenix/OTLP exporter configured via the standard `OTEL_*` env vars picks spans up. If they're absent — the common case — it silently falls back to a **lightweight JSON tracer** that records one timed span per step, returns the trace inline in every API response (so the "under the hood" panel always works), and appends one newline-delimited record per request to `app/traces/agent_traces.jsonl` for offline inspection. Either way the agent loop calls the *same* tracer interface, so the loop code never branches on which one is active.

---

## Cost discipline (why nothing bills you by accident)

Three rules, kept faithfully:

1. **The server never calls a provider directly.** Every model call goes through `lib/providers/llm`, `…/embed`, or `…/rerank`, each of which cost-logs through `lib/costlog.py`. That's what keeps the ledger complete.
2. **The free path is genuinely free.** BM25 + local dense + local reranker + heuristic grading = $0. Paid behavior is always behind an explicit choice (a paid toggle, or the agent reasoning with the LLM).
3. **Each query shows its own price.** Both answer paths report the per-query delta plus the cumulative session total, so a cheap question never reads as the whole project's spend.

A note on the headline endpoint: **`POST /api/query` and `POST /api/ask` make paid LLM calls** — that's the point of them (the agent reasons with the model; the single-shot path synthesizes the answer). Inspecting the wiring is free: `python app/server.py` runs a self-check that prints the registered tools, the model variables, the observability status, and whether the web deps are installed — *without* starting the server or making a single billable call.

---

## Running it

```bash
# one-time: add the two web deps to the step_7 venv
pipeline_v3/step_7/venv/bin/pip install "fastapi>=0.110" "uvicorn[standard]>=0.29"

# launch (works from any directory, thanks to app/serve.py):
pipeline_v3/step_7/venv/bin/python app/serve.py --port 8000
# then open http://127.0.0.1:8000
```

FastAPI + uvicorn are **required**; the OpenTelemetry/Phoenix stack is **optional** (the JSON tracer fallback kicks in automatically). If FastAPI is missing, importing the module still works (so tests and the self-check run on a bare machine), and the `app` object becomes a tiny stub that answers any request with a clear, actionable "here's exactly what to `pip install`" message instead of an opaque ImportError.

---

## The endpoints at a glance

| Endpoint | What it does |
|---|---|
| `POST /api/query` | The tool-using agent. Paid. Returns `{answer, citations, trace, grade, cost}`. |
| `POST /api/ask` | Single-shot toggle RAG (the UI's primary call). Paid for synthesis. |
| `GET /api/config` | Self-describing config the UI builds its menus from (LLMs, built embedding spaces, rerankers, defaults, technique toggles). |
| `GET /api/tools` | The tool registry manifest + observability status + model lists. |
| `GET /api/graph` | A bounded, cached, legible subgraph (semantic relations by default; focus/center/kind filters). |
| `GET /api/timeline` | Records-per-year histogram for the career strip. |
| `GET /api/map` | Geocoded place entities for the Map tab. |
| `GET /api/region` / `GET /api/source` | Citation payload: text + polygon + OCR confidence + image/PDF/IIIF URLs. |
| `GET /api/image/<section>/<pdf_page>` | The page scan PNG (what OpenSeadragon tiles). |
| `GET /api/pdf/<section>?page=` | The page's searchable source PDF (download). |
| `GET /api/manifest/<section>` | The IIIF v3 manifest for a volume, if built. |
| `GET /api/walkthrough` | The editable walkthrough JSON (read live). |
| `GET /api/explain` | The docs/*.md write-ups for the Explanations tab. |
| `GET /api/search_pdf` | Direct full-text (FTS5) page search. Free, local. |
| `GET /api/cost` | The running dollar ledger. |
| `GET /api/health` | Liveness check the UI pings on boot. |
| `GET /` + `/static/*` + `/sources/*` | The SPA, its assets, and read-only source scans. |
