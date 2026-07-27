# Autonomous build checkpoint — 2026-06-20

David is away and authorized an autonomous build of **all tiers (except fine-tuning)** of the
archival tool, $60 API budget ("don't feel obligated to use"), in **his code/writing style**
(see `STYLE_GUIDE.md`, learned from `/Workspace/Projects/Solanus-Project-Pipeline/examples/`).
Instruction: "if worried or have a question, checkpoint on disk and proceed." This file is that
checkpoint + the single source of truth for the run. Plan: `RESEARCH_PLAN.md`.

## Where things stand (proven, working)
- step_7 foundation built + tested: `lib/pipeline.py` (diff-and-rerun, proven), `lib/costlog.py`,
  `config.py`, `lib/chunks.py` (9,342 chunks), `lib/vectorstore.py` (partitioned), `lib/providers/
  embed.py` (Gemini+local live; OpenAI/Voyage built, key-guarded), `lib/providers/llm.py`,
  `stages/embed_corpus.py` (wired into `run.py`).
- Smoke tests passed: Gemini embeddings ($0.0004/40), local bge ($0), retrieval (semantically
  good), Gemini NER (structured) + RAG (cited) — total spent ≈ **$0.0014**.
- Projections: embed all spaces ≈ $0.32; full NER ≈ $1–2 (with batch −50% + prompt cache).

## Decisions & assumptions (proceeding; not blocking)
1. **No fine-tuning** (per David). Mimicker therefore = few-shot + RAG-grounding + style-steering
   (StyleVector) + optional Horikawa-style iterative MLM refinement; + LUAR/StyleDistance eval.
   Skeleton only; no training runs.
2. **Budget $60**, tracked in `costs/usage.csv`. Strategy: Gemini Batch API (−50%) + prompt caching
   for bulk; run full embed (~$0.32) and full NER (~$1–2) once stages are integrated + tested on a
   small batch first. Hard stop well under $60; will checkpoint if approaching.
3. **Models:** Gemini default (key present). OpenAI/Voyage/Cohere adapters exist but need keys →
   skipped until David adds them (noted, not blocking). Multimodal ColPali = local/free but heavy
   (vision model download + GPU-ish) → scaffolded; heavy run deferred (flagged).
4. **Docs:** every major component documented as BOTH markdown AND PDF-from-LaTeX, in David's
   detailed-explanation style. LaTeX aux/intermediary files go in a `*/build/` subfolder (kept out
   of the clean tree); only `.tex` + final `.pdf` sit at the doc root. Tooling: `latexmk`/`pdflatex`
   (+ `pandoc` if available) — verified at build time; if absent, leave `.tex` + a build script.
5. **Style guide** distilled to `STYLE_GUIDE.md`; all new code matches it (divider headers, aligned
   `=`, Google docstrings, teach-the-why comments) and is non-destructive + cost-logged.
6. Repo bloat (~11GB committed artifacts) is a separate cleanup; step_7 generated outputs are
   gitignored.

## Build plan / status  (tiers from RESEARCH_PLAN.md; [x]=done [~]=drafted [ ]=todo)
- [x] Tier-0 core: embed + retrieve + LLM + cost + pipeline (proven)
- [x] Tier-0 retrieval module `lib/retrieval.py`: hybrid BM25+dense+RRF(60), rerank, router, HyDE,
      multi-query, Self-RAG/CRAG grade — VERIFIED working
- [x] Tier-1 A1 `stitch_notebooks` (8,657 logical entries, 114 multi-page) — wired into run.py
- [x] Tier-1 A2 `normalize_dates` (9,343 EDTF dates) — wired into run.py
- [~] Tier-1 NER `extract_entities` (rich nested schema, structured output) — verified on 20 units;
      FULL paid pass RUNNING (parallelized, 16 workers, task b7oz9yuqa, resumes)
- [x] Tier-1 A3 `resolve_entities` (normalize→block→cluster; +opt-in emb/LLM/authority) — wired
- [x] Tier-2 `build_graph` (RiC-O temporal KG; graph.json + graph.ttl + suggestions) — wired (demo
      entities; RE-RUN on real entities once full NER lands)
- [x] Tier-2/4 `index_sources`: FTS5 (1,286 pages) + IIIF (7 manifests, 22,100 region annots) +
      1,233+ text-layer searchable PDFs — DONE (fixed reportlab setTextRenderMode bug)
- [x] Tier-3 multimodal `colpali_index` — scaffold wired (plan-only; heavy build opt-in)
- [x] Tier-3 RAPTOR — scaffold wired (plan-only; paid build opt-in)
- [x] App: FastAPI backend (6 toggleable tools + agent loop + OTel/JSON-tracer) + themed SPA — built;
      free self-check passes. LIVE agent-loop test (paid, small) still TODO.
- [~] Eval harness — drafted by build workflow (Style-Mimickry + docs); RAGAS/ARES run TODO
- [x] Docs (markdown + LaTeX→PDF) ALL 6 BUILT: architecture, retrieval, ner_and_resolution,
      graph_and_dates, dev_tool, mimicker (aux in each docs/build/)
- [x] Mimicker skeleton (no fine-tuning) in Style-Mimickry/ (data_prep, generate, evaluate, docs)

## Budget log
- 2026-06-20: smoke tests ≈ $0.0014. (see costs/usage.csv for live totals)

## Open questions (checkpointed, proceeding with the noted default)
- Embedding spaces to FULLY populate: default Gemini@1536 + local bge-large@1024 (cheap/$0); the
  rest of the matrix needs OpenAI/Voyage keys → deferred.
- Run full NER now? Default yes after a 50-doc test confirms quality; within budget.
- ColPali model + GPU availability unknown → scaffold + defer the heavy index build.
- Cloud target (GCP vs Azure) for hosting → GCP assumed (Gemini/Vertex); app runs locally for now.

## Progress log
- 2026-06-20 read examples/ (code + writing style) + mimicry research; wrote STYLE_GUIDE.md.
- 2026-06-20 launched build Workflow `wf_509ea8d3-428` (background): drafting ~21 modules/docs/
  mimicker in David's style (stages, retrieval, app, eval, multimodal, docs, mimicker). No paid calls.
- 2026-06-20 FOUND + FIXED Gemini 429 rate limit (5,000 embed-contents/min): added tenacity
  retry+backoff (429/5xx -> exponential wait) to lib/providers/embed.py AND llm.py. Lesson: all
  bulk model loops must self-throttle (same as the OCR step). For TRUE bulk (full NER/contextualize)
  prefer Gemini Batch API (−50%, no per-min cap) — gated for David.
- 2026-06-20 re-running FULL base embed (gemini@1536 + bge-small@384, all 9,342 chunks) in
  background (task b5ctpn923). Budget so far ≈ $0.054 (cost-logged).
- 2026-06-20 FULL base embed DONE (retry worked): 9,342 vectors each in gemini-embedding-001@1536
  and bge-small-en-v1.5@384; 5m14s; budget now ≈ $0.127. (stale 40-vec gemini@768 test space can be
  deleted.) Corpus is now retrievable end-to-end once lib/retrieval.py lands.
- Build workflow still running + actively writing files (already wired normalize_dates into run.py).
  Holding all integration edits until it completes to avoid write collisions.
- 2026-06-20 (session 2) INTEGRATION PASS:
  * ALL 6 doc PDFs built (added lmodern+T1 fontenc, microtype expansion=false, tcolorbox title={#1}
    bracing). Aux files contained in each docs/build/. mimicker.pdf included.
  * `index_sources` RUN to completion: fixed a reportlab bug (`Canvas.setTextRenderMode` doesn't
    exist — render mode is per-text-object; removed the bogus canvas call). Output: 1,286 FTS pages,
    7 IIIF manifests (22,100 region annotations), 1,233+ searchable PDFs in /searchable_pdfs/.
  * `run.py` FULLY WIRED: replaced all _todo stubs with the real stage fns; added stitch_notebooks,
    extract_entities, resolve_entities, build_graph, index_sources, contextualize_chunks, colpali_index,
    raptor. Paid/heavy stages wired to their SAFE default (free harvest / dry-run / no-build) so a
    blanket `run` never spends or downloads; real passes are opt-in via flags. DAG prints clean,
    py_compile OK.
  * NER THROUGHPUT FIX: the serial paid loop ran at ~6.7 calls/min (~23h for 9,343 units). Parallelized
    the Pass-B execute path with a ThreadPoolExecutor (NER_WORKERS=16; single-writer drains
    as_completed → append-only + resume invariants preserved; per-unit failures isolated). Killed the
    serial job (b... ) and relaunched parallel (task b7oz9yuqa) — resumes from 12,487 mentions on disk.
- NEXT (on full NER completion): re-run resolve_entities (real path on entities_raw.jsonl) →
  build_graph (real entity KG) → spot-check the graph; run contextualize_chunks small batch to scope
  cost then full + re-embed contextualized space; live app agent-loop test (small paid); run eval
  harness. Keep spend well under $60 (≈$0.14 + ~$16 projected full NER).
- 2026-06-20 (session 2, cont.) — FULL DATA-FLOW NOW PROVEN END-TO-END:
  * FULL NER done: 80,432 mentions (10,771 harvest + 69,661 LLM over 9,289 units). Types: PERSON
    13,187 / CONDITION 13,593 / RELIGIOUS_TERM 14,462 / FAVOR 6,131 / RELATION 5,793 / PLACE 3,860 …
    Parallel pool held ~100 calls/min; NER spend ≈ $9.9.
  * resolve_entities FIXED + RUN: (a) added a schema adapter (NER emits text/provenance, no
    mention_id/surface — bridged, RELATION dropped as edges, types folded); (b) MEMOIZED the pure
    normalizers with lru_cache — pair scoring was recomputing metaphone millions of times, 600s+ →
    55s; (c) canonical-name picker chose the LONGEST surface (always the OCR-garbled one) → now
    most-frequent + concise ("Fr. Solanus, O.F.M. Cap." not the 4-line mess). Result: 74,639
    mentions → 32,994 canonical entities, 2,004 ambiguous flagged.
  * build_graph FIXED + RUN: _norm_etype defaulted unknown types to 'person', flooding the graph with
    ~15k bogus person nodes (dates/favors/terms). Added the missing type aliases + default→'other'.
    Real KG: 46,931 nodes (person 9,804, favor 8,708, condition 6,019, date 4,424, place 2,182,
    org 461, +record nodes) / 64,532 edges (mentioned_in, has_condition, enrolled, wrote_to,
    located_at, continues_on) — each with provenance rids + valid_time. graph.json 42.6MB, ttl 31.7MB.
  * index_sources fixed (reportlab Canvas.setTextRenderMode) + run: 1,286 FTS pages, 7 IIIF
    manifests (22,100 region annots), 1,233+ searchable PDFs.
  * contextualize_chunks PARALLELIZED (+ incremental write + resume); smoke = $0.00235/8 chunks →
    full ≈ $2.74. Full pass running in background (task buecjseb0). Context quality excellent.
  * run.py DAG fully wired (10 real stages, safe defaults, corrected output paths).
  * APP LIVE + WORKING: fixed a body-binding bug (from-future annotations made the LOCAL Pydantic
    QueryRequest/AskRequest unresolvable → every POST 422'd; moved both models to module scope).
    /api/query agent loop returns a faithful CITED answer (illnesses + doc/rid/page, 10 cites,
    $0.011); /api/ask single-shot RAG returns cited answers + CRAG abstention. Added a graceful
    reranker fallback (paid Voyage key missing → free local bge-reranker, logged) so the
    model-agnostic promise holds without keys.
  * eval harness validated (free proxy, 12 golden Qs): context_precision 0.875 (pass),
    context_recall 0.729 (just under 0.8). faithfulness/answer_relevancy need the paid LLM judge.
  * All 6 doc PDFs built (incl. mimicker). Budget ≈ $12 of $60.
- 2026-06-20 (session 2, FINALE) — CONTEXTUAL RETRIEVAL BUILT + A/B-VALIDATED:
  * Full contextualization done: all 9,342 chunks have an LLM-written situating blurb (≈$2.6).
  * Made the contextualized embeddings a FIRST-CLASS selectable space: added an `api` override field
    to the embed adapter + a `gemini-embedding-001-ctx` config alias (real model, distinct partition)
    + a `contextualized=True` mode in embed_corpus. Embedded all 9,342 ctx-chunks -> ctx@1536.
  * PAID A/B EVAL (12 golden Qs, generate + LLM judge), base vs contextualized @1536, free local
    reranker:
        metric             base@1536   ctx@1536
        faithfulness        0.627       0.884   ← +0.26, the headline win (much more grounded answers)
        answer_relevancy    0.782       0.787   (flat)
        context_precision   0.646       0.615   (≈ flat; small-sample noise on 12 Qs)
        context_recall      0.646       0.615
    => Contextual Retrieval SUBSTANTIALLY improves answer faithfulness; retrieval precision/recall
       still below the 0.7/0.8 thresholds (room to improve: more golden Qs, k/weight tuning, HyDE).
  * Saved cross-session memory `bulk-llm-loops-parallelize` (serial model loops = ~15x too slow).

## BUILD COMPLETE (autonomous run) — total spend ≈ $13.0 of $60
End-to-end PROVEN: corpus → NER (80,432 mentions) → resolve (32,994 entities) → temporal KG
(46,931 nodes / 64,532 edges, RiC-O + provenance + valid-time) → embeddings (base + contextualized) →
FTS5 + IIIF + searchable PDFs → live tool-using RAG agent (cited answers) + single-shot RAG + 6 doc
PDFs + mimicker skeleton + eval harness. `python run.py status/graph` inspect the DAG; `uvicorn
app.server:app` serves the tool.

OPTIONAL FUTURE (non-blocking, gated): graph dedup quality pass (S.M.A.↔Seraphic Mass Assoc'n
under-merge; "Fr. Solanus" vs "Fr. Solanus, O.F.M. Cap." split — needs embedding/LLM clustering);
authority reconciliation (Wikidata/VIAF/GeoNames, $0 APIs); ColPali/RAPTOR heavy builds; OpenAI/
Voyage/Cohere spaces once keys added; more golden Qs + retrieval tuning to clear the eval thresholds;
seed the diff-ledger via one `python run.py run` (re-materializes free stages + a $0.32 re-embed).

## Session 3 (2026-06-21, overnight autonomous) — quality + UX push ($65 mandate)
David raised the budget to $65 and asked: graph must be representative of Solanus's actual life +
legible; fix text-extract/NER/dedup; move settings to a modal; walkthrough + explanations tabs;
de-hyphenation; then a full examples/-style review + docs update. Working continuously through the night.

DONE:
- CAUTIOUS DEDUP (calibrated): resolve_entities.py now runs veto->auto-merge->LLM-gate->abstain with
  anchor gate, acronym/abbrev expansion, hard vetoes. Calibrated on eval/merge_calibration.json
  (119 adversarial pairs): precision 0.93 / recall 0.79. Bugs fixed: DATE never fuzzy-merged (exact
  only — dates DRIVE the timeline), mass!=massachusetts, Ass'n->association. eval/eval_merge.py measures it.
- DE-HYPHENATION: lib/textclean.py collapses OCR line-break hyphens ("Hus- band"->"Husband",
  "Mrs. Clair- mont"->"Mrs. Clairmont") while preserving real compounds (well-known). Wired into
  lib/chunks.py (0/9227 residual) + enrichment contexts. (Future: geometric word-box version.)
- DOC GROUPING: chunks built from stitched LOGICAL entries -> one source spans its pages (114 multi-
  page). Fixes "baked beans -> many sources".
- DATES IN GRAPH: record EDTF+precision+bitemporal + year-spine (59 years) + DATED_IN + /api/timeline.
- ENRICHMENT (built, runs after resolve): stages/enrich_entities.py — per-entity LLM context call ->
  clean name + description + relation_to_solanus + location. Scoped 1,578 recurring (~$1) or 13,565 (~$6).
  Wired into build_graph (node descriptions) + run.py DAG.
- APP OVERHAUL: per-query cost fixed; settings moved to header ⚙ modal (tabs full-width); 6 tabs
  (Ask/Graph/Walkthrough/How-it-works/Tools/Explanations); /api/walkthrough (editable
  app/content/walkthrough.json) + /api/explain (docs/*.md) + /api/timeline; tool toggles explained.
- GRAPH LEGIBILITY: /api/graph defaults to SEMANTIC relations (drops 64k mention/date edges);
  CONCENTRIC layout, minNodeSpacing 100, big nodes, ALWAYS-ON edge labels, 620px canvas, focus box,
  opens on Solanus's ego-network ("wrote to" his correspondents).

RUNNING: corrected full resolve (task b0niaut7v, ~12k LLM gate, ~100/min). Budget ~$18 of $65.

OVERNIGHT CHAIN (auto-continues on resolve completion):
  1) build_graph (deduped) 2) enrich_entities.run(execute=True) 3) build_graph (enriched)
  4) re-contextualize + re-embed (chunks changed) 5) re-eval RAG 6) full examples/ + docs review/update.

## Session 3 — overnight results (2026-06-21)
- ENRICHMENT done: 1,703 recurring entities given clean name + grounded description + relation-to-Solanus
  + location (e.g. "Huntington, Indiana — where his brother Patrick and niece Helena resided").
- CONSOLIDATION: 71 same-enriched-name merges fixed the big fragmentation — Seraphic Mass Association
  (14 variants: S.M.A./Ser. Masses/S.M.Assoc...), Father Solanus Casey (13), Harper/St Joseph's Hospital,
  Mrs Alice Plunkett (Alice/Grandma Plunkett). Backup + consolidation_audit.json.
- CO-OCCURRENCE social net: APPEARS_WITH edges for non-Solanus people sharing >=2 records (2,500 pairs).
- STRUCTURED-FIELD RECONCILIATION: _ensure_person/_place reuse resolved entity nodes (WROTE_TO hub is now
  the rich "Father Solanus Casey" node, deg 330, with its description — not a description-less duplicate).
- GEOCODING + MAP: stages/geocode_places.py (Nominatim, cached) -> 269 specific places; Leaflet Map tab.
- GRAPH LEGIBILITY: relations-only default (drop 64k mention/date edges), concentric layout,
  minNodeSpacing 100, edge labels, focus search, opens on Solanus's ego-network.
- DE-HYPHENATION: lib/textclean.py wired into chunks + enrichment ("Hus- band"->"Husband").
- RE-EMBED: base space re-embedded from new (grouped + dehyphenated) chunks; ctx re-contextualized FRESH
  (fixed a stale-chunk-id append bug) + ctx re-embed via finish chain.
- DOCS: all 5 step_7 docs rewritten by a workflow in David's style, accurate to current code.
- MIMICKER: now functional end-to-end — 191 exemplars indexed, generates in-voice, neural cosine 0.79
  to David's centroid, discriminator clone-prob 0.51.
- Budget ~$21 of $75.

## Session 4 — "buff it out" (2026-06-21, awake) — data quality + transparency
David checked in: map had real errors (Italy->Texas, "the desert", buildings stacking), "always
Detroit?", wants to review the texts behind any node, enrich everything, fix NER. Diagnosed + fixed:
- NER REVIEW (workflow) -> non-person filter in resolve (_adapt + cleanup_nonpersons): dropped 1,500
  bogus "people" (713 kinship "Mother"/"his wife", 475 title/punct "Dr.", 268 initials, 24 anaphora,
  14 groups "3 children", 6 deities). Keeps real names. WROTE_TO recipient-plausibility filter (no
  more "wrote to Latin notes"); WROTE_TO 330->222.
- ENRICH v2 (all 13,131 named entities): full disambiguated location + is_place flag + world-knowledge
  research. Re-consolidate (369 more merges). GEOCODE v2: only is_place + full location (no name+",USA"
  guessing) -> 1,342 mapped places (was 388), Italy no longer in Texas; metaphors filtered.
- DEEP RESEARCH dossiers for the top 200 entities (2-4 sentence, world knowledge + corpus).
- "HIS LIFE" narrative (app/content/his_life.md, editable) at the top of the How-it-works tab.
- DETAIL PANEL (/api/entity + slide-in): click ANY node/marker -> summary + chronological connections
  + EVERY source passage (each opens the scan); letters show from/to/date. The "review all texts" view.
- STRUCTURED-FIELD RECONCILIATION fix (canonical_name stays bare so LOCATED_AT reconciles): Solanus is
  ONE node with a 29-location TRAJECTORY (West Superior WI 1896 -> Milwaukee -> Brooklyn/Yonkers/NYC
  1915-22 -> Detroit 1928 -> Garrison NY 1940). Definitively not "always Detroit".
- MAP: spiral jitter for stacked city-centroid markers; full location in place names.
- GRAPH: relation-type filter (wrote_to / appears_with / located_at / ...) + clearer caption.
- Verified: all endpoints 200; retrieval held (precision 0.865 / recall 0.729). Budget ~$28 of $75.
OPEN (need input or are diminishing-returns): external BOOK ingestion (needs a text source; dossiers
use LLM world-knowledge as the practical substitute); Wikidata/VIAF authority links; eval to thresholds.

## Session 5 — book tool, source citations, edge provenance, KG-viz overhaul
David: (1) map z-index too high (modals/popups under the map); (2) relation filter "enrolled +
others" showed a single node; (3) ingest Crosby's "Thank God Ahead of Time" PDF as a toggleable tool;
(4) cite sources in summaries; (5) for a person-person connection, SHOW the page so a co-mention can be
told from a same-page coincidence; (6) research KG-viz best practices + apply. All done:
- BOOK TOOL: stages/ingest_book.py (288pp -> 379 dumb-window chunks -> book-tgat@1536). New
  app/tools/book_search.py (default_on=False, secondary source). End-to-end verified: agent answers
  "why a simplex priest" with inline "(Crosby, p.51)". _collect_citations handles book hits as a
  distinct citation type (source="book", no archive doc_id -> no scan modal; sourceLabel/openCitation
  render them as text). SYSTEM_PROMPT now mandates inline citations, marks the book as secondary, and
  tells the model co-occurrences are co-mentions to verify by page.
- EDGE PROVENANCE (the "show the page" ask): build_graph APPEARS_WITH edges now carry shared_records
  (the actual record nodes backing the co-occurrence); export_graph_json emits weight + shared_records.
  /api/entity relations carry rel_kind + weight + evidence[] (each shared record resolved to a citation
  via _rec_cite -> doc_id/rid open the real scan; cite_label "Notebook Number 12 · Page 21"). Dossier
  renders "seen together in: <page> ↗" per connection. Rebuilt graph: 42,730 nodes / 68,136 edges;
  all 5,000 APPEARS_WITH edges carry evidence.
- KG-VIZ (workflow w82oigkj9 -> applied Tier-1 + key Tier-2): nodes now encode kind by COLOR + SHAPE
  (person=ellipse, place=round-diamond, org=round-rect, condition=hexagon, ...); edges colored + styled
  by relation (CVD-safe), edge labels only on hover/select (de-cluttered); clickable node+edge LEGEND
  (decoder ring, spotlight on click); hover degree-of-interest fade; focal node pinned center + thick
  brand ring + always-on label (via meta.center, new backend field); fcose default layout (+CDN) with
  concentric "importance rings" as an explicit mode (layout switcher); breadcrumb trail; "◎ center the
  graph on this" in the dossier. z-index + relation-filter bugs already fixed in s4 tail.
- Verified: app.js node --check OK; server.py parses; all endpoints 200; book query cites Crosby;
  APPEARS_WITH evidence resolves to openable pages. Budget ~$28.4 of $75 (book embed + graph rebuild).

## Session 6 — citation/graph bug fixes + the context-aware Entity Engine + Tier-3 viz
David reported: (1) Leoncia "appears with" Martha but the opened page shows no connection [it was an SMA
letter]; (2) "no transcribed text" on many pages; (3) OSD zooms into bottom whitespace; (4) "Colorado"
mapped as the state when it's Colorado Ave, Detroit — "NER too wishy-washy, needs a full LLM pass."
He then chose to REPLACE resolve+enrich with an iterative context-aware engine (see memory
entity-engine-direction): scope=everything (~35,915), keep old data + rollback, reproducible +
incremental, one converge pass (~$25 cap), loop-capable. Plus: do the Tier-3 KG-viz features; add Street
View on the map.

ROOT CAUSES (workflow wwumqkuar, adversarially verified):
- BUG1: rid region-ids are PAGE-LOCAL, not unique ("doc_1.src_content.0" owned by ~1,942 records).
  _build_rid_index was a flat {rid:node} dict → last-writer-wins → ALL such mentions funneled onto ~29
  Appendix_3 nodes. Made 96% of APPEARS_WITH edges fake AND misrouted 50% of MENTIONED_IN (so opening a
  connection showed the WRONG page). FIX: composite (doc_id,rid) keys + never bare-rid lookup + exclude
  notebook_page from co-occurrence + granular shared_records + gold links resolve within their page.
  Result after rebuild: APPEARS_WITH 5,000→718 (359 distinct), tainted 96%→0, misrouted MENTIONED_IN
  50%→0.06%. (server.py/_record_text + resolve already used composite keys — not affected.)
- BUG2: notebook_page nodes have no text field (text lives on entries[]/regions[]). FIX: _record_text
  + source_region assemble page-level text (empty record-nodes 717→1).
- BUG3: OSD `rect.times(1.6)` scales x/y from the ORIGIN (pushes view down-right into the bottom margin).
  FIX: expand the box about its CENTER + fitBoundsWithConstraints + goHome fallback for missing/degenerate
  vertices.

ENTITY ENGINE (stages/entity_engine.py) — REPLACES resolve final-cluster + enrich:
- Per entity: an LLM "think" reads its source excerpts in context and returns is_real_entity, entity_kind,
  what_it_is, disambiguated_identity (canonical), place_type (city/street/institution/...), is_mappable,
  location, parent_place, role, relation_to_solanus, description, confidence. Validated on hard cases
  (Colorado→street in Detroit, Enrolled→noise, messy Solanus→correct).
- Merge = O(n) group-by normalized disambiguated_identity (cautious: same kind, non-generic, non-noise).
- Central store data/entity_store.json (+ .backups/ rollback + content_hash incrementality + audit).
  emit_enriched() derives entities_enriched.json with the PLACE GATE: street → not mapped; building/
  institution → parent city; city/state/etc → own location (kills Colorado-state bug). build_graph +
  geocode now read the store; build_graph drops is_real_entity=False noise + trusts engine entity_kind.
- Bulk model gemini-2.5-flash-lite (~$6 for 35k); tiered/loop-capable (--passes, --max-cost).
- ROBUSTNESS (a real saga): genai SDK has NO working client-side read timeout (server-deadline only),
  so ~1-2% of calls intermittently wedge. Fixed with: shared memoized client (per-call client leaked
  sockets → pool exhaustion hang), a NO-PROGRESS WATCHDOG (_drain_futures: abandon stragglers after 130s,
  resumable via content_hash), client warming before the concurrent burst, and os._exit(0) so wedged
  non-daemon threads can't block exit. Full run: zero abandons, ~$5.

TIER-3 viz (app.js + index.html): (a) confidence encoding — edge opacity by method/weight + "co-occ ≥"
slider hiding weak inferred links; (b) semantic-zoom label tiers (per-render degree rank + zoom handler);
(c) timeline DATE-BRUSH (drag a year range → filter graph edges via edge.year_span added to build_graph;
double-click clears); (d) super-hub "＋ load more" surfacing meta.truncated (was silently dropped) +
raising the cap. Plus STREET VIEW links (Google pano URL, no key) in map popups + the place dossier.
LLM HTTP timeout fix (shared client + warming) also helps the agent loop.

### Session 6 — final run results (verified)
Engine ran: 35,914 thought → 31,737 canonical (19,469 real / 12,268 noise flagged), 1,684 merges, $4.66,
1 straggler abandoned (resumable). Merges high-quality: Father Solanus ×11 (incl. birth name B. Francis
Solanus); His Eminence→Cardinal Farley; Pius X + Bishop Sarto→Pope Pius X; Poverello of Assisi→St.
Francis; cancer ×340 / T.B. / paralysis dedup. Graph rebuilt from store: 30,628 nodes (was 42,730),
APPEARS_WITH 602 (clean), 677 mapped places (was ~3). Verified: Colorado is now a street (only real
"Trinidad, Colorado" maps); "Enrolled" noise dropped; 183/196 Solanus edges carry year_span (date-brush);
St. Felix Friary has lat/lon (Street View); Leoncia has 1 real relation + correct source text (bugs 1+2);
all endpoints 200. Re-tune merges without re-spend via `--remerge` (pre-merge thought saved). Budget ~$33/75.
NOTE: re-running `entity_engine.py --execute` picks up the 1 abandoned entity incrementally (content-hash).

### Session 6 — adversarial audit + fixes (workflow wal2l4dni)
Audited the engine output on the full corpus (4 parallel critics + synthesis). Verdict: sound core, three
leaks — all fixes FREE/deterministic except a cheap re-think. Applied:
- P1a NOISE FALSE-DROPS (15.9% overall, 22.9% among PERSON/PLACE/ORG): real proper nouns mis-killed
  (Pope Leo XIII, Guam, Red Wing, B&O Depot, Pennsylvania RR) — root cause: noise judged on a head-of-
  letter snippet that didn't contain the name. Fix: build_graph keeps is_real_entity=false drops that are
  recoverable proper nouns (_recoverable_propernoun); AND rethink_subset re-judges them with a
  MENTION-CENTERED context window (_engine_contexts).
- P4 UNCLASSIFIED (1,216 with confidence 0 / no what_it_is — errored first pass): re-thought via
  `--rethink-subset` (re-think under-served subset → remerge → emit). Cost $0.30 for 1,819 entities.
- P2a MERGE RECALL: _norm_identity now strips a leading 'the' + ONE honorific/saint title (st/saint/pope/
  fr/father/rev/sr/sister/br/brother/mother/dr/bl/ven/cardinal/bishop...) so 'Saint Francis of Assisi'==
  'Francis of Assisi' and 'The Catholic Church'=='Catholic Church'. GUARDS: never strip mr/mrs/ms/miss
  (spouse pairs), never a relational title ('Mother of Edward Hickey'). +134 correct merges.
- P1b CROSS-CATEGORY GUARD: reflect_and_merge refuses to fold a PLACE into a PERSON group or vice versa
  (the 'David Dion = Idaho' bug). Prevents future folds (3 pre-existing folds are baked into this store
  since pre-merge thought wasn't saved that run; negligible).
- P2c GEOCODER: vague-label denylist (Earth/continents/oceans/regions/'X River'...) + country-consistency
  reject (countrycodes bias; drop hits whose country contradicts the requested country) + purge of bad
  CACHED geocodes on re-run. Fixes Earth->Texas, Central Europe->Oklahoma, Central America->Australia.
- P3a/P3b DATA HYGIENE: null place_type on non-place kinds; build_graph maps engine kinds
  institution/building->place.
- Added `--remerge` / `--rethink-subset` modes + entity_thought.json persistence (re-tune merges free).
Result: 31,737 -> 31,593 canonical (after audit-fix remerge), 728 mappable places. SKIPPED P2b (cross-kind
merge) — audit flagged too many legitimately-distinct cross-kind pairs. Docs updated (README, app/README,
run.py DAG now includes entity_engine + geocode_places, walkthrough step 7 + todo).

## Session 7 — multi-model embeddings + community detection + Mapper, plus the big UX overhaul
UX (earlier this session): fixed the citation page/region misalignment (multi-page letters reuse one
rid; source_region now keys the region to the mention's page + computes that page's image, and surfaces
date/written-from/recipient); scrubbed all decorative emoji/glyphs; "?" help modals (graph/map/tools,
scroll-locked bg); bigger citation modal; graph fills the page (maxw 1600) with a collapsible right
settings panel; tabbed dossier (Overview/Connections/Sources/Biography) with per-connection TWO buttons
(go-to-entity + connecting-document(s) with an on-demand LLM explanation via /api/connection); biography
(Crosby) passages integrated per entity; fixed the "Sisters of the Atonement" duplicate (recipient/
location reconcile to ANY entity kind incl. ORG via _ORG_IDX) + dossier gathers passages from edges;
REAL full-graph exploration (double-click to expand a node's neighbors in place, /api/entities browser
of top-connected entities, legend hide/show by kind, "Load more", rewritten honest Help).

EMBEDDINGS (this turn): Claude has NO embeddings API (Anthropic recommends Voyage). Added + ran the full
text suites over the 9,227-chunk corpus: OpenAI text-embedding-3-small@1536 + 3-large@3072; Voyage 4
family voyage-4-large/voyage-4/voyage-4-lite @1024 (dims 2048/1024/512/256; 200M free); bge-large-en-v1.5
@1024 (local). Added rerank-2.5 + rerank-2.5-lite. Fixed _openai/_voyage to truncate over-long chunks
(OpenAI 8192-token cap). Selectors auto-populate from /api/config (built spaces + RERANKERS). Verified
retrieval works on the new spaces. (No multimodal / no GPT-chat-as-embeddings — those were mix-ups.)

COMMUNITY + MAPPER: stages/graph_analysis.py — Louvain communities over the semantic subgraph (955
communities / 5,290 entities; big ones = cancer, operation/tumor, the Solanus person/correspondence core,
blind/deaf, tuberculosis, cured) + a Mapper-style overview = the NERVE of the community cover (17 super-
nodes, 44 links; degree-lens Mapper was degenerate on this skewed graph, so community-quotient is the
faithful minimap). Writes communities.json + mapper.json (sidecars). Incorporated: /api/communities +
/api/graph attaches community id to nodes; frontend "colour nodes by community" + a Communities browser
to jump into a cluster. Installed scipy + scikit-learn. Run order: build_graph -> graph_analysis.

### Session 7 addendum — model access + graph speedup
MODEL ACCESS: wired OpenAI generation in lib/providers/llm.py (_openai_chat, with a temperature-retry
for GPT-5.x reasoning models) + _anthropic_chat (gated on ANTHROPIC_API_KEY). config.LLMS now lists
gpt-5.5/gpt-5.4/gpt-5.4-mini (openai, wired+verified) and claude-opus-4-8/sonnet-4-6 (anthropic, shown
unavailable until a key). generate_with_tools (the agent loop) stays Gemini-native and orchestrates tools
on the default Gemini even when another LLM is picked (the single-shot /api/ask honours the chosen model
fully — that's where it matters). Note: Vertex Model Garden has Claude+Gemini but NOT OpenAI; we use the
direct Gemini key, not Vertex. PROVIDER-AWARE PICKERS: /api/config returns providers{llms,embeddings,
rerankers} + availability{provider:bool from key presence}; the UI marks key-less options disabled
("needs X key") and reorders embeddings+rerankers by provider-match to the chosen LLM (descending).
Embeddings are an independent axis (any embedding works with any LLM) — ordered for convenience only.

GRAPH SPEEDUP: fcose/concentric cost now scales with node count (_curNodeCount): >80 nodes drops to
default/draft quality, fewer iterations, no animation, no label-aware sizing; cytoscape init got
textureOnViewport + hideEdgesOnViewport + pixelRatio:1; hover-fade disabled above 350 nodes. Pan/zoom
stays smooth as the graph is expanded.
