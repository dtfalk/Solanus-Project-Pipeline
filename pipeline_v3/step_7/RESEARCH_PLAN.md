# Deep-Research Plan — Maxing out NER, the Knowledge Graph, and RAG

How far we can take the Solanus archival tool, grounded in 2026 research. Ordered as you asked:
**structural corpus problems first** (cross-page continuation → dates → entity grouping), then the
full retrieval/generation frontier, then a tiered roadmap. Everything plugs into the existing
step_7 diff-and-rerun pipeline + model-agnostic adapters; every technique is a stage or a
toggleable agent tool. Sources at the bottom.

Cross-cutting principles: model-agnostic (LLM/embed/rerank swappable), provenance-first (cite
`doc_id+rid+page+vertices` → IIIF region), reproducible (hashed stages), always cost-logged,
and **non-destructive** (every layer is a new artifact; refine upstream → re-run propagates up).

---

## PART A — Structural corpus problems (build these first)

### A1. Cross-page notebook continuation  ← *start here*
The notebooks are a running register: entries spill across pages, a date isn't re-listed, an entry
is truncated and continues, and pages literally say **"Page 178 Cont."** Our raw data already gives
us strong signals to exploit (in `notebooks.json`): each entry has `page_label` (e.g. "Page N",
"Page N Cont."), `date`, `linked` (degree-1 `src_content` links from the gold connection graph),
plus page order (`pdf_page_number` / `page_number_in_type`) and the page `notebook` name.

A **`stitch_notebooks` stage** that reconstructs *logical* entries:
1. **Order** pages by `pdf_page_number` within each notebook; entries by reading order on the page.
2. **Continuation flags:** mark pages whose `page_label` matches `/cont/i`; mark entries that are
   *truncated* (text ends without terminal punctuation / mid-clause) or *orphan-start* (begins
   lower-case / mid-thought). This is exactly the "send the entry to an LLM and ask if it's
   truncated" pattern proven on historical patent/register digitization.
3. **Stitch:** join a truncated tail-of-page entry with the head-of-next-page entry. Validate
   ambiguous joins with a **multimodal LLM** (Gemini vision over the *pair of page images* — we have
   the masked PNGs) — the 2026 approach for archival continuation. Keep both the fragments (with
   provenance) and the stitched logical entry (non-destructive).
4. **Carry-forward** the last-seen date/notebook/page context across continuation entries (see A2).
5. Emit `continued_from` / `continues_on` edges into the KG, and a `logical_entry_id` grouping the
   fragments — so retrieval returns whole entries while citations still point to exact regions.
6. Later: **KG link-prediction** (KGE) can *propose* missing continuation/same-entry links for
   David to confirm — turning "missing connections" into a review queue, not a dead end.

### A2. Date reconstruction (split + uncertain dates)
Dates are scattered: an entry has month/day ("Mar. 30"), sometimes only a day ("22"), the **year
lives in the page's archival field** (`archv_date` e.g. "1933, October"), and many are circa/ranges
("c. 1945", "1940, February — October"). A **`normalize_dates` stage**:
1. **Hierarchical assembly:** combine entry month/day + page/notebook year → full date. When only a
   day is present, inherit the month from page/previous entry; when nothing, carry-forward.
2. **Two times per entry (bitemporal):** capture *enrollment* vs *report/outcome* dates ("Enrolled
   May 3 1923 … Reports Dec 8") as distinct timestamps — critical for the favors timeline.
3. **Canonical encoding = EDTF** (Extended Date/Time Format, ISO 8601-2, Library of Congress) — the
   archival standard for *uncertain/approximate/partial* dates: `1945~` (circa), `1940-02/1940-10`
   (range), `1933-10-XX` (unknown day). This is the right home for our fuzziness.
4. **Normalizer:** LLM-assisted temporal tagging (HeidelTime-style) → EDTF, cost-logged. Keep the
   raw string + the EDTF value + a confidence/precision flag.
5. Feeds temporal retrieval + the temporal KG (A4/Part C).

### A3. Entity grouping / resolution (same thing, many names)
"Grace." vs "Grace Panyard", O'Donnell OCR variants, abbreviations. A **`resolve_entities` stage**
(blocking → compare → match → cluster, the standard ER pipeline):
1. **Normalize:** strip titles/punct, expand abbreviations, double-metaphone phonetics.
2. **Block** on phonetic key + location + date proximity (cheap candidate generation).
3. **Match:** **Splink** (probabilistic record linkage, Fellegi-Sunter; scales to millions on a
   laptop) for the bulk, with **LLM in-context clustering / LLM-as-judge** to adjudicate ambiguous
   clusters (2025 SOTA, ~94% on name variants). Embedding similarity as a third signal.
4. **Canonicalize:** one authority record per entity {canonical_name, variants[], attrs, all
   mentions+provenance, confidence}. Family clusters (Casey/Panyard/O'Donnell) as groups.
5. **Authority reconciliation** (archival authority control, NACO 2025): link persons → Wikidata/
   VIAF, places → GeoNames/Getty TGN. Gives global IDs, disambiguation, enrichment.
6. **KG completion** (KGE link prediction) proposes likely `same_as` merges for review.
7. Coreference (A-NER) feeds this: resolve within- and cross-document mentions first.

---

## PART B — Advanced NER & extraction (depth)

- **Rich schema:** fine-grained types — PERSON (+age, role, religious_order), PLACE (+type:
  city/address/institution), ORG (orders, parishes, Seraphic Mass Association, hospitals), CONDITION
  (medical/behavioral/spiritual, body-part), FAVOR/OUTCOME (enrollment, reported result, "Cured"),
  RELATION (family/correspondent/advocate), ROLE/TITLE, DATE. **Nested** entities allowed.
- **Reliable structured output:** Gemini `response_schema` (or **Outlines/XGrammar** constrained
  decoding for local models) → guaranteed-valid JSON. (Watch the small "constrained-decoding
  reasoning tax"; keep a free-form reasoning field then the schema.)
- **Two-tier extractor (cost + quality):** **GLiNER fine-tuned on David's gold labels** for fast,
  cheap, *local* bulk NER over 9k chunks; escalate hard/low-confidence pages to Gemini. (GLiNER is
  proven on historical docs, supports custom types + nested spans.)
- **Coreference → Relation extraction → Event extraction** (the KG-construction trinity, e.g.
  LINK-KG / Extract-Define-Canonicalize / Relik): mentions → entities → relations (WROTE_TO,
  ENROLLED_FOR, LOCATED_AT…) → *events* (enrollment event, outcome event) with time + provenance.
- Structured fields are a free head-start: recipient/sender/location/date are already separated per
  letter — harvest them directly, LLM only the free text.

---

## PART C — Knowledge graph (depth) & temporal layer

- **Schema:** align to **RiC-O** (Records in Contexts; proven on scientific correspondence) +
  optional CIDOC-CRM/Linked Art for objects; export RDF/JSON-LD for scholarly interop.
- **Temporal / bitemporal KG:** validity intervals on edges; **event-time vs ingestion-time**;
  point-in-time ("as-of") retrieval. Patterns: TG-RAG (TKG + hierarchical time graph), IA-RAG
  (interval algebra), Graphiti/Zep as a ready engine to evaluate vs a custom RiC-O temporal graph.
- **KG embeddings + link prediction:** infer *missing* edges — continuation links (A1), same-as
  (A3), and latent relationships — surfaced as review suggestions, never auto-asserted.
- **Community detection + summaries** (GraphRAG/LightRAG) for thematic/global questions
  ("what conditions recur", "the Casey family network").
- Every node/edge carries provenance (rids) → IIIF region citation.

---

## PART D — Retrieval frontier (how advanced we can go)

Ranked by leverage for *our* corpus:

1. **Contextual Retrieval (highest leverage).** LLM prepends a 2–3 sentence context to each chunk
   before embedding *and* BM25 ("This is a Nov 1933 Notebook No.6 entry about a cancer cure for…").
   Anthropic reports **−67% retrieval failures** with contextual-embeddings + contextual-BM25 +
   rerank. Perfect for our terse, date-less notebook entries — it injects exactly the context the
   continuation/date work recovers.
2. **Hybrid + RRF + rerank.** BM25 + dense fused with RRF(k=60) → cross-encoder rerank
   (Voyage rerank-2.5 / Cohere Rerank 4 Pro / Zerank / local bge-reranker — *swappable axis #3*).
3. **Multimodal / visual retrieval (ColPali / ColQwen).** Retrieve over the **page images** with
   late-interaction (MaxSim over visual patches) — *no OCR needed*; catches marginalia/layout/OCR
   misses. On scanned docs visual recall ≫ text (e.g. 84% vs 62%). Run as its own tool and fuse
   (RRF) with text retrieval; two-phase (mean-pool coarse → MaxSim rerank top-100).
4. **Query transforms:** HyDE (embed a hypothetical answer), RAG-Fusion/multi-query (+RRF),
   decomposition (multi-hop), step-back (generalize) — generated cheaply, big recall gains.
5. **RAPTOR:** recursive cluster+summarize tree over the corpus → coarse-to-fine retrieval for
   global/thematic questions (cheaper than full GraphRAG; +~20% on long-doc QA).
6. **GraphRAG / KG-RAG:** traverse the entity/temporal graph + community summaries for relational
   and aggregate queries.
7. **Adaptive / router RAG:** a query-complexity classifier routes simple→direct, moderate→single
   retrieval, complex→full agentic loop; metadata pre-filter to the right index. ~−28% cost,
   ~+8% accuracy. This *is* the agent's orchestration policy.
8. **Self-RAG / CRAG:** grade retrieved context, reflect, re-retrieve or abstain — guards against
   confident-but-wrong answers (vital for an archive).
9. **Contextual compression / late chunking** to trim context to what matters.

All of the above become **toggleable tools** in the dev harness, so you can measure each model's
behavior with any subset enabled.

---

## PART E — Generation, agent, multi-model, eval, cost

- **Generation:** strictly grounded + cited (→ IIIF region + PDF), optional self-critique, optional
  structured answers (timelines, tables of favors).
- **Agent (dev tool):** the toggleable-tools harness + adaptive router; full OpenTelemetry trace.
- **Multi-model everywhere:** LLM, embedding (multi-space store), reranker all swappable; evaluate
  every combination on the eval set and pick per-task winners.
- **Evaluation & observability:** RAGAS (claim-decomposition faithfulness) + ARES (synthetic
  queries + judge) + a **golden archival Q&A set**; thresholds (faithfulness 0.75, ctx-precision
  0.7, recall 0.8); embedding-drift + retrieval-relevance via Arize Phoenix (OpenInference/OTel).
- **Cost engineering:** **Batch API −50%** for all bulk extraction/embedding (24h async, fine
  offline); **prompt caching −50–90%** on the repeated NER instruction/few-shot prefix; local
  models ($0) for bulk where quality allows. Updated: Gemini 2.5 Flash ≈ $0.15 in / $0.60 out /M →
  full embed all-spaces ≈ $0.32, full NER ≈ **$1–2** (was $4.32) with batch+cache.

---

## PART F — Tiered roadmap ("how advanced", choose your altitude)

- **Tier 0 — solid RAG (cheap, days):** contextual retrieval + hybrid + RRF + rerank + adaptive
  router + cited generation + RAGAS/Phoenix eval. Already-proven embed/LLM core. This alone is a
  strong archival search/QA tool.
- **Tier 1 — structural truth (the corpus problems):** stitch_notebooks (A1) + normalize_dates/EDTF
  (A2) + resolve_entities/Splink+authority (A3) + rich NER (B). Unlocks accuracy + browsability.
- **Tier 2 — knowledge graph + GraphRAG (C):** RiC-O temporal KG, coref/relation/event, community
  summaries, KG-RAG tool, graph viz.
- **Tier 3 — multimodal + frontier (D):** ColPali/ColQwen visual retrieval + fusion; RAPTOR; HyDE/
  RAG-Fusion; Self-RAG/CRAG; KG link-prediction review queue.
- **Tier 4 — scholarly platform:** IIIF deep-zoom citations, Linked-Data exports (RiC-O/EDTF/
  authority IDs), fine-tuned GLiNER + fine-tuned embeddings on David's labels, web hosting +
  monitoring/drift.

Recommended path: **Tier 0 → Tier 1 (A1 first) → Tier 2 → Tier 3**, with eval gating each step.

---

## Research basis (sources)
- Advanced RAG: [12 advanced techniques](https://atlan.com/know/advanced-rag-techniques/) · [RAG 2026 blueprint](https://dev.to/suraj_khaitan_f893c243958/-rag-in-2026-a-practical-blueprint-for-retrieval-augmented-generation-16pp) · [beyond vector search](https://machinelearningmastery.com/beyond-vector-search-5-next-gen-rag-retrieval-strategies/)
- Contextual Retrieval: [DataCamp guide](https://www.datacamp.com/tutorial/contextual-retrieval-anthropic) · [contextual embeddings + hybrid](https://www.freecodecamp.org/news/how-contextual-embeddings-and-hybrid-search-fix-retrieval-failures/)
- RAPTOR: [RAGFlow long-context RAPTOR](https://ragflow.io/blog/long-context-rag-raptor) · [RAG vs GraphRAG eval](https://arxiv.org/html/2502.11371v3)
- Multimodal/ColPali: [late-interaction overview (Weaviate)](https://weaviate.io/blog/late-interaction-overview) · [ColPali on GPU cloud](https://www.spheron.network/blog/colpali-multimodal-document-rag-gpu-cloud/) · [HF multimodal RAG + reranker cookbook](https://huggingface.co/learn/cookbook/multimodal_rag_using_document_retrieval_and_reranker_and_vlms)
- NER: [GLiNER repo](https://github.com/urchade/GLiNER) · [fine-tuning generalist NER](https://labelstud.io/blog/fine-tuning-generalist-models-for-named-entity-recognition/) · [historical NER (Zibaldone)](https://arxiv.org/pdf/2505.20113)
- Entity resolution: [Splink](https://github.com/moj-analytical-services/splink) · [ER intro](https://medium.com/@adev94/entity-resolution-an-introduction-fb2394d9a04e) · [LLM in-context clustering ER](https://arxiv.org/abs/2506.02509)
- Coref/RE/EE → KG: [LINK-KG](https://arxiv.org/html/2510.26486) · [Extract-Define-Canonicalize](https://arxiv.org/pdf/2404.03868) · [Relik in LlamaIndex/Neo4j](https://neo4j.com/blog/developer/entity-linking-relationship-extraction-relik-llamaindex/)
- Dates/temporal: [EDTF validation (UNT)](https://digital2.library.unt.edu/edtf/) · [EDTF overview](https://www.librarianshipstudies.com/2016/05/extended-date-time-format-edtf.html) · [continuation via multimodal LLM (German patents)](https://arxiv.org/pdf/2512.19675)
- Temporal RAG/KG: [RAG meets temporal graphs](https://arxiv.org/abs/2510.13590) · [Zep/Graphiti](https://www.emergentmind.com/topics/zep-a-temporal-knowledge-graph-architecture) · [IA-RAG](https://arxiv.org/html/2606.06044)
- KG completion: [KG completion review](https://www.mdpi.com/2078-2489/13/8/396) · [type-augmented KGE](https://www.nature.com/articles/s41598-023-38857-5)
- Structured output: [Outlines (AWS)](https://aws.amazon.com/blogs/machine-learning/generate-structured-output-from-llms-with-dottxt-outlines-in-aws/) · [JSONSchemaBench](https://arxiv.org/pdf/2501.10868)
- Adaptive/router RAG: [lightweight query routing](https://arxiv.org/pdf/2604.03455) · [agentic retrieval (LlamaIndex)](https://www.llamaindex.ai/blog/rag-is-dead-long-live-agentic-retrieval)
- Eval: [RAG eval metrics 2026](https://blog.premai.io/rag-evaluation-metrics-frameworks-testing-2026/) · [RAGAS/ARES](https://benchmarkingagents.com/rag-eval/)
- Cost: [prompt caching 2026](https://www.digitalapplied.com/blog/prompt-caching-2026-cut-llm-costs-engineering-guide) · [Gemini pricing 2026](https://costbench.com/software/llm-api-providers/google-gemini-api/)
