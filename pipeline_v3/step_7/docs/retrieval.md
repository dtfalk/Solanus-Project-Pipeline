# Retrieval — how the system finds the right page (step_7)

*Quick-read companion to `retrieval.tex`. Everything here is true to the code as it stands:
`lib/retrieval.py`, `lib/chunks.py`, `lib/textclean.py`, `stages/contextualize_chunks.py`,
`stages/embed_corpus.py`, and `lib/vectorstore.py`.*

---

## TL;DR

You ask a question about Father Solanus Casey's letters and notebooks; retrieval hands the generator
the handful of **passages** that actually answer it, each one carrying enough provenance to cite the
**exact scanned region** (`doc_id → rid → vertices → IIIF deep-zoom box`). The default ("free") path
is already a strong retriever: it runs both a **sparse lexical search (BM25)** — great at exact names
and dates like *Panyard* or *Nov. 8th* — and a **dense semantic search** over embeddings — great at
paraphrase like *favor ≈ miracle ≈ answered prayer* — then merges the two ranked lists with
**Reciprocal Rank Fusion (RRF)**, and finally **grades** the result so it can honestly say "not
found." Everything that costs money — a cross-encoder **reranker**, **HyDE**, **multi-query /
RAG-Fusion**, the LLM router — is a toggle that defaults off and is cost-logged when on. Underneath
all of it sits the part that decides what a "passage" even is: **one chunk per letter and one per
*stitched* notebook entry** (a cross-page entry is a single chunk, not three fragments), with OCR
line-break hyphens repaired first, optionally **contextualized** (a one-line situating preamble
prepended before indexing), and embedded into per-space partitions — including a dedicated
`…-ctx` partition for the contextualized version so you can A/B it against the raw one.

---

## A 60-second primer (skip if you know RAG)

> *Skip this if you already know what BM25, embeddings, a cross-encoder, and RRF are.*

- **BM25** is keyword search done right. For each query word it scores a passage by how often that
  word appears (with diminishing returns, so 10 copies aren't 10× better than 1), how *rare* the word
  is across the whole corpus (rare words are more telling), and a length correction so long passages
  don't win just for being long. It only sees the literal words.
- **Dense / embeddings.** A model turns a passage into a vector — a point in high-dimensional space —
  positioned so that passages *about the same thing* land near each other even if they share no words.
  "Search" is then "find the nearest vectors to the query's vector" (we use cosine similarity).
- **Cross-encoder reranker.** A heavier model that reads the query and one candidate passage *together*
  and outputs a single relevance score. Far more accurate than either search above, but too slow to run
  over the whole corpus — so you use it only to re-order a small shortlist.
- **RRF (Reciprocal Rank Fusion).** A trick for combining two ranked lists when their scores aren't
  comparable (BM25 numbers and cosine numbers live on different scales). Throw the scores away; use only
  *rank position*. More on this below.

---

## What's wired vs. what's planned

| Piece | What it does | Status in code |
|---|---|---|
| **Chunking** | One chunk per letter; one per **stitched** notebook entry | **Wired** (`lib/chunks.py`) |
| **De-hyphenation** | Repair OCR line-break hyphens before indexing | **Wired** (`lib/textclean.py`) |
| **Heuristic router** | Classify query simple/moderate/complex, free | **Wired** (`route_query`) |
| **LLM router** | Same, LLM decides ambiguous phrasing | **Wired**, gated (`use_llm_router`) |
| **BM25** | Sparse lexical search | **Wired**, free (`rank_bm25` + correct fallback) |
| **Dense** | Semantic search over a chosen embedding space | **Wired**, free if the space is local |
| **RRF** | Fuse ranked lists by rank, not score | **Wired** (`reciprocal_rank_fusion`) |
| **Rerank** | Cross-encoder re-reads (query, passage) pairs | **Wired**, gated (`use_rerank`; local free) |
| **HyDE** | Embed a hypothetical *answer* instead of the question | **Wired**, gated (`use_hyde`) |
| **Multi-query / RAG-Fusion** | Retrieve for several paraphrases, fuse | **Wired**, gated (`use_multiquery`) |
| **CRAG / Self-RAG grade** | Trust the set? answer / caveat / abstain | **Wired** free (`grade_context`) + gated per-doc (`grade_document`) |
| **Contextual Retrieval** | LLM-written situating preamble, prepended pre-index | **Wired as a stage** (`contextualize_chunks.py`), not yet run |
| **Adaptive pipeline** | route → transform → retrieve → grade | **Wired** (`route_and_retrieve`) |

Everything in `retrieval.py` is **pure functions that return ranked `Hit`s** — nothing mutates the
corpus or writes files. That keeps each piece trivially testable and lets the dev harness flip any
subset on while it measures model combos.

---

## Part 1 — What is a "chunk"? (the unit retrieval ranks)

Before any search can run, the corpus has to be cut into the passages we rank. This happens in
`lib/chunks.py`, and the cutting decisions matter more than people expect — a search can only ever
find a chunk that was drawn sensibly in the first place.

### Letters: one chunk = one letter

A letter's text is split across labelled regions on the page — recipient, greeting, body, farewell,
signature, date, place. `_letter_text` joins them back in reading order (`src_recipient`,
`src_greeting`, `src_content`, …) into **one chunk for the whole letter**. The chunk's `meta` keeps the
`doc_id`, section, recipient, date, and page/pdf-page so a hit can always be cited back to the page.

### Notebooks: one chunk = one *stitched logical entry*

The notebooks are the interesting case. They are **registers** — running lists of prayer-favor entries
— and a single entry routinely **spills across a page break** ("Page 178 Cont."). If you naively chunk
per page-region, that one favor becomes two or three disconnected fragments, none of which reads as a
complete thought. We call this the **"baked beans" problem**: a source artificially shattered into
crumbs that individually retrieve badly.

So the chunker prefers `data/stitched_notebooks.json` when it exists. An upstream stitch stage has
already merged cross-page continuations into **logical entries**, and `build_chunks` makes **one chunk
per logical entry**. A multi-page entry is therefore a *single* chunk whose provenance lists **every**
page and region it spans:

- `meta.rids` and `meta.pages` / `meta.pdf_pages` — the full set of regions/pages the entry covers;
- `meta.spans` — a compact per-fragment list (`doc_id`, `rid`, `page`, `vertices`, `min_conf`,
  `page_label`) the citation modal can iterate, so the UI can box **each** page the entry touches;
- `meta.is_multi_page` / `meta.n_fragments` — flags so downstream code knows it's a stitched entry.

If the stitched file isn't built yet, `build_chunks` **falls back** to one chunk per raw page-entry
(`<page_id>::<rid>`) so the pipeline still works — just without the cross-page merge.

> One source, possibly many pages → one chunk. That's the whole point of stitching: retrieval ranks
> *meanings*, not *page fragments*.

---

## Part 2 — De-hyphenation (clean the text before you index it)

The scans break words across lines with a trailing hyphen — `"Hus-\nband"`, `"Diabe- tis"`,
`"En-\nrolled"`. Azure OCR keeps that as a hyphen plus whitespace, and if you index it raw, three
things suffer: BM25 never matches *husband* (it sees `hus` and `band`), the embedding is computed on
mangled text, and the answer you show the user has garbage hyphens in it.

`lib/textclean.py` fixes this **conservatively**, on every chunk, before chunking/embedding/display.
The trick is one reliable signal: **a hyphen followed by whitespace and a lowercase letter is almost
always a line-break artifact**, because a *real* compound is written with **no space** after the
hyphen. So:

- `"Hus- band"` → `"Husband"`, `"Diabe-\ntis"` → `"Diabetis"` (artifacts collapsed), but
- `"well-known"` and `"twenty-one"` are **left untouched** (no space → real compound).

It iterates a couple of passes to catch chains like `"Dia- be- tis"`, then normalizes trailing spaces
before newlines. Deliberately minimal — better to leave a rare odd case than to damage a genuine
hyphenated word. (A fuller version using per-word polygon geometry to disambiguate every case is noted
as a future step_5/6 enhancement; this string-level pass already catches the overwhelming majority at
near-zero risk.)

---

## Part 3 — Contextual Retrieval (the biggest win for *these* notebooks)

Here is the single hardest retrieval problem in this corpus. A notebook entry, in full, might read:

```
22  Cured.
```

Embed that and BM25-index that, and a search for *"cancer cure reported November 1933"* sails right
past it — even though it **is** the answer. The date, the notebook, and the favor being reported all
live *around* the entry (on the page, in the running register), not *in* it. Both BM25 and dense search
can only see the words that are present, so terse register entries become nearly invisible.

Anthropic's **Contextual Retrieval** fixes this directly. `stages/contextualize_chunks.py` asks an LLM
to write a short (≈15–50 word) **situating preamble** for each chunk and **prepends** it to the chunk
text before indexing. The "22 Cured." entry becomes something like:

> *This is a Nov 1933 entry in Fr. Solanus Notebook No. 6 reporting a favor: a cancer condition
> described as cured through the Seraphic Mass Association.*

Now the same vector and BM25 index carry the date, the notebook, and the topic, so the query and the
chunk finally land in the same neighborhood. Anthropic reports this can cut retrieval failures by up to
~67% when paired with contextual BM25 and a reranker; for our date-less, continuation-laden registers
it's the most direct win available.

How the stage builds a faithful preamble without hallucinating:

- It harvests the situating facts the chunk text *can't* see from the raw step_6 records — the page's
  **archival year/month** (`archv_date` / `src_date`, e.g. "1933, October"), the entry's
  **`page_label`** ("Page 178 Cont."), and a small **window of neighbouring entries** on the same page
  (continuation context). The LLM mostly *phrases known facts* rather than inventing them, with explicit
  rules to omit anything genuinely unknown.
- It's **non-destructive**: output goes to a new file, `data/contextualized_chunks.jsonl`, one row per
  chunk `{id, kind, context, text, meta}`. The chunk `id` and provenance are untouched, so citations
  still point at the exact region. The **raw `text` is preserved** alongside `context` so the UI can
  always display the original passage (not the synthetic preamble) and we can re-generate context if we
  change the prompt.

Cost discipline (this is a paid LLM pass over ~9.3k chunks, **wired but not yet run**): the long,
identical instruction is sent as a **cached system prefix** (so per-chunk you only pay for the short
chunk + its page context), and the full run is meant to go through the provider's async **Batch API**
(~−50%, since contextualization is offline and embarrassingly parallel). The interactive path that
exists today **resumes** (rows with a non-empty context are kept), runs on a **thread pool**, and
writes **append-only** so a crash leaves a valid prefix.

---

## Part 4 — Embedding-space partitions (incl. the `…-ctx` space)

Dense search needs vectors, and we keep them in a **partitioned local vector store** (`lib/vectorstore.py`).
A **space** is the string `model@dim`, e.g. `gemini-embedding-001@1536`. Each space gets its own folder
under `data/vectors/<space>/` holding `vectors.npy` + `ids.json` + `metas.jsonl`. Search is brute-force
cosine over pre-normalized vectors — instant at this corpus size (~9k) and swappable for LanceDB later
without touching callers. RAG picks **one** space (a runtime variable; default
`gemini-embedding-001@1536`) and queries only that partition.

Why partitions at all? So you can build several embedding spaces side by side and **A/B them** — a 768-d
Gemini space vs. a 3072-d one vs. a local `bge` space vs. an OpenAI/Voyage space — without any of them
interfering. `stages/embed_corpus.py` builds the default matrix (`config.EMBEDDING_MATRIX`), one
partition per space.

### The contextualized space: `…-ctx`

The contextualized chunks get their **own** partition so you can compare "raw" vs. "contextualized"
retrieval directly. The mechanism is a clean alias:

- In `config.EMBEDDINGS`, the key `gemini-embedding-001-ctx` has an `"api": "gemini-embedding-001"`
  override. The **distinct key** gives it its own vector-store partition and its own cost line; the
  `api` override means the **same real model** does the embedding. Same embedder, separate shelf.
- `embed_corpus.py --contextualized` reads `contextualized_chunks.jsonl`, skips any empty-context
  (dry-run) rows, and embeds **`context + "\n\n" + text`** — the preamble prepended to the original
  passage — into the `gemini-embedding-001-ctx@1536` space by default. It stashes the **original**
  `text` (truncated) in `meta` and flags `contextualized: true`, so a hit still shows the source
  passage, not the synthetic preamble.

Net effect: `gemini-embedding-001@1536` is the raw space and `gemini-embedding-001-ctx@1536` is the same
model over contextualized text. Flip `RetrievalConfig.embedding_model` between them to measure exactly
what the contextual preamble buys you.

---

## Part 5 — Hybrid retrieval: BM25 + dense, fused with RRF

This is the core of `retrieve()`. Each enabled retriever produces a ranked candidate list (default
`candidate_k = 50` from each), and the lists are fused.

### BM25 (sparse, lexical) — free, local

`bm25_search` tokenizes into lowercase word/number tokens that **keep apostrophes** (so *O'Donnell*
stays one token) and **keep digits** (so dates like *1933* match) — both matter here. It prefers the
`rank_bm25` package; if that isn't installed it falls back to a small but **correct** `BM25Okapi`
(standard `k1=1.5`, `b=0.75`), so the free path never depends on an install. Hits with **zero** lexical
overlap are dropped (no point fusing a non-match). Unbeatable on exact tokens — surnames, dates,
notebook shorthand.

### Dense (semantic) — free if the space is local

`dense_search` embeds the query in the configured space with `task="query"` (asymmetric query-side
encoding, which matters for Gemini/Voyage) and cosine-searches that partition. It catches paraphrase the
lexical side is blind to. Crucially it **degrades gracefully**: inside `retrieve`, if the partition isn't
built (`FileNotFoundError`) or a hosted embedder has no API key (`RuntimeError`), it falls back to
**BM25-only** rather than crashing. (A genuine bug like a bad model name still surfaces as `ValueError`
— that one is deliberately *not* swallowed.)

### RRF — fusing two lists whose scores aren't comparable

BM25 scores and cosine similarities live on **totally different scales**; adding them lets whichever
retriever happens to have bigger numbers dominate. **Reciprocal Rank Fusion** throws the scores away and
uses only each item's **rank position**:

```
score(d) = Σ_lists  weight / (k + rank_d)
```

A passage near the top of *either* list gets a big boost. `k` (default **60**, the published default)
damps the very top ranks so no single retriever unilaterally decides the winner. The fused row also
records each list's contributing rank in `components`, so you can debug "why did this rank here?". When
only one retriever ran (e.g. dense unavailable), `retrieve` passes its list through unchanged. RRF is
the **one combiner used everywhere** — it also fuses **across queries** in RAG-Fusion (next part).

After fusion, `retrieve` builds `Hit` objects for a generous slice (enough to feed the reranker if it's
on), optionally reranks, and returns the top `top_k` (default **8**).

---

## Part 6 — Rerank, HyDE, multi-query (the paid dials)

### Cross-encoder rerank (gated; local free)

First-stage retrieval casts a wide cheap net; a **cross-encoder reranker** then re-reads the query
*jointly* with each candidate for a far sharper score. Because that's expensive, `rerank_hits` feeds it
only `rerank_pool` (default **30**) candidates — "retrieve 50, rerank to a precise few." The reranker is
the third swappable model axis (`lib/providers/rerank.py`): **local** `bge-reranker` is free and offline;
**Voyage `rerank-2.5`** / **Cohere `rerank-4-pro`** are real but paid. After reranking, the reranker's
score becomes the Hit's authority and we re-sort; candidates beyond the pool keep their fused order and
are appended. If a paid reranker has no key, it **degrades to the free local one** rather than failing the
query (the model-agnostic promise is "swap any model," not "crash without a key").

### HyDE — embed a hypothetical *answer* (gated)

A question and its answer use different words ("Who cured?" vs. "Mrs. Panyard's cancer was reported cured
after enrollment…"). **HyDE** asks the LLM to write a short plausible answer *in the style of these
documents*, then embeds **that** instead of the question — it lands nearer the real passages in embedding
space, lifting dense recall. Falls back to the raw query if the LLM returns nothing.

### Multi-query / RAG-Fusion — many phrasings, one fused list (gated)

`multi_queries` asks the LLM for several diverse phrasings (default up to **4**, original always kept),
`route_and_retrieve` retrieves for each, and the per-query lists are **RRF-fused** (same combiner as
Part 5). Different phrasings surface different true passages — one says *favor*, another *miracle*,
another *answered prayer*. JSON parsing is forgiving: a bad parse just means "no extra variants," never a
crash.

All three are off by default and cost-logged inside their providers. HyDE and multi-query are mutually
exclusive per call (multi-query takes precedence if both are set).

---

## Part 7 — CRAG / Self-RAG grading (earn the right to answer, or abstain)

In an **archive**, a confident-but-unsupported answer is the worst outcome — worse than "not found." So
after retrieval, `grade_context` scores the set with signals we **already have** (no model needed):

- **top fused score** and the **margin** between #1 and #3 (a flat distribution = ambiguous retrieval);
- **lexical overlap** between query terms and the top hits (did we match the words at all?);
- **OCR `min_conf`** on the cited regions (low-confidence transcription = shaky evidence).

It returns a CRAG-style verdict — **`answer`** (strong match), **`caveat`** (present but shaky: low OCR
confidence or flat ranking), or **`abstain`** (top score / overlap too low) — plus a rough confidence
number (a heuristic blend, *not* a probability) for dashboards. Thresholds are deliberately conservative:
for an archive, prefer "not found."

There's also a gated, per-document **Self-RAG** grader (`grade_document`): hand the LLM one
(query, passage) pair for a relevance label, to *filter* a reranked set before generation. If that call
fails it **keeps** the doc (`partial`) and lets a human decide — graders never silently drop evidence.

---

## Part 8 — The router and the adaptive pipeline

`route_query` buckets a query into **simple / moderate / complex** from cheap signals (length, question
words like *who/when*, aggregate markers like *every/how many/timeline*, conjunctions). It only
**recommends** — it never silently spends. An LLM classifier (`use_llm_router`) is available for
ambiguous phrasing and degrades to the heuristic on any failure.

`route_and_retrieve` wires the whole brain the way the agent calls it:

1. **Route** the query (free heuristic unless `use_llm_router`).
2. **Transform**: if `use_multiquery` (or the router suggests it *and* `auto_apply_route=True`), generate
   phrasings; else if `use_hyde`, embed the hypothetical answer; else use the query as-is.
3. **Retrieve + fuse** via `retrieve` (BM25 + dense + RRF + optional rerank), and RRF-fuse across queries
   if multi-query produced several.
4. **Grade** the final set.

The key safety property: **multi-query is a paid call, so it runs only when you explicitly set
`use_multiquery`, or when `auto_apply_route=True` lets an agent escalate on its own.** With default
config the entire function makes **exactly zero paid calls — even on a "complex" query.**

---

## How to use it

```python
from lib.retrieval import RetrievalConfig, route_and_retrieve, retrieve

# FREE path — no key, no spend. (The __main__ smoke test runs exactly this.)
res = route_and_retrieve("What favors were reported for cancer?")
print(res["grade"]["verdict"], res["grade"]["confidence"])   # answer / caveat / abstain
for h in res["hits"]:
    p = h["provenance"]
    print(h["score"], h["kind"], p["doc_id"], p["rid"], p["page"])

# Quality dial-up (opt in to spend; each is cost-logged):
cfg = RetrievalConfig(
    embedding_model  = "gemini-embedding-001-ctx",  # the contextualized space (A/B vs. raw)
    embedding_dim    = 1536,
    use_rerank       = True,    # local bge = free; voyage/cohere = paid
    use_multiquery   = True,    # RAG-Fusion (paid LLM)
    auto_apply_route = True,    # let the router escalate complex asks on its own
)
hits = retrieve("List every cancer favor reported in 1933", cfg, kinds=["notebook_entry"])
```

- **Pre-filter with `where`** to cut the search to the right slice for free, e.g.
  `where={"kind": "notebook_entry"}`, a list (`{"section": ["A", "B"]}`), or even a callable predicate.
- **Build a dense partition first** (`stages/embed_corpus.py`) before enabling dense for a hosted space;
  until then the free path is lexical-only and that's fine.
- **Smoke test, free:** `python -m lib.retrieval "your question"` — prints route, grade, hits with
  citations, and the `$0` cost ledger.

---

## Caveats

- **Dense needs an index.** Pointing at a space with no built partition does nothing — `retrieve` quietly
  degrades to BM25-only (by design), so you get lexical results, not an error.
- **`…-ctx` needs the contextualize pass to have run.** Embedding the contextualized space requires
  `contextualized_chunks.jsonl` (a paid LLM pass, wired but not yet run); empty-context dry-run rows are
  skipped on embed.
- **Stitched chunks depend on the stitch stage.** Without `data/stitched_notebooks.json`, notebook
  chunking falls back to one chunk per raw page-entry — cross-page entries are *not* merged, and terse
  fragments retrieve worse.
- **Grading is a guardrail, not truth.** `grade_context` blends rank margin, overlap, and OCR confidence
  into a confidence number that is **not** a probability. Tune the thresholds against a golden Q&A set.
- **Local reranker ≠ the named weights.** The local backend packages `bge-reranker-base`, not the
  `bge-reranker-v2-m3` named in config; the local path substitutes the base model.
- **Prices in `config.py` are placeholders.** Nothing is billed until a stage actually runs; confirm
  provider pricing before any billed sweep.

---

## Sources

- Contextual Retrieval (Anthropic, −67% failures):
  <https://www.anthropic.com/news/contextual-retrieval>
- Hybrid + RRF + rerank, advanced RAG: <https://atlan.com/know/advanced-rag-techniques/>
- HyDE / RAG-Fusion / query transforms: <https://atlan.com/know/advanced-rag-techniques/>
- Self-RAG / CRAG (grade, reflect, abstain): RAG 2026 blueprint —
  <https://dev.to/suraj_khaitan_f893c243958/-rag-in-2026-a-practical-blueprint-for-retrieval-augmented-generation-16pp>
- Adaptive / router RAG: <https://www.llamaindex.ai/blog/rag-is-dead-long-live-agentic-retrieval>
