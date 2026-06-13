# STEP_5 — split the labeled corpus into constituent documents

_2026-06-13. Written after the corpus reached complete gold (1,390+ pages, all 7 sources)
and the front/back matter was added back (V1/V2/V3 renumbered to true source pages). This is
the plan David asked for: two output views + a document-boundary review tool. Some pieces are
deterministic and built now; the document-assembly piece needs a few semantic decisions from
David (§5) before it is built, because it depends on what counts as a "document" — exactly the
call the machine should not make alone._

## 0. Inputs (what step_5 consumes)

Per source page, gold lives across three places (now all in TRUE SOURCE page numbering):
- `step_4/reviewed/<Vol>/page_NNN/page_NNN.json` — the gold labels: `documents{doc_1,doc_2,…}`,
  each a set of category→polygons; every polygon = `{vertices, connections, id}`. **Geometry
  only — there is NO text in the labels** (by design). `num_documents` + `page_width/height`.
- `step_4/polygon_cropped_pdfs/<Vol>/pages/page_NNN.pdf` — the page image.
- `step_2/polygon_page_data/<Vol>/{page_sizes,polygons}/` — upstream crop polygons (in the OLD
  docs_only numbering; step_5 translates via `qa_output/<Vol>/frontmatter_map.json`).

Two facts that shape everything below:
1. **Within-page multi-document is already solved.** David maintained `doc_1/doc_2/doc_3` per page
   (e.g. 4 mass cards = 4 docs; V2 alone has 34 multi-doc pages). Co-located documents need no
   inference — read them straight from the schema.
2. **Cross-page documents are NOT linked in the data.** A letter continued on the next page has no
   stored link to its start (connections are within-page only), and there is no text to read
   "Page 2 Cont." from. Cross-page assembly must be inferred structurally and **confirmed by David**.

## 1. Output structure

```
step_5/
  by_source/                       # Folder A — faithful per-source-page bundle (DETERMINISTIC)
    <Vol>/
      page_NNN/
        page_NNN.pdf               # the page image (copied/linked from step_4 crops)
        labels.json                # the gold labels for this page (verbatim)
        page_meta.json             # {source_page, width, height, dpi, num_documents,
                                   #  crop_polygons: step_2 polygon data for this page}
      <Vol>.index.json             # ordered page list + per-page doc counts
  by_document/                     # Folder B — one entry per LOGICAL document (NEEDS §5)
    <Vol>/
      doc_0001/
        document.json              # manifest: ordered parts [(page_NNN, doc_k), …], category
                                   #   counts, span (single|multi-page|multi-per-page), boundary
                                   #   provenance (proposed vs David-confirmed)
        regions/                   # cropped image(s) of this document's area, one per page-part
        labels.json                # this document's polygons (merged across its parts)
  manifests/
    document_index.json            # every document across the corpus, with source back-refs
```

`by_source` is a clean, lossless reorganization — safe to regenerate anytime. `by_document` is the
derived, human-validated view that downstream (OCR → RAG, per Project_Thoughts) will consume.

## 2. Folder A — `build_by_source.py` (deterministic, BUILT)

For each gold page: copy the page PDF, copy labels verbatim, and emit `page_meta.json` joining
the page dims + the step_2 crop polygons (translated to source numbering). No judgment, no API.
Run AFTER the front matter is reviewed into gold so it includes title/ToC/appendix pages.

## 3. Folder B — the document-assembly model

A **logical document** = a maximal ordered chain of page-parts `(page, doc_k)`:
- **Single-page, single-doc** (most letters, every casebook page): one part. Trivial.
- **Multi-doc-per-page** (mass cards, stacked notes): each `doc_k` on the page is its own document
  (already separated). Trivial — read from schema.
- **Multi-page span** (a letter running onto "Page N Cont."): parts on consecutive pages that must
  be joined. This is the only inferred case.

**Continuation proposer (heuristic, deterministic, geometry-only):** page P+1's `doc_1` is proposed
as a CONTINUATION of the last open document on page P when, structurally, it (a) carries a
top-of-page `struct_doc` marker and/or a `struct_other` running header, AND (b) lacks a fresh
opening — no `src_greeting`, no `src_origin`, no inside-address `src_recipient` block, no leading
top-corner `archv_date`. Otherwise page P+1 starts a NEW document. Each proposal carries the
evidence + a confidence; **David confirms/splits/merges** — the proposer never finalizes a boundary.

Per assembled document, emit: the ordered parts, the union of their polygons, and a cropped region
image per part (bounding box of the doc's polygons on that page) for downstream OCR/RAG.

## 4. The document-boundary review tool (the "modified editor")

A thin mode over the existing editor rather than a rewrite — it reuses the renderer and the
within-page Merge ▲/Split ▼ you already have, adding the cross-page dual:
- Walk the volume in page order. At each page boundary show the proposer's call —
  "page N: CONTINUATION of doc ####" vs "NEW document" — with the structural evidence highlighted
  on the rendered page (the top struct_doc/running-header, or the fresh greeting/origin).
- One key to **toggle** continuation↔new; **Merge to previous** / **Split here** for cross-page
  joins, mirroring the in-page doc Merge/Split. Writes only the boundary decisions (a sidecar,
  never the page label JSONs), then `build_by_document.py` reads them to emit Folder B.
This keeps the same discipline as everywhere else: the machine proposes structure, David decides,
and gold labels are never mutated by the assembly step.

## 5. DECISIONS NEEDED FROM DAVID (before by_document is built)

1. **What is a "document" in the CASEBOOKS (V3, V4)?** Each page is one notebook page with many
   per-person `src_content` boxes. Is a constituent document: (a) one per notebook PAGE, (b) one
   per PERSON entry (each `src_content` its own document — matches the per-person convention and
   the RAG "retrieve a person" goal), or (c) the whole notebook = one document? **(a)/(b)/(c)?**
2. **Span assembly for letters:** confirm multi-page letters should be joined into ONE document
   (yes per "some span multiple pages") — the proposer + your review handle the boundaries.
3. **Front/back matter** (title, ToC, appendix): each its own "document", grouped as one
   "front-matter" unit, or excluded from by_document? 
4. **Region crops:** should by_document emit cropped images per document (for later OCR/RAG)? At
   what DPI?

## 6. Build order

- ✅ `build_by_source.py` — built; run after front-matter review.
- ⏳ `continuation_proposer.py` + `build_by_document.py` + the boundary-review mode — built once §5
  is answered (chiefly the casebook-document definition, which changes the whole shape of Folder B).
