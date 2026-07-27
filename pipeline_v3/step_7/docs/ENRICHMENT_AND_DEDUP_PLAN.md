# Plan — TOC-leveraged dedup, entity enrichment, document grouping, legibility

Written 2026-06-20 in response to David's asks: cautious dedup (incl. *using the table of contents*),
"getting more info about locations / a research call for all named entities in context," the "baked
beans → one source over multiple pages" grouping bug, a legible graph, and explanatory UI. Every item
is a **reproducible pipeline stage** (diff-and-rerun) — nothing is a one-off.

---

## A. Cautious dedup — IN PROGRESS (this is what's being built now)
`resolve_entities.py` now implements the spec in `docs/cautious_merge_spec.json`, calibrated on
`eval/merge_calibration.json` (119 adversarially-labeled pairs). Ladder: **veto → auto-merge →
LLM-gate → abstain**, anchor-token gate, acronym/abbrev expansion, hard vetoes (title-gender,
generational suffix, polarity, generic words). Measure with `python eval/eval_merge.py [--use-llm]`.

### A2. Leverage the TABLE OF CONTENTS as a name authority  ← David's idea
The corpus has real TOCs at `step_6/3_enriched/<section>/0_table_of_contents/page_*/extract_merged.json`,
and step_6 already built a TOC **connection graph** to group multi-page letters. The TOC lists each
document with its **recipient name + date + page** — a *cleaner, curated* spelling than the OCR'd body.
Plan (new stage `build_toc_authority.py`):
1. Parse TOC pages → a list of `{recipient_name, date, page_range, section}` authority records.
2. Treat each TOC recipient as a **high-confidence canonical PERSON** (the spelling scholars will see).
3. When resolving, **prefer the TOC spelling** as `canonical_name`, and **boost a merge** between two
   body-mention variants when both map (phonetic/initials/anchor) to the *same* TOC entry — a strong,
   cheap, deterministic signal that needs no LLM.
4. Cross-check: a proposed merge whose two sides map to **different** TOC entries → veto.
This turns the archive's own finding aid into ground truth for the people axis.

---

## B. Entity enrichment — "a research call for all named entities in context"  ← David's ask
New stage `enrich_entities.py` (paid, batched, parallel, resumable — same harness as NER):
For each canonical entity (PERSON/PLACE/ORG first), gather **all its mention contexts** (the snippet
around every mention, with doc/date) and make ONE structured LLM "research" call → an enriched record:
- **PERSON**: one-line description, role/title, relation to Fr. Solanus, recurring condition/favor,
  active date-range, home place (linked to a PLACE entity).
- **PLACE**: type (city / hospital / friary / parish), city+state, **geocode candidate** (lat/long via
  free GeoNames/Nominatim), and a confidence.
- **ORG**: type/purpose, parent (e.g. Capuchin Province), date-range.
Output `data/entities_enriched.json` (joined to `entities.json` by id) → richer graph nodes + better
retrieval (the description text is embeddable) + map-able locations. Cost-bounded; cost-logged.

### B2. Authority reconciliation (free APIs, gated)
Persons → Wikidata/VIAF; places → GeoNames/Getty TGN. Already scaffolded in `resolve_entities`
(`reconcile=True`). Adds real-world IDs + coordinates for the map.

---

## C. Document grouping in retrieval — the "baked beans" bug  (NOT David's job — a pipeline gap)
Today `lib/chunks.py` chunks each raw notebook *entry* and each letter, and retrieval returns per-chunk
hits — so a multi-page entry shows as several "sources." Fixes:
1. **Wire `stitched_notebooks.json` into `chunks.py`**: a cross-page logical entry becomes ONE chunk
   spanning its pages (provenance = the list of page rids). One source, multiple pages.
2. **Group citations by parent document** in the app: collapse hits that share a `doc_id`/logical-entry
   into a single source card that lists "pages X, Y, Z," each linking to its IIIF region.
3. For correspondence, group by the **TOC document** (a letter spanning pages = one source).

---

## D. Make the graph legible  ← David: "this graph is not currently legible"
- Default to an **ego-network**: pick/search a node → show its 1–2 hop neighborhood (already supported
  by `/api/graph?center=…`), not a 300-node hairball.
- **Layout**: switch Cytoscape to `cose`/`fcose` force layout with degree-scaled node size, fewer
  labels (label only high-degree / hovered), edge bundling.
- **Filters in the UI**: node-kind toggles, year range (uses the new year spine), min-degree slider.
- A **"focus" search box**: type an entity → center the graph there.

---

## E. Explain the system in-app  ← David: "multiple tabs that fully explain what is happening" +
"tool toggles … with complete explanations of what each does"
- An **Overview tab**: the pipeline as a diagram (corpus → NER → resolve → graph → embed → index →
  agent), each stage with a one-paragraph plain-English explanation + live counts (entities, edges, …).
- A **Tools tab**: each retrieval tool as a card with what it does, when the agent picks it, cost
  ($0 vs paid), and an example — the toggles become a labeled, explained control panel, not bare
  checkboxes.
- A **Data/health tab**: corpus stats, eval numbers (precision/recall), cost ledger, what's built.

---

## Sequencing
1. Finish + calibrate cautious dedup (A). 2. Doc grouping (C) — fixes a visible bug. 3. Legible graph
(D) + explanatory tabs (E) — the "I'm a bit lost" fixes. 4. TOC authority (A2). 5. Entity enrichment +
geocoding (B). All wired into `run.py` so `python run.py status` shows the DAG and re-runs only what changed.
