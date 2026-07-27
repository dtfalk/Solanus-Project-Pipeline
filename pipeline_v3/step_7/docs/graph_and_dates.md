# The RiC-O temporal knowledge graph & the date model

*How step_7 turns 570 letters + 716 notebook pages into a graph you can reason over and cite — and
how it recovers a real, machine-comparable date for entries that often wrote down only "the 17th".*

Covers three stages plus the map geocoder, built in dependency order:
`stages/stitch_notebooks.py` → `stages/normalize_dates.py` → `stages/geocode_places.py` →
`stages/build_graph.py`.

---

## TL;DR

We build a **knowledge graph**: a network of *nodes* (the records — letters, notebook pages,
notebook entries — and the *entities* they mention: people, places, organizations, medical
conditions, outcomes) joined by *semantically-typed edges* (`WROTE_TO`, `HAS_CONDITION`, `ENROLLED`,
`LOCATED_AT`, `APPEARS_WITH`, …). Two design choices make it trustworthy for an archive. First,
**every edge carries provenance** — the exact handwritten region IDs (`rids`) it was read from — so
the web app can deep-zoom straight to the ink that justifies any claim. Second, **every edge carries
a *when*** in **EDTF** (the Library-of-Congress standard for fuzzy dates), so the timeline is real
data, not a guess. The dates themselves are reconstructed by a separate stage that solves the
register's quirk — the year lives once at the top of the page, the entry only writes a month/day, and
a favor often records *two* dates (enrolled vs. reported). Dates are modeled as a **temporal spine**:
canonical `YEAR` nodes that every dated record links to via `DATED_IN`, giving a clean, queryable,
visualizable timeline. The graph is exported twice — `graph.json` for the web visualization, and
`graph.ttl` in **RiC-O** RDF so the archive is interoperable Linked Data. Nothing here is
destructive, nothing is billed by default, and no edge is ever invented: a link-prediction scaffold
can *propose* missing edges, but only into a review queue for David.

---

## Skip-if-you-know-this primer

A few terms recur. If these are old hat, jump to **The big picture**.

- **Knowledge graph.** A set of *nodes* and *edges*. A node is a thing (a person, a letter). An edge
  is a typed, directed relationship between two things (`Solanus —WROTE_TO→ Grace Panyard`). Unlike a
  table, a graph lets you *walk*: "start at Solanus, follow `WROTE_TO`, then follow each recipient's
  `LOCATED_AT`" answers "where did the people he wrote to live?" in two hops.
- **EDTF (Extended Date/Time Format).** ISO 8601 with a few archive-friendly extensions. It can say
  "circa 1945" as `1945~`, "October 1933, day unknown" as `1933-10-XX`, "some time in 1929" as just
  `1929`, and a closed range as `1940-02/1940-10`. It lets us record *uncertainty as data* instead of
  throwing it away or faking precision we don't have.
- **RiC-O (Records in Contexts Ontology).** The International Council on Archives' standard vocabulary
  for describing archival records *and their relationships*. Mapping onto it means our graph is
  standard Linked Data that other archival tools understand, not a bespoke blob.
- **Provenance.** The receipt. Here, the specific scanned region(s) a fact was read from, identified
  by an `rid` like `doc_1.src_content.0`, which the viewer turns into a deep-zoom (IIIF) citation.
- **Valid-time / bitemporal.** *Valid-time* is "when was this true in the world" (the date a letter
  was written), as opposed to when we recorded it. *Bitemporal* here means a single favor entry can
  carry **two** valid-times: when the petition was *enrolled*, and the later date the outcome was
  *reported*.

---

## The big picture

Embeddings (the retrieval layer) answer *"which passages look like my question?"*. A graph answers
the *relational* and *aggregate* questions an archivist actually asks:

> "Whom did Solanus write to in 1896, and from where?"
> "Which favors reported a *cure*, and what conditions recurred across the Casey family?"
> "Which notebook entries continue onto the next page?"

The graph is built in-memory with **networkx** (a property-graph library that's easy to traverse, run
community detection on, and dump to JSON for the web viz) and then *projected* into **RiC-O RDF**. The
in-memory networkx graph is the single source of truth; the two exports (`graph.json` and `graph.ttl`)
are two views of the same facts, so they can never drift apart.

Three stages feed it, each writing a brand-new artifact and never touching the source `documents.json`
/ `notebooks.json`:

| Stage | What it solves | Output |
|---|---|---|
| `stitch_notebooks` | A favor split across a page break is two half-thoughts. Rejoin them. | `data/stitched_notebooks.json` |
| `normalize_dates`  | The year is missing from entries; favors carry two dates. Reconstruct EDTF. | `data/dates.json` |
| `geocode_places`   | Places need coordinates for the map. | `data/geocodes.json` |
| `build_graph`      | Assemble everything into the RiC-O temporal graph. | `data/graph.json`, `data/graph.ttl`, `data/graph_suggestions.json` |

`build_graph` *tolerates* the absence of every optional input: if `dates.json`,
`stitched_notebooks.json`, `entities.json`, or `geocodes.json` is missing, it degrades gracefully
(e.g. a record-only graph, a cheap continuation heuristic, no map coordinates) rather than crashing.

---

## Nodes: records and entities

The graph has two families of node.

**Record nodes** are the documents themselves — the things you can cite:

- `letter` — one per correspondence document (the 570 letters). Carries the joined letter text
  (trimmed to a 600-char snippet on the node), section, type, page numbers, raw date, and the full
  normalized-date attributes (more below).
- `notebook_page` — one per notebook page (the 716 pages).
- `notebook_entry` — one per entry within a page. Its node id is *composite*:
  `notebook_entry:<page_id>::<rid>` (e.g. `Volume_3__p001::doc_1.src_content.0`). This composite is
  exactly how `dates.json` keys notebook entries, so the two line up by construction. Every entry is
  tied to its page with a `MENTIONED_IN` edge (RiC-O `isOrWasIncludedIn`, method `structure`).
- `favor` — an *Activity* node minted for any notebook entry that shows favor signals (an
  enrollment, a condition, or an outcome). The favor is the event; the entry is the record that
  documents it.

**Entity nodes** are the canonical things records talk about, loaded from `data/entities.json`
(produced by the entity-resolution stage):

- `person`, `organization`, `place`, plus two SKOS-ish `Concept` kinds: `condition` and `outcome`.
- A special `year` node kind forms the temporal spine (below).

Two guardrails keep the entity side honest:

1. **Typed node ids never collide.** Every id is prefixed with its kind (`person:grace_panyard`,
   `place:detroit`), so a person and a place that share a name stay separate.
2. **Unknown types never become "person".** `resolve_entities` emits coarse upper-cased types
   (`PERSON`/`PLACE`/`ORG`/…); `_norm_etype` folds them to our localnames and defaults anything
   unrecognized to `other`, *never* `person`. This is deliberate: silently relabeling unknown types
   as people is the exact bug that once flooded the graph with ~15k bogus "person" nodes (dates,
   favors, religious terms). `DATE` "entities" are dropped entirely as nodes — dates belong *on*
   records and on the year spine, not as free-floating noise.

If `entities.json` is missing or still a stub, the build logs a warning and produces a **record-only
graph** — the entity and relation edges simply populate later, once resolution lands.

---

## Edges: the verbs of the graph

Every edge kind is declared once, in the `EDGE_KINDS` table, together with its human label, its
RiC-O predicate, and whether it's directed. Keeping this in one place is what guarantees the JSON viz
and the TTL export agree.

| Edge | Means | RiC-O predicate | Directed? |
|---|---|---|---|
| `WROTE_TO` | Solanus wrote to a recipient | `hasCorrespondent` | yes |
| `LOCATED_AT` | a person is at a place | `hasOrHadLocation` | yes |
| `MEMBER_OF` | a person belongs to an order/body | `isMemberOf` | yes |
| `FAMILY` | family relation | `hasFamilyRelationTo` | **no** (symmetric) |
| `ENROLLED` | a favor enrolled (Seraphic Mass Assoc.) | `enrolled` | yes |
| `PETITIONED_FOR` | a favor petition | `petitionedFor` | yes |
| `HAS_CONDITION` | a favor concerns a condition | `hasCondition` | yes |
| `HAS_OUTCOME` | a favor's reported outcome | `hasOutcome` | yes |
| `MENTIONED_IN` | entity/record referenced in a record | `isOrWasIncludedIn` | yes |
| `CONTINUES_ON` | one fragment/page continues onto the next | `continues` | yes |
| `DATED_IN` | a record falls in a year | `hasBeginningDate` | yes |
| `APPEARS_WITH` | two people co-occur in records | `isAssociatedWith` | **no** (symmetric) |

Every edge, regardless of kind, carries the same four pieces of metadata via the internal `add_edge`
helper:

- `kind` — the verb above.
- `rids` — the provenance: the region ids this edge was read from (deduped, sorted).
- `valid_time` — the EDTF date attached to the assertion (when known).
- `method` — *how we knew it*. This is the trust label, and it matters. Values include `source` (a
  structured field on a letter), `structure` (an entry belonging to its page), `mention` (an
  entity-to-record mention), `stitch` (an upstream-validated continuation), `page_label_heuristic`
  (a cheap fallback guess), `lexical_cue` (a shallow substring match on a favor), `co_occurrence`,
  `gold_link` (David's hand-curated cross-references), `resolved`, and `normalize_dates`. A reviewer
  can always see whether an edge was *read* or *inferred*, and downgrade the weak ones.

Symmetric kinds (`FAMILY`, `APPEARS_WITH`) are stored as two directed edges, one each way, so a
traversal finds them from either endpoint.

Below, the edge kinds the prompt singles out, in detail.

### WROTE_TO and LOCATED_AT — the cheapest, most reliable edges

The correspondence is a gift: each letter already has its recipient, sender, and locations as
*separate labeled fields*. So `WROTE_TO` is essentially free and high-precision. The build links
Solanus → each recipient, attaching the recipient field's rids as provenance and the letter's date as
valid-time, with `via=<letter node>` so you can jump to the letter. `LOCATED_AT` comes from the
`src_location_recipient` / `src_location_sender` fields — Solanus is located at his sender location,
the recipient at theirs.

A subtle but important detail: these helpers (`_ensure_person`, `_ensure_place`) **reuse the resolved
entity nodes** rather than minting description-less duplicates. Before this section runs, the build
indexes every resolved person/place node by its canonical name *and variants* (honorifics stripped),
preferring higher-mention entities for ambiguous keys. So "Father Solanus" in a letter field snaps
onto the same rich, enriched node the rest of the graph uses — the correspondence network isn't a
shadow graph.

### HAS_CONDITION, ENROLLED, HAS_OUTCOME — the favors backbone

The notebooks are a *register of favors*. Until the full LLM extractor runs, the build seeds this
backbone with **conservative lexical cues** — and is honest about it: every such edge is tagged
`method="lexical_cue"`. The cues are deliberately shallow substring matches (e.g. `enroll`;
conditions like `cancer`, `tuberculosis`, `paralysis`; outcomes like `cured`, `recovered`, `deo
gratias`). The philosophy is *we'd rather miss an edge than assert a wrong one in an archive*.

For each entry with any signal, a `favor` Activity node is created and linked to its entry, then:
`ENROLLED` if an enroll cue fired, `HAS_CONDITION` to a condition concept node per condition cue, and
`HAS_OUTCOME` to an outcome concept node per outcome cue. The valid-times here are *bitemporal*
(below): the `ENROLLED` edge gets the enrollment date, the `HAS_OUTCOME` edge gets the (later) report
date, falling back to the entry's single date when the two weren't split.

### APPEARS_WITH — the co-occurrence social network

Two people who appear together in the same record are probably connected in Solanus's life — a
petitioner and the sick person they're praying for, recurring associates, a family. `APPEARS_WITH`
captures this. As the build attaches person mentions to records, it remembers *which people appear in
which record*, then counts pairs that co-occur.

Three things keep it legible rather than a hairball:

- **Generic role-words are excluded.** "husband", "wife", "mother", "patient", etc. aren't real
  people, so they don't enter the co-occurrence set.
- **Solanus is excluded.** He co-occurs with nearly everyone — he'd just be a giant hub. His real
  ties are the `WROTE_TO` edges.
- **It's kept sparse:** only pairs sharing **≥ 2 records**, and only the **top 2500** by weight. Each
  edge stores its `weight` (number of shared records).

### CONTINUES_ON — reconstructing split entries

A favor often runs off the bottom of one page and onto the next, literally labeled "Page 1 Cont." The
`stitch_notebooks` stage reconstructs these (see its section below) and emits `relations`.
`build_graph` reads them: entry→entry continuations become `CONTINUES_ON` between the two
`notebook_entry` nodes, page→page (`page_continues_on`) ones connect the `notebook_page` nodes. These
carry `method="stitch"` plus the upstream confidence and reason. If `stitched_notebooks.json` is
absent, the build falls back to a cheap `page_label =~ /cont/i` heuristic (tagged
`method="page_label_heuristic"`) so *some* continuation structure exists either way.

### MENTIONED_IN and gold links

`MENTIONED_IN` is the workhorse tie from an entity (or record) to the record it appears in.
Provenance lives under each mention's `provenance` (rid / doc_id / page); when an rid is blank (e.g. a
recipient field) it falls back to the doc_id to still anchor the mention. A `rid → record-node` index
makes this snap onto the right letter/entry/page fast.

A special, high-trust case: each notebook entry may carry a `linked` list — **degree-1 rids from
David's gold connection graph**, human-curated cross-references. The build surfaces these as
record↔record `MENTIONED_IN` edges tagged `method="gold_link"`. These are exactly the kind of
trustworthy edge we want to honor.

---

## The temporal spine: YEAR nodes + DATED_IN (the timeline)

A naive approach treats every date mention as its own node — and drowns in noise ("June", "to-day",
"the 17th"). Instead, dates are treated as a **property of records**, and the timeline is built from a
small set of canonical **`year` nodes**.

After all record nodes exist with their EDTF dates, the build scans every record (`letter` and
`notebook_entry`), reads the leading 4-digit year off its EDTF string, creates the `year:<YYYY>` node
once if needed (with `year` as an integer attribute), and adds a `DATED_IN` edge from the record to
that year (tagged `method="normalize_dates"`, carrying the record's EDTF as valid-time).

The payoff: "show me everything in 1945" is a one-hop lookup (the neighbors of `year:1945`) instead of
a string scan at query time. It's a clean, queryable, **visualizable** axis — a real timeline — and a
genuine point-in-time spine the viz can scrub along. RiC-O-wise, `DATED_IN` maps to `hasBeginningDate`.

---

## Bitemporal EDTF dates (the `normalize_dates` stage)

This is the temporal-truth layer, and it's worth understanding the corpus problem it solves.

**The problem.** The notebooks are a running register. An entry usually carries only a month and day
("Mar. 30"), sometimes only a bare day ("22"). The **year almost never appears in the entry** — it
lives once, at the top of the page, in the archival field `archv_date` ("1923, November"). On top of
that the dates are gloriously human: circa values ("c. 1929"), ranges ("c. 1897 to 1904"), feast days
("Feast of the Presentation."), and — crucially — *two* dates inside one favor ("Enrolled May 3rd
1923 … report — Recovered"): an **enrollment** date and a later **report/outcome** date.

**The fix.** Reconstruct a full, machine-comparable date for every record by combining four signals,
then encode the result in EDTF:

1. **Year from the page's `archv_date`** — the most reliable source of the year the entries omit.
2. **Month/day from the entry's own `date`** (or `src_date` for letters).
3. **Carry-forward** — when an entry omits the month (or year), inherit it from the previous entry on
   the page, exactly how a human reads a register: "the 17th" means "the 17th of whatever month we're
   in." Each inference is a small, transparent **confidence** debit (e.g. −0.10 for a carried month,
   −0.05 for a carried year), so the score in `[0,1]` reflects how much we *read* vs. *inferred* —
   `1.0` means fully read.
4. **A bitemporal body scan** — look just after "enroll…" and "report…/recovered/cured…" cue words
   for a date, capturing the enrollment and report dates separately.

**The year-boundary subtlety.** A single page often straddles a year boundary, and the archival field
records it as month/year pairs ("1923, December 1924, January"). Naively taking the first year (1923)
would date every January entry a year early. So `year_from_archv` also builds a **month → year map**:
a January entry on such a page correctly resolves to 1924. This turns a systematic error into a
correctly split timeline.

**EDTF encoding** is done at exactly the precision available — never inventing more:

| Situation | EDTF |
|---|---|
| circa | `1945~` |
| unknown day | `1933-10-XX` |
| unknown month + day | `1929` |
| closed range | `1940-02/1940-10` |
| no anchorable year | `""` (precision `unknown`) |

Every record in `data/dates.json` has one stable shape: `raw` (what was written), `edtf` (the
canonical machine value), `precision` (`unknown`/`year`/`month`/`day`/`range`), `confidence` (the
rule-based score), `circa`, `method` (`rule` or `llm`), and the two optional bitemporal fields
**`enrolled`** and **`reported`** (each `{edtf, precision}` or null). Keeping `raw` next to `edtf`
means every reconstruction is auditable.

**Two code paths, free by default.** A pure-Python **rule path** (`reconstruct_record`) does the
whole job with the stdlib — deterministic, reproducible, costs nothing — and is what `run()` executes.
An **LLM-assisted normalizer** exists for the genuinely hard strings (feast days, garbled OCR, prose
dates), but it is **gated off** (`use_llm=False`, `llm_max=0`), is only ever consulted for the
lowest-confidence records up to a hard call cap, and every call is cost-logged. We never bill David
without him asking.

**How the graph consumes this.** When `dates.json` is present, `build_graph` *prefers* it over its own
conservative inline parser. It writes the *full* normalized-date attributes onto each record node —
not just a bare EDTF string but `edtf_precision`, `edtf_confidence`, `circa`, `date_method`, and the
`enrolled_edtf` / `reported_edtf` pair — so the date is fully described on the node. The bitemporal
split is what lets the `ENROLLED` edge and the `HAS_OUTCOME` edge each carry *their own* valid-time
instead of one blurred date. (The graph also keys `dates.json` lookups by both the record's `doc_id`
and the entry's composite id, with the bare rid as a fallback, so every node finds its date.)

---

## Stitching split entries (the `stitch_notebooks` stage)

`CONTINUES_ON` edges and the multi-page logical entries both come from here. The stage:

1. **Groups pages into registers.** Continuation never crosses a *section* (Volume_1…4, Appendix_*).
   Within a section it refines by a *normalized* notebook number parsed robustly from the noisy OCR'd
   header (the same physical book appears as "NOTEBOOK NO. 5.", "NOTEBOCK NO. S.", etc.; grouping on
   the raw string would shatter one book into 50+ micro-registers and we'd never test the real
   page-break joins).
2. **Orders each register physically** by `pdf_page_number` (deterministic, so re-runs are identical).
3. **Scores each page boundary** with transparent, additive cheap signals: an explicit "Page N Cont."
   label (strongest), a numbered "Page N → Page N Cont." chain, a *truncated tail* (the previous
   entry doesn't end on a terminator/dash), and an *orphan head* (the next entry opens lower-case or
   on a connective). The default auto-accept threshold is 0.55, tuned so a "Cont." label alone clears
   the bar while text-only signals must corroborate.
4. **Distinguishes two kinds of continuation — the critical correctness point.** A "Page N Cont."
   label means the *register* continues, **not** that the last favor on page N is the same favor as
   the first on the next page. The stage only **fuses** two entries into one logical entry when the
   *thought itself* is split (truncated tail or orphan head). When the label says Cont. but both
   halves are complete favors, it records a **page-level** `page_continues_on` edge (the register
   flows on) **without** merging — avoiding a false fusion of two different people's stories.

It writes `logical_entries` (the whole-favor unit retrieval should embed) and a flat `relations` edge
list (`continues_on` / `continued_from` at entry level, `page_continues_on` at page level).
Fragments keep their exact rids and vertices, so a citation can still deep-zoom to the precise region
even while retrieval sees the whole logical entry. An optional multimodal-LLM adjudicator for the
ambiguous band is documented but **gated off and never called** by `run()`.

---

## Structured-field reconciliation to entity nodes

A recurring theme: the corpus's *structured fields* (a letter's recipient/sender/location, an entry's
`linked` list, an entity's pre-extracted `family` / `member_of`) are a free, high-precision
head-start, and the build is careful to reconcile them to the **same** canonical entity nodes the
resolver produced rather than minting duplicates. The mechanism:

- After loading resolved entities, the build indexes every `person` and `place` node by its canonical
  name and all variants, with honorifics and punctuation stripped (`_match_key`), preferring the
  higher-mention node on key collisions.
- The structured-field helpers (`_ensure_person`, `_ensure_place`) consult that index first and reuse
  the resolved (and, where enrichment ran, *enriched*) node — which already carries a clean name, a
  grounded description, a relation-to-Solanus, a geocodable location, and any reconciled authority IDs
  (Wikidata / VIAF / GeoNames). Only if there's no match do they mint a `provisional` node, which
  later clusters under the resolver's canonical id once it lands.

This is why the WROTE_TO/LOCATED_AT correspondence network and the mention network point at the same
rich nodes, not two separate description-less copies. Pre-extracted `family` and `member_of` links on
an entity are likewise honored as `FAMILY` / `MEMBER_OF` edges, each with provenance.

---

## Place geocoding for the map (the `geocode_places` stage)

The Map tab needs coordinates. `geocode_places` resolves each recurring `place` entity to lat/long via
**OpenStreetMap's Nominatim** — **free, no API key**, but rate-limited, so the stage:

- Only geocodes places with **≥ 2 mentions** (`min_mentions`, configurable), using the enrichment's
  normalized "City, State" location when available, else a cleaned canonical name (appending ", USA"
  when there's no comma to disambiguate).
- **Caches every lookup** to `data/geocodes.json` (including misses, so they aren't retried
  endlessly), which makes it **resumable** — re-runs only hit genuinely new places — and **sleeps
  ~1s between calls** per Nominatim's usage policy.

`build_graph` then attaches `lat`/`lon` to each `place` node (looking the place up by its enriched
location query), and the web app's `/api/map` + Map tab plot them. If `geocodes.json` is absent,
place nodes simply have `lat=lon=None` and the rest of the graph is unaffected.

---

## The two exports (and how RDF carries provenance)

The same networkx graph is serialized two ways:

- **`data/graph.json`** — a compact, D3/Cytoscape-friendly node/edge list for the front-end viz, with
  a `meta` block counting node and edge kinds plus the config fingerprint. Long `text` is trimmed to a
  snippet; the viz fetches full text and the IIIF region on demand using the `rids`.
- **`data/graph.ttl`** — **RiC-O RDF** (Turtle) via rdflib. Each node becomes a typed instance IRI
  (`Person`, `Place`, `Record`, `RecordPart`, `Activity`, `Concept`, …) with `rico:name` and, for
  records, a `rico:date` EDTF *literal*. Reconciled authority URIs are emitted as
  `rico:hasOrHadIdentifier`.

**Why RDF needs a trick for provenance.** A statement like "Solanus wrote to X *in 1896*, *as
evidenced by region doc_5.src_recipient.0*" is a statement *about* a statement — the time and the
receipts are metadata on the *edge*, and a plain RDF triple can't carry attributes. So when an edge
has rids or a valid-time, the export **reifies** it: it mints an `rdf:Statement` blank node naming the
subject/predicate/object, hangs the EDTF `rico:date` on it, and links each provenance rid with PROV-O
`prov:wasDerivedFrom` pointing at a region IRI. That's how the receipts stay attached in standard
Linked Data.

Where RiC-O has no tidy property (e.g. a medical condition), the mapping uses an honest, clearly
project-namespaced predicate or a SKOS-ish `Concept` rather than silently mislabeling something as
standard vocabulary.

---

## What we never do: invent edges

The graph asserts only what the source text and resolved entities support. A documented **KG
link-prediction (KGE) scaffold** can *propose* plausible missing edges — a continuation that wasn't
stitched, two name variants that should merge, an implied family tie — by learning an embedding per
entity and relation and scoring un-asserted triples. But:

- It writes **only** `data/graph_suggestions.json`, a **review queue**. Nothing auto-merges. Gold is
  David-only.
- It's **idle by default** (`train=False` writes a well-formed empty queue). A tiny pure-NumPy
  DistMult smoke trainer exists for a CPU sanity check (`--kge`); the production path is a clearly
  marked TODO that plugs in pykeen. Candidate edges are type-constrained (e.g. `CONTINUES_ON` only
  between notebook records, `FAMILY` only between persons) so proposals are at least type-sane, and
  each suggestion ships the neighboring evidence rids so a reviewer sees *why*.
- Even though it's local and free, the run is still routed through `lib.costlog` for auditability —
  every model-shaped step in this project leaves a trace.

---

## Caveats

- **Lexical-cue favors are a seed, not the truth.** `HAS_CONDITION` / `ENROLLED` / `HAS_OUTCOME` edges
  tagged `method="lexical_cue"` are shallow substring matches meant to exist *before* the real LLM
  extractor runs. Trust them as scaffolding, and filter on `method` when precision matters.
- **`dates.json` is preferred but optional.** Without it, the graph falls back to a conservative
  inline EDTF parser that handles only common, unambiguous shapes — precision/confidence will read as
  unknown for those.
- **`networkx` and `rdflib` are deferred dependencies.** They're imported lazily inside the functions
  so merely importing the module (which `run.py` does to build the DAG) never fails before the libs
  are installed (`pip install networkx rdflib`).
- **Geocoding is best-effort.** Nominatim can miss or mis-resolve a noisy place name; misses are
  cached so they don't get retried endlessly, and a place with no hit simply isn't plotted.

---

## Sources (the code itself)

- `stages/build_graph.py` — node/edge model, `EDGE_KINDS`, the temporal spine, structured-field
  reconciliation, both exports, the KGE scaffold.
- `stages/normalize_dates.py` — the rule path, carry-forward, year-boundary map, bitemporal scan, EDTF
  encoding, the gated LLM normalizer.
- `stages/stitch_notebooks.py` — register grouping, the cheap join signals, the entry-vs-page
  continuation distinction.
- `stages/geocode_places.py` — Nominatim geocoding, caching, rate-limiting.
