# NER, Entity Resolution, and Enrichment

**TL;DR.** We turn scanned letters and notebook entries into a knowledge graph of *people, places,
things, and what happened to them*. Three stages do the work. **NER** (`extract_entities.py`) reads
the entities out of the corpus: a *free* pass that simply harvests the fields step_6 already
separated, and a *paid* pass that sends the prose to Gemini under a strict schema so the JSON it
returns is always valid. **Resolution** (`resolve_entities.py`) decides which of those many mentions
are really the same thing — "Grace.", "Grace Panyard", and "Mrs. Grace Panyard" should become one
record — using a deliberately *cautious* decision ladder (**veto → auto-merge → LLM-gate → abstain**)
that prefers leaving two records apart over wrongly fusing them. **Enrichment** (`enrich_entities.py`)
hands each important entity its mention contexts and gets back a clean name, a one-sentence grounded
description, the entity's relation to Fr. Solanus, and (for places) a geocodable location — and then
folds together any entities the LLM gave the *same* clean name. The cautious merge is calibrated on
an adversarial 119-pair gold set to **0.93 precision / 0.79 recall** (with the LLM gate on). Every
paid call is cost-logged, and every paid stage is gated behind an explicit `execute=True` so nothing
spends money by accident.

---

## Why this exists (the 60-second version)

A search box over raw OCR text is shallow: it can find the string "Panyard", but it can't tell you
*who* Grace Panyard was, that she's the same person as "Mrs. Grace Panyard" three letters later, that
she lived in Detroit, or that she brought a petition to Fr. Solanus and reported a cure. A
**knowledge graph** can. To build one you need three things in order:

1. **Mentions** — every place a person/place/thing/event is *named* in the text (NER).
2. **Entities** — the real-world things those mentions point at, with the duplicates collapsed
   (resolution).
3. **Context** — a human-readable label and description for each entity, plus how it connects to
   Solanus (enrichment).

Everything here is **non-destructive** (we only ever write new files), **provenance-first** (every
mention remembers the exact polygon on the scanned page it came from, so the final tool can cite an
answer down to a box on the manuscript), and **cost-disciplined** (paid work is gated and logged).

---

## Part 1 — NER: finding the mentions

### Primer: what "NER" means (skip if you know it)

Named-Entity Recognition is the task of reading text and pulling out the *typed* things in it:
this span is a **PERSON**, that one is a **PLACE**, this is a **DATE**. We use a richer ontology than
the textbook three, because this corpus has its own characteristic objects — a **CONDITION** ("cancer
of the lung"), a **FAVOR** (a petition brought to Solanus), an **OUTCOME** ("Cured"), a
**RELIGIOUS_TERM** ("Deo Gratias", "novena"). The full list (`ENTITY_TYPES`) is: PERSON, PLACE, ORG,
DATE, CONDITION, FAVOR, OUTCOME, RELATION, ROLE_TITLE, RELIGIOUS_TERM.

### Two passes, and why the order matters

NER runs in **two passes**, cheap first.

**Pass A — the structured harvest (FREE, no model call).** step_6 already split each letter into
labeled fields — `src_recipient`, `src_signature`, `src_location_sender`, `src_date`, `archv_date`,
and so on — and each notebook entry already carries its own `date`. Those fields *are* entities whose
type we already know. So Pass A just reads them off and emits typed mentions, spending zero tokens.
The map `LETTER_FIELD_MENTIONS` does the typing: `src_recipient` → a PERSON (role: recipient),
`src_location_sender` → a PLACE (role: sender_location), `src_date` → a DATE (role: letter_date), and
so on. For notebooks, `harvest_notebook_page` grabs each entry's raw `date` string (kept raw, e.g.
"Nov. 8th," — a later stage assembles the full year). Think of Pass A as picking up the money already
lying on the table before paying anyone to look for more. It has a second payoff too: it removes the
*easy* entities so the paid pass can spend its tokens on the genuinely hard prose.

**Pass B — the LLM extraction (PAID, deferred).** The rich entities — a person with an age and a
role, a condition tied to a body part, a favor and its outcome, a family relation — hide in the
unstructured prose: a letter's `src_content`, a notebook entry's `text`. Pass B sends that text to
Gemini (`config.DEFAULTS["llm"]`, e.g. `gemini-2.5-flash`) and gets typed entities back.

Both passes emit mentions in **one shared shape** (`_mention(...)`), each carrying a provenance block
(`doc_id` + `rid` + `page` + `vertices` + `min_conf`). Because the constructor is shared, the
resolver downstream never has to care which pass produced a mention.

### The trick that makes Pass B reliable: a response schema

A naive "please return JSON" prompt drifts — a key gets renamed, a list becomes a string, the model
wraps the JSON in prose, and now you're writing a parser to repair broken output. Instead we pass
Gemini an explicit `response_schema` (`build_response_schema`), which switches it into **constrained
decoding**: the output is *grammatically forced* to match our shape, so it is always parseable. There
is a small "constrained-decoding reasoning tax," so we give the model a free-text `reasoning` field
**first** (via `property_ordering`) — a scratchpad to think in before it commits to the typed lists.

The schema is deliberately **nested**, because in this corpus the facts arrive bundled. A sentence
like *"Marg. Quinn enrolled her neighbor Mr. Maughan, cured of cancer"* contains a PERSON who carries
a RELATION ("neighbor of") and whose neighbor carries a CONDITION ("cancer") and an OUTCOME ("cured").
So a `person` object may hold its own `conditions` and `relations` lists, and a `favor` carries its
`outcome` inline. Every leaf string is **nullable** — handwriting is sparse, and "this attribute is
absent" has to be expressible, or the model is forced to hallucinate a value.

After the call, `_flatten_extraction` turns that nested object into the flat mention records the
resolver wants. Crucially it *lifts* a person-attached condition or relation into its own mention too
(so it's independently searchable) while preserving the link via an `of_person` attribute. The LLM
only ever sees text, never geometry, so every flattened mention is stamped with the unit's provenance
on the way out.

The system prompt (`SYSTEM_INSTRUCTION`) is held **constant** across every call, on purpose: a
constant prefix is exactly what makes prompt caching and the Batch API pay off (more below). It tells
the model to keep OCR-imperfect spelling verbatim, attach conditions/relations to the person when the
text ties them, capture devotional vocabulary, record dates as written without inferring the year,
and never invent entities for an empty passage.

### Running it cheaply: gating, batch, caching, resume

The expensive path is off by default. `run(limit, execute=False, emit_batch=False)`:

- always runs Pass A and writes `data/entities_raw.jsonl`;
- **previews** Pass B — it builds the real prompt and schema, projects the cost (`_project_cost`),
  and logs a $0 row to the ledger so even a dry run leaves an auditable trace;
- spends nothing unless you pass `execute=True`.

The cost projection isn't just a sticker price. It shows three numbers: the sticker, the price with
**prompt caching** on the constant system prefix (cached input reads bill at ~10% of normal), and the
price with **Batch API** (−50%) *and* caching combined. `write_batch_requests` emits the Batch-API
JSONL (one self-contained request per unit, keyed so results re-join to provenance) without submitting
it — submission is the deferred paid step.

When you do run the live pass, it's **parallel and resumable**. Each unit is an independent ~9-second
structured-output round-trip, so a serial loop would waste almost all its wall-clock waiting on the
network (the whole corpus would take roughly a day). A `ThreadPoolExecutor` (`NER_WORKERS`, default
16) overlaps the calls; the main loop draining `as_completed` is the *sole* writer, so the
append-only invariant holds without locking. On restart, `_done_unit_ids` reads the output and skips
every unit already extracted, so a run that dies at 4,000 of 9,000 units never re-pays for the first
4,000.

---

## Part 2 — Resolution: many names, one entity

### Primer: the entity-resolution pipeline (skip if you know it)

Entity resolution (ER) decides which surface **mentions** point at the same real-world **entity**, and
mints one tidy authority record per entity. The classic shape, which we follow end-to-end:

```
normalize  →  block  →  match  →  cluster  →  canonicalize  →  (authority reconcile)
```

The one idea you need: comparing every mention to every other is **O(n²)** — at a few thousand
mentions that's millions of comparisons, nearly all of them obviously-not-a-match ("Grace" vs
"Detroit"). So we **block**: bucket mentions by a cheap key so only plausibly-equal mentions ever get
compared. Inside a block we **match** pairs with a small ensemble of signals, **cluster** the agreeing
pairs into groups (transitive closure: if A~B and B~C then A, B, C are one entity), and
**canonicalize** each group into one record.

### Step 1 — Normalize: messy surface → stable keys

We never "fix" the original text (the variant is preserved). Normalization just derives keys a
computer can compare. `normalize_tokens` runs a surface through: fold accents → lowercase → rejoin
OCR line-break hyphenation ("Hus- band" → "husband") → strip leading list-ordinals → expand
apostrophe-contracted abbreviations ("Ass'n" → "association") → collapse punctuation to spaces (so
"O'Donnell" / "ODonnell" / "O Donnell" all become `["o", "donnell"]`) → apply multi-word alias
phrases ("s m a" → "seraphic mass association") → drop honorific **TITLES** ("mrs", "fr") → expand
single-token **ABBREVIATIONS** ("st" → "saint", "mich" → "michigan", "tb" → "tuberculosis").

Two deliberate *non*-expansions worth knowing: in this Capuchin corpus "Mass" is overwhelmingly the
religious Mass (not Massachusetts) and "la" is usually an article (not Louisiana), so expanding those
state abbreviations would corrupt far more than it fixes.

On top of the cleaned tokens we compute a **phonetic key** via Double Metaphone — an algorithm that
maps a word to a code for how it *sounds*, so spelling and OCR variants of the same sound collide
("Panyard"/"Panyord", "Smith"/"Smyth"). The file ships a compact, dependency-free Double Metaphone so
the stage runs with zero installs, and `_phonetic` automatically prefers the real `metaphone` /
`jellyfish` package if present. We actually keep **two** phonetic keys: `phonetic_key` encodes
token-by-token, and `phonetic_key_joined` encodes the tokens concatenated first — that second key is
the bridge across different tokenizations of the *same* name ("O'Donnell" written as one token vs "O
Donnell" as two). These pure functions are `lru_cache`-memoized, which collapses an ~10-minute
clustering pass into seconds (a surface in a 1,400-member block would otherwise be re-tokenized ~1,400
times).

### Step 2 — Block: cheap candidate generation

`blocking_keys` emits **several** keys per mention (multi-pass / "canopy" blocking), and two mentions
become a candidate pair if they share *any* key. Every key is prefixed with the coarse type, so
cross-type pairs are never even generated. The keys: per-token phonetic; per-token phonetic + coarse
location; per-token phonetic + coarse year (people recur across years, so a year bucket separates
unrelated same-name people in different decades); and the tokenization-robust joined phonetic.

Blocking deliberately does **not** key on a single shared first name or surname alone — that would
create a 279-member "Mary" mega-block. The phonetic key covers the *whole* normalized-token sequence,
which prevents that. Singleton blocks are kept (a lone mention still becomes its own one-member
entity), and any block larger than `BLOCK_MAX` (1500) is refused for all-pairs comparison — it's
almost always a generic-token pile-up, and real pairs in it still meet inside the tighter sub-key
blocks.

### Step 3 — Match: scoring a pair with an interpretable ensemble

`pair_score` combines four cheap, *human-readable* signals into one similarity in [0, 1], because
David has to be able to read the audit log and agree or disagree with each merge — an interpretable
score beats a black box here. The weights favor precision:

- **phonetic equality** (0.35) — same metaphone key (either the per-token or the joined one);
- **Levenshtein ratio** (0.30) — character-level closeness of the normalized names (catches a single
  OCR slip, "panyard" vs "panyord"); computed on the *spaceless* form so a stray OCR space doesn't
  penalize an otherwise-identical name;
- **token Jaccard** (0.20) — set overlap (catches reordering, "grace panyard" vs "panyard grace");
- **attribute agreement** (0.15) — do shared known attrs (city/role/order) agree? Neutral 0.5 when
  neither side supplies one.

An optional **embedding cosine** blends in as a bonus *only* if the gated embedding signal ran
(`score = 0.85·rule_score + 0.15·cos`), so the decision stays mostly interpretable. Embeddings catch
*meaning* that phonetics can't — "the Capuchin friary at Harlem" and "St. Bonaventure Monastery"
share no characters but embed near each other. That signal (`embedding_signal`) is **gated** by
default; its free local path (`bge-small-en-v1.5`) costs $0, and the paid path bills per token. The
**LLM cluster adjudication** and **authority reconciliation** (Wikidata/VIAF for persons,
GeoNames/Getty TGN for places) are likewise wired but gated.

### The CAUTIOUS merge ladder (the heart of it)

Here is the key asymmetry that drives every design choice: **a wrong MERGE is far worse than a wrong
SPLIT.** Wrongly fusing two people silently corrupts the archive and is nearly impossible to notice
later; wrongly splitting one person into two records is obvious and easily fixed. So every decision
*favors precision*. For each candidate pair, `decide_pair` walks a four-rung ladder and returns one
of `veto` / `merge` / `llm` / `abstain`. (Design: `docs/cautious_merge_spec.json`.)

**STEP A — HARD VETO (never merge, don't even ask the LLM).** `_hard_veto` fires on any of:

- **generic words** — a PERSON surface that is *only* relationship/pronoun words ("husband", "my
  mother", "the patient"). These are per-letter slot-fillers for *different* real people and must
  never merge. (`GENERIC_PERSON_WORDS`.)
- **title gender** — "Mr." vs "Mrs." of the same surname are spouses/family, i.e. different people.
- **generational suffix** — "Sr." vs "Jr." vs "III" are separators, different individuals.
- **polarity** (for FAVOR/CONDITION/OUTCOME/MISC) — a negation/avoidance mismatch ("cure of cancer"
  vs "averted operation"; `NEG_WORDS`/`AVERT_WORDS`), or a good-vs-bad outcome clash ("cured" vs
  "died"; `GOOD_OUTCOME`/`BAD_OUTCOME`).
- **institution type** (for PLACE/ORG) — a friary is not a monastery is not a hospital, even at the
  same saint or site (`INSTITUTION_HEADS`).

**STEP B — DETERMINISTIC AUTO-MERGE (high precision, no LLM).** Merge outright when one of these
holds:

- **exact + low-risk** — the normalized names are identical and the type is in `LOW_RISK_AUTO`
  {PLACE, ORG, CONDITION, FAVOR, MISC, DATE};
- **exact + anchored** — identical names for a PERSON/ROLE, where PERSON additionally requires an
  *anchor* (see below) so identical bare "Mary" surfaces don't pile up;
- **token-set equal** — same tokens, maybe reordered, when it's not a single bare common token;
- **OCR-garble + anchored** — `phonetic_eq == 1` and `lev_ratio ≥ 0.92` (`LEV_EXACT_AUTO`) *and* a
  shared anchor;
- **abbreviation/acronym expansion** — `abbrev_expansion_match` is true and there's a corroborating
  anchor/exact match (or it's an ORG/MISC/PLACE).

**The anchor gate** is what makes fuzzy merging safe. `shared_anchor` asks: do the two surfaces share
a *distinctive* token — a surname, a rare given name, a proper place/institution token — as opposed to
sharing only a *common* name or a generic head noun? A token is "common" (non-distinctive) if its
per-type **document frequency** is in the top ~1% (`ANCHOR_DF_TOP_FRAC`); `build_common_tokens`
builds that table from the actual corpus. A distinctive shared anchor is **required-true for any
fuzzy (non-exact) merge** — which is precisely why "Mary X" never merges with "Mary Y": they share
only the common token "mary".

**Acronym / abbreviation expansion** is its own gate (`abbrev_expansion_match`). It returns true when
the two surfaces are identical after alias/abbreviation expansion, *or* when one is a verified
initialism of the other (the single expanded token equals the other's initials, and the other side is
genuinely multi-token). `initials_of` builds initials from the *expanded* content tokens (stopwords
dropped via `STOP_INIT`), so "Seraphic Mass Association" → "sma" and "S.M.A." → "sma" land on the
same value and can merge. Initialisms shorter than `INIT_MIN_LEN` (3) are too collision-prone to act
on. Two *bare* acronyms with no spelled-out form present never merge on letters alone.

**STEP C — LLM PRECISION GATE (the grey zone).** Anything not auto-merged and not vetoed, but
plausible, routes to the LLM with `decision == "llm"`. The routing is deliberately *tight* for named
entities (a generous "any shared token" rule would flood the gate with ~1.5M pairs inside big phonetic
blocks): a pair routes only if it has an anchor, an abbreviation match, a subset/set-equal relation, a
shared non-generic token, or an initial-expansion ("S." → "Solanus" alongside a shared surname). For
*concept* types (CONDITION/FAVOR/MISC/OUTCOME) there's no "same string, different entity" risk, so we
*also* route near-string matches (phonetic-equal or `lev ≥ 0.80`) to catch OCR/abbrev/plural variants.
**DATE is exact-only** — two close-but-unequal dates are different dates, never a fuzzy merge, so dates
that don't auto-merge go straight to abstain.

The gate itself (`llm_same_pair`) asks the model one yes/no question per pair, and the prompt makes it
**default to "different"** — it may answer `same` only for a clear acronym↔expansion, an OCR/spelling
variant, or an identical full name, and explicitly *not* for two people who merely share a surname or
first name, a Mr/Mrs gender clash, a Sr/Jr clash, or a generic word. The verdict is structured JSON
(`{same, confidence, reason}`); errors fail safe to "different"; calls run in parallel
(`adjudicate_pairs`, `RESOLVE_LLM_WORKERS`); and a `same` only upgrades the pair to a merge edge if
its confidence clears `LLM_CONFIDENCE_MIN` (0.80).

One efficiency detail in `run`: the grey *mention*-pairs are first de-duplicated to unique
**component** pairs (we merge entities, not mention-pairs — inside a big block millions of mention
pairs collapse to a handful of cluster-vs-cluster questions once the auto-merges are in). They're
ranked strongest-first (abbreviation and high-edit candidates outrank a weak surname-share), capped at
`MAX_LLM_PAIRS` (default 12,000), and any deferred tail is written to `merge_deferred.jsonl` so no
pair is ever silently lost.

**STEP D — ABSTAIN (the safe default).** Everything else stays separate. Abstaining keeps two records
apart, which is always preferred over a wrong merge.

So the cautious bias is enforced three ways at once: **vetoes pre-empt everything; fuzzy merges
require an anchor; the LLM defaults to "different".**

### Step 4 — Cluster, then canonicalize

Confirmed edges (auto-merges plus LLM-upgraded pairs) go into a tiny **union-find** (`_UnionFind`,
path compression + union by rank), and each connected component becomes one entity. A component
touched by any grey edge that *wasn't* upgraded is flagged `needs_review` for David's queue.

`canonicalize` turns each component into an authority record. Picking the display name (`_canonical_name`)
is subtler than "longest wins" — under OCR the longest surface is usually a garbled multi-line run
("Fr. Solanus O.F.M. Cap. Delegate Fr. Solanus, O.7.M. Cap."), so a length-first rule reliably picks
the *worst* variant. Instead we drop outlier-long surfaces (>6 tokens), then rank the rest by
frequency, preferring the form people actually *wrote* most. Each record carries: a stable
human-legible id (`person:grace_panyard:0007`), every distinct surface as a `variant`, majority-voted
attrs, every mention with its provenance (so retrieval can cite regions), a `confidence` score
(`0.6 · name-agreement + 0.4 · OCR-confidence`), the `needs_review` flag, and empty `authority` slots
for the gated reconciliation.

Outputs are non-destructive: `data/entities.json` (the records) and `data/entities_merge_audit.jsonl`
(one row per non-abstain decision, with the signals and reason — the paper trail David needs to trust
or overrule any grouping).

### Calibration: 0.93 precision / 0.79 recall

The thresholds and gates are tuned against an **adversarial gold set**, `eval/merge_calibration.json`:
**119 pairs** (65 labeled same, 54 different), spread evenly across the seven coarse types (17 each
of person/place/org/condition/favor/role/misc), each pair `gold_same`-labeled by two skeptics. The
pairs are chosen to be *hard* — Solanus's order suffix mangled by OCR ("O.F.I.C", "O.F.K. Cap." vs
"OFM Cap."), shared-surname-different-person traps, look-alike conditions (deaf/death, fever/liver).

`eval/eval_merge.py` runs the real `decide_pair` (and optionally the live LLM gate, `--use-llm`) over
those pairs and reports precision / recall / F1 on the **merge** decision, plus a printout of the
dangerous **false merges** and the missed **false splits**. With the LLM gate on, the calibrated
result is **precision ≈ 0.93, recall ≈ 0.79** — i.e. of the merges we make, ~93% are correct (very
few of the costly false fusions), and we recover ~79% of the truly-same pairs (the rest land safely in
abstain or as a low-confidence "different"). That asymmetry is the design working as intended: we'd
rather miss a real merge than make a wrong one. Without the gate, grey pairs simply stay separate
(`merged = False`), so deterministic-only precision is even higher and recall lower — this is the
free, instant tuning loop before a full resolve run.

The numeric thresholds (`MATCH_TAU` 0.62, `AMBIG_TAU` 0.50, `LEV_EXACT_AUTO` 0.92, etc.) are starting
points to tune on gold. The **invariants** are categorical, not numeric: anchor required for any fuzzy
merge; LLM defaults to "different"; hard vetoes are absolute.

---

## Part 3 — Enrichment: giving each entity meaning

Resolution gives you canonical entities, but a node labeled "Mrs. Clairmont" or "S.M.A." — or worse,
an OCR run-on like "Mrs. Clairmont (today reported and" — tells a reader nothing. `enrich_entities.py`
sends each significant entity's **mention contexts** (the actual text of the records it appears in) to
the LLM and gets back a structured record:

- **`canonical_name`** — a clean, correctly-spelled display name, with OCR run-on noise dropped;
- **`description`** — *one* factual sentence, grounded **only** in the supplied excerpts plus
  well-known facts about Fr. Solanus (the system prompt forbids inventing biography; unknown → null);
- **`role`** — petitioner / priest / doctor / hospital / city, etc.;
- **`relation_to_solanus`** — one of {correspondent, petitioner, fellow_friar, family, benefactor,
  subject_of_favor, none, unknown} — this is the field that turns a flat list of names into a graph
  *of Solanus's life*;
- **`location`** — for a place, a geocodable "City, State" / "City, Country" for mapping; else null;
- **`confidence`**.

It only enriches the entities that carry "who/what/where" meaning (`ENRICH_TYPES` = PERSON, PLACE,
ORG) and only those above `min_mentions` (default 2) — the recurring figures that make the graph
representative; raise the threshold to spend less, lower it to cover the long tail. Targets are sorted
**most-central-first**. Each entity is given up to `MAX_CONTEXTS` (4) distinct excerpts of
`CONTEXT_CHARS` (360) characters, de-hyphenated through `textclean`, which keeps the token count — and
thus the cost — small. The design mirrors NER exactly: parallel (`ENRICH_WORKERS`, default 16),
resumable (skip already-enriched ids, checkpoint every 200), every call cost-logged, and **paid →
gated** behind `execute=True` (the default is a dry preview that prints a token projection). Output is
`data/entities_enriched.json` ({id → record}); `build_graph` reads it to label and connect nodes, and
it never overwrites the resolver's `entities.json`.

### Same-name consolidation: a cheap second merge

The cautious resolver, by design, leaves some real duplicates split — "Fr. Solanus" vs "Fr. Solanus,
O.F.M. Cap." stayed apart because the shared tokens were too common to anchor. But the enrichment LLM,
having *read the context*, gave those variants **one clean canonical name** — and that clean name is a
strong, cheap merge key the resolver didn't have. `consolidate` uses it, but carefully:

- it groups entities by `(type, normalized-clean-name, location)`;
- the clean name must be non-empty and not a generic word (`_GENERIC_KEY` blocks "", "unknown",
  "church", "hospital", "god", "doctor", …);
- a **PERSON** must be a multi-token full name to merge (a lone given name like "Mary" is too risky) —
  with "solanus" as the one explicit exception;
- a **PLACE/ORG** must also share the enriched `location`, so two different "St. Joseph's" in different
  cities don't collapse.

The surviving primary keeps the union of all mentions and variants and records `consolidated_from`.
Every merge is logged to `consolidation_audit.json`, and the pre-merge file is backed up to
`entities_preconsolidation.json` before `entities.json` is rewritten — same non-destructive discipline
as everywhere else.

---

## Files at a glance

| File | Role |
|---|---|
| `stages/extract_entities.py` | NER — free structured harvest (Pass A) + gated LLM extraction (Pass B) → `data/entities_raw.jsonl` |
| `stages/resolve_entities.py` | Resolution — normalize/block/match/cluster/canonicalize via the cautious ladder → `data/entities.json` + `data/entities_merge_audit.jsonl` |
| `stages/enrich_entities.py` | Enrichment — grounded name/description/relation/location + same-name consolidation → `data/entities_enriched.json` |
| `docs/cautious_merge_spec.json` | The merge-ladder design spec (blocking keys, signals, decision rule, per-type rules, hard-veto guards, adjudicator prompt) |
| `eval/eval_merge.py` + `eval/merge_calibration.json` | The 119-pair calibration harness; reports precision/recall/F1 and lists false merges/splits |

## Caveats

- **Paid stages are deferred.** NER Pass B and enrichment do nothing but preview + project cost until
  `execute=True`. The resolver runs fully offline by default; its embedding, LLM-gate, and authority
  signals are each separately gated.
- **The calibration numbers describe the calibration set.** 0.93 / 0.79 is measured on 119
  adversarial pairs; the live corpus will differ, which is exactly why every merge is audited and
  grey-touched clusters are flagged `needs_review` rather than trusted blindly.
- **The cautious bias is intentional.** Expect the resolver to *under*-merge in ambiguous cases. That
  is the safe failure mode here: a missed merge is easy to fix in review; a wrong merge corrupts the
  archive silently.
