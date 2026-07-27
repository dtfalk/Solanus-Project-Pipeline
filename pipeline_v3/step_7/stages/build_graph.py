"""stages/build_graph.py — Tier-2 temporal knowledge graph, aligned to RiC-O.

This stage turns the resolved corpus into a *graph you can reason over and cite*. Where the chunk/
embedding layer answers "which passages look similar to my question?", the knowledge graph answers
the *relational* and *aggregate* questions an archivist actually asks:

    "Whom did Solanus write to in 1896, and from where?"
    "Which favors reported a *cure*, and what conditions recurred across the Casey family?"
    "Which notebook entries continue onto the next page?"

We build it with **networkx** (an in-memory property graph that's trivial to traverse, run community
detection on, and dump to JSON for the web viz) and then *project* the same facts into **RDF**
aligned to **RiC-O** (Records in Contexts — the ICA's ontology, proven on scholarly correspondence)
so the archive is interoperable Linked Data, not a one-off blob.

Two ideas make this graph trustworthy for an *archive* specifically:

  1. **Provenance on every edge.** No assertion floats free: each edge records the `rids` (region
     ids like ``doc_1.src_content.0``) it was read from, so the web app can deep-zoom straight to
     the handwritten region that justifies it (doc_id + rid + page + vertices → IIIF). An archive
     that can't show its receipts is just a rumor with footnotes.

  2. **Valid-time, encoded in EDTF.** Edges carry a *when* — the date the letter was written, the
     date a favor was enrolled vs. reported — using **EDTF** (Extended Date/Time Format, ISO 8601-2),
     the Library-of-Congress standard built precisely for *uncertain/partial* dates ("c. 1945" →
     ``1945~``, unknown day → ``1933-10-XX``). The corpus is full of fuzzy dates; EDTF is their
     natural home, and it lets the viz do "as-of 1933" point-in-time views later.

What we DON'T do here: we do not *invent* edges. The graph asserts only what the source text and the
resolved entities support. A documented **KG link-prediction (KGE) scaffold** at the bottom can
*propose* plausible missing edges (continuations, same-as merges, latent ties) — but those land in
``data/graph_suggestions.json`` as a **review queue for David**, never auto-merged into the graph.
(Gold is David-only; agents never assert conventions.)

Inputs (declared in run.py):
  - data/entities.json     canonical entities from resolve_entities (tolerated-optional: we degrade
                           gracefully to a record-only graph if it's absent or still a stub)
  - documents.json         570 letters (correspondence)
  - notebooks.json         716 notebook pages, each with entries[]
  - data/dates.json                (optional) EDTF-normalized dates from normalize_dates
  - data/stitched_notebooks.json   (optional) logical-entry stitching from stitch_notebooks

Outputs (non-destructive, new artifacts only):
  - data/graph.json              node/edge list for the web visualization (D3/Cytoscape-friendly)
  - data/graph.ttl               RiC-O RDF (Turtle) via rdflib
  - data/graph_suggestions.json  KGE-proposed edges for human review (written by the scaffold; may
                                 be an empty, well-formed stub until the KGE step is run)

DEPENDENCIES: needs ``networkx`` and ``rdflib`` in the venv (neither is installed yet — see the
NOTE near the imports). Nothing here is billed; the only model-shaped step (KGE training) is a local
CPU scaffold and is still cost-logged for symmetry/auditability.
"""
from __future__ import annotations

# ======================================================================
# Imports — grouped, with the usual step_7 sys.path shim so this file
# runs both as `python stages/build_graph.py` and as an imported stage.
# ======================================================================
# Core Python Imports
import argparse
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Local File Imports — make step_7/ importable, then pull in the shared foundation.
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config              # noqa: E402  (paths + model registry)
from lib import costlog    # noqa: E402  (every model-shaped call is logged, even local ones)
from stages.entity_engine import _recoverable_propernoun  # noqa: E402  (audit P1a: don't drop named proper nouns)

# Third-party graph libraries.
#
# NOTE (deferred dependency): `networkx` and `rdflib` are NOT yet in
# step_7/venv. Add them when you're ready to run this stage:
#     step_7/venv/bin/pip install networkx rdflib
# We import lazily *inside* the functions (not at module top) so that simply
# importing this module — which run.py does to build the DAG — never explodes
# if the libs aren't present. The build pipeline stays inspectable regardless.

log = logging.getLogger("build_graph")

# ======================================================================
# RiC-O / namespace vocabulary
# ----------------------------------------------------------------------
# RiC-O (Records in Contexts Ontology) is the ICA's standard for describing
# archival records *and their relationships* — exactly our problem. We don't
# need the whole ontology; we map our handful of node/edge kinds onto the RiC-O
# classes/properties that fit, and mint our own project namespace for the
# instance IRIs (one stable IRI per entity/record). Where RiC-O lacks a tidy
# property we fall back to a clearly-namespaced custom predicate so nothing is
# silently mislabeled as standard vocabulary.
# ======================================================================
RICO = "https://www.ica.org/standards/RiC/ontology#"
EDTF_NS = "http://id.loc.gov/datatypes/edtf/EDTF"     # datatype IRI for EDTF literals
BASE = "https://solanuscasey.archive/id/"             # our instance namespace (project-local)
PROV = "http://www.w3.org/ns/prov#"                   # PROV-O, for "derived from region" provenance

# Our edge *kinds* (the verbs of the graph) → how each projects into RiC-O RDF.
# Keeping this table in one place means the JSON viz and the TTL export can never
# drift apart: both read these same definitions.
#
#   label       human-readable relation name (also the JSON edge "type")
#   rico        the RiC-O (or custom-but-namespaced) predicate localname used in TTL
#   directed    whether the edge has a meaningful direction (FAMILY is symmetric)
EDGE_KINDS = {
    "WROTE_TO":        {"label": "wrote to",            "rico": "hasCorrespondent",     "directed": True},
    "ENROLLED":        {"label": "enrolled",            "rico": "enrolled",             "directed": True},
    "PETITIONED_FOR":  {"label": "petitioned for",      "rico": "petitionedFor",        "directed": True},
    "HAS_CONDITION":   {"label": "has condition",       "rico": "hasCondition",         "directed": True},
    "HAS_OUTCOME":     {"label": "has outcome",         "rico": "hasOutcome",           "directed": True},
    "LOCATED_AT":      {"label": "located at",          "rico": "hasOrHadLocation",     "directed": True},
    "MEMBER_OF":       {"label": "member of",           "rico": "isMemberOf",           "directed": True},
    "FAMILY":          {"label": "family of",           "rico": "hasFamilyRelationTo",  "directed": False},
    "MENTIONED_IN":    {"label": "mentioned in",        "rico": "isOrWasIncludedIn",    "directed": True},
    "CONTINUES_ON":    {"label": "continues on",        "rico": "continues",            "directed": True},
    "DATED_IN":        {"label": "dated in",            "rico": "hasBeginningDate",     "directed": True},
    "APPEARS_WITH":    {"label": "appears with",        "rico": "isAssociatedWith",     "directed": False},
}

# Node kinds → RiC-O class localname. Records (letters / notebook pages / entries)
# are RiC-O *Record* / *RecordPart*; canonical entities are Agent / Place / etc.
NODE_RICO_CLASS = {
    "person":         "Person",
    "organization":   "CorporateBody",
    "place":          "Place",
    "condition":      "Concept",          # RiC-O has no medical class; a SKOS-ish Concept is honest
    "outcome":        "Concept",
    "letter":         "Record",
    "notebook_page":  "Record",
    "notebook_entry": "RecordPart",
    "favor":          "Activity",         # an enrollment/report is best modeled as an Activity/event
}


# ======================================================================
# Small helpers — IDs, EDTF, and reading the (possibly-absent) inputs
# ======================================================================
def _slug(text: str) -> str:
    """Turn an arbitrary string into a safe, stable IRI/JSON id fragment.

    We lower-case, keep word characters, and collapse runs of anything else to a single
    underscore. Stability matters: the *same* entity name must always slugify to the same id so
    re-runs are idempotent and the web viz can keep selections across rebuilds.

    Args:
        text: Any human string (an entity name, a place, ...).

    Returns:
        A lower-snake slug safe for both JSON keys and IRI localnames.
    """
    s = re.sub(r"[^\w]+", "_", (text or "").strip().lower())
    return s.strip("_") or "unknown"


def _node_id(kind: str, key: str) -> str:
    """Compose a typed node id, e.g. ``person:grace_panyard`` or ``letter:Volume_1__p001``.

    Prefixing with the kind guarantees a person and a place that happen to share a name never
    collide into one node.
    """
    return f"{kind}:{_slug(key)}" if kind in ("person", "organization", "place", "condition", "outcome") else f"{kind}:{key}"


# Month-name → number, so we can assemble dates like "Aug. 23rd, 1896" into EDTF "1896-08-23".
_MONTHS = {m[:3].lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"], start=1)}


def to_edtf(raw: str | None) -> str | None:
    """Best-effort conversion of a corpus date string into an **EDTF** literal.

    This is a *deliberately conservative* parser: the dedicated ``normalize_dates`` stage (and its
    LLM-assisted temporal tagging) is the real source of truth, and if ``data/dates.json`` is
    present we prefer it (see :func:`_load_dates`). This helper only handles the common, unambiguous
    shapes so the graph still has *some* valid-time even before that stage runs — and it never
    *guesses* precision it doesn't have.

    Why EDTF? Because archival dates are fuzzy. EDTF (ISO 8601-2 / Library of Congress) can say
    "circa 1945" as ``1945~``, "October 1933, day unknown" as ``1933-10-XX``, and a range as
    ``1940-02/1940-10`` — capturing uncertainty *as data* instead of throwing it away.

    Args:
        raw: A source date string such as ``"Aug. 23rd, 1896"``, ``"1933, October"``, ``"c. 1945"``.

    Returns:
        An EDTF string, or ``None`` if we can't responsibly parse one.
    """
    if not raw:
        return None
    s = raw.strip().lower()

    # ---- approximate / circa → EDTF qualifier '~'
    approx = bool(re.search(r"\b(c\.?|ca\.?|circa|about|abt)\b", s))

    # ---- year (the one part almost always present somewhere)
    ym = re.search(r"(1[89]\d\d|20\d\d)", s)
    if not ym:
        return None
    year = ym.group(1)

    # ---- month (named or numeric)
    month = None
    mm = re.search(r"\b([a-z]{3,9})\.?\b", s)
    if mm and mm.group(1)[:3] in _MONTHS:
        month = f"{_MONTHS[mm.group(1)[:3]]:02d}"

    # ---- day (a 1-2 digit number that isn't the year)
    day = None
    dm = re.search(r"\b([0-3]?\d)(?:st|nd|rd|th)?\b", s.replace(year, ""))
    if dm:
        d = int(dm.group(1))
        if 1 <= d <= 31:
            day = f"{d:02d}"

    # ---- assemble at the precision we actually have; 'XX' marks a *known-unknown* component.
    if month and day:
        edtf = f"{year}-{month}-{day}"
    elif month:
        edtf = f"{year}-{month}-XX"
    else:
        edtf = year
    return edtf + "~" if approx else edtf


def _read_json(path: Path, default):
    """Read a JSON file if it exists and is non-empty, else return ``default``.

    Several of our inputs are *optional upstream artifacts* (dates.json, stitched_notebooks.json) or
    may still be an unimplemented stub (entities.json). We never want a missing or empty optional to
    crash the build — the graph should degrade gracefully and just carry less.
    """
    try:
        if path.exists() and path.stat().st_size > 0:
            return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning("could not read %s (%s) — using default", path, e)
    return default


def _load_entities() -> list:
    """Load canonical entities from ``data/entities.json``; tolerate absence/stub.

    Expected shape (the contract ``resolve_entities`` is being built to emit) — each item::

        {
          "id":             "person:grace_panyard",   # optional; we mint one if missing
          "type":           "person|organization|place|condition|outcome",
          "canonical_name": "Grace Panyard",
          "variants":       ["Grace.", "Mrs. Panyard"],
          "attrs":          {"role": "...", "religious_order": "...", ...},
          "mentions":       [ {"doc_id": "...", "rid": "doc_1.src_content.0",
                               "page": 1, "vertices": [[x,y],...]}, ... ]
        }

    Returns:
        A list of entity dicts (possibly empty). Empty is fine — we then build a record-only graph
        and the entity/relation edges simply won't appear until resolve_entities lands.
    """
    # Prefer the entity ENGINE's central store (entity_store.json) when present — it carries the
    # context-aware, merged canonical entities (and their understanding). Fall back to the raw
    # resolve output (entities.json) when the engine hasn't run yet.
    store = _read_json(config.DATA / "entity_store.json", {})
    if isinstance(store, dict) and store.get("entities"):
        log.info("loading %d entities from the engine store (entity_store.json)", len(store["entities"]))
        return store["entities"]
    raw = _read_json(config.DATA / "entities.json", [])
    # resolve_entities might wrap the list as {"entities": [...]}; accept either.
    if isinstance(raw, dict):
        raw = raw.get("entities", [])
    if not raw:
        log.warning("data/entities.json missing/empty — building a RECORD-ONLY graph "
                    "(entity & relation edges will populate once resolve_entities runs).")
    return raw


def _load_dates() -> dict:
    """Load ``data/dates.json`` (EDTF-normalized dates), keyed for lookup by rid and by doc_id.

    Shape we expect from ``normalize_dates`` (tolerant — extra keys ignored)::

        { "doc_1.src_content.0": {"edtf": "1933-11-08", "kind": "report", "raw": "Nov. 8th"}, ... }

    Returns:
        ``{}`` if absent; otherwise the mapping as-is. The graph builder prefers these EDTF values
        over its own :func:`to_edtf` fallback whenever a key matches.
    """
    return _read_json(config.DATA / "dates.json", {}) or {}


def _load_stitch() -> dict:
    """Load ``data/stitched_notebooks.json`` (logical-entry stitching from stitch_notebooks).

    Shape we expect (tolerant)::

        { "links": [ {"from_rid": "...", "to_rid": "...", "logical_entry_id": "...",
                      "confidence": 0.9, "method": "rule|llm"} , ... ] }

    These become authoritative ``CONTINUES_ON`` edges. If absent, we fall back to the cheap
    ``page_label =~ /cont/i`` heuristic so *some* continuation structure still appears.

    Returns:
        The dict (possibly empty).
    """
    return _read_json(config.DATA / "stitched_notebooks.json", {}) or {}


# ======================================================================
# Letter (correspondence) text helpers — mirror lib/chunks.py exactly so
# the graph and the retrieval layer agree on what a letter "says".
# ======================================================================
_PREF = ["src_recipient", "src_greeting", "src_content", "src_farewell", "src_signature",
         "src_location_recipient", "src_location_sender", "src_origin", "src_date", "archv_commentary"]


def _letter_text(d: dict) -> str:
    """Join a letter's labeled regions into one body string (same ordering as lib/chunks.py).

    We keep this in lock-step with the chunk builder on purpose: if the two disagreed about a
    letter's text, a citation from retrieval and a fact in the graph could point at "the same"
    letter yet describe different content.
    """
    tbl = d.get("text_by_label", {})
    ordered = [tbl[k] for k in _PREF if tbl.get(k)] + [v for k, v in tbl.items() if k not in _PREF and v]
    return "\n".join(ordered).strip()


def _rids_for_label(d: dict, label: str) -> list:
    """Collect the region ids backing a given label on a letter — our provenance trail.

    Args:
        d:     a letter record from documents.json.
        label: a ``src_*``/``archv_*`` category (e.g. "src_recipient").

    Returns:
        The rids of every region with that category (e.g. ``["doc_1.src_recipient.0"]``). These are
        what the web app turns into a deep-zoom IIIF citation.
    """
    return [r["rid"] for r in d.get("regions", []) if r.get("category") == label]


# ======================================================================
# Lightweight, *conservative* relation cues for notebook favors
# ----------------------------------------------------------------------
# Real relation/event extraction is the LLM-driven extract_entities job (see RESEARCH_PLAN PART B).
# Here we only mine a few HIGH-PRECISION lexical cues so the temporal "favors" backbone exists even
# before that lands — and we tag each such edge with method="lexical_cue" so it's clearly
# distinguishable (and downgradeable) versus a model-extracted edge. We would rather miss an edge
# than assert a wrong one in an archive.
# ======================================================================
_ENROLL_RE  = re.compile(r"\benroll", re.I)
_OUTCOME_RE = re.compile(r"\b(cured|cure|recover|improv|healed|better|grateful|"
                         r"deo gratias|thanksgiving|favor granted|wonderful)\b", re.I)
_CONDITION_RE = re.compile(r"\b(cancer|rheumatism|tuberculosis|tumou?r|paralysis|blind|deaf|"
                           r"drink|operation|illness|sick|disease|fever|infection|"
                           r"inflam\w*|consumption)\b", re.I)


def _favor_signals(text: str) -> dict:
    """Scan one notebook entry for conservative favor signals (enroll / condition / outcome).

    This is intentionally shallow — substring cues, not understanding. It exists so the graph has a
    first-pass ``favor`` event with HAS_CONDITION / HAS_OUTCOME / ENROLLED edges before the proper
    extractor runs. Everything it emits is tagged ``method="lexical_cue"`` downstream.

    Args:
        text: an entry's transcribed text.

    Returns:
        ``{"enrolled": bool, "conditions": [..], "outcomes": [..]}`` — deduped, lower-cased cue
        tokens for conditions/outcomes (the canonical concept node is keyed on these).
    """
    return {
        "enrolled":   bool(_ENROLL_RE.search(text)),
        "conditions": sorted({m.group(0).lower() for m in _CONDITION_RE.finditer(text)}),
        "outcomes":   sorted({m.group(0).lower() for m in _OUTCOME_RE.finditer(text)}),
    }


# ======================================================================
# The graph builder
# ======================================================================
def build_networkx_graph():
    """Assemble the temporal knowledge graph in networkx (the single source of truth).

    Build order (each step adds nodes/edges; everything is provenance- and time-stamped):
      1. Record nodes  — one per letter, one per notebook page, one per notebook entry.
      2. CONTINUES_ON  — page→page / entry→entry continuations (stitched if available, else /cont/).
      3. Entity nodes  — canonical persons/orgs/places/conditions/outcomes from entities.json.
      4. Relation edges— WROTE_TO, LOCATED_AT, MEMBER_OF, FAMILY, ENROLLED/PETITIONED_FOR,
                         HAS_CONDITION, HAS_OUTCOME, MENTIONED_IN — each with valid-time + rids.

    Returns:
        A ``networkx.MultiDiGraph`` (multi: two entities can be tied by more than one relation; di:
        most of our relations are directed — symmetric ones like FAMILY are added both ways).

    Raises:
        ModuleNotFoundError: if networkx isn't installed (see the install NOTE at the top).
    """
    import networkx as nx   # lazy import: keeps module import safe before the lib is installed

    docs   = _read_json(config.DOCUMENTS, [])
    pages  = _read_json(config.NOTEBOOKS, [])
    ents   = _load_entities()
    dates  = _load_dates()
    stitch = _load_stitch()
    enriched = _read_json(config.DATA / "entities_enriched.json", {}) or {}   # context pass (optional)
    geocodes = _read_json(config.DATA / "geocodes.json", {}) or {}            # place lat/long (optional)

    G = nx.MultiDiGraph()

    def edtf_for(*keys, raw_fallback: str | None = None) -> str | None:
        """Prefer the normalize_dates EDTF for any of these keys; else parse the raw string ourselves.

        ``data/dates.json`` is keyed *both* by record id (a letter's ``doc_id`` like
        ``Volume_1__p001``) *and* by an entry's composite id (``Volume_3__p001::doc_1.src_content.0``)
        — so we try each candidate key in order and take the first that carries an ``edtf``. Only if
        none match do we fall back to our own conservative :func:`to_edtf` on the raw string.

        Args:
            *keys:        candidate lookup keys (composite entry id, doc_id, bare rid), most-specific
                          first.
            raw_fallback: the original date string to parse if no key matches.

        Returns:
            An EDTF string or None.
        """
        for k in keys:
            rec = dates.get(k) if k else None
            if rec and rec.get("edtf"):
                return rec["edtf"]
        return to_edtf(raw_fallback)

    def date_attrs(*keys, raw_fallback: str | None = None) -> dict:
        """Full normalized-date attributes for a record, so the date is WRITTEN OUT on the node — not
        reduced to a bare EDTF string. Surfaces precision, confidence, circa, method, and the bitemporal
        enrolled/reported pair from normalize_dates. Falls back to a conservative parse of the raw string
        when the record isn't in dates.json (then precision/confidence are unknown)."""
        for k in keys:
            rec = dates.get(k) if k else None
            if rec and rec.get("edtf"):
                return {"edtf": rec.get("edtf"), "edtf_precision": rec.get("precision"),
                        "edtf_confidence": rec.get("confidence"), "circa": bool(rec.get("circa")),
                        "date_method": rec.get("method"), "enrolled_edtf": rec.get("enrolled"),
                        "reported_edtf": rec.get("reported")}
        return {"edtf": to_edtf(raw_fallback), "edtf_precision": None, "edtf_confidence": None,
                "circa": False, "date_method": "fallback_parse" if raw_fallback else None,
                "enrolled_edtf": None, "reported_edtf": None}

    def bitemporal_for(*keys) -> tuple:
        """Return (enrolled_edtf, reported_edtf) for a favor, if normalize_dates split them.

        The favors timeline is *bitemporal*: an entry can record an *enrollment* date and a later
        *report/outcome* date ("Enrolled May 3 … Reports Dec 8"). normalize_dates captures these as
        ``enrolled``/``reported`` on the date record; we surface them so ENROLLED vs HAS_OUTCOME
        edges can each carry their *own* valid-time instead of one blurred date.
        """
        for k in keys:
            rec = dates.get(k) if k else None
            if rec:
                return rec.get("enrolled"), rec.get("reported")
        return None, None

    def add_edge(src, dst, kind, *, rids=None, edtf=None, method="source", **attrs):
        """Add one provenance- and time-stamped edge (and the symmetric twin for undirected kinds).

        Every edge carries: ``kind`` (our verb), ``rids`` (region provenance), ``valid_time``
        (EDTF), and ``method`` (how we knew — "source" structured field, "lexical_cue", "stitch",
        "kge_suggestion"). The KGE scaffold reuses this same signature so a *confirmed* suggestion
        is indistinguishable from a hand-built edge once David accepts it.
        """
        G.add_edge(src, dst, key=kind, kind=kind, rids=sorted(set(rids or [])),
                   valid_time=edtf, method=method, **attrs)
        if not EDGE_KINDS[kind]["directed"]:
            G.add_edge(dst, src, key=kind, kind=kind, rids=sorted(set(rids or [])),
                       valid_time=edtf, method=method, **attrs)

    # ------------------------------------------------------------------
    # 1) Record nodes — letters
    # ------------------------------------------------------------------
    for d in docs:
        nid = _node_id("letter", d["id"])
        da = date_attrs(d["id"], raw_fallback=d.get("date"))
        G.add_node(nid, kind="letter", label=d.get("parent_doc") or d["id"],
                   doc_id=d["id"], section=d.get("section"), type=d.get("type"),
                   date_raw=d.get("date", ""),
                   page=d.get("page_number_in_type"), pdf_page=d.get("pdf_page_number"),
                   text=_letter_text(d)[:600], **da)

    # ------------------------------------------------------------------
    # 1) Record nodes — notebook pages + their entries
    # ------------------------------------------------------------------
    for page in pages:
        pid = _node_id("notebook_page", page["id"])
        G.add_node(pid, kind="notebook_page", label=page.get("notebook") or page["id"],
                   doc_id=page["id"], section=page.get("section"),
                   notebook=page.get("notebook", ""),
                   page=page.get("page_number_in_type"), pdf_page=page.get("pdf_page_number"))
        for e in page.get("entries", []):
            composite = f"{page['id']}::{e['rid']}"          # how dates.json keys notebook entries
            eid = _node_id("notebook_entry", composite)
            da = date_attrs(composite, e["rid"], raw_fallback=e.get("date"))
            G.add_node(eid, kind="notebook_entry", label=(e.get("text") or "")[:60],
                       doc_id=page["id"], rid=e["rid"], notebook=page.get("notebook", ""),
                       page_label=e.get("page_label", ""), date_raw=e.get("date", ""),
                       text=(e.get("text") or "")[:600], **da)
            # An entry is a *part of* its page → RiC-O isOrWasIncludedIn (provenance is its own rid).
            add_edge(eid, pid, "MENTIONED_IN", rids=[e["rid"]], edtf=da["edtf"], method="structure")

    # ------------------------------------------------------------------
    # 1c) Temporal spine — YEAR nodes + DATED_IN edges
    # ------------------------------------------------------------------
    # Dates are a PROPERTY of records, not free-floating entities, so instead of the noisy NER
    # date-mention nodes we link every dated record to a canonical year node. This gives a clean,
    # queryable, VISUALIZABLE timeline ("everything in 1945") and a real point-in-time axis without
    # scanning date strings at query time.
    _RECORD_KINDS = {"letter", "notebook_entry"}
    for _nid, _nd in list(G.nodes(data=True)):
        if _nd.get("kind") not in _RECORD_KINDS:
            continue
        _m = re.match(r"^(-?\d{4})", _nd.get("edtf") or "")
        if not _m:
            continue
        _yr = _m.group(1)
        _yn = _node_id("year", _yr)
        if not G.has_node(_yn):
            G.add_node(_yn, kind="year", label=_yr, year=int(_yr), canonical_name=_yr)
        add_edge(_nid, _yn, "DATED_IN", edtf=_nd.get("edtf"), method="normalize_dates")

    # ------------------------------------------------------------------
    # 2) CONTINUES_ON — reconstruct cross-page continuations
    # ------------------------------------------------------------------
    # stitch_notebooks emits its joins under "relations" (the real artifact) — accept "links" too.
    stitched_links = None
    if isinstance(stitch, dict):
        stitched_links = stitch.get("relations") or stitch.get("links")
    if stitched_links:
        # Preferred: explicit stitch joins (rule- or LLM-validated upstream). Each carries the full
        # fragment ids (``{page_id}::{rid}``) which are EXACTLY our notebook_entry node suffixes, so
        # we can build the node id directly — no scan needed. We keep only the *forward* direction
        # (type "continues_on"/"page_continues_on"); "continued_from" is its mirror and would just
        # duplicate the edge in reverse.
        for ln in stitched_links:
            ltype = (ln.get("type") or "").lower()
            if ltype == "continued_from":
                continue
            f_frag, t_frag = ln.get("from_fragment"), ln.get("to_fragment")
            f_rid, t_rid = ln.get("from_rid"), ln.get("to_rid")
            if ltype == "page_continues_on":
                # page→page continuation: fragments name the page via their record id prefix.
                src = _node_id("notebook_page", f_frag.split("::", 1)[0]) if f_frag else None
                dst = _node_id("notebook_page", t_frag.split("::", 1)[0]) if t_frag else None
            else:
                # entry→entry continuation (the logical-entry stitch).
                src = _node_id("notebook_entry", f_frag) if f_frag else None
                dst = _node_id("notebook_entry", t_frag) if t_frag else None
            if src and dst and G.has_node(src) and G.has_node(dst):
                add_edge(src, dst, "CONTINUES_ON",
                         rids=[r for r in (f_rid, t_rid) if r],
                         method="stitch", confidence=ln.get("confidence"),
                         reason=ln.get("reason"))
    else:
        # Fallback heuristic: a page labelled "... Cont." continues the previous page in the same
        # notebook (ordered by pdf_page_number). Cheap, transparent, and clearly tagged as a guess.
        log.info("no stitched_notebooks.json — using page_label '/cont/i' heuristic for CONTINUES_ON")
        by_nb: dict = defaultdict(list)
        for page in pages:
            by_nb[page.get("notebook", "")].append(page)
        for nb_pages in by_nb.values():
            nb_pages.sort(key=lambda p: (p.get("pdf_page_number") or 0))
            for prev, cur in zip(nb_pages, nb_pages[1:]):
                labels = [e.get("page_label", "") for e in cur.get("entries", [])]
                if any(re.search(r"cont", lab, re.I) for lab in labels):
                    add_edge(_node_id("notebook_page", prev["id"]),
                             _node_id("notebook_page", cur["id"]),
                             "CONTINUES_ON", method="page_label_heuristic")

    # ------------------------------------------------------------------
    # 3) Entity nodes — canonical persons / orgs / places / conditions
    # ------------------------------------------------------------------
    # Build a rid → record-node index so MENTIONED_IN edges can attach an entity to the exact record
    # (letter / entry) it was read from.
    rid_index = _build_rid_index(docs, pages)
    record_persons: dict = defaultdict(set)   # record_node -> {person nid} (for the co-occurrence net)
    _GENERIC_PEOPLE = {"husband", "wife", "mother", "father", "son", "daughter", "parents", "child",
                       "children", "baby", "sister", "brother", "family", "friend", "patient", "wife."}
    for ent in ents:
        # The entity ENGINE (entity_engine.py) is the authority on what each thing IS: drop what it
        # judged NOISE / not a real entity (e.g. "Enrolled", "God bless you"), and trust its reclassified
        # kind over the raw NER type. Fall back to the NER type when no engine verdict is present.
        u = ent.get("understanding") or {}
        enr0 = enriched.get(ent.get("id", "")) or {}
        if u.get("is_real_entity") is False or enr0.get("is_real_entity") is False:
            # RECOVERY (audit P1a): keep a dropped entity that is actually a real named PERSON/PLACE/ORG
            # (proper noun the noise judge mis-killed) — only generic-type noise stays dropped.
            if not _recoverable_propernoun(ent):
                continue
        ekind = u.get("entity_kind") or enr0.get("entity_kind") or ent.get("type")
        etype = _norm_etype(ekind)
        # DATE/noise "entities" are NER fragments ("June", "To-day") — noise as nodes. Dates belong on
        # records (full EDTF attrs) + the YEAR temporal spine (below), so we skip them here.
        if etype == "date" or (ekind or "").strip().lower() in ("date", "noise"):
            continue
        name  = ent.get("canonical_name") or ent.get("name") or "Unknown"
        nid   = ent.get("id") or _node_id(etype, name)
        # Enrichment (if run) gives a CLEAN name + a grounded description + relation-to-Solanus + a
        # geocodable location — so the node reads as a real person/place, not an OCR fragment.
        enr = enriched.get(nid) or enriched.get(ent.get("id", ""))
        if enr and enr.get("canonical_name"):
            name = enr["canonical_name"]
        lat = lon = None                                    # geocode places for the map (real places only)
        display = name                                       # label may get the location appended; the
        if etype == "place" and (enr or {}).get("is_place"):  # canonical_name stays BARE so structured
            q = ((enr or {}).get("location") or "").strip()   # fields (LOCATED_AT) still reconcile to it
            geo = geocodes.get(q) if q else None
            if geo:
                lat, lon = geo.get("lat"), geo.get("lon")
            loc = (enr or {}).get("location")                # "full location in the name" (display only)
            if loc and loc.split(",")[0].strip().lower() not in name.lower():
                display = f"{name} — {loc}"
        G.add_node(nid, kind=etype, label=display, lat=lat, lon=lon,
                   canonical_name=name, variants=ent.get("variants", []),
                   attrs=ent.get("attrs", {}), confidence=ent.get("confidence"),
                   needs_review=ent.get("needs_review", False),
                   authority=ent.get("authority", {}),  # Wikidata/VIAF/GeoNames IDs once reconciled
                   mention_count=len(ent.get("mentions", [])),
                   description=(enr or {}).get("description"), role=(enr or {}).get("role"),
                   relation_to_solanus=(enr or {}).get("relation_to_solanus"),
                   location=(enr or {}).get("location"))
        # MENTIONED_IN: tie the entity to every record a mention came from. Provenance lives under
        # mention["provenance"] (rid/doc_id/page); rid is sometimes "" (e.g. a recipient field), so
        # we fall back to the doc_id to still anchor the mention to its record.
        for m in ent.get("mentions", []):
            prov = m.get("provenance", m)               # tolerate both nested + flat shapes
            rid, doc_id = prov.get("rid"), prov.get("doc_id")
            # resolve by the COMPOSITE (doc_id, rid) — NEVER the bare rid, which collides across
            # ~1,942 records and would funnel this mention onto the wrong record (the collapse bug).
            rec_node = rid_index.get((doc_id, rid)) \
                or (_node_id("notebook_page", doc_id) if doc_id and G.has_node(_node_id("notebook_page", doc_id)) else None) \
                or (_node_id("letter", doc_id) if doc_id else None)
            if rec_node and G.has_node(rec_node):
                add_edge(nid, rec_node, "MENTIONED_IN",
                         rids=[rid] if rid else [],
                         edtf=edtf_for(rid, doc_id, raw_fallback=G.nodes[rec_node].get("date_raw")),
                         method="mention")
                # collect for the co-occurrence net: which people appear in this record (skip generic
                # role-words; Solanus himself is dropped later — he co-occurs with everyone)
                # co-occurrence only at GRANULAR records (entry/letter). A whole notebook_page holds
                # many independent favor entries from different petitioners, so people who merely share
                # a page are NOT connected — excluding page nodes kills that same-page artifact.
                if etype == "person" and name.strip().lower() not in _GENERIC_PEOPLE \
                        and G.nodes[rec_node].get("kind") != "notebook_page":
                    record_persons[rec_node].add(nid)
        # FAMILY / MEMBER_OF often arrive pre-extracted on the entity (resolve_entities clusters
        # families and links orders); honor them when present, each with provenance.
        for fam in ent.get("family", []):
            add_edge(nid, _resolve_ref(G, fam), "FAMILY",
                     rids=fam.get("rids", []) if isinstance(fam, dict) else [], method="resolved")
        for org in ent.get("member_of", []):
            add_edge(nid, _resolve_ref(G, org), "MEMBER_OF",
                     rids=org.get("rids", []) if isinstance(org, dict) else [], method="resolved")

    # ------------------------------------------------------------------
    # 3a) Index resolved person/place entities by name (+ variants) so the structured-field helpers
    # (_ensure_person/_ensure_place in the correspondence section) REUSE these rich nodes instead of
    # minting description-less duplicates. Higher-mention entities win an ambiguous key.
    # ------------------------------------------------------------------
    _PERSON_IDX.clear()
    _PLACE_IDX.clear()
    _ORG_IDX.clear()
    _ent_nodes = sorted(((nid, nd) for nid, nd in G.nodes(data=True)
                         if nd.get("kind") in ("person", "place", "organization")),
                        key=lambda x: x[1].get("mention_count") or 0, reverse=True)
    for nid, nd in _ent_nodes:
        k = nd.get("kind")
        idx = _PERSON_IDX if k == "person" else (_PLACE_IDX if k == "place" else _ORG_IDX)
        for nm in [nd.get("canonical_name") or nd.get("label")] + list(nd.get("variants") or []):
            mk = _match_key(nm or "")
            if mk and mk not in idx:
                idx[mk] = nid

    # ------------------------------------------------------------------
    # 3b) APPEARS_WITH — the co-occurrence social network (people in the same record)
    # ------------------------------------------------------------------
    # Two people who appear together in >= 2 records are genuinely connected in his life (family, a
    # petitioner and the sick person prayed for, recurring associates). Solanus is excluded — he
    # co-occurs with everyone, so he'd just be a hub (his ties are the WROTE_TO edges). Kept sparse
    # (>= 2 shared records, top 2500 by weight) so the result is a legible network, not a hairball.
    solanus_nids = {n for n, d in G.nodes(data=True)
                    if d.get("kind") == "person" and "solanus" in (d.get("label", "") or "").lower()}
    cooc: dict = defaultdict(int)
    cooc_recs: dict = defaultdict(list)         # (a,b) -> [record_node, ...] backing the co-occurrence
    for rec, persons in record_persons.items():
        ps = sorted(p for p in persons if p not in solanus_nids and G.has_node(p))
        for a in range(len(ps)):
            for b in range(a + 1, len(ps)):
                cooc[(ps[a], ps[b])] += 1
                cooc_recs[(ps[a], ps[b])].append(rec)
    cooc_pairs = sorted(((w, k) for k, w in cooc.items() if w >= 2), reverse=True)[:2500]
    for w, (a, b) in cooc_pairs:
        # carry the shared RECORD nodes so the UI can show the actual page(s) the two share — letting a
        # reader verify a genuine co-mention vs. a mere same-page coincidence (David's explicit ask).
        add_edge(a, b, "APPEARS_WITH", rids=[], method="co_occurrence", weight=w,
                 shared_records=sorted(set(cooc_recs[(a, b)]))[:12])
    log.info("APPEARS_WITH: %d co-occurrence edges (>=2 shared records)", len(cooc_pairs))

    # ------------------------------------------------------------------
    # 4a) Correspondence relations — WROTE_TO + LOCATED_AT from letter structure
    # ------------------------------------------------------------------
    # These are the cheapest, most reliable edges in the whole corpus: the recipient/sender/location
    # are *already separated fields* per letter (RESEARCH_PLAN PART B: "structured fields are a free
    # head-start"). Solanus is the sender for his own letters; we link him → recipient.
    solanus = _ensure_person(G, "Father Solanus Casey", aliases=["Bernard F. Casey", "Solanus Casey"])
    for d in docs:
        lid = _node_id("letter", d["id"])
        edtf = to_edtf(d.get("date"))
        recipient = d.get("recipient") or d.get("text_by_label", {}).get("src_recipient")
        if recipient and not _plausible_recipient(recipient):
            recipient = None                                # drop "Latin notes"/"Retreat Notes" etc.
        if recipient:
            rnode = _ensure_person(G, recipient)
            add_edge(solanus, rnode, "WROTE_TO",
                     rids=_rids_for_label(d, "src_recipient") or _rids_for_label(d, "src_content"),
                     edtf=edtf, method="source", via=lid)
            add_edge(rnode, lid, "MENTIONED_IN", rids=_rids_for_label(d, "src_recipient"), edtf=edtf)
        # LOCATED_AT — sender/recipient locations are explicit fields too.
        for lab, who in (("src_location_recipient", recipient), ("src_location_sender", "Father Solanus Casey")):
            place = d.get("text_by_label", {}).get(lab)
            if place and who:
                add_edge(_ensure_person(G, who), _ensure_place(G, place), "LOCATED_AT",
                         rids=_rids_for_label(d, lab), edtf=edtf, method="source")

    # ------------------------------------------------------------------
    # 4b) Favors — ENROLLED / HAS_CONDITION / HAS_OUTCOME from notebook entries
    # ------------------------------------------------------------------
    # The notebooks are a *register of favors*. Until the LLM extractor runs, we seed the favor
    # backbone with conservative lexical cues, each clearly tagged method="lexical_cue".
    for page in pages:
        for e in page.get("entries", []):
            text = e.get("text") or ""
            if not text:
                continue
            sig = _favor_signals(text)
            if not (sig["enrolled"] or sig["conditions"] or sig["outcomes"]):
                continue
            composite = f"{page['id']}::{e['rid']}"
            eid  = _node_id("notebook_entry", composite)
            edtf = edtf_for(composite, e["rid"], raw_fallback=e.get("date"))
            # Bitemporal: prefer a distinct enrollment date and report/outcome date when the date
            # stage split them; otherwise both fall back to the entry's single date.
            enr_edtf, rep_edtf = bitemporal_for(composite, e["rid"])
            enr_edtf, rep_edtf = enr_edtf or edtf, rep_edtf or edtf
            # Model the favor as its own Activity node, anchored to the entry that records it.
            fid = _node_id("favor", composite)
            G.add_node(fid, kind="favor", label=text[:60], doc_id=page["id"], rid=e["rid"], edtf=edtf)
            add_edge(fid, eid, "MENTIONED_IN", rids=[e["rid"]], edtf=edtf, method="lexical_cue")
            if sig["enrolled"]:
                # PETITIONED_FOR vs ENROLLED: "enroll" is the Seraphic Mass Association act; the
                # enrollment carries the *enrollment* valid-time.
                add_edge(fid, eid, "ENROLLED", rids=[e["rid"]], edtf=enr_edtf, method="lexical_cue")
            for c in sig["conditions"]:
                add_edge(fid, _ensure_concept(G, "condition", c), "HAS_CONDITION",
                         rids=[e["rid"]], edtf=edtf, method="lexical_cue")
            for o in sig["outcomes"]:
                # an outcome is *reported* later — tag it with the report valid-time when we have it.
                add_edge(fid, _ensure_concept(G, "outcome", o), "HAS_OUTCOME",
                         rids=[e["rid"]], edtf=rep_edtf, method="lexical_cue")

    # ------------------------------------------------------------------
    # 4c) Gold connection links (entry.linked) → MENTIONED_IN between records
    # ------------------------------------------------------------------
    # Each entry may carry `linked`: degree-1 rids from David's gold connection graph. These are
    # human-curated cross-references — exactly the kind of trustworthy edge we want, so we surface
    # them as record↔record MENTIONED_IN ties with method="gold_link".
    for page in pages:
        for e in page.get("entries", []):
            src = _node_id("notebook_entry", f"{page['id']}::{e['rid']}")
            for ref_rid in e.get("linked", []):
                # gold links are intra-page cross-references → resolve within THIS page (composite key).
                dst = rid_index.get((page["id"], ref_rid))
                if dst and G.has_node(dst):
                    add_edge(src, dst, "MENTIONED_IN", rids=[e["rid"], ref_rid], method="gold_link")

    log.info("graph built: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
    return G


# ----------------------------------------------------------------------
# Node-ensuring helpers — create-or-return a canonical node by name/type.
# Before resolve_entities exists, these mint *provisional* entity nodes straight from letter fields
# so the correspondence graph is usable immediately; once entities.json lands, the resolver's
# canonical ids take precedence and these provisional names cluster under them.
# ----------------------------------------------------------------------
# Name -> resolved-entity-node id indexes, populated from the entity nodes in run(). They let the
# structured-field helpers below REUSE a resolved+enriched entity node instead of minting a duplicate
# (otherwise the WROTE_TO correspondence network is a separate, description-less shadow of the graph).
_PERSON_IDX: dict = {}
_PLACE_IDX: dict = {}
_ORG_IDX: dict = {}                  # organizations, so an ORG recipient/location reconciles here (not a bogus person)
_TITLES_MATCH = {"father", "fr", "rev", "reverend", "mr", "mrs", "miss", "ms", "br", "brother",
                 "sister", "sr", "st", "saint", "dr", "mother", "v", "very", "rt", "most"}


def _match_key(name: str) -> str:
    """Normalize a name for entity reconciliation: lower, drop honorifics + punctuation."""
    toks = [t for t in re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).split() if t not in _TITLES_MATCH]
    return " ".join(toks)


# Words that mark a "recipient" field as NOT a person (OCR/segmentation grabbed a note/heading instead).
_NONPERSON_CUES = {"notes", "note", "book", "books", "dates", "date", "rule", "testament", "list",
                   "prayers", "prayer", "mass", "masses", "card", "cards", "letter", "letters", "memo",
                   "diary", "entry", "page", "chapter", "verse", "psalm", "gospel", "sermon", "retreat",
                   "schedule", "record", "latin", "notebook", "address", "subject", "re"}


def _plausible_recipient(name: str) -> bool:
    """Is this 'recipient' field actually a person name (not a heading/note the OCR captured)?"""
    if not name:
        return False
    toks = re.sub(r"[^a-z ]", " ", name.lower()).split()
    if not toks or len(toks) > 7:
        return False
    if any(t in _NONPERSON_CUES for t in toks):
        return False
    return bool(re.search(r"[A-Z][a-z]", name))            # needs a capitalized name-like word


def _ensure_person(G, name: str, aliases: list | None = None) -> str:
    mk = _match_key(name)
    hit = _PERSON_IDX.get(mk) or _ORG_IDX.get(mk)           # reuse the resolved node (a recipient may be an ORG)
    if hit and G.has_node(hit):
        return hit
    nid = _node_id("person", name)
    if not G.has_node(nid):
        G.add_node(nid, kind="person", label=name, canonical_name=name,
                   variants=aliases or [], provisional=True)
    return nid


def _ensure_place(G, name: str) -> str:
    mk = _match_key(name)
    hit = _PLACE_IDX.get(mk) or _ORG_IDX.get(mk)            # a "location" may be a named institution/org
    if hit and G.has_node(hit):
        return hit
    nid = _node_id("place", name)
    if not G.has_node(nid):
        G.add_node(nid, kind="place", label=name, canonical_name=name, provisional=True)
    return nid


def _ensure_concept(G, kind: str, name: str) -> str:
    """Create-or-return a condition/outcome concept node (keyed on the cue token)."""
    nid = _node_id(kind, name)
    if not G.has_node(nid):
        G.add_node(nid, kind=kind, label=name, canonical_name=name, provisional=True)
    return nid


def _resolve_ref(G, ref) -> str:
    """Resolve a family/member reference that may be a bare id, a name, or a {id|name} dict."""
    if isinstance(ref, dict):
        return ref.get("id") or _ensure_person(G, ref.get("name", "Unknown"))
    if isinstance(ref, str) and ref in G:
        return ref
    return _ensure_person(G, str(ref))


# resolve_entities labels its coarse types in upper-case (PERSON/PLACE/ORG/...). We fold those onto
# the node-kind localnames this graph uses (which double as RiC-O class lookups + id prefixes).
_ETYPE_ALIASES = {
    "person": "person", "per": "person",
    "place": "place", "loc": "place", "location": "place", "gpe": "place",
    "organization": "organization", "org": "organization", "corporatebody": "organization",
    "condition": "condition", "outcome": "outcome",
    # resolve_entities also emits these coarse types — map them explicitly so they DON'T fall through
    # to the default (which must never silently relabel a date/favor/term as a person).
    "favor": "favor", "fav": "favor",
    "date": "date",
    "role": "role", "role_title": "role", "title": "role",
    "event": "event",
    "institution": "place", "building": "place",          # engine sometimes emits these as entity_kind
    "misc": "other", "religious_term": "other", "term": "other", "concept": "other", "other": "other",
}


def _norm_etype(raw: str | None) -> str:
    """Normalize an entity type string to one of our node kinds.

    Defaults to 'other' — NEVER 'person'. Silently relabeling an unknown type as a person is exactly
    the bug that flooded the graph with 15k bogus 'person' nodes (dates, favors, religious terms).
    """
    return _ETYPE_ALIASES.get((raw or "other").strip().lower(), "other")


def _build_rid_index(docs: list, pages: list) -> dict:
    """Map every region id → the record node it belongs to (for fast provenance attachment).

    A letter's regions all map to that letter's node; a notebook entry's rid maps to the entry node;
    a notebook page's other regions map to the page node. This lets a mention or a gold `linked` rid
    snap straight onto the right record.

    CRITICAL: region ids (rid) are PAGE-LOCAL, not globally unique — the literal string
    "doc_1.src_content.0" is reused on ~1,942 distinct records. A flat {rid: node} dict therefore
    overwrites on every collision (last-writer-wins) and funnels the whole corpus's mentions onto a
    handful of wrong record nodes (the Appendix_3 collapse bug). So we key by the COMPOSITE
    (owning_record_id, rid), which is unique, and callers MUST look up with (doc_id, rid).
    """
    idx: dict = {}
    for d in docs:
        lid = _node_id("letter", d["id"])
        for r in d.get("regions", []):
            idx[(d["id"], r["rid"])] = lid
    for page in pages:
        pid = _node_id("notebook_page", page["id"])
        for r in page.get("regions", []):
            idx[(page["id"], r["rid"])] = pid
        for e in page.get("entries", []):
            idx[(page["id"], e["rid"])] = _node_id("notebook_entry", f"{page['id']}::{e['rid']}")
    return idx


# ======================================================================
# Export 1 — data/graph.json for the web visualization
# ======================================================================
def export_graph_json(G, path: Path) -> dict:
    """Serialize the networkx graph to a compact node/edge JSON for the front-end viz.

    Shape (D3/Cytoscape-friendly)::

        {
          "meta":  {"n_nodes": .., "n_edges": .., "node_kinds": {..}, "edge_kinds": {..}},
          "nodes": [ {"id","kind","label", ...attrs} ],
          "edges": [ {"source","target","kind","label","valid_time","rids","method"} ]
        }

    We keep node attrs but trim long ``text`` to a snippet (the viz fetches full text + the IIIF
    region on demand using the rids). Non-destructive: writes a brand-new file.

    Args:
        G:    the built networkx graph.
        path: destination (data/graph.json).

    Returns:
        The serialized dict (also useful for tests/asserts).
    """
    nodes = []
    for n, d in G.nodes(data=True):
        nd = {"id": n, **d}
        nodes.append(nd)

    import re as _re
    def _leading_year(s):
        m = _re.search(r"(1[89]\d\d|20\d\d)", str(s or ""))
        return int(m.group(1)) if m else None

    def _edge_year_span(d):
        """[minYear, maxYear] for an edge so the timeline date-brush can filter it. Prefer the edge's
        own valid_time; else derive from the dates of the records it links (shared_records)."""
        y = _leading_year(d.get("valid_time"))
        if y:
            return [y, y]
        years = []
        for rec in d.get("shared_records", []) or []:
            nd = G.nodes.get(rec, {})
            years.append(_leading_year(nd.get("edtf") or nd.get("date_raw") or nd.get("year")))
        years = [v for v in years if v]
        return [min(years), max(years)] if years else None

    edges = []
    for u, v, d in G.edges(data=True):
        kind = d.get("kind")
        e = {"source": u, "target": v, "kind": kind,
             "label": EDGE_KINDS.get(kind, {}).get("label", kind),
             "valid_time": d.get("valid_time"), "rids": d.get("rids", []),
             "method": d.get("method", "")}
        # carry optional evidence fields when present (co-occurrence weight + the shared records that
        # back an APPEARS_WITH edge — so the UI can show the page and prove a real co-mention)
        if d.get("weight") is not None:
            e["weight"] = d.get("weight")
        if d.get("shared_records"):
            e["shared_records"] = d.get("shared_records")
        span = _edge_year_span(d)
        if span:
            e["year_span"] = span                          # [minYear, maxYear] for the timeline brush
        edges.append(e)
    out = {
        "meta": {
            "n_nodes": G.number_of_nodes(),
            "n_edges": G.number_of_edges(),
            "node_kinds": dict(Counter(d.get("kind") for _, d in G.nodes(data=True))),
            "edge_kinds": dict(Counter(d.get("kind") for *_e, d in G.edges(data=True))),
            "config_fingerprint": config.fingerprint(),
        },
        "nodes": nodes,
        "edges": edges,
    }
    # A rebuild regenerates the BASE graph from entity_store and would otherwise silently overwrite the
    # in-place post-passes (kinship/descriptions/connection verdicts). Take a timestamped backup first so
    # any prior (possibly layered) graph.json is always recoverable — this is what makes run() honestly
    # "non-destructive". Write atomically so a crash can't leave a truncated graph.json.
    from lib.safeio import backup, atomic_write_text
    backup(path, "build")
    atomic_write_text(path, json.dumps(out, ensure_ascii=False, indent=2))
    log.info("wrote %s (%d nodes, %d edges)", path, out["meta"]["n_nodes"], out["meta"]["n_edges"])
    return out


# ======================================================================
# Export 2 — data/graph.ttl, RiC-O RDF via rdflib
# ======================================================================
def export_graph_ttl(G, path: Path) -> str:
    """Project the same graph into **RiC-O** RDF and serialize as Turtle.

    The mapping (kept honest — RiC-O class/property where one fits, a clearly project-namespaced
    predicate where it doesn't):
      - each node → an instance IRI ``BASE + id`` typed with its :func:`NODE_RICO_CLASS`,
        ``rico:name`` from the label, and (records) a ``rico:date`` EDTF *literal*.
      - each edge → a triple ``subj  rico:<predicate>  obj`` plus a small reified statement that
        hangs the **provenance rids** and **valid-time** off the edge (RDF edges can't carry
        attributes directly, so we mint a blank node and use PROV-O ``prov:wasDerivedFrom``).

    Why reify? Because "Solanus wrote to X *in 1896*, *as evidenced by region doc_5.src_recipient.0*"
    is a statement-about-a-statement — provenance and time are metadata *on the edge*, and RDF needs
    a node to hang them on. This keeps the receipts attached.

    Args:
        G:    the built networkx graph.
        path: destination (data/graph.ttl).

    Returns:
        The Turtle text (also handy for tests).

    Raises:
        ModuleNotFoundError: if rdflib isn't installed (see the install NOTE at the top).
    """
    from rdflib import Graph, Namespace, Literal, URIRef, BNode      # lazy import
    from rdflib.namespace import RDF, RDFS

    rico = Namespace(RICO)
    base = Namespace(BASE)
    prov = Namespace(PROV)
    edtf_dt = URIRef(EDTF_NS)

    g = Graph()
    g.bind("rico", rico)
    g.bind("prov", prov)
    g.bind("id", base)
    g.bind("rdfs", RDFS)

    def iri(node_id: str) -> URIRef:
        # IRIs can't contain spaces/colons in the localname; slug keeps them legal + stable.
        return URIRef(BASE + _slug(node_id))

    # ---- nodes → typed RiC-O instances -------------------------------
    for n, d in G.nodes(data=True):
        subj = iri(n)
        cls = NODE_RICO_CLASS.get(d.get("kind"), "Thing")
        g.add((subj, RDF.type, rico[cls]))
        if d.get("label"):
            g.add((subj, rico.name, Literal(d["label"])))
            g.add((subj, RDFS.label, Literal(d["label"])))
        if d.get("edtf"):
            g.add((subj, rico.date, Literal(d["edtf"], datatype=edtf_dt)))
        # Carry reconciled authority IDs (Wikidata/VIAF/GeoNames) as owl:sameAs-style links.
        for _scheme, uri in (d.get("authority") or {}).items():
            if uri:
                g.add((subj, rico.hasOrHadIdentifier, URIRef(uri)))

    # ---- edges → predicate triples + reified provenance/time ----------
    for u, v, d in G.edges(data=True):
        kind = d.get("kind")
        pred_local = EDGE_KINDS.get(kind, {}).get("rico", _slug(kind))
        s, o = iri(u), iri(v)
        g.add((s, rico[pred_local], o))
        # Reify when we have provenance or time to attach (otherwise keep the graph lean).
        if d.get("rids") or d.get("valid_time"):
            stmt = BNode()
            g.add((stmt, RDF.type, RDF.Statement))
            g.add((stmt, RDF.subject, s))
            g.add((stmt, RDF.predicate, rico[pred_local]))
            g.add((stmt, RDF.object, o))
            if d.get("valid_time"):
                g.add((stmt, rico.date, Literal(d["valid_time"], datatype=edtf_dt)))
            for rid in d.get("rids", []):
                # provenance: this assertion was DERIVED FROM a specific handwritten region.
                g.add((stmt, prov.wasDerivedFrom, URIRef(BASE + "region/" + _slug(rid))))

    ttl = g.serialize(format="turtle")
    path.write_text(ttl)
    log.info("wrote %s (%d triples)", path, len(g))
    return ttl


# ======================================================================
# KG LINK-PREDICTION (KGE) SCAFFOLD  — PROPOSES edges; never auto-adds
# ----------------------------------------------------------------------
# Knowledge graphs are always *incomplete*: a continuation link wasn't stitched, two name variants
# weren't merged, a family tie is implied but never written. Knowledge-Graph Embedding (KGE) models
# (TransE / DistMult / ComplEx / RotatE) learn a vector per entity and per relation such that a true
# triple (h, r, t) scores high; you can then score *un-asserted* triples and rank the most plausible
# missing edges. (RESEARCH_PLAN PART C: "KG embeddings + link prediction ... surfaced as review
# suggestions, never auto-asserted.")
#
# This is a SCAFFOLD on purpose:
#   * Training a real KGE needs torch/pykeen (a heavy dependency) and a GPU is nice-to-have. We do
#     NOT install or train here. The function below has the complete control flow and a tiny, pure-
#     NumPy DistMult fallback you *can* run on CPU for a smoke test, but the production path is the
#     clearly-marked TODO that plugs in pykeen.
#   * It writes only data/graph_suggestions.json (a REVIEW QUEUE). David confirms; nothing merges
#     automatically. Gold is David-only.
#   * Even though it's local/free, we route a cost-log row through lib.costlog for auditability —
#     every model-shaped step in this project leaves a trace.
# ======================================================================
def propose_links(G=None, top_k: int = 50, train: bool = False) -> dict:
    """Propose plausible *missing* edges for human review (does NOT modify the graph).

    Pipeline (standard KGE link-prediction, scaffolded):
      1. Serialize the graph to ``(head, relation, tail)`` triples (the training set).
      2. Train a KGE model (DistMult/ComplEx/RotatE via **pykeen** — the TODO path) OR, if
         ``train=False`` (default), skip training and emit a well-formed *empty* suggestion file so
         downstream tooling (the review UI) always has something valid to read.
      3. For each relation we care to complete (CONTINUES_ON, FAMILY, MEMBER_OF, and same-as
         merges), score candidate (h, r, t) pairs that aren't already edges; keep the top-K.
      4. Write ``data/graph_suggestions.json`` = ranked candidates with score + the evidence rids of
         the neighboring true edges, so a reviewer sees *why* it was proposed.

    Args:
        G:     a prebuilt networkx graph; if None we build one.
        top_k: how many suggestions to surface per relation.
        train: if True, run the local NumPy DistMult smoke-trainer (CPU, free, rough). Default False
               keeps this side-effect-free and fast — production should set the pykeen TODO instead.

    Returns:
        The suggestions dict that was written (``{"suggestions": [...], "meta": {...}}``).
    """
    out_path = config.DATA / "graph_suggestions.json"
    config.DATA.mkdir(parents=True, exist_ok=True)

    if G is None:
        G = build_networkx_graph()

    # ---- 1) triples -------------------------------------------------
    triples = [(u, d.get("kind"), v) for u, v, d in G.edges(data=True)]
    entities = list(G.nodes())
    relations = sorted({r for _h, r, _t in triples})
    log.info("KGE scaffold: %d triples, %d entities, %d relations",
             len(triples), len(entities), len(relations))

    suggestions: list = []

    if train and triples:
        # ------------------------------------------------------------------
        # Tiny pure-NumPy DistMult — a *smoke* trainer so the control flow is
        # real and runnable on CPU, NOT a production KGE. score(h,r,t)=<e_h,r,e_t>.
        # Production: replace this block with pykeen (see the TODO below).
        # ------------------------------------------------------------------
        import numpy as np

        dim = 32
        rng = np.random.default_rng(0)
        e_idx = {e: i for i, e in enumerate(entities)}
        r_idx = {r: i for i, r in enumerate(relations)}
        E = rng.normal(scale=0.1, size=(len(entities), dim))
        R = rng.normal(scale=0.1, size=(len(relations), dim))
        pos = np.array([(e_idx[h], r_idx[r], e_idx[t]) for h, r, t in triples])

        def _unit(v):
            """Project an embedding back onto the unit ball — the single most important trick for
            keeping plain SGD-DistMult numerically sane. Without it the dot-products grow without
            bound and overflow to NaN (the bug this smoke run caught); standard DistMult constrains
            entity norms to ≤1 for exactly this reason."""
            n = np.linalg.norm(v)
            return v / n if n > 1.0 else v

        lr, epochs, clip = 0.05, 20, 1.0
        for _ep in range(epochs):                          # logistic-loss SGD w/ negative sampling
            neg_t = rng.integers(0, len(entities), size=len(pos))
            for (h, r, t), nt in zip(pos, neg_t):
                s_pos = float(np.clip(np.sum(E[h] * R[r] * E[t]), -clip * 30, clip * 30))
                s_neg = float(np.clip(np.sum(E[h] * R[r] * E[nt]), -clip * 30, clip * 30))
                gp, gn = _sig(s_pos) - 1.0, _sig(s_neg)    # logistic loss gradients (in [-1, 1])
                # gradient-clip each update so one bad step can't explode the embeddings
                E[h] = _unit(E[h] - lr * np.clip(gp * R[r] * E[t] + gn * R[r] * E[nt], -clip, clip))
                E[t] = _unit(E[t] - lr * np.clip(gp * R[r] * E[h], -clip, clip))
                E[nt] = _unit(E[nt] - lr * np.clip(gn * R[r] * E[h], -clip, clip))
                R[r] = R[r] - lr * np.clip(gp * E[h] * E[t] + gn * E[h] * E[nt], -clip, clip)

        # ---- 3) score candidate missing edges for a few target relations ----
        # Cap the candidate pool per side: scoring is O(heads×tails), so on a big corpus we sample a
        # bounded set of the most-connected nodes (by degree) rather than enumerate millions of pairs.
        # This keeps the smoke run instant; the pykeen TODO path scores exhaustively/efficiently.
        MAX_SIDE = 200
        existing = {(h, r, t) for h, r, t in triples}
        targets = [rel for rel in ("CONTINUES_ON", "FAMILY", "MEMBER_OF") if rel in r_idx]
        for rel in targets:
            ri = r_idx[rel]
            cand = []
            heads = [e for e in entities if _kind_ok(G, e, rel, "head")]
            tails = [e for e in entities if _kind_ok(G, e, rel, "tail")]
            heads = sorted(heads, key=lambda e: G.degree(e), reverse=True)[:MAX_SIDE]
            tails = sorted(tails, key=lambda e: G.degree(e), reverse=True)[:MAX_SIDE]
            for h in heads:
                for t in tails:
                    if h == t or (h, rel, t) in existing:
                        continue
                    score = float(np.sum(E[e_idx[h]] * R[ri] * E[e_idx[t]]))
                    cand.append((score, h, t))
            cand.sort(reverse=True)
            for score, h, t in cand[:top_k]:
                suggestions.append({
                    "head": h, "relation": rel, "tail": t, "score": round(score, 4),
                    "evidence_rids": _neighbor_rids(G, h) + _neighbor_rids(G, t),
                    "status": "proposed",     # review states: proposed → accepted | rejected
                    "note": "KGE(DistMult, NumPy smoke) suggestion — confirm before adding.",
                })
        # cost-log the (free, local) training pass for audit symmetry.
        costlog.log("local", "distmult-numpy-smoke", "kge_train", items=len(triples), usd=0.0,
                    meta=f"epochs={epochs};dim={dim};suggestions={len(suggestions)}")
    else:
        # Default no-train path: emit an empty-but-valid review queue + log a zero-cost no-op.
        log.info("KGE scaffold idle (train=False) — writing empty suggestion queue. "
                 "Set train=True for the NumPy smoke run, or wire pykeen (see TODO).")
        costlog.log("local", "kge-scaffold", "kge_noop", items=len(triples), usd=0.0,
                    meta="train=False; no suggestions generated")

    # TODO (production KGE): replace the NumPy block with pykeen, e.g. —
    #   from pykeen.pipeline import pipeline
    #   from pykeen.triples import TriplesFactory
    #   tf = TriplesFactory.from_labeled_triples(np.array(triples, dtype=str))
    #   result = pipeline(training=tf, model="RotatE", training_kwargs=dict(num_epochs=200))
    #   ...score the inverse/missing triples with result.model, keep top-K per relation...
    #   ...still write ONLY to graph_suggestions.json — never mutate graph.json/graph.ttl here.
    # Add `pykeen` (pulls torch) to the venv when ready; until then this stays a documented scaffold.

    payload = {
        "meta": {
            "n_triples": len(triples), "n_entities": len(entities),
            "relations": relations, "top_k": top_k, "trained": bool(train and triples),
            "model": "DistMult-numpy-smoke" if (train and triples) else "none",
            "config_fingerprint": config.fingerprint(),
        },
        "suggestions": suggestions,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    log.info("wrote %s (%d proposed edges for review)", out_path, len(suggestions))
    return payload


def _sig(x: float) -> float:
    """Numerically-stable logistic sigmoid (used by the smoke trainer)."""
    import math
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, x))))


def _kind_ok(G, node: str, rel: str, end: str) -> bool:
    """Cheap type constraint so KGE only proposes *type-sane* candidate edges.

    E.g. CONTINUES_ON should connect notebook records to notebook records; FAMILY/MEMBER_OF connect
    persons (→ persons/orgs). This shrinks the candidate space enormously and stops obviously-silly
    proposals (a place "continuing onto" a condition).
    """
    k = G.nodes[node].get("kind")
    table = {
        "CONTINUES_ON": ({"notebook_page", "notebook_entry"}, {"notebook_page", "notebook_entry"}),
        "FAMILY":       ({"person"}, {"person"}),
        "MEMBER_OF":    ({"person"}, {"organization"}),
    }
    heads, tails = table.get(rel, (set(), set()))
    return k in (heads if end == "head" else tails)


def _neighbor_rids(G, node: str, limit: int = 5) -> list:
    """Gather a few provenance rids from a node's existing edges — the 'why' shown to reviewers."""
    rids: list = []
    for _u, _v, d in G.edges(node, data=True):
        rids.extend(d.get("rids", []))
        if len(rids) >= limit:
            break
    return sorted(set(rids))[:limit]


# ======================================================================
# Stage entry point — run() (what run.py calls)
# ======================================================================
def run() -> None:
    """Build the temporal KG and write all three artifacts (non-destructive).

    Declared outputs (must match run.py): data/graph.json + data/graph.ttl. We also (re)write
    data/graph_suggestions.json as an always-valid review queue (empty until KGE is run with
    train=True or pykeen is wired). Prints a ``=``*60 SUMMARY block, per the style guide.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    config.DATA.mkdir(parents=True, exist_ok=True)

    # ----- build ------------------------------------------------------
    G = build_networkx_graph()

    # ----- export -----------------------------------------------------
    gj = export_graph_json(G, config.DATA / "graph.json")
    export_graph_ttl(G, config.DATA / "graph.ttl")

    # ----- link-prediction review queue (idle by default; no edges auto-added) -----
    sug = propose_links(G, train=False)

    # ----- SUMMARY ----------------------------------------------------
    print("=" * 60)
    print("build_graph SUMMARY")
    print("=" * 60)
    print(f"  nodes ............. {gj['meta']['n_nodes']}")
    print(f"  edges ............. {gj['meta']['n_edges']}")
    print(f"  node kinds ........ {gj['meta']['node_kinds']}")
    print(f"  edge kinds ........ {gj['meta']['edge_kinds']}")
    print(f"  graph.json ........ {config.DATA / 'graph.json'}")
    print(f"  graph.ttl ......... {config.DATA / 'graph.ttl'}")
    print(f"  suggestions ....... {config.DATA / 'graph_suggestions.json'} "
          f"({len(sug['suggestions'])} proposed; review-only)")
    print("=" * 60)
    # Cost recap — even a graph build that made no paid calls prints the (zero) tally, so a reader
    # always sees the cost discipline. summary() reads costs/usage.csv.
    print("cost log to date (costs/usage.csv):", json.dumps(costlog.summary(), indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build the RiC-O temporal knowledge graph (step_7).")
    ap.add_argument("--kge", action="store_true",
                    help="also run the local NumPy DistMult smoke link-predictor (writes suggestions)")
    ap.add_argument("--top-k", type=int, default=50, help="suggestions per relation (with --kge)")
    a = ap.parse_args()
    run()
    if a.kge:
        propose_links(top_k=a.top_k, train=True)
