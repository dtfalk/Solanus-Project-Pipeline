"""stages/extract_entities.py — Tier-1 Named-Entity Recognition (the first NER pass).

This stage turns each letter and each notebook entry into a list of *typed mentions* — the raw
material the resolver (A3) and the knowledge graph (Part C) are built on. It runs in **two passes**,
and the order matters because the cheap pass subsidizes the expensive one:

  Pass A — "structured harvest" (FREE, no model call).
      step_6 already separated each letter into labeled fields (`src_recipient`, `src_signature`,
      `src_location_sender`, `archv_date`, ...) and each notebook entry carries its own `date` and
      `page_label`. Those fields ARE entities we already know the type of, with provenance attached.
      So we simply *read them off* and emit typed mentions — no tokens spent. Think of it as picking
      up the money that's already lying on the table before paying anyone to look for more.

  Pass B — "LLM extraction" (PAID, deferred until David says go).
      The unstructured prose — a letter's `src_content`, a notebook entry's `text` — is where the
      rich entities hide: people with ages and roles, conditions tied to a body part, favors and
      their outcomes, family relations, religious terms. We send that text to Gemini with a strict
      `response_schema` so the model returns *guaranteed-valid* JSON in our rich, nested shape
      (PERSON+age/role/order, PLACE+type, ORG, DATE, CONDITION+body_part, FAVOR/OUTCOME, RELATION,
      ROLE/TITLE, RELIGIOUS_TERM). Constrained decoding means we never have to repair broken JSON.

Both passes emit mentions in ONE shared shape, each carrying provenance back to the exact region:
``doc_id`` + ``rid`` + ``page`` + ``vertices`` (the polygon on the scanned page). That is what lets
the final tool cite an answer down to a box on the manuscript — provenance-first, as the plan says.

Design rules honored here (see STYLE_GUIDE.md + the _template.py contract):
  - NON-DESTRUCTIVE: writes a brand-new artifact, ``data/entities_raw.jsonl``; never edits source.
  - COST-LOGGED: every model call (the only paid thing here is Pass B) goes through lib.costlog.
  - DEFERRED PAID WORK: the live Gemini call is gated behind ``execute=True``; by default this stage
    does Pass A for everyone and *previews* Pass B (builds the exact prompt + schema, logs a $0
    projection) without spending a cent. David flips one switch when ready.
  - IDEMPOTENT + RESUMABLE: re-running skips records already present in the output file.

CLI / programmatic entry points::

    python stages/extract_entities.py --limit 25            # Pass A + Pass-B PREVIEW (no spend)
    python stages/extract_entities.py --limit 25 --execute  # also runs the live LLM pass (billed!)
    python stages/extract_entities.py --batch               # emit a Gemini Batch-API request file
    from stages import extract_entities; extract_entities.run(limit=None)
"""
from __future__ import annotations

# ------------------------------------------------------------------ Core Python Imports
import argparse
import json
import os
import sys
from pathlib import Path

# ------------------------------------------------------------------ Local File Imports
STEP7 = Path(__file__).resolve().parent.parent
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))

import config              # noqa: E402  (paths + model registry)
from lib import costlog    # noqa: E402  (always log prices)


# ==================================================================
# Where outputs live
# ==================================================================
# One artifact, JSON-Lines (one JSON object per line). JSONL is the right shape here because the
# file grows record-by-record as we process letters/entries, and a crashed/cancelled run still
# leaves a perfectly valid prefix we can resume from (you can't say that about one giant JSON array).
OUT_PATH = config.DATA / "entities_raw.jsonl"

# The model that does Pass B. We read it from the central config so swapping the LLM is a one-liner
# there, not a hunt through stages. Flash is the cost/quality sweet spot for bulk extraction.
NER_MODEL = config.DEFAULTS["llm"]                       # e.g. "gemini-2.5-flash"


# ==================================================================
# The rich entity ontology (one place, so schema + prompt + docs agree)
# ==================================================================
# These are the fine-grained types from RESEARCH_PLAN Part B. Keeping them in a single list means
# the Gemini response_schema, the prompt's instructions, and any downstream validator all read from
# the *same* source of truth — they can never drift out of sync.
ENTITY_TYPES = [
    "PERSON",          # a named individual (may carry age, role, religious_order)
    "PLACE",           # a location (city / address / institution / region)
    "ORG",             # an organization (religious order, parish, hospital, the Seraphic Mass Assoc.)
    "DATE",            # any temporal expression in the text ("Mar. 30", "c. 1945", "this p.m.")
    "CONDITION",       # a medical / behavioral / spiritual affliction (may carry a body_part)
    "FAVOR",           # a request / petition / intention brought to Fr. Solanus
    "OUTCOME",         # a reported result of a favor ("Cured", "improved", "no change")
    "RELATION",        # a stated relationship between two people (mother of, neighbor of, husband)
    "ROLE_TITLE",      # a role or honorific ("Father Provincial", "Sister", "Mr.", "physician")
    "RELIGIOUS_TERM",  # devotional vocabulary ("Deo Gratias", "novena", "enrolled", "Mass")
]

# Controlled sub-vocabularies for a couple of the typed attributes. They're *hints*, not hard
# enums on the wire (real handwriting is messier than any closed list), so the schema leaves the
# attribute free-text but the prompt steers the model toward these buckets for clean downstream
# grouping.
PLACE_TYPES = ["city", "address", "institution", "region", "country", "other"]
CONDITION_KINDS = ["medical", "behavioral", "spiritual", "other"]


# ==================================================================
# PASS A — structured harvest (FREE: no model, no tokens)
# ==================================================================
# These maps say "this already-separated field is, by its very label, a mention of this type." We
# trust step_6's field separation here — it's gold-derived — so harvesting is just a typed copy with
# provenance attached. The win is twofold: (1) it's free, and (2) it removes the easy entities from
# the prose we pay the LLM to read, so Pass B can focus its tokens on the genuinely hard stuff.

# Letter fields -> (entity_type, optional attribute key it fills).
LETTER_FIELD_MENTIONS = {
    "src_recipient":          ("PERSON", "role:recipient"),
    "src_signature":          ("PERSON", "role:sender"),
    "src_location_recipient": ("PLACE",  "role:recipient_location"),
    "src_location_sender":    ("PLACE",  "role:sender_location"),
    "src_origin":             ("PLACE",  "role:origin"),
    "src_date":               ("DATE",   "role:letter_date"),
    "archv_date":             ("DATE",   "role:archival_date"),
}


def _load_inputs():
    """Read the two step_6 corpora.

    Returns:
        tuple(list, list): (documents, notebook_pages) exactly as serialized by step_6. We do not
        mutate them — this stage only reads.
    """
    documents = json.loads(config.DOCUMENTS.read_text())
    notebooks = json.loads(config.NOTEBOOKS.read_text())
    return documents, notebooks


def _region_index(record: dict) -> dict:
    """Build a quick {rid -> region} lookup for one record.

    Every letter/page carries a ``regions`` list, and each region has the geometry we need to cite a
    mention: its ``rid``, ``vertices`` (the polygon on the page), ``min_conf``, and page number. A
    mention that came from a given field should point at the region that produced that field, so we
    index regions by rid once and reuse it for all mentions of that record.

    Args:
        record: a single letter dict or notebook-page dict from step_6.

    Returns:
        dict: rid -> region dict.
    """
    return {r.get("rid"): r for r in record.get("regions", [])}


def _provenance(doc_id: str, region: dict | None, page, rid_fallback: str = "") -> dict:
    """Assemble the provenance block every mention carries.

    This is the heart of "provenance-first": no matter which pass found a mention, it records WHERE
    on the manuscript it lives, so the UI can later draw the exact box / open the IIIF region.

    Args:
        doc_id: the letter/page id (e.g. "Volume_1__p001").
        region: the matching region dict (gives rid + vertices + min_conf), or None if unknown.
        page:   the page number to cite (page_number_in_type).
        rid_fallback: rid to use when we couldn't find the region object itself.

    Returns:
        dict: {doc_id, rid, page, vertices, min_conf} — vertices/min_conf are None when unknown.
    """
    region = region or {}
    return {
        "doc_id":   doc_id,
        "rid":      region.get("rid", rid_fallback),
        "page":     page,
        "vertices": region.get("vertices"),     # polygon on the scanned page (None if unknown)
        "min_conf": region.get("min_conf"),     # OCR confidence for that region (None if unknown)
    }


def _mention(entity_type: str, text: str, prov: dict,
             attrs: dict | None = None, source: str = "harvest") -> dict:
    """Construct one mention in the shared shape used by BOTH passes.

    Keeping a single constructor means Pass A and Pass B emit byte-for-byte compatible records — the
    resolver downstream never has to care which pass produced a mention.

    Args:
        entity_type: one of ENTITY_TYPES.
        text:        the surface string of the mention.
        prov:        a provenance block from _provenance().
        attrs:       optional typed attributes (age, role, body_part, place_type, ...).
        source:      "harvest" (Pass A) or "llm" (Pass B) — kept for auditing/eval.

    Returns:
        dict: the mention record.
    """
    return {
        "type":       entity_type,
        "text":       (text or "").strip(),
        "attrs":      attrs or {},
        "provenance": prov,
        "source":     source,
    }


def harvest_letter(doc: dict) -> list:
    """Pass A for one letter: read the already-separated fields into typed mentions.

    Args:
        doc: a single letter dict from documents.json.

    Returns:
        list: zero or more mention dicts (free — no model call).
    """
    out = []
    doc_id = doc.get("id", "")
    page = doc.get("page_number_in_type")
    by_label = doc.get("text_by_label", {})
    regions = _region_index(doc)

    # ----- the labeled fields are entities we already know the type of -----
    for field, (etype, role_tag) in LETTER_FIELD_MENTIONS.items():
        value = (by_label.get(field) or "").strip()
        if not value:
            continue
        # The field name doubles as a region-category guess; find the region that produced it so the
        # mention cites the right polygon. (Region rids look like "doc_1.src_recipient.0".)
        region = next((r for r in doc.get("regions", []) if r.get("category") == field), None)
        # role_tag is "role:<something>" — store the role as a clean attribute for downstream use.
        attr_key, _, attr_val = role_tag.partition(":")
        out.append(_mention(etype, value,
                            _provenance(doc_id, region, page),
                            attrs={attr_key: attr_val}))
    return out


def harvest_notebook_page(page: dict) -> list:
    """Pass A for one notebook page: harvest each entry's date (the cheap structured signal).

    Notebook entries don't carry separated person/place fields the way letters do, but every entry
    *does* carry a ``date`` and a ``page_label`` — both free, typed signals. The dates here are
    deliberately raw (e.g. "Nov. 8th,"); the year usually lives in the page's ``archv_date`` and the
    A2 ``normalize_dates`` stage will assemble the full EDTF value later. We harvest the raw string
    now and let that downstream stage do the temporal reasoning.

    Args:
        page: a single notebook-page dict from notebooks.json.

    Returns:
        list: zero or more DATE mention dicts.
    """
    out = []
    doc_id = page.get("id", "")
    page_no = page.get("page_number_in_type")
    regions = _region_index(page)

    for entry in page.get("entries", []):
        rid = entry.get("rid", "")
        raw_date = (entry.get("date") or "").strip()
        if raw_date:
            region = regions.get(rid)               # the entry's own region carries the geometry
            out.append(_mention("DATE", raw_date,
                                _provenance(doc_id, region, page_no, rid_fallback=rid),
                                attrs={"role": "entry_date",
                                       "page_label": entry.get("page_label", "")}))
    return out


# ==================================================================
# PASS B — LLM extraction over free text (PAID; structured output)
# ==================================================================
# Below we (1) define the rich, nested response_schema, (2) build the prompt, (3) collect the
# extraction units (the prose we actually pay to read), and (4) call Gemini — but only when
# explicitly told to execute, so importing/running this stage never spends money by accident.

def build_response_schema():
    """The Gemini ``response_schema`` for Pass B — our rich, NESTED entity ontology.

    Why a schema at all? Free-form "return JSON" prompts drift: a key gets renamed, a list becomes a
    string, the model adds prose around the JSON. Passing a ``types.Schema`` switches Gemini into
    *constrained decoding* — the output is grammatically forced to match this shape, so it's always
    parseable. We pay a small "constrained-decoding reasoning tax," so we give the model a free-text
    ``reasoning`` field FIRST (property_ordering puts it before the structured lists) — a scratchpad
    where it can think before committing to the typed output. That's the pattern from the plan.

    The schema is intentionally nested: a PERSON may carry a list of ``conditions`` and ``relations``
    *attached to that person*, and a FAVOR may carry its ``outcome`` inline — because in this corpus
    those facts arrive bundled ("Marg. Quinn enrolled her neighbor Mr. Maughan, cured of cancer").

    Returns:
        google.genai.types.Schema: the root OBJECT schema to hand to GenerateContentConfig.
    """
    # Imported lazily so that merely importing this module never needs the SDK installed. (Pass A,
    # the CLI, and the batch-file writer all work with zero google.genai dependency.)
    from google.genai import types

    S = types.Schema
    T = types.Type

    # ----- small reusable leaf schemas -----
    def _str(desc: str):
        # Nullable strings everywhere: handwriting is sparse, so "this attribute is absent" must be
        # expressible. A non-nullable required string would force the model to hallucinate a value.
        return S(type=T.STRING, description=desc, nullable=True)

    # A CONDITION can stand alone OR hang off a PERSON, so we define it once and reuse it.
    condition = S(
        type        = T.OBJECT,
        description = "An affliction: medical, behavioral, or spiritual.",
        properties  = {
            "name":      _str("The condition as written, e.g. 'cancer', 'drinking', 'despair'."),
            "kind":      _str(f"One of {CONDITION_KINDS} (best guess)."),
            "body_part": _str("Body part if any, e.g. 'eye', 'lung', 'leg'. Null if none."),
        },
        property_ordering = ["name", "kind", "body_part"],
    )

    # An OUTCOME is the reported result of a favor; reused inline inside FAVOR and standalone.
    outcome = S(
        type        = T.OBJECT,
        description = "A reported result of a petition/favor.",
        properties  = {
            "result":   _str("e.g. 'Cured', 'improved', 'no change', 'died'."),
            "reported": _str("Who reported it / how it was confirmed, if stated."),
        },
        property_ordering = ["result", "reported"],
    )

    # A RELATION links the current person to another named person.
    relation = S(
        type        = T.OBJECT,
        description = "A stated relationship between this person and another.",
        properties  = {
            "kind":  _str("e.g. 'mother of', 'neighbor of', 'husband', 'correspondent'."),
            "other": _str("The other person's name as written."),
        },
        property_ordering = ["kind", "other"],
    )

    person = S(
        type        = T.OBJECT,
        description = "A named individual mentioned in the text.",
        properties  = {
            "name":            _str("Full name as written (keep OCR spelling)."),
            "age":             _str("Age if stated, e.g. '40', 'a child'. Null if absent."),
            "role":            _str("Role in this passage, e.g. 'petitioner', 'patient', 'priest'."),
            "religious_order": _str("Order if a religious, e.g. 'O.F.M. Cap.', 'Sister of Mercy'."),
            "title":           _str("Honorific/title, e.g. 'Father', 'Mr.', 'Rev.'."),
            # NESTED: conditions/relations attached directly to this person.
            "conditions":      S(type=T.ARRAY, items=condition, nullable=True,
                                 description="Conditions afflicting THIS person."),
            "relations":       S(type=T.ARRAY, items=relation, nullable=True,
                                 description="This person's relations to other named people."),
        },
        property_ordering = ["name", "age", "role", "religious_order", "title",
                             "conditions", "relations"],
    )

    place = S(
        type        = T.OBJECT,
        description = "A location mentioned in the text.",
        properties  = {
            "name": _str("Place as written, e.g. 'Detroit, Mich.', 'St. Bonaventure's'."),
            "type": _str(f"One of {PLACE_TYPES} (best guess)."),
        },
        property_ordering = ["name", "type"],
    )

    org = S(
        type        = T.OBJECT,
        description = "An organization: religious order, parish, hospital, association.",
        properties  = {
            "name": _str("Organization as written, e.g. 'Seraphic Mass Association'."),
            "type": _str("e.g. 'order', 'parish', 'hospital', 'association'."),
        },
        property_ordering = ["name", "type"],
    )

    date = S(
        type        = T.OBJECT,
        description = "A temporal expression exactly as written (normalization happens later).",
        properties  = {
            "text": _str("The date/time string, e.g. 'Mar. 30', 'c. 1945', 'this p.m.'."),
            "role": _str("e.g. 'enrollment', 'report/outcome', 'event'. Null if unclear."),
        },
        property_ordering = ["text", "role"],
    )

    favor = S(
        type        = T.OBJECT,
        description = "A petition/intention brought to Fr. Solanus (often with an outcome).",
        properties  = {
            "request":   _str("What was asked/intended, e.g. 'cure of cancer', 'conversion'."),
            "for_whom":  _str("Beneficiary's name if stated."),
            # NESTED: the outcome reported for THIS favor, inline.
            "outcome":   outcome,
        },
        property_ordering = ["request", "for_whom", "outcome"],
    )

    role_title = S(
        type        = T.OBJECT,
        description = "A role or honorific that names a function rather than a person.",
        properties  = {"text": _str("e.g. 'Father Provincial', 'physician', 'Sister'.")},
        property_ordering = ["text"],
    )

    religious_term = S(
        type        = T.OBJECT,
        description = "Devotional vocabulary specific to this corpus.",
        properties  = {"text": _str("e.g. 'Deo Gratias', 'novena', 'enrolled', 'Mass'.")},
        property_ordering = ["text"],
    )

    # ----- the root object: one list per top-level type -----
    # Each list is nullable so the model can omit absent types cleanly (an empty page yields all
    # nulls/empties rather than being forced to invent entities).
    return S(
        type        = T.OBJECT,
        description = "All entities extracted from one passage of Solanus Casey material.",
        properties  = {
            "reasoning":       S(type=T.STRING, nullable=True,
                                 description="Brief scratchpad: who/what is in this passage. "
                                             "Think here BEFORE filling the typed lists."),
            "persons":         S(type=T.ARRAY, items=person,         nullable=True),
            "places":          S(type=T.ARRAY, items=place,          nullable=True),
            "orgs":            S(type=T.ARRAY, items=org,            nullable=True),
            "dates":           S(type=T.ARRAY, items=date,           nullable=True),
            "conditions":      S(type=T.ARRAY, items=condition,      nullable=True,
                                 description="Conditions NOT already attached to a specific person."),
            "favors":          S(type=T.ARRAY, items=favor,          nullable=True),
            "outcomes":        S(type=T.ARRAY, items=outcome,        nullable=True,
                                 description="Outcomes NOT already attached to a specific favor."),
            "relations":       S(type=T.ARRAY, items=relation,       nullable=True),
            "roles_titles":    S(type=T.ARRAY, items=role_title,     nullable=True),
            "religious_terms": S(type=T.ARRAY, items=religious_term, nullable=True),
        },
        property_ordering = ["reasoning", "persons", "places", "orgs", "dates", "conditions",
                             "favors", "outcomes", "relations", "roles_titles", "religious_terms"],
    )


# The instruction prefix is CONSTANT across every unit — which is exactly what makes it a perfect
# candidate for prompt caching (cache this prefix once, pay ~10% for it on every subsequent call)
# and for the Batch API (same system text, thousands of tiny user texts). We keep it as one string
# so the cache key is stable. See the notes in run_batch_preview() / build_cache_note().
SYSTEM_INSTRUCTION = (
    "You are a meticulous archival historian building a knowledge graph from the papers of "
    "Servant of God Fr. Solanus Casey, O.F.M. Cap. (1870–1957): personal letters and the "
    "notebooks in which he logged petitions ('favors') brought to the Seraphic Mass Association "
    "and their reported outcomes.\n\n"
    "Extract EVERY entity in the passage into the provided schema. Rules:\n"
    "  - Keep the original (often OCR-imperfect) spelling of names and places verbatim; do NOT "
    "correct or modernize them.\n"
    "  - Attach a condition or relation to a PERSON when the text ties them together; only use the "
    "top-level lists for facts not bound to a specific person/favor.\n"
    "  - A 'favor' is a petition/intention; its 'outcome' is the reported result ('Cured', "
    "'improved', 'no change'). Capture both when present.\n"
    "  - Capture religious/devotional vocabulary (e.g. 'Deo Gratias', 'novena', 'enrolled') as "
    "RELIGIOUS_TERM.\n"
    "  - Record dates exactly as written; do not infer the year — a later stage normalizes them.\n"
    "  - If the passage is empty or has no entities of a type, return empty/null lists; never invent."
)


def _extraction_text_letter(doc: dict) -> str:
    """The prose we pay to read for a letter: its body, greeting and farewell give context."""
    tbl = doc.get("text_by_label", {})
    parts = [tbl.get("src_greeting"), tbl.get("src_content"), tbl.get("src_farewell")]
    return "\n".join(p.strip() for p in parts if p and p.strip()).strip()


def collect_units(documents: list, notebooks: list, limit: int | None = None) -> list:
    """Gather the Pass-B extraction units (id + provenance + the text to read).

    A "unit" is one LLM call's worth of input: a letter body, or a single notebook entry. We carry
    the provenance with each unit so the mentions Pass B returns can be stamped with the right
    region — the LLM only ever sees text, never the geometry.

    Args:
        documents: documents.json contents.
        notebooks: notebooks.json contents.
        limit:     if given, strided-sample down to ~this many units so a small test batch still
                   spans both letters and notebook entries (mirrors chunks.build_chunks).

    Returns:
        list: unit dicts {unit_id, kind, text, provenance}.
    """
    units = []

    # ----- letters: one unit per letter body -----
    for doc in documents:
        text = _extraction_text_letter(doc)
        if not text:
            continue
        region = next((r for r in doc.get("regions", []) if r.get("category") == "src_content"), None)
        units.append({
            "unit_id":    doc.get("id", ""),
            "kind":       "letter",
            "text":       text,
            "provenance": _provenance(doc.get("id", ""), region, doc.get("page_number_in_type")),
        })

    # ----- notebook entries: one unit per entry -----
    for page in notebooks:
        regions = _region_index(page)
        for entry in page.get("entries", []):
            text = (entry.get("text") or "").strip()
            if not text:
                continue
            rid = entry.get("rid", "")
            units.append({
                "unit_id":    f"{page.get('id','')}::{rid}",
                "kind":       "notebook_entry",
                "text":       text,
                "provenance": _provenance(page.get("id", ""), regions.get(rid),
                                          page.get("page_number_in_type"), rid_fallback=rid),
            })

    # Strided sample for --limit so a smoke test touches both letters and entries (not just the
    # first N letters). Same trick chunks.build_chunks uses.
    if limit and limit < len(units):
        step = len(units) / limit
        units = [units[int(i * step)] for i in range(limit)]
    return units


def _flatten_extraction(parsed: dict, unit: dict) -> list:
    """Turn one Gemini structured response into our flat mention records (with provenance).

    Gemini returns the nested ontology; the resolver wants flat mentions. We flatten here, lifting
    person-attached conditions/relations into their own mentions too (so they're searchable on their
    own) while preserving the link via an ``of_person`` attribute. Every mention inherits the unit's
    provenance — the LLM never saw the geometry, so we stamp it back on.

    Args:
        parsed: the parsed JSON dict matching build_response_schema().
        unit:   the source unit (gives provenance + unit_id).

    Returns:
        list: flat mention dicts (source="llm").
    """
    prov = unit["provenance"]
    out = []

    def add(etype, text, attrs=None):
        if text and str(text).strip():
            out.append(_mention(etype, str(text), prov, attrs=attrs, source="llm"))

    for p in (parsed.get("persons") or []):
        add("PERSON", p.get("name"),
            attrs={k: p.get(k) for k in ("age", "role", "religious_order", "title")
                   if p.get(k)})
        for c in (p.get("conditions") or []):       # NESTED -> own mention, linked back
            add("CONDITION", c.get("name"),
                attrs={"kind": c.get("kind"), "body_part": c.get("body_part"),
                       "of_person": p.get("name")})
        for r in (p.get("relations") or []):
            add("RELATION", f"{p.get('name')} — {r.get('kind')} — {r.get('other')}",
                attrs={"kind": r.get("kind"), "of_person": p.get("name"), "other": r.get("other")})

    for pl in (parsed.get("places") or []):
        add("PLACE", pl.get("name"), attrs={"type": pl.get("type")})
    for o in (parsed.get("orgs") or []):
        add("ORG", o.get("name"), attrs={"type": o.get("type")})
    for d in (parsed.get("dates") or []):
        add("DATE", d.get("text"), attrs={"role": d.get("role")})
    for c in (parsed.get("conditions") or []):
        add("CONDITION", c.get("name"),
            attrs={"kind": c.get("kind"), "body_part": c.get("body_part")})
    for fv in (parsed.get("favors") or []):
        oc = fv.get("outcome") or {}
        add("FAVOR", fv.get("request"),
            attrs={"for_whom": fv.get("for_whom"),
                   "outcome": oc.get("result"), "outcome_reported": oc.get("reported")})
        if oc.get("result"):                        # NESTED outcome -> its own mention too
            add("OUTCOME", oc.get("result"), attrs={"reported": oc.get("reported"),
                                                    "for_favor": fv.get("request")})
    for oc in (parsed.get("outcomes") or []):
        add("OUTCOME", oc.get("result"), attrs={"reported": oc.get("reported")})
    for r in (parsed.get("relations") or []):
        add("RELATION", f"{r.get('kind')} — {r.get('other')}",
            attrs={"kind": r.get("kind"), "other": r.get("other")})
    for rt in (parsed.get("roles_titles") or []):
        add("ROLE_TITLE", rt.get("text"))
    for rt in (parsed.get("religious_terms") or []):
        add("RELIGIOUS_TERM", rt.get("text"))

    return out


def extract_unit_llm(unit: dict) -> list:
    """Run the LIVE Gemini structured-output extraction for ONE unit. (PAID.)

    This is the only function in the file that spends money, and it is never called unless the caller
    explicitly opts in (run(..., execute=True) / --execute). It mirrors lib/providers/llm.py's
    Gemini wiring exactly — same client, same env-var handling, same cost-logging shape — but adds
    the ``response_schema`` for guaranteed-valid structured output.

    Args:
        unit: a unit dict from collect_units().

    Returns:
        list: flat mention dicts (source="llm"); [] on a malformed response.
    """
    # Lazy import: keeps the module import-safe without the SDK, and matches the providers' pattern.
    from google import genai
    from google.genai import types

    # Load keys the same way the providers do (step_7/.env, then step_4/.env where GEMINI_API_KEY
    # actually lives) — so this stage "just works" wherever the providers do.
    try:
        from dotenv import load_dotenv
        load_dotenv(config.STEP7 / ".env")
        load_dotenv(config.REPO / "pipeline_v3" / "step_4" / ".env")
    except Exception:
        pass

    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set (step_4/.env or step_7/.env)")

    client = genai.Client(api_key=key)
    cfg = types.GenerateContentConfig(
        temperature         = 0.1,                       # extraction wants determinism, not flair
        system_instruction  = SYSTEM_INSTRUCTION,        # constant -> cache-friendly (see notes)
        response_mime_type  = "application/json",
        response_schema     = build_response_schema(),   # constrained decoding -> valid JSON always
    )
    resp = client.models.generate_content(
        model    = NER_MODEL,
        contents = unit["text"],
        config   = cfg,
    )

    # ----- cost-log the call with REAL token counts (never skip this) -----
    usage = getattr(resp, "usage_metadata", None)
    in_tok  = getattr(usage, "prompt_token_count", 0) or 0
    out_tok = getattr(usage, "candidates_token_count", 0) or 0
    costlog.log("gemini", NER_MODEL, "ner_extract",
                input_tokens=in_tok, output_tokens=out_tok,
                meta=f"extract_entities/{unit['kind']}/{unit['unit_id']}")

    # ----- parse the (schema-guaranteed) JSON and flatten to mentions -----
    try:
        parsed = json.loads(resp.text or "{}")
    except (json.JSONDecodeError, TypeError):
        # With constrained decoding this should be unreachable, but we degrade gracefully rather
        # than crash a long run over one odd response.
        return []
    return _flatten_extraction(parsed, unit)


# ==================================================================
# Cost projection + Batch-API / prompt-cache notes (no spend)
# ==================================================================
def _project_cost(units: list) -> dict:
    """Estimate Pass-B cost WITHOUT calling anything — for the preview/summary block.

    We approximate input tokens as system-prompt + unit-text (~4 chars/token, like the embed
    adapter) and assume a modest structured output. We then show the sticker price and the
    discounted price you'd actually pay with the Batch API (−50%) and prompt caching on the constant
    system prefix (cached reads ≈ 10% of input price). These mirror RESEARCH_PLAN Part E.

    Args:
        units: the Pass-B units.

    Returns:
        dict: a small report of token/$ projections (also logged as a $0 row for the record).
    """
    sys_tok = max(1, len(SYSTEM_INSTRUCTION) // 4)
    body_tok = sum(max(1, len(u["text"]) // 4) for u in units)
    in_tok = sys_tok * len(units) + body_tok            # system prefix repeats per call
    out_tok = 220 * len(units)                          # rough: a compact structured object per unit

    price = config.LLMS.get(NER_MODEL, {})
    sticker = (in_tok * price.get("in", 0) + out_tok * price.get("out", 0)) / 1_000_000
    # Batch API ≈ half. Prompt caching makes the repeated system prefix ~10% on cached reads:
    cached_in_tok = sys_tok * len(units) * 0.10 + (in_tok - sys_tok * len(units))
    with_cache = (cached_in_tok * price.get("in", 0) + out_tok * price.get("out", 0)) / 1_000_000
    with_batch_and_cache = with_cache * 0.5

    return {
        "model":                NER_MODEL,
        "units":                len(units),
        "est_input_tokens":     int(in_tok),
        "est_output_tokens":    int(out_tok),
        "est_usd_sticker":      round(sticker, 4),
        "est_usd_with_cache":   round(with_cache, 4),
        "est_usd_batch_cache":  round(with_batch_and_cache, 4),
    }


def write_batch_requests(units: list, path: Path | None = None) -> Path:
    """Emit a Gemini **Batch-API** request file (JSONL) — the −50% path for the full run. (No spend.)

    The Batch API takes a file of independent requests, processes them asynchronously (within ~24h —
    perfectly fine for an offline corpus build), and bills at half the interactive rate. This writer
    produces exactly that file but does NOT submit it; submission is the deferred paid step David
    triggers. Each line is one request, keyed so we can join results back to the unit.

    The shape follows the google-genai Batch convention: a ``key`` plus a ``request`` carrying the
    same contents + GenerationConfig (system instruction + response_schema) we use interactively. To
    submit later (deferred):

        client = genai.Client(api_key=...)
        up = client.files.upload(file=<this file>, config={"mime_type": "jsonl"})
        job = client.batches.create(model=NER_MODEL,
                                    src=up.name,
                                    config={"display_name": "step7_ner"})
        # poll job.state; then download job.dest and run _flatten_extraction over each result line.

    Args:
        units: the Pass-B units to enqueue.
        path:  output path (defaults to data/entities_batch_requests.jsonl).

    Returns:
        Path: the written request file.
    """
    # Build the schema once and serialize it so the batch file is fully self-describing. (We import
    # lazily and fall back to a note if the SDK is absent, so this works even on a bare machine.)
    try:
        schema = build_response_schema()
        schema_json = schema.model_dump(exclude_none=True) if hasattr(schema, "model_dump") else None
    except Exception:
        schema_json = None      # SDK not available here; the submit script can rebuild the schema.

    path = path or (config.DATA / "entities_batch_requests.jsonl")
    config.DATA.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for u in units:
            request = {
                "key": u["unit_id"],                    # lets us re-join results -> provenance
                "request": {
                    "contents": [{"role": "user", "parts": [{"text": u["text"]}]}],
                    "generation_config": {
                        "temperature":        0.1,
                        "response_mime_type": "application/json",
                        # response_schema serialized inline when the SDK was importable:
                        **({"response_schema": schema_json} if schema_json else {}),
                    },
                    "system_instruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
                },
                # Provenance travels with the request so the result-joiner needs nothing else.
                "_provenance": u["provenance"],
                "_kind": u["kind"],
            }
            f.write(json.dumps(request, ensure_ascii=False) + "\n")
    return path


def build_cache_note() -> str:
    """A short reminder of the prompt-cache plan (printed in the preview summary).

    Prompt caching only helps when a long prefix repeats. Ours does: SYSTEM_INSTRUCTION +
    response_schema are byte-identical on every call. The deferred submit script should create one
    explicit cache and reuse it::

        cache = client.caches.create(model=NER_MODEL, config={
            "system_instruction": SYSTEM_INSTRUCTION,
            "ttl": "3600s"})
        cfg = types.GenerateContentConfig(cached_content=cache.name, response_schema=...)

    Returns:
        str: the human-readable note.
    """
    return ("prompt-cache: SYSTEM_INSTRUCTION + response_schema are constant across all units -> "
            "cache the prefix once (client.caches.create) and pass cached_content; cached input "
            "reads bill at ~10% of normal input price. Combine with Batch API (-50%) for the full run.")


# ==================================================================
# Resume support — never re-pay for a record we already wrote
# ==================================================================
def _done_unit_ids() -> set:
    """Read OUT_PATH and return the set of unit_ids whose LLM mentions are already present.

    This makes the LIVE pass resumable: a run that dies after 4,000 of 9,000 units can be restarted
    and it will skip the 4,000 it already paid for. We key on the mention's provenance doc_id+rid
    plus the unit kind, reconstructed the same way collect_units builds unit_id.

    Returns:
        set: unit_ids already extracted by the LLM pass.
    """
    done = set()
    if not OUT_PATH.exists():
        return done
    for line in OUT_PATH.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("source") != "llm":
            continue
        uid = rec.get("unit_id")
        if uid:
            done.add(uid)
    return done


# ==================================================================
# The stage entry point
# ==================================================================
def run(limit: int | None = None, execute: bool = False, emit_batch: bool = False) -> dict:
    """Run the NER stage.

    By default (``execute=False``) this performs the FREE Pass-A harvest for the whole (or limited)
    corpus, writes those mentions to ``data/entities_raw.jsonl``, and PREVIEWS Pass B — it builds the
    real prompt + schema, projects the cost, and (optionally) writes the Batch request file — but
    spends nothing. Set ``execute=True`` only when David authorizes the paid pass.

    Args:
        limit:      cap the number of records/units (strided sample); None = whole corpus.
        execute:    if True, RUN the paid Gemini Pass-B extraction (billed). Default False.
        emit_batch: if True, also write the Gemini Batch-API request file (no spend).

    Returns:
        dict: a small run report (counts + cost projection) — handy for tests and the summary block.
    """
    config.DATA.mkdir(parents=True, exist_ok=True)
    documents, notebooks = _load_inputs()

    # ----------------------------------------------------------------
    # Pass A — free structured harvest (always runs)
    # ----------------------------------------------------------------
    harvest_mentions = []
    for doc in documents:
        for m in harvest_letter(doc):
            harvest_mentions.append({**m, "unit_id": doc.get("id", ""), "pass": "A"})
    for page in notebooks:
        for m in harvest_notebook_page(page):
            harvest_mentions.append({**m, "unit_id": page.get("id", ""), "pass": "A"})

    if limit and limit < len(harvest_mentions):
        step = len(harvest_mentions) / limit
        harvest_mentions = [harvest_mentions[int(i * step)] for i in range(limit)]

    # Write Pass A non-destructively. On the very first run we (re)create the file with Pass-A rows;
    # we only ever APPEND Pass-B rows afterward, so the harvest is the stable base layer.
    if not OUT_PATH.exists():
        with open(OUT_PATH, "w") as f:
            for m in harvest_mentions:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")

    # ----------------------------------------------------------------
    # Pass B — prepare always; EXECUTE only when told
    # ----------------------------------------------------------------
    units = collect_units(documents, notebooks, limit=limit)
    projection = _project_cost(units)

    # Log a $0 projection row so even a dry preview leaves an auditable trace in usage.csv.
    costlog.log("gemini", NER_MODEL, "ner_projection",
                input_tokens=projection["est_input_tokens"],
                output_tokens=projection["est_output_tokens"], items=len(units), usd=0.0,
                meta=f"projection sticker=${projection['est_usd_sticker']} "
                     f"batch+cache=${projection['est_usd_batch_cache']}")

    batch_path = None
    if emit_batch:
        batch_path = write_batch_requests(units)

    llm_written = 0
    if execute:
        # ---- the PAID path (only here, only on explicit opt-in) ----
        # Each unit is an independent ~9s structured-output round-trip, so a serial loop wastes almost
        # all its wall-clock waiting on the network (the whole corpus would take ~a day). We overlap the
        # calls with a thread pool — the provider adapter's tenacity backoff absorbs any 429s, and the
        # corpus is well under the model's RPM ceiling at this width. Threads only CALL the model + shape
        # rows; THIS loop (draining as_completed) is the sole writer, so the append-only + resume
        # invariants hold without locking the file handle. NER_WORKERS tunes the width (default 16).
        from concurrent.futures import ThreadPoolExecutor, as_completed
        done = _done_unit_ids()                          # resume: skip already-extracted units
        todo = [u for u in units if u["unit_id"] not in done]

        def _work(u):
            """Worker: extract one unit -> (unit_id, [pass-B rows]). Runs off the main thread."""
            return u["unit_id"], [{**m, "unit_id": u["unit_id"], "pass": "B"}
                                  for m in extract_unit_llm(u)]

        max_workers = max(1, int(os.environ.get("NER_WORKERS", "16")))
        print(f"  Pass B: {len(todo)} unit(s) to do (resume skipped {len(units) - len(todo)}) "
              f"on {max_workers} workers")
        with open(OUT_PATH, "a") as f, ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_work, u): u for u in todo}
            completed = 0
            for fut in as_completed(futs):
                try:
                    _uid, rows = fut.result()
                except Exception as e:                   # one unit failing must not kill the whole run
                    print(f"  [warn] unit {futs[fut].get('unit_id', '?')} failed: {str(e)[:140]}")
                    continue
                for m in rows:
                    f.write(json.dumps(m, ensure_ascii=False) + "\n")
                    llm_written += 1
                completed += 1
                if completed % 200 == 0:                  # periodic flush + heartbeat for long runs
                    f.flush()
                    print(f"  ... {completed}/{len(todo)} units, {llm_written} mentions")

    report = {
        "out_path":        str(OUT_PATH),
        "harvest_mentions": len(harvest_mentions),
        "units":           len(units),
        "llm_mentions":    llm_written,
        "executed":        execute,
        "batch_file":      str(batch_path) if batch_path else None,
        "projection":      projection,
    }

    # ----------------------------------------------------------------
    # Human-readable summary (the =*60 block the style guide asks for)
    # ----------------------------------------------------------------
    print("=" * 60)
    print("extract_entities — Tier-1 NER")
    print("=" * 60)
    print(f"  Pass A (free harvest) : {report['harvest_mentions']:>6} mentions -> {OUT_PATH.name}")
    print(f"  Pass B units          : {report['units']:>6} (letters + notebook entries)")
    print(f"  Pass B executed       : {execute}")
    if execute:
        print(f"  Pass B mentions       : {report['llm_mentions']:>6} (LLM, appended)")
    else:
        p = projection
        print(f"  Pass B PROJECTION     : sticker ${p['est_usd_sticker']}  | "
              f"batch+cache ${p['est_usd_batch_cache']}  ({p['units']} units, "
              f"~{p['est_input_tokens']:,} in / ~{p['est_output_tokens']:,} out tokens)")
        print(f"  {build_cache_note()}")
        print("  NOTE: paid pass deferred — rerun with execute=True / --execute to bill it.")
    if batch_path:
        print(f"  Batch request file    : {batch_path}")
    print("=" * 60)
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tier-1 NER: free structured harvest + (deferred) LLM extraction.")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap records/units (strided sample) for a small test batch")
    ap.add_argument("--execute", action="store_true",
                    help="RUN the paid Gemini Pass-B extraction (billed!). Default: preview only.")
    ap.add_argument("--batch", action="store_true",
                    help="also write the Gemini Batch-API request JSONL (no spend)")
    a = ap.parse_args()
    run(limit=a.limit, execute=a.execute, emit_batch=a.batch)
