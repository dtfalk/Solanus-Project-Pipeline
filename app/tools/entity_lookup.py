"""app/tools/entity_lookup.py — look up a resolved person/place/organization by any of its names.

The ``resolve_entities`` stage clusters the corpus's messy surface forms ("Grace.", "Mrs. Panyard",
an O'Donnell OCR variant) into ONE canonical authority record per real-world entity, with every
mention's provenance attached (``data/entities.json``). This tool lets the agent ask "who/what is
*X*?" and get back that canonical record — the canonical name, the alias variants, any attributes,
and the list of mentions (each with doc_id / rid / page) it can then cite or hand to ``graph_query``.

Why the agent wants this as a distinct tool (instead of just searching text):

  * **Disambiguation.** "Grace" might be three different people; the resolved record collapses the
    *right* variants together and keeps the others apart, so the agent reasons over a person, not a
    string.
  * **Recall across spellings.** Matching on the normalized + phonetic key means a query for
    "O'Donnell" finds the cluster even if a given page OCR'd it "ODonnel" — something a literal
    search would miss.
  * **A jumping-off point.** Each returned mention carries provenance, so the agent can pivot to
    ``vector_search`` for the surrounding text or ``graph_query`` for relationships.

Pure local JSON lookup — no model call, no network, free. If ``entities.json`` isn't built yet it
returns an empty, well-formed result (the agent falls back to text search) rather than erroring.
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (wires step_7 onto sys.path), then stdlib + config
# ==================================================================
import json
import re
import unicodedata
from pathlib import Path

from . import base                            # importing base sets up the step_7 import path
import config                                 # step_7 paths (resolves via base's sys.path wiring)


# ==================================================================
# TOOL_SPEC — name / description / JSON-Schema params the LLM sees
# ==================================================================
TOOL_SPEC = {
    "name": "entity_lookup",
    "description": (
        "Resolve a person, place, or organization by any spelling of its name to a single canonical "
        "record: its preferred name, alias variants, attributes, and every mention (each with a "
        "citable source region). Use when the user names someone/somewhere and you need to "
        "disambiguate variants or find all places they appear. Forgiving of titles, accents, and OCR "
        "spelling differences."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "The name to resolve (any surface form, e.g. 'Mrs. O'Donnell', "
                               "'Grace Panyard', 'Seraphic Mass Association').",
            },
            "type": {
                "type": "string",
                "enum": ["person", "place", "organization"],
                "description": "Optional filter to a specific entity type.",
            },
            "max_results": {
                "type": "integer",
                "description": "How many candidate entities to return (default 3).",
            },
        },
        "required": ["name"],
    },
}


# ==================================================================
# Loading + light caching of the entities authority file
# ==================================================================
# We cache the parsed entities in-process (the file is small — ~hundreds of records) so repeated
# lookups in one agent turn don't re-read/parse the JSON. The cache is keyed on the file's mtime so
# that if ``resolve_entities`` re-runs and rewrites the file, the next lookup transparently picks up
# the new version instead of serving stale data.
_CACHE: dict = {"mtime": None, "entities": []}


def _load_entities() -> list:
    """Read ``data/entities.json`` (tolerating the ``{"entities": [...]}`` wrapper), cached by mtime.

    Returns:
        A list of canonical entity dicts (possibly empty if the file is absent/stub).
    """
    path: Path = config.DATA / "entities.json"
    if not path.exists() or path.stat().st_size == 0:
        return []
    mtime = path.stat().st_mtime
    if _CACHE["mtime"] != mtime:                          # file changed (or first read) -> reload
        raw = json.loads(path.read_text())
        if isinstance(raw, dict):                         # resolve_entities may wrap the list
            raw = raw.get("entities", [])
        _CACHE["mtime"] = mtime
        _CACHE["entities"] = raw or []
    return _CACHE["entities"]


# ==================================================================
# Matching — normalize names so "Mrs. O'Donnell" finds the "O'Donnell" cluster
# ==================================================================
def _normalize(s: str) -> str:
    """Casefold + strip accents/punctuation/titles to a bag-of-words key for fuzzy name matching.

    This is a deliberately small echo of resolve_entities' own normalization: we lower-case, drop
    accents (so "Panyàrd" == "Panyard"), remove common honorifics, and keep only word characters.
    It's enough to make membership tests ("does the query appear in this entity's names?") forgiving
    of the surface noise the OCR + period style introduces, without re-importing the heavy resolver.

    Args:
        s: A raw name string (query or a stored variant).

    Returns:
        A normalized, space-joined token string (e.g. "grace panyard").
    """
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"\b(mr|mrs|ms|miss|rev|fr|father|sr|sister|br|brother|dr|msgr|very)\.?\b", " ", s)
    toks = re.findall(r"[a-z0-9']+", s)
    return " ".join(toks)


def _names_of(ent: dict) -> list[str]:
    """All searchable surface names for an entity: its canonical name + every variant + surfaces."""
    names = [ent.get("canonical_name") or ent.get("name") or ""]
    names += list(ent.get("variants") or [])
    # mention surfaces are another place a queried spelling might live.
    for m in ent.get("mentions", []):
        if m.get("surface"):
            names.append(m["surface"])
    return [n for n in names if n]


def _score(query_norm: str, ent: dict) -> float:
    """Score how well a normalized query matches an entity (0 = no match, higher = better).

    Scoring is simple and explainable (this is a dev tool — a reviewer should be able to predict it):
      • exact normalized match of canonical/variant  -> strong (2.0)
      • query is a substring of a name, or vice versa -> medium (1.0), scaled by length overlap
      • token overlap (shared words)                  -> weak  (up to 0.5)
    We add a tiny tie-breaker for more-mentioned entities so the better-attested cluster wins ties.
    """
    best = 0.0
    qtoks = set(query_norm.split())
    for name in _names_of(ent):
        n = _normalize(name)
        if not n:
            continue
        if n == query_norm:
            best = max(best, 2.0)
        elif query_norm and (query_norm in n or n in query_norm):
            # substring hit, weighted by how much of the longer string is covered.
            cover = len(query_norm) / max(len(n), len(query_norm), 1)
            best = max(best, 1.0 * cover)
        else:
            shared = qtoks & set(n.split())
            if shared:
                best = max(best, 0.5 * len(shared) / max(len(qtoks), 1))
    # tiny boost (≤0.05) for better-attested entities, only as a tie-breaker.
    return best + min(0.05, len(ent.get("mentions", [])) / 1000.0) if best > 0 else 0.0


# ==================================================================
# The implementation — name -> canonical record(s) + cited mentions
# ==================================================================
def run(args: dict) -> dict:
    """Resolve a name to canonical entity record(s) with alias variants and cited mentions.

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`:
            ``name`` (required, any surface form — titles and accents are ignored), ``type`` (optional
            "person" | "place" | "organization"; coarse resolver labels like PERSON/ORG/LOC are folded
            automatically), ``max_results`` (default 3), and ``max_mentions`` (per-entity cap on the
            citable mentions returned, default 12).

    Returns:
        ``{"query": str, "available": bool, "matches": [...]}`` where each match is
        ``{id, type, canonical_name, variants, attrs, mention_count, mentions:[{provenance...}]}``.
        ``available`` is False with empty matches if entities.json hasn't been built yet.
    """
    name         = (args.get("name") or "").strip()
    type         = args.get("type")
    max_results  = args.get("max_results", 3)
    max_mentions = args.get("max_mentions", 12)
    if not name:
        return {"query": "", "available": True, "matches": [], "note": "empty name"}

    ents = _load_entities()
    if not ents:
        return {"query": name, "available": False, "matches": [],
                "note": "data/entities.json not built yet — run stages/resolve_entities.py to enable "
                        "entity lookup."}

    q = _normalize(name)
    want_type = (type or "").strip().lower() or None
    type_aliases = {"per": "person", "loc": "place", "location": "place", "gpe": "place",
                    "org": "organization", "corporatebody": "organization"}

    scored = []
    for ent in ents:
        etype = (ent.get("type") or "").strip().lower()
        etype = type_aliases.get(etype, etype)
        if want_type and etype != want_type:
            continue
        s = _score(q, ent)
        if s > 0:
            scored.append((s, etype, ent))
    scored.sort(key=lambda t: t[0], reverse=True)

    matches = []
    for s, etype, ent in scored[: max(1, int(max_results))]:
        # Each mention becomes a citable hit-like record: carry its provenance verbatim so the agent
        # can cite it or feed doc_id/rid back into vector_search / the source modal.
        mentions = []
        for m in ent.get("mentions", [])[: max(1, int(max_mentions))]:
            prov = m.get("provenance", m)             # tolerate nested or flat provenance
            mentions.append({
                "surface":    m.get("surface"),
                "provenance": {
                    "doc_id":   prov.get("doc_id"),
                    "rid":      prov.get("rid") or None,
                    "page":     prov.get("page"),
                    "pdf_page": prov.get("pdf_page"),
                    "date":     prov.get("date"),
                },
            })
        matches.append({
            "id":             ent.get("id"),
            "type":           etype,
            "canonical_name": ent.get("canonical_name") or ent.get("name"),
            "variants":       ent.get("variants", []),
            "attrs":          ent.get("attrs", {}),
            "authority":      ent.get("authority", {}),   # Wikidata/VIAF/GeoNames ids once reconciled
            "mention_count":  len(ent.get("mentions", [])),
            "match_score":    round(s, 3),
            "mentions":       mentions,
        })
    return {"query": name, "available": True, "matches": matches}


# ==================================================================
# Register the tool
# ==================================================================
TOOL = base.tool_from_module(TOOL_SPEC, run, default_on=True,
                             cost_note="free, local (entities.json lookup)")
