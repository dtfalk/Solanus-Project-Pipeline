"""app/tools/temporal_query.py — find records in a date range (the favors/correspondence timeline).

The corpus is, at heart, a *chronicle*: letters dated across decades and a notebook register of
favors with enrollment and report dates. ``stages/normalize_dates.py`` already turned the messy
surface dates ("Mar. 30", "c. 1945", a year hiding in the page's archival field) into canonical
**EDTF** values in ``data/dates.json``. This tool reads those and answers the *when* questions —
"what was written in 1933?", "favors reported between 1940 and 1942?" — that neither a similarity
search nor a graph hop handles cleanly.

How it works (all local, free, no model):
  1. Parse the requested window (``year``, or ``start``/``end`` as years or full EDTF) into a numeric
     [lo, hi] year span.
  2. For every record with a normalized date, extract the year(s) its EDTF covers (a point like
     ``1933-11-08``, a partial like ``1933-10-XX``, or a range like ``1940-02/1940-10`` — each maps
     to one or more years), and keep the records whose span overlaps the window.
  3. Return them **sorted chronologically**, each carrying its EDTF, the bitemporal
     enrolled/reported split when present, the raw date string, and citable provenance (doc_id / rid /
     page). That ordered, dated, cited list is exactly what the agent needs to *build a timeline*.

If ``dates.json`` isn't built yet we return an empty, well-formed result (the agent can fall back to
``vector_search`` with a year in the query) rather than erroring.
"""
from __future__ import annotations

# ==================================================================
# Imports — base FIRST (wires step_7 onto sys.path), then stdlib + step_7
# ==================================================================
import json
import re
from pathlib import Path

from . import base                            # importing base sets up the step_7 import path
import config                                 # step_7 paths (resolves via base)


# ==================================================================
# TOOL_SPEC — name / description / JSON-Schema params the LLM sees
# ==================================================================
TOOL_SPEC = {
    "name": "temporal_query",
    "description": (
        "Find archive records dated within a year window, returned in chronological order — the tool "
        "for building timelines and answering 'when' / 'what happened in YEAR' / 'between YEAR and "
        "YEAR' questions. Uses normalized EDTF dates and surfaces the bitemporal enrolled-vs-reported "
        "split for favors. Each record carries its date and a citable source region."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "year": {"type": "integer", "description": "A single year to match (e.g. 1933)."},
            "start": {"type": "string",
                      "description": "Window start as a year or EDTF (e.g. '1940'); omit for open lower bound."},
            "end": {"type": "string",
                    "description": "Window end as a year or EDTF (e.g. '1942'); omit for open upper bound."},
            "kind": {"type": "string", "enum": ["letter", "notebook_entry"],
                     "description": "Optional filter to letters or notebook entries. ('notebook' is "
                                    "accepted as an alias for 'notebook_entry'.)"},
            "max_results": {"type": "integer", "description": "Max records to return (default 30)."},
        },
        "required": [],
    },
}


# ==================================================================
# Loading dates.json (cached by mtime)
# ==================================================================
_CACHE: dict = {"mtime": None, "dates": None}


def _load_dates() -> dict:
    """Read ``data/dates.json`` (a ``{record_key -> date_record}`` map), cached by mtime."""
    path: Path = config.DATA / "dates.json"
    if not path.exists() or path.stat().st_size == 0:
        return {}
    mtime = path.stat().st_mtime
    if _CACHE["mtime"] != mtime:
        _CACHE["dates"] = json.loads(path.read_text())
        _CACHE["mtime"] = mtime
    return _CACHE["dates"] or {}


# ==================================================================
# EDTF -> year span. We only need YEAR granularity for windowing, which keeps
# the parser tiny and robust to all the partial/approximate shapes EDTF allows.
# ==================================================================
_YEAR_RE = re.compile(r"(1[89]\d\d|20\d\d)")             # 4-digit years in the corpus's plausible range


def _edtf_year_span(edtf: str | None):
    """Return the inclusive (min_year, max_year) an EDTF literal covers, or None if undatable.

    EDTF shapes we handle, all at year resolution:
      • point/partial: ``1933``, ``1933-10``, ``1933-10-XX``, ``1945~`` (circa) → that year.
      • range:         ``1940-02/1940-10`` or ``1897/1901`` (a ``/`` splits start/end) → min..max of
        the two ends' years.
    We deliberately ignore qualifiers (``~`` approximate, ``?`` uncertain, ``X`` unknown digit) for
    *windowing* — an approximate 1945 still belongs to 1945's bucket. The raw string + confidence are
    carried through untouched so the agent/UI can still show the nuance.

    Args:
        edtf: An EDTF literal (from normalize_dates), e.g. ``"1933-11-08"`` or ``"1940-02/1940-10"``.

    Returns:
        ``(lo, hi)`` inclusive year span, or None if no parseable year is present.
    """
    if not edtf:
        return None
    parts = edtf.split("/") if "/" in edtf else [edtf]    # a range is "start/end"
    years: list = []
    for p in parts:
        m = _YEAR_RE.search(p)
        if m:
            years.append(int(m.group(1)))
    if not years:
        return None
    return min(years), max(years)


def _parse_window(year, start, end):
    """Turn the request's year / start / end into a numeric [lo, hi] year window (inclusive).

    Accepts ``year`` (single year), or ``start``/``end`` given as a year (int or "1933") or any EDTF
    string (we extract its year). Missing bounds open the window (year 0 .. 9999) so "everything
    before 1940" or "from 1933 on" both work.
    """
    if year is not None:
        return int(year), int(year)

    def _bound(v, default):
        if v is None or v == "":
            return default
        if isinstance(v, int):
            return v
        m = _YEAR_RE.search(str(v))
        return int(m.group(1)) if m else default

    lo = _bound(start, 0)
    hi = _bound(end, 9999)
    return (lo, hi) if lo <= hi else (hi, lo)             # tolerate a swapped pair


# ==================================================================
# run — the single args-dict entry point (the tool's source of truth)
# ==================================================================
def run(args: dict) -> dict:
    """Return records whose normalized (EDTF) date overlaps a year window, sorted chronologically.

    Args:
        args: Parsed tool arguments matching :data:`TOOL_SPEC`:
            ``year`` (single year shortcut for start==end==year), ``start`` / ``end`` (window bounds
            given as a year or EDTF string — omit either to open that bound), ``kind`` (optional
            "letter" | "notebook" filter on the record kind normalize_dates tagged), and
            ``max_results`` (cap after chronological sort, default 30).

    Returns:
        ``{"window": [lo, hi], "available": bool, "count": int, "records": [...]}`` where each record
        is ``{key, doc_id, kind, edtf, year_span, raw, enrolled, reported, confidence, circa,
        provenance:{doc_id, rid, page}}`` — dated, ordered, and citable. ``available`` is False with
        empty records if dates.json hasn't been built.
    """
    year        = args.get("year")
    start       = args.get("start")
    end         = args.get("end")
    kind        = args.get("kind")
    max_results = args.get("max_results", 30)

    dates = _load_dates()
    lo, hi = _parse_window(year, start, end)
    if not dates:
        return {"window": [lo, hi], "available": False, "count": 0, "records": [],
                "note": "data/dates.json not built yet — run stages/normalize_dates.py to enable "
                        "temporal queries."}

    # normalize_dates tags entry records as "notebook_entry"; accept the friendlier "notebook" the LLM
    # is likely to reach for (and which the previous enum advertised) as an alias so neither spelling
    # silently returns nothing.
    want_kind = (kind or "").strip().lower() or None
    if want_kind == "notebook":
        want_kind = "notebook_entry"
    matches = []
    for key, rec in dates.items():
        if want_kind and (rec.get("kind") or "").lower() != want_kind:
            continue
        span = _edtf_year_span(rec.get("edtf"))
        if span is None:
            continue
        # overlap test: the record's [a, b] intersects the window [lo, hi].
        a, b = span
        if a > hi or b < lo:
            continue
        doc_id = rec.get("doc_id") or key.split("::", 1)[0]
        # dates.json stores `rid` as the COMPOSITE entry id ("{page_id}::{region_rid}") for notebook
        # entries, and the doc_id for letters. The citation layer (mirroring lib.retrieval) wants the
        # BARE region rid so doc_id + rid resolves to the region's vertices; the part after '::' is that
        # bare rid. Letters have no single region → rid is None (page-level citation).
        bare_rid = key.split("::", 1)[1] if "::" in key else None
        matches.append({
            "key":        key,
            "doc_id":     doc_id,
            "rid":        bare_rid,
            "kind":       rec.get("kind"),
            "edtf":       rec.get("edtf"),
            "year_span":  [a, b],
            "raw":        rec.get("raw"),
            "enrolled":   rec.get("enrolled"),         # bitemporal: enrollment date (favors)
            "reported":   rec.get("reported"),         # bitemporal: report/outcome date (favors)
            "confidence": rec.get("confidence"),
            "circa":      rec.get("circa"),
            # citation-ready provenance in the project-standard shape (base.provenance), so a temporal
            # hit cites identically to a vector/entity/graph hit.
            "provenance": base.provenance(doc_id=doc_id, rid=bare_rid, edtf=rec.get("edtf"),
                                          date_raw=rec.get("raw"), archv_date=rec.get("archv_date"),
                                          precision=rec.get("precision"), circa=rec.get("circa")),
        })

    # Sort chronologically by the start of each record's span (then by key for a stable order).
    matches.sort(key=lambda r: (r["year_span"][0], r["year_span"][1], r["key"]))
    total = len(matches)
    return {"window": [lo, hi], "available": True, "count": total,
            "records": matches[: max(1, int(max_results))]}


# ==================================================================
# Register the tool (runs at import time -> app.tools picks it up)
# ==================================================================
# default_on=True: temporal queries only need data/dates.json (already produced by normalize_dates),
# and the tool is fully free + local — so, like vector_search/pdf_fulltext_search/entity_lookup whose
# artifacts also exist, it starts ON. (graph_query/community_summary stay OFF because their graph.json
# artifact isn't built yet — an honest UI offers a switch only when it can actually fire.)
# tool_from_module keeps TOOL_SPEC + run() as the single source of truth.
TOOL = base.tool_from_module(
    TOOL_SPEC, run,
    default_on=True,
    cost_note="free, local (reads data/dates.json EDTF values)",
)
