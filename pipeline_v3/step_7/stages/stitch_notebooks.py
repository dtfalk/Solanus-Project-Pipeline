"""stages/stitch_notebooks.py — Tier-1 A1: cross-page notebook continuation ("stitching").

================================================================================
WHY this stage exists (the corpus problem, in plain language)
================================================================================
Fr. Solanus's notebooks are a *running register*. He kept writing favors one after
another, and when he ran out of room on a page he simply kept going on the next
one. The archive even labels these spill-over pages literally: a page will say
"Page 1 Cont." meaning "this is still page 1, continued." A single thought — one
person's favor — can therefore be split across the bottom of one page and the top
of the next, and the *date* for that thought might only be written once, several
entries earlier.

If we feed those raw fragments to retrieval as-is, we get half-sentences with no
date and no context ("...continued for a year --- Reports Dec. 8th"). The reader
(and the embedding model) never sees the *whole* favor. So before any retrieval or
knowledge-graph work, we reconstruct the **logical entry**: the complete unit of
meaning, made of one or more raw fragments, with its date/notebook context carried
forward.

This is the first structural-truth stage (RESEARCH_PLAN.md, PART A → A1). It is the
proven pattern from historical patent/register digitization: order the pages, flag
the truncated tails and orphan heads, join them, and (optionally) let a multimodal
LLM adjudicate the genuinely ambiguous joins by *looking at the two page images*.

================================================================================
WHAT it produces (non-destructive — a brand-new artifact)
================================================================================
data/stitched_notebooks.json :
  - "logical_entries": each a reconstructed unit with
        logical_entry_id, joined text, carried-forward date + notebook context,
        member fragments [{doc_id, rid, page, pdf_page, vertices, min_conf, ...}].
  - "relations": a flat edge list for the KG, of two kinds:
        * continued_from / continues_on — ENTRY-level: the *thought itself* was split
          across the page break, so the fragments are fused into one logical entry;
        * page_continues_on — PAGE-level: a labelled "Page N Cont." page continues the
          register, but the two favors are distinct, so they are NOT fused (we keep the
          navigational edge without inventing a false merge of two people's stories).
  - "stats" + "config": provenance so the diff-and-rerun DAG can hash this stage.

We NEVER modify notebooks.json. Fragments keep their exact rids + vertices, so a
citation can still deep-zoom to the precise region while retrieval sees the whole
logical entry. That is the "keep both" rule from the research plan.

================================================================================
THE HARD RULE about the LLM
================================================================================
There is an OPTIONAL multimodal-LLM confirmation step for *ambiguous* joins
(Gemini vision over the pair of page images, routed through lib.providers.llm and
cost-logged). It is GATED behind `use_llm=False` and is NOT invoked in run(). It
exists, documented and ready, but costs nothing until David flips the flag.
"""
from __future__ import annotations

# ----------------------------------------------------------------- Core Python Imports
import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

# ----------------------------------------------------------------- Local File Imports
# Same sys.path bootstrap every step_7 module uses: make the step_7 root importable so
# `import config` and `from lib import ...` resolve no matter where we're run from.
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config            # noqa: E402  (paths + model registry)
from lib import costlog  # noqa: E402  (always-on cost logging)


# ==================================================================
# Tunables — the heuristic "dials"
# ==================================================================
# These live up top so a reviewer can see (and tweak) every threshold that decides a
# join. They're intentionally conservative: we'd rather UNDER-stitch (leave two
# fragments separate) than wrongly glue two unrelated favors together, because a
# false merge corrupts a logical entry while a false split just misses a link David
# can recover later via KG link-prediction (PLAN A1 step 6).
CONT_PATTERN = re.compile(r"\bcont", re.I)          # "Cont.", "Continued", "cont'd" in a page_label

# A tail looks TRUNCATED if it does NOT end on a sentence terminator. We treat the
# usual western terminators + the dashes Solanus loved as "finished"; anything else
# (a bare word, a hyphenated word-break, a comma) reads as "more is coming".
TERMINAL_PUNCT = tuple(".!?…")                  # . ! ? …
SOFT_TERMINAL = tuple("-–—")              # - – —  (Solanus ends many finished entries on a dash)

# A head looks like an ORPHAN-START (the back half of a split thought) if it begins
# lower-case, or with a connective/closing token, i.e. it reads like a continuation
# rather than the start of a fresh favor (which usually starts with a Name/Capital).
ORPHAN_HEAD_TOKENS = (
    "and", "but", "or", "nor", "so", "for", "yet",          # coordinating conjunctions
    "because", "although", "though", "while", "when", "where",
    "who", "which", "that", "with", "to", "of", "in", "on", "at",
    "reports", "reported", "enrolled", "continued",          # register-specific connectives
)


# ==================================================================
# Step 1 — Load notebooks.json and index regions for provenance
# ==================================================================
def _load_pages() -> list[dict]:
    """Read notebooks.json (the raw register pages).

    Returns:
        The list of page records exactly as step_6 produced them — untouched.
    """
    return json.loads(config.NOTEBOOKS.read_text())


def _region_index(page: dict) -> dict[str, dict]:
    """Map a page's region rid -> its geometry record, so we can attach exact
    `vertices` + `min_conf` to every fragment we cite.

    Why: an *entry* carries text + date + page_label, but the pixel geometry
    (the box on the scan) lives in the page's `regions[]`. We verified that every
    entry rid is also a region rid, so this join is total — but we default safely
    just in case a future page drifts.

    Args:
        page: one page record from notebooks.json.

    Returns:
        dict rid -> {"vertices": [...], "min_conf": float, "category": str}.
    """
    idx: dict[str, dict] = {}
    for r in page.get("regions", []):
        idx[r["rid"]] = {
            "vertices": r.get("vertices"),
            "min_conf": r.get("min_conf"),
            "category": r.get("category"),
        }
    return idx


# ==================================================================
# Step 2 — Order pages, then entries (reading order)
# ==================================================================
def _notebook_number(notebook: str):
    """Recover the notebook *number* from a noisy OCR'd header string.

    This matters more than it looks. The `notebook` field is read off the top of each
    scan, and the OCR is wildly inconsistent — we see "FR. SOLANUS, NOTEBOOK NO. 5.",
    "FR. SOLANUS, NOTEBOCK NO. S.", "FR. SOLNAUS, NOTEBOOK NO. 5.", "FATHER SOLANUS
    NOTEBOOK NO. 6.", and dozens more spellings of the SAME physical book. If we
    grouped registers on the raw string, one notebook would shatter into 50+
    micro-registers and we'd never test the page-break joins between consecutive pages
    (we measured 154 adjacent page-pairs broken this way). So we throw away the noisy
    prose and keep only the stable signal: the book number.

    We also rescue the common OCR confusion where "5" is read as the letter "S".

    Args:
        notebook: the raw notebook header string (may be blank).

    Returns:
        The notebook number as int if we can read one, else None (caller falls back to
        section-only grouping rather than trusting the noisy string).
    """
    s = (notebook or "")
    # "NO. 5", "NO. S" (S->5), "Number 12", "No.6." — grab the token right after NO/Number.
    m = re.search(r"\b(?:no|number|num)\.?\s*([0-9]{1,3}|[sS])\b", s, re.I)
    if not m:
        m = re.search(r"\b([0-9]{1,3})\b", s)     # last resort: any small number in the header
    if not m:
        return None
    tok = m.group(1)
    if tok in ("s", "S"):
        return 5                                  # OCR almost always means "5" here
    return int(tok)


def _group_key(page: dict) -> tuple:
    """The grouping that defines "one notebook's running register".

    Continuation never crosses a *section* (Volume_1…4, Appendix_*) — a "Cont." page
    always belongs to the same physical book — so `section` is the reliable backbone.
    We refine with the *normalized notebook number* (not the raw, OCR-noisy header
    string) when we can parse one; if we can't, we fall back to section-only so OCR
    noise can never fragment a register and silently drop real joins.

    Args:
        page: a page record.

    Returns:
        A hashable key (section, notebook_number_or_None) bucketing pages into
        independent registers.
    """
    return (page.get("section", ""), _notebook_number(page.get("notebook", "")))


def _ordered_pages(pages: list[dict]) -> list[tuple[tuple, list[dict]]]:
    """Bucket pages by register, then sort each bucket into physical reading order.

    Physical order = `pdf_page_number` (the true scan order). We fall back to
    `page_number_in_type` then id so the sort is always total and deterministic
    (idempotency requirement — re-running yields the identical artifact).

    Args:
        pages: all page records.

    Returns:
        A list of (group_key, [pages in reading order]) — itself ordered by key so
        the output file is stable across runs.
    """
    buckets: dict[tuple, list[dict]] = {}
    for pg in pages:
        buckets.setdefault(_group_key(pg), []).append(pg)

    def _page_sort(pg: dict) -> tuple:
        return (
            pg.get("pdf_page_number") if pg.get("pdf_page_number") is not None else 1_000_000,
            pg.get("page_number_in_type") if pg.get("page_number_in_type") is not None else 1_000_000,
            pg.get("id", ""),
        )

    # The group key's 2nd element (notebook number) can be None when the OCR'd header
    # was unparseable; sort it last so None and ints never get compared directly.
    def _key_sort(key: tuple) -> tuple:
        section, nb_no = key
        return (section, nb_no is None, nb_no if nb_no is not None else 0)

    ordered = []
    for key in sorted(buckets, key=_key_sort):
        ordered.append((key, sorted(buckets[key], key=_page_sort)))
    return ordered


# ==================================================================
# Step 3 — Truncation / continuation heuristics (the cheap signals)
# ==================================================================
def _page_label_number(page_label: str):
    """Extract the integer page number from a label like "Page 28 Cont." -> 28.

    Why we want it: the archive's "Page N" / "Page N Cont." pair is the single
    strongest continuation signal in this corpus. When the first entry of a page
    is labelled "Page N Cont." we know with near-certainty it continues the
    register that was on "Page N".

    Args:
        page_label: the raw label string (may be empty).

    Returns:
        The page number as int, or None if the label has no number.
    """
    m = re.search(r"(\d+)", page_label or "")
    return int(m.group(1)) if m else None


def _is_cont_label(page_label: str) -> bool:
    """True if a page_label is explicitly marked continued ("... Cont.")."""
    return bool(CONT_PATTERN.search(page_label or ""))


def _looks_truncated_tail(text: str) -> bool:
    """Does this entry's text read like it was cut off mid-thought (more on next page)?

    The signal is the *ending*. A finished register entry ends on a terminator
    (. ! ? …) or, very commonly for Solanus, on a dash. If instead it ends on a
    bare word, a comma, or a hyphenated word-break ("As- "), the thought spills over.

    Args:
        text: the fragment text.

    Returns:
        True if the tail looks truncated (a candidate to be *continued onto* the
        next page).
    """
    t = (text or "").rstrip()
    if not t:
        return False
    # A trailing hyphen on the LAST token (e.g. "As-") is an OCR'd word-break: the
    # word literally continues. That's a strong truncation signal, so check it first
    # before we treat a dash as a "soft terminal".
    last_token = t.split()[-1] if t.split() else ""
    if last_token.endswith("-") and len(last_token) > 1 and last_token[-2].isalpha():
        return True
    if t.endswith(TERMINAL_PUNCT):
        return False
    if t.endswith(SOFT_TERMINAL):
        return False                              # a clean dash ending = a finished entry
    return True                                   # ends on a bare word/comma -> truncated


def _looks_orphan_head(text: str) -> bool:
    """Does this entry's text read like the *back half* of a split thought?

    A fresh favor almost always opens with a proper noun (a Capitalised name:
    "Mary Briggi -", "Wm Rhead."). A continuation, by contrast, tends to open
    lower-case or with a connective ("and ...", "Reports ...", "to-day ..."). We use
    that asymmetry as the orphan-start signal.

    Args:
        text: the fragment text.

    Returns:
        True if the head looks like a continuation rather than a new entry.
    """
    t = (text or "").lstrip()
    if not t:
        return False
    first = t.split()[0].strip(".,;:-")
    if not first:
        return False
    if first[0].islower():
        return True                               # opens lower-case -> mid-thought
    if first.lower() in ORPHAN_HEAD_TOKENS:
        return True                               # opens on a connective -> mid-thought
    return False


def _join_confidence(tail_truncated: bool, head_orphan: bool, cont_label: bool,
                     label_chain: bool) -> tuple[float, str]:
    """Combine the cheap signals into a single confidence + a human-readable reason.

    This is deliberately a transparent, additive rubric (not a black box) so David
    can audit *why* any two fragments were joined. The weights encode our priors:
      - an explicit "Page N Cont." label is the most trustworthy single signal,
      - a numbered "Page N -> Page N Cont." chain is near-certain,
      - text truncation/orphan-head signals corroborate but don't decide alone.

    Args:
        tail_truncated: the previous-page tail looks cut off.
        head_orphan:    the next-page head looks like a continuation.
        cont_label:     the next page is labelled "... Cont.".
        label_chain:    the "Cont." label's number matches the prior page's number.

    Returns:
        (confidence in [0, 1], reason string). Confidence >= STITCH_THRESHOLD (set in
        run()) is auto-accepted; the band just below it is "ambiguous" — the zone the
        optional multimodal LLM would adjudicate.
    """
    score = 0.0
    reasons = []
    if cont_label:
        score += 0.55
        reasons.append("page_label=Cont.")
    if label_chain:
        score += 0.25
        reasons.append("page-number chain matches")
    if tail_truncated:
        score += 0.20
        reasons.append("prev tail truncated")
    if head_orphan:
        score += 0.15
        reasons.append("head looks orphaned")
    return min(score, 1.0), "; ".join(reasons) if reasons else "no signal"


# ==================================================================
# Step 4 — Carry-forward date / notebook context
# ==================================================================
def _page_year(page: dict):
    """Pull the year/period from the page's archival date field for carry-forward.

    The entry-level `date` is partial ("22", "Mar. 30") or empty (545/… entries),
    but the page records an archival date like "1923, November". That's where the
    *year* lives. We return the raw archival string; the dedicated normalize_dates
    stage (PLAN A2) turns "Mar. 30" + "1923, November" into a canonical EDTF value —
    here we only carry the context forward so the logical entry isn't date-less.

    Args:
        page: a page record.

    Returns:
        The page's archival date string, or "" if none.
    """
    tbl = page.get("text_by_label") or {}
    return (tbl.get("archv_date") or "").strip()


# ==================================================================
# Step 5 — Build fragment records (the citeable units)
# ==================================================================
def _fragment(page: dict, entry: dict, reg_idx: dict, order_on_page: int) -> dict:
    """Package one raw notebook entry as a fragment with full provenance.

    A *fragment* is exactly one raw entry plus the geometry needed to cite it back to
    the scanned page (vertices + min_conf), plus the reading-order position so we can
    reason about "tail of page" vs "head of page".

    Args:
        page:          the parent page record.
        entry:         the raw entry (rid/text/date/page_label/linked).
        reg_idx:       rid -> geometry, from _region_index().
        order_on_page: 0-based index of this entry within the page's reading order.

    Returns:
        A self-describing fragment dict (the member-level provenance the KG cites).
    """
    geom = reg_idx.get(entry["rid"], {})
    return {
        "fragment_id":    f"{page['id']}::{entry['rid']}",
        "doc_id":         page["id"],
        "rid":            entry["rid"],
        "section":        page.get("section", ""),
        "notebook":       page.get("notebook", ""),
        "pdf_page":       page.get("pdf_page_number"),
        "page":           page.get("page_number_in_type"),
        "page_label":     entry.get("page_label", ""),
        "order_on_page":  order_on_page,
        "text":           entry.get("text", ""),
        "entry_date":     entry.get("date", ""),
        "linked":         entry.get("linked", []),
        # --- pixel provenance: lets a citation deep-zoom to the exact region (IIIF) ---
        "vertices":       geom.get("vertices"),
        "min_conf":       geom.get("min_conf"),
    }


# ==================================================================
# Step 6 — The stitcher: walk the register and join split entries
# ==================================================================
def stitch_group(group_key: tuple, pages: list[dict], threshold: float,
                 ambiguous_band: float) -> tuple[list[dict], list[dict], list[dict]]:
    """Reconstruct the logical entries for ONE notebook register.

    The walk is intentionally simple and local — it only ever considers joining the
    *last fragment of one page* to the *first fragment of the very next page*, which
    is the only place a physical page-break can split a thought. Within a page,
    entries are already separate favors, so we don't merge across them.

    Args:
        group_key:      (section, notebook) — for stamping provenance.
        pages:          this register's pages, already in reading order.
        threshold:      join confidence >= this is auto-accepted.
        ambiguous_band: the width below `threshold` that is flagged "ambiguous"
                        (the zone the optional multimodal LLM would adjudicate).

    Returns:
        (logical_entries, relations, ambiguous_joins) for this register.
        - logical_entries: grouped fragments (the retrieval unit).
        - relations:       continued_from / continues_on edges (the KG edges).
        - ambiguous_joins: near-miss candidates queued for LLM/human review (never
                           auto-joined).
    """
    section, notebook_no = group_key
    # The group key's second element is the *normalized number* (OCR-robust). For the
    # human-readable label on each logical entry we pick a representative raw header
    # from the pages — the most common non-blank one wins, so we show David a real
    # title ("FR. SOLANUS, NOTEBOOK NO. 5.") rather than a bare number.
    _names = Counter(pg.get("notebook", "") for pg in pages if (pg.get("notebook") or "").strip())
    notebook = _names.most_common(1)[0][0] if _names else ""

    # ------------------------------------------------------------------
    # 6a. Flatten this register into an ordered fragment stream, remembering
    #     which fragments sit at the start/end of their page (the only join sites).
    # ------------------------------------------------------------------
    stream: list[dict] = []          # all fragments, in reading order across pages
    last_year = ""                   # carry-forward archival year/period
    for pg in pages:
        reg_idx = _region_index(pg)
        year = _page_year(pg)
        if year:
            last_year = year         # update the running context when a page states it
        entries = pg.get("entries", [])
        for i, e in enumerate(entries):
            if not (e.get("text") or "").strip():
                continue             # skip empty regions — nothing to stitch
            frag = _fragment(pg, e, reg_idx, i)
            frag["is_page_head"] = (i == 0)
            frag["is_page_tail"] = (i == len(entries) - 1)
            frag["carried_year"] = year or last_year
            stream.append(frag)

    # ------------------------------------------------------------------
    # 6b. Carry the date forward fragment-by-fragment. A continuation fragment
    #     inherits the most recent non-empty entry_date so it's never date-less.
    #     (The canonical EDTF assembly happens later in normalize_dates — this is
    #     just the *context* so retrieval/grouping have something to hold onto.)
    # ------------------------------------------------------------------
    running_date = ""
    for frag in stream:
        if (frag.get("entry_date") or "").strip():
            running_date = frag["entry_date"].strip()
        frag["carried_date"] = running_date

    # ------------------------------------------------------------------
    # 6c. Decide page-break joins. Union-find lite: each fragment starts in its own
    #     logical group; an accepted join merges the head fragment into the tail's
    #     group. We only ever test (page-tail fragment) -> (next-page-head fragment).
    # ------------------------------------------------------------------
    parent = {f["fragment_id"]: f["fragment_id"] for f in stream}

    def find(x: str) -> str:
        # path-compressed find: collapse the chain so repeated lookups are cheap.
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    relations: list[dict] = []
    ambiguous: list[dict] = []

    for a, b in zip(stream, stream[1:]):
        # Only a true page boundary can split a thought: a is the last entry of its
        # page AND b is the first entry of the *immediately following* page.
        crosses_page_break = a["is_page_tail"] and b["is_page_head"]
        if not crosses_page_break:
            continue
        # Never stitch across a different physical book (defensive — pages are already
        # grouped by section, but pdf_page must actually be adjacent-ish).
        if a.get("pdf_page") is not None and b.get("pdf_page") is not None:
            if b["pdf_page"] - a["pdf_page"] > 1:
                continue

        cont_label = _is_cont_label(b["page_label"])
        # A numbered chain: b says "Page N Cont." and a sat on "Page N".
        a_num = _page_label_number(a["page_label"])
        b_num = _page_label_number(b["page_label"])
        label_chain = bool(cont_label and a_num is not None and a_num == b_num)
        tail_trunc = _looks_truncated_tail(a["text"])
        head_orphan = _looks_orphan_head(b["text"])

        # ----------------------------------------------------------------------
        # CRITICAL distinction — page-level vs entry-level continuation.
        #
        # "Page N Cont." tells us the *register* continues onto the next page, but
        # NOT that the last favor on page N is the same favor as the first favor on
        # the Cont. page. Most of the time the Cont. page simply lists the NEXT
        # person ("...joined for one year." | "Mary Briggi - joined her brother..."):
        # two distinct favors that happen to span a page break. Gluing those into one
        # logical entry is a false merge — it fuses two unrelated people's stories.
        #
        # We only fuse the two ENTRIES into one logical entry when the *thought
        # itself* is split — i.e. the tail is truncated (cut mid-clause) OR the head
        # is an orphan-start (opens mid-thought). The "Cont." label corroborates that
        # an entry-split is plausible (we sit at the right boundary) but it can't by
        # itself justify merging two complete favors.
        #
        # When the label says Cont. but both halves are complete favors, we still
        # record a *page-level* relation (the register flows on) so the KG keeps the
        # navigational edge — we just don't merge the entries.
        # ----------------------------------------------------------------------
        thought_is_split = tail_trunc or head_orphan
        conf, reason = _join_confidence(tail_trunc, head_orphan, cont_label, label_chain)

        edge = {
            "from_fragment": a["fragment_id"],
            "to_fragment":   b["fragment_id"],
            "from_rid":      a["rid"],
            "to_rid":        b["rid"],
            "confidence":    round(conf, 3),
            "reason":        reason,
            "signals": {
                "cont_label":   cont_label,
                "label_chain":  label_chain,
                "tail_trunc":   tail_trunc,
                "head_orphan":  head_orphan,
            },
        }

        if thought_is_split and conf >= threshold:
            # ENTRY-LEVEL continuation: b is the back half of a's thought. Merge b's
            # group into a's, and record both directed entry relations.
            parent[find(b["fragment_id"])] = find(a["fragment_id"])
            relations.append({"type": "continues_on", **edge})
            relations.append({
                "type": "continued_from",
                "from_fragment": b["fragment_id"], "to_fragment": a["fragment_id"],
                "from_rid": b["rid"], "to_rid": a["rid"],
                "confidence": round(conf, 3), "reason": reason, "signals": edge["signals"],
            })
        elif cont_label:
            # PAGE-LEVEL continuation: the register flows onto a labelled "Cont." page
            # but these are two complete favors. Keep the navigational edge for the KG;
            # do NOT merge the entries (no false fusion of distinct stories).
            relations.append({"type": "page_continues_on", **edge})
        elif thought_is_split and conf >= threshold - ambiguous_band:
            # Near-miss entry-split: real enough to flag, not confident enough to
            # auto-join. This is precisely the queue the OPTIONAL multimodal LLM would
            # adjudicate (see confirm_join_with_vision) — but we DO NOT call it here.
            ambiguous.append(edge)

    # ------------------------------------------------------------------
    # 6d. Materialize logical entries from the union-find groups, preserving the
    #     reading order of member fragments and carrying context onto the whole unit.
    # ------------------------------------------------------------------
    groups: dict[str, list[dict]] = {}
    for frag in stream:
        groups.setdefault(find(frag["fragment_id"]), []).append(frag)

    logical_entries: list[dict] = []
    for root, frags in groups.items():
        # Members already arrive in stream order, but sort defensively by
        # (pdf_page, order_on_page) so the joined text always reads top-to-bottom.
        frags = sorted(frags, key=lambda f: (f.get("pdf_page") or 0, f.get("order_on_page") or 0))
        head = frags[0]
        # The logical id is stable + descriptive: it's anchored on the FIRST member's
        # fragment_id, so the same inputs always yield the same id (idempotency).
        le_id = f"LE::{head['fragment_id']}"
        joined_text = " ".join((f["text"] or "").strip() for f in frags).strip()

        # Pick the best available date/context for the whole logical entry: the first
        # explicit entry_date among members, else the carried-forward date.
        le_date = next((f["entry_date"] for f in frags if (f.get("entry_date") or "").strip()),
                       head.get("carried_date", ""))
        le_year = next((f["carried_year"] for f in frags if (f.get("carried_year") or "").strip()), "")

        logical_entries.append({
            "logical_entry_id": le_id,
            "section":          section,
            "notebook":         notebook,
            "is_multi_page":    len(frags) > 1,
            "n_fragments":      len(frags),
            "date":             le_date,         # raw/partial — EDTF assembly is normalize_dates' job
            "archival_year":    le_year,         # carried-forward page archival date ("1923, November")
            "page_label":       head.get("page_label", ""),
            "text":             joined_text,     # the WHOLE favor — what retrieval should embed
            "member_fragments": [
                {
                    "fragment_id": f["fragment_id"], "doc_id": f["doc_id"], "rid": f["rid"],
                    "pdf_page": f["pdf_page"], "page": f["page"], "page_label": f["page_label"],
                    "order_on_page": f["order_on_page"], "vertices": f["vertices"],
                    "min_conf": f["min_conf"], "entry_date": f["entry_date"], "linked": f["linked"],
                }
                for f in frags
            ],
        })

    # Stable output order: by first member's physical position.
    logical_entries.sort(key=lambda le: (le["member_fragments"][0].get("pdf_page") or 0,
                                         le["member_fragments"][0].get("order_on_page") or 0))
    return logical_entries, relations, ambiguous


# ==================================================================
# Step 7 — OPTIONAL multimodal-LLM confirmation (GATED — never called by run())
# ==================================================================
def confirm_join_with_vision(tail_fragment: dict, head_fragment: dict,
                             model: str | None = None) -> dict:
    """Ask a multimodal LLM whether two fragments are truly one split entry — by
    *looking at the two page images*. THIS IS A PAID CALL AND IS GATED.

    The cheap text heuristics above resolve the overwhelming majority of joins. A
    thin band stays genuinely ambiguous (the OCR is messy, the dash is unclear, the
    label is missing). For those, the 2026 archival-continuation approach is to show
    a vision model the bottom of page A and the top of page B and ask: "does the
    thought at the bottom of the first image continue at the top of the second?"
    That sees layout cues (indentation, a bracket, a ditto mark) that pure text loses.

    HARD RULE COMPLIANCE:
        * run() passes use_llm=False and NEVER reaches this function.
        * Every call would be routed through lib.providers.llm.generate (Gemini
          vision) and therefore cost-logged automatically.
        * We DO NOT execute it here; the heavy image-loading + API call is left as a
          clearly marked TODO so flipping the flag is a one-line change later.

    Args:
        tail_fragment: the page-tail fragment (member from member_fragments).
        head_fragment: the next-page-head fragment.
        model:         vision-capable LLM id (defaults to config.DEFAULTS["llm"];
                       gemini-2.5-flash is multimodal).

    Returns:
        A verdict dict {"joined": bool, "confidence": float, "rationale": str,
        "model": str, "called": bool}. With the gate off, returns called=False and a
        note — no network, no cost.
    """
    model = model or config.DEFAULTS["llm"]

    # ------------------------------------------------------------------
    # The prompt we WOULD send (kept here so the design is reviewable now). The two
    # masked page PNGs live under config.ENRICHED / <section> ... — we resolve them by
    # doc_id when this is actually wired. We deliberately ask for a free-form rationale
    # first, then a strict verdict, to dodge the "constrained-decoding reasoning tax"
    # the research plan warns about (PART B).
    # ------------------------------------------------------------------
    _system = (
        "You are an archivist reconstructing Fr. Solanus Casey's notebook register. "
        "You are shown the BOTTOM of one page and the TOP of the next. Decide whether "
        "the final entry on the first page continues as the first entry on the second."
    )
    _prompt = (
        "First page (tail) entry text:\n"
        f"  {tail_fragment.get('text', '')!r}\n\n"
        "Second page (head) entry text:\n"
        f"  {head_fragment.get('text', '')!r}\n\n"
        "Looking at BOTH page images, do these form ONE continued entry? "
        "Give a one-sentence rationale, then answer JOINED=yes or JOINED=no with a "
        "confidence 0-1."
    )

    # TODO(deferred-paid): when David enables this:
    #   1. resolve the two masked PNGs from config.ENRICHED via doc_id (and ideally
    #      crop to the tail box / head box using the fragments' `vertices`);
    #   2. build a multimodal request (image parts + _system + _prompt) and call
    #      lib.providers.llm.generate(...) — which cost-logs the Gemini-vision usage;
    #   3. parse JOINED=yes/no + confidence into the verdict below.
    # Until then we make ZERO calls and surface that plainly:
    return {
        "joined":     None,
        "confidence": 0.0,
        "rationale":  "LLM confirmation gated off (use_llm=False); no call made.",
        "model":      model,
        "called":     False,
        "_system":    _system,
        "_prompt":    _prompt,
    }


# ==================================================================
# Step 8 — run(): the stage entrypoint
# ==================================================================
def run(stitch_threshold: float = 0.55, ambiguous_band: float = 0.30,
        use_llm: bool = False, limit_groups: int | None = None) -> dict:
    """Reconstruct logical notebook entries and write data/stitched_notebooks.json.

    This is the stage the DAG calls. It reads notebooks.json, orders every register,
    stitches split entries with the cheap heuristics, and emits the non-destructive
    artifact. The multimodal LLM is GATED OFF by default and not invoked.

    Args:
        stitch_threshold: join confidence >= this is auto-accepted (default 0.55 —
                          tuned so an explicit "Cont." label alone clears the bar,
                          while text-only signals must corroborate).
        ambiguous_band:   width below the threshold that gets flagged for review
                          (the would-be LLM queue). Default 0.30.
        use_llm:          if True, ambiguous joins WOULD be sent to
                          confirm_join_with_vision. HARD-GATED: even when True the
                          confirmation function makes no call until its TODO is wired,
                          so nothing is ever billed from this stage as written.
        limit_groups:     process only the first N registers (smoke test). None = all.

    Returns:
        A summary dict (also the top-level shape written to disk minus the big lists),
        so a caller/test can assert on counts without re-reading the file.
    """
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("stitch_notebooks")

    config.DATA.mkdir(parents=True, exist_ok=True)

    # ----- read + order -----
    pages = _load_pages()
    ordered = _ordered_pages(pages)
    if limit_groups is not None:
        ordered = ordered[:limit_groups]
    log.info("loaded %d pages across %d notebook register(s)", len(pages), len(ordered))

    # ----- stitch each register -----
    all_logical: list[dict] = []
    all_relations: list[dict] = []
    all_ambiguous: list[dict] = []
    for key, reg_pages in ordered:
        le, rel, amb = stitch_group(key, reg_pages, stitch_threshold, ambiguous_band)
        all_logical.extend(le)
        all_relations.extend(rel)
        all_ambiguous.extend(amb)

    # ----- optional (gated) LLM adjudication of the ambiguous band -----
    # We honor the flag's *shape* so wiring it later is trivial, but the confirmation
    # function itself makes no call (returns called=False). Nothing is billed.
    llm_verdicts: list[dict] = []
    if use_llm and all_ambiguous:
        log.warning("use_llm=True: %d ambiguous join(s) WOULD be sent to vision LLM "
                    "(confirmation is stubbed — no call made). Wire the TODO to enable.",
                    len(all_ambiguous))
        frag_by_id = {f["fragment_id"]: f
                      for le in all_logical for f in le["member_fragments"]}
        for edge in all_ambiguous:
            verdict = confirm_join_with_vision(
                tail_fragment = frag_by_id.get(edge["from_fragment"], {"text": ""}),
                head_fragment = frag_by_id.get(edge["to_fragment"], {"text": ""}),
            )
            llm_verdicts.append({"edge": edge, "verdict": verdict})

    # ----- assemble + write the artifact (non-destructive) -----
    # Two relation families now: ENTRY-level (continues_on / continued_from — fragments
    # fused into one logical entry) and PAGE-level (page_continues_on — register flows on
    # but the favors stay distinct). We count them separately so the summary is honest.
    multi = sum(1 for le in all_logical if le["is_multi_page"])
    entry_joins = sum(1 for r in all_relations if r["type"] == "continues_on")
    page_joins = sum(1 for r in all_relations if r["type"] == "page_continues_on")
    out = {
        "stage": "stitch_notebooks",
        "source": str(config.NOTEBOOKS.relative_to(config.REPO)),
        "config": {
            "stitch_threshold": stitch_threshold,
            "ambiguous_band":   ambiguous_band,
            "use_llm":          use_llm,
            "config_fingerprint": config.fingerprint(),
        },
        "stats": {
            "pages":                  len(pages),
            "registers":              len(ordered),
            "logical_entries":        len(all_logical),
            "multi_page_entries":     multi,
            "entry_continuations":    entry_joins,   # split thoughts fused into one entry
            "page_continuations":     page_joins,    # "Cont." page edges (entries kept distinct)
            "relations":              len(all_relations),
            "ambiguous_joins":        len(all_ambiguous),
        },
        "logical_entries":  all_logical,
        "relations":        all_relations,
        "ambiguous_joins":  all_ambiguous,
        "llm_verdicts":     llm_verdicts,
    }
    out_path = config.DATA / "stitched_notebooks.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))

    # ----- a small SUMMARY block, per the style guide -----
    log.info("=" * 60)
    log.info("STITCH SUMMARY")
    log.info("  registers ................. %d", len(ordered))
    log.info("  logical entries ........... %d", len(all_logical))
    log.info("  multi-page (entry-stitched) %d", multi)
    log.info("  entry continuations ....... %d", entry_joins)
    log.info("  page continuations ........ %d", page_joins)
    log.info("  KG relations (all) ........ %d", len(all_relations))
    log.info("  ambiguous (review) ........ %d", len(all_ambiguous))
    log.info("  wrote ..................... %s", out_path)
    log.info("=" * 60)

    # No model/API calls were made by this stage, so there's nothing to cost-log here.
    # (When the gated vision step is wired, lib.providers.llm.generate cost-logs it.)
    return out["stats"]


# ==================================================================
# CLI
# ==================================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tier-1 A1: stitch split notebook entries into logical entries.")
    ap.add_argument("--threshold", type=float, default=0.55, help="auto-accept join confidence")
    ap.add_argument("--ambiguous-band", type=float, default=0.30, help="width below threshold flagged for review")
    ap.add_argument("--use-llm", action="store_true", help="(gated) route ambiguous joins to vision LLM — no call is made")
    ap.add_argument("--limit-groups", type=int, default=None, help="process only the first N registers (smoke test)")
    a = ap.parse_args()
    stats = run(stitch_threshold=a.threshold, ambiguous_band=a.ambiguous_band,
                use_llm=a.use_llm, limit_groups=a.limit_groups)
    print(json.dumps(stats, indent=2))
