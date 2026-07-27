"""stages/normalize_dates.py — Tier-1 A2 date reconstruction to EDTF (the temporal-truth layer).

THE PROBLEM, in plain language.
    Solanus's notebooks are a running register. An entry usually carries only a *month and day*
    ("Mar. 30"), and sometimes only a bare *day* ("22"). The **year almost never appears in the
    entry** — it lives once, at the top of the page, in the archival field `archv_date`
    (e.g. "1923, November"). On top of that the dates are gloriously human: circa values
    ("c. 1929"), ranges ("c. 1897 to 1904"), feast days instead of dates ("Feast of the
    Presentation."), and — crucially for the favors timeline — *two* dates inside one entry
    ("Enrolled May 3rd 1923 ... report - Recovered"), i.e. an ENROLLMENT date and a later
    REPORT/OUTCOME date.

THE FIX, in plain language.
    Reconstruct a full, machine-comparable date for every record by:
      1. taking the YEAR from the page's `archv_date`,
      2. taking the month/day from the entry's own `date` (or, for letters, `src_date`),
      3. carrying forward the last-seen month (and year) when the entry omits it — exactly how a
         human reads a register: "the 17th" means "the 17th of whatever month we're in",
      4. additionally scanning the entry *text* for an explicit enrollment date and a later
         report/outcome date (the bitemporal pair),
    then encoding EVERY result in **EDTF** (Extended Date/Time Format, ISO 8601-2, the Library of
    Congress standard for *uncertain / approximate / partial* dates). EDTF is the natural home for
    our fuzziness:
        circa .............. 1945~
        unknown day ........ 1933-10-XX
        unknown month+day .. 1929-XX-XX  (or just 1929)
        closed range ....... 1940-02/1940-10
    We keep the RAW string, the EDTF VALUE, and a PRECISION + CONFIDENCE flag for every date, so a
    scholar can always see what we read and how sure we are.

WHY TWO CODE PATHS.
    - A **pure-Python rule path** (`reconstruct_record`) does the whole job with stdlib only, for
      free, deterministically, and is what `run()` executes by default. It needs no API key and no
      model download — so the temporal layer is reproducible and costs nothing.
    - An **LLM-assisted normalizer** (`llm_normalize_date`, via lib.providers.llm) is provided for
      the genuinely hard, ambiguous strings a regex shouldn't be trusted with (feast days, garbled
      OCR, prose dates). It is **gated OFF by default** and every call is cost-logged. We never bill
      David without him asking — see the `use_llm` flag and the TODO in `run()`.

NON-DESTRUCTIVE: writes a new artifact `data/dates.json` keyed by record id (rid). It never edits
documents.json / notebooks.json. Re-running with unchanged inputs yields the same output.

    python stages/normalize_dates.py                 # full corpus, free rule path -> data/dates.json
    python stages/normalize_dates.py --limit 50      # smoke test on a strided sample
    python stages/normalize_dates.py --print doc_1.src_content.10   # inspect one record
"""
from __future__ import annotations

# ----------------------------------------------------------------- Core Python Imports
import argparse
import json
import re
import sys
from pathlib import Path

# ----------------------------------------------------------------- Local File Imports
# Same sys.path dance every step_7 module does: make the step_7 root importable so `config` and
# `lib.*` resolve no matter where this script is launched from.
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))

import config              # noqa: E402  (paths + model registry)
from lib import costlog    # noqa: E402  (every model call is logged here)
from lib.providers import llm  # noqa: E402  (model-agnostic generation adapter; only used if gated on)


# ==================================================================
# Month vocabulary — how we turn "Nov.", "Febr", "Sept" into a number
# ==================================================================
# The OCR gives us a zoo of month spellings: full ("November"), abbreviated ("Nov."), and slightly
# mangled ("Febr", "Sept", the classic "Hov." for "Nov."). We map every form we expect to its
# 1..12 number. Keys are lowercase and punctuation-free so lookup is forgiving.
MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "febr": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11, "hov": 11,   # "Hov." is a recurring OCR slip for "Nov."
    "dec": 12, "december": 12,
}

# A regex that finds any month token (longest-first so "february" wins over "feb").
_MONTH_ALT = "|".join(sorted(MONTHS, key=len, reverse=True))
RE_MONTH = re.compile(rf"\b({_MONTH_ALT})\b\.?", re.IGNORECASE)

# A day-of-month: 1..31, optionally with an ordinal suffix ("3rd", "21st", "8th").
RE_DAY = re.compile(r"\b([0-3]?\d)\s*(?:st|nd|rd|th)?\b", re.IGNORECASE)

# A 4-digit year, 1880..1959 — the corpus is Solanus's lifetime + a little margin. Bounding the
# range keeps stray 3- or 5-digit OCR noise from being read as a year.
RE_YEAR = re.compile(r"\b(18[89]\d|19[0-5]\d)\b")

# "circa" markers — any of these makes the whole date APPROXIMATE (EDTF '~').
RE_CIRCA = re.compile(r"\b(c\.?|ca\.?|circa|about|approx\.?|abt\.?)\b", re.IGNORECASE)

# Range connectors ("to", "—", "–", "-", "thru", "through", "et seq.") between two dates.
RE_RANGE = re.compile(r"\s*(?:--|—|–|-|to|thru|through|et\s*seq\.?)\s*", re.IGNORECASE)

# In an entry's BODY text, the words that introduce the two bitemporal dates. We look just *after*
# these cue words for a date. "Enrolled May 3rd 1923" / "Reports Dec. 8th" / "report - Recovered".
RE_ENROLL_CUE = re.compile(r"\benroll(?:ed|s|ing)?\b", re.IGNORECASE)
RE_REPORT_CUE = re.compile(r"\b(report(?:s|ed|ing)?|reply|replied|outcome|result|cured|recovered)\b",
                           re.IGNORECASE)


# ==================================================================
# EDTF encoding — render a (year, month, day, circa) tuple as EDTF text
# ==================================================================
# EDTF (ISO 8601-2) is just ISO 8601 with a few extensions for archives:
#   - 'X' is an UNKNOWN digit:   1933-10-XX  (October 1933, day unknown)
#   - '~' means APPROXIMATE:     1945~       ("circa 1945")
#   - 'A/B' is a closed RANGE:   1940-02/1940-10
# We never invent precision we don't have: a missing day becomes '-XX', a missing month '-XX-XX',
# and if even the year is missing we emit the empty string (caller records 'unknown').
def edtf_part(year: int | None, month: int | None, day: int | None, circa: bool = False) -> str:
    """Encode one date as an EDTF string at the precision we actually have.

    Args:
        year:  4-digit year, or None if unknown.
        month: 1..12, or None if unknown.
        day:   1..31, or None if unknown.
        circa: True to mark the whole value approximate (appends EDTF's '~').

    Returns:
        An EDTF string such as '1933-10-XX', '1945~', '1929', or '' when no year is known.
    """
    if year is None:
        # Without a year there is nothing anchorable to encode. We return empty and let the caller
        # mark the record's precision as 'unknown' — honest beats fabricated.
        return ""

    s = f"{year:04d}"
    if month is not None:
        s += f"-{month:02d}"
        # A day only makes sense once we have a month; 'X' fills an unknown day at month precision.
        s += f"-{day:02d}" if day is not None else "-XX"
    # If month is unknown we simply stop at the year ('1929'); that already *means* "some time in
    # 1929" in EDTF, which is cleaner than padding with -XX-XX.

    if circa:
        s += "~"   # the approximate qualifier applies to the whole value
    return s


def edtf_range(start: str, end: str, circa: bool = False) -> str:
    """Join two EDTF parts into an EDTF interval 'start/end'.

    Args:
        start: EDTF string for the earlier endpoint.
        end:   EDTF string for the later endpoint.
        circa: True to mark both endpoints approximate.

    Returns:
        'start/end' (e.g. '1940-02/1940-10'); or a single endpoint if the other is empty.
    """
    if circa:
        # Apply '~' to whichever endpoint doesn't already carry it (idempotent).
        start = start if (not start or start.endswith("~")) else start + "~"
        end = end if (not end or end.endswith("~")) else end + "~"
    if start and end:
        return f"{start}/{end}"
    return start or end   # a "range" with one missing side degrades gracefully to a point


# ==================================================================
# Precision label — a human-readable summary of how exact a date is
# ==================================================================
def precision_of(year, month, day, is_range: bool) -> str:
    """Name the precision tier of a reconstructed date (for filtering + UI).

    Returns one of: 'unknown', 'year', 'month', 'day', 'range'. This is the field a scholar can
    facet on ("show me only day-precise enrollments") and the UI can use to render fuzziness.
    """
    if is_range:
        return "range"
    if year is None:
        return "unknown"
    if month is None:
        return "year"
    if day is None:
        return "month"
    return "day"


# ==================================================================
# Low-level parsing — pull (year, month, day, circa, range) out of one string
# ==================================================================
def _clean(s: str) -> str:
    """Tidy an OCR date fragment: drop stray leading bullets/dots and squeeze whitespace.

    The OCR sometimes prefixes a date with a list bullet ("· Dec. 19") or trailing punctuation
    ("Febr .- 17th :"). None of that is part of the date, so we strip it before parsing.
    """
    s = (s or "").strip()
    s = re.sub(r"^[·•*.\-\s]+", "", s)   # leading bullets/dashes/dots
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def parse_fragment(s: str) -> dict:
    """Parse ONE date fragment (no range splitting) into its components.

    This is deliberately conservative: it reads the first month and first day it sees and a year if
    present, and notes whether a circa marker appeared. Carry-forward (filling a missing month/year
    from context) is the caller's job — this function only reports what *this string* contains.

    Args:
        s: a single date fragment, e.g. "Nov. 8th", "17", "Feb 1924", "c. 1929".

    Returns:
        {'year': int|None, 'month': int|None, 'day': int|None, 'circa': bool,
         'has_alpha_month': bool}  — 'has_alpha_month' tells the caller a real month word was read
        (vs. carried forward), which matters for carry-forward bookkeeping.
    """
    s = _clean(s)
    circa = bool(RE_CIRCA.search(s))

    mo = RE_MONTH.search(s)
    month = MONTHS[mo.group(1).lower()] if mo else None

    yr = RE_YEAR.search(s)
    year = int(yr.group(1)) if yr else None

    # For the DAY, search the substring that is NOT the year (so "1924" isn't read as day "19" or
    # "24"). We blank out any year match first, then look for a 1..31 token.
    s_noyear = RE_YEAR.sub(" ", s)
    day = None
    for m in RE_DAY.finditer(s_noyear):
        v = int(m.group(1))
        if 1 <= v <= 31:
            day = v
            break

    return {"year": year, "month": month, "day": day, "circa": circa,
            "has_alpha_month": month is not None}


def parse_date_string(raw: str) -> dict:
    """Parse a possibly-range date string into endpoints + components.

    Splits on a range connector ("to", "—", "-") IF doing so yields two parsable date-bearing
    halves; otherwise treats the whole thing as a single fragment. (We don't split "8th" or
    "6th-16th" carelessly — a range needs real content on both sides.)

    Args:
        raw: the raw date text from an entry/letter, e.g. "c. 1897 to 1904", "Nov. 8th",
             "Dec. 8th; Dec, 1st".

    Returns:
        {'raw': str, 'is_range': bool, 'start': frag, 'end': frag|None, 'circa': bool}
        where each `frag` is the dict from parse_fragment(). For a non-range, 'end' is None.
    """
    raw = (raw or "").strip()
    circa = bool(RE_CIRCA.search(raw))

    # Try a range split. We only accept it when BOTH halves carry a year or a month — otherwise
    # "6th-16th" (two days of the same month) and "Dec. 22nd 23rd 24th" (a list) would be mistaken
    # for cross-month ranges, which they are not.
    parts = RE_RANGE.split(raw, maxsplit=1)
    if len(parts) == 2:
        left, right = parse_fragment(parts[0]), parse_fragment(parts[1])
        both_anchored = (left["year"] or left["month"]) and (right["year"] or right["month"])
        if both_anchored:
            return {"raw": raw, "is_range": True, "start": left, "end": right, "circa": circa}

    # Non-range: a single fragment. If there are multiple dates (e.g. "Dec. 8th; Dec, 1st") we take
    # the FIRST as the primary; the bitemporal scan over the body text recovers the second one with
    # its cue word (enrolled/reports), which is the meaningful split.
    primary = re.split(r"[;]", raw, maxsplit=1)[0]
    return {"raw": raw, "is_range": False, "start": parse_fragment(primary), "end": None,
            "circa": circa}


# ==================================================================
# Year from the page archival field — the anchor every entry inherits
# ==================================================================
def year_from_archv(archv_date: str) -> dict:
    """Extract the page-level YEAR(S) (and any circa flag) from an `archv_date` string.

    The archival field is the curator's tidy summary at the top of the page, e.g. "1923, November"
    or "c. 1897 to 1904". It is our most reliable source of the YEAR, which the entries omit.

    One subtlety pays off a lot here: a single notebook page often straddles a YEAR BOUNDARY, and
    the archival field records this as month/year *pairs* — e.g. "1923, December 1924, January".
    A page like that holds both December-1923 entries AND January-1924 entries. If we naively took
    only the first year (1923), every January entry on the page would be dated a year early. So we
    also build a small **month -> year map** from the field: any "<year>, <Month>" or
    "<Month> <year>" pairing teaches us which year a given month belongs to. The reconstruction
    then picks the year that matches the entry's resolved month — turning a year-boundary page from
    a systematic error into a correctly split timeline.

    Args:
        archv_date: the page's `text_by_label['archv_date']` value (may be None/empty).

    Returns:
        {'year': int|None, 'year_end': int|None, 'circa': bool, 'month_year': {month:int -> year:int}}
        - 'year' is the page's primary (first) year anchor;
        - 'year_end' is set only when the field spans years (a multi-year page like "c. 1897-1904");
        - 'month_year' maps any month named alongside a year to that year (empty when none).
    """
    if not archv_date:
        return {"year": None, "year_end": None, "circa": False, "month_year": {}}
    circa = bool(RE_CIRCA.search(archv_date))
    years = [int(y) for y in RE_YEAR.findall(archv_date)]
    if not years:
        return {"year": None, "year_end": None, "circa": circa, "month_year": {}}

    # ----- month -> year map -------------------------------------------------
    # Scan left-to-right; remember the most recent year, and whenever a month word appears bind it
    # to that year. This handles both orders the curator uses: "1923, December" (year-then-month)
    # and "December 1924" (month-then-year), because we also let a month adopt a year that follows
    # it within the same comma-free run.
    month_year: dict[int, int] = {}
    tokens = re.findall(rf"\b(18[89]\d|19[0-5]\d|{_MONTH_ALT})\b", archv_date, re.IGNORECASE)
    last_year = None
    pending_months: list[int] = []
    for tok in tokens:
        if tok.isdigit():
            last_year = int(tok)
            for m in pending_months:          # a month that came *before* its year now adopts it
                month_year[m] = last_year
            pending_months = []
        else:
            m = MONTHS[tok.lower()]
            if last_year is not None:
                month_year[m] = last_year     # month-after-year ("1923, December")
            else:
                pending_months.append(m)      # month-before-year ("December 1924"); bind when seen

    # First year anchors the page; a distinct max year (if any) gives the page's span end.
    year = years[0]
    year_end = max(years) if max(years) != year else None
    return {"year": year, "year_end": year_end, "circa": circa, "month_year": month_year}


# ==================================================================
# Bitemporal scan — find enrollment + report/outcome dates in the body
# ==================================================================
def scan_bitemporal(text: str) -> dict:
    """Look in an entry's BODY for an explicit enrollment date and a report/outcome date.

    Solanus's favor entries are bitemporal: "Mrs. ... **Enrolled May 3rd 1923** ... **report** -
    Recovered". The enrollment is when the petition began; the report is when the outcome came in.
    Capturing both as separate timestamps is what makes a real favors *timeline* possible.

    Strategy: find each cue word (enroll* / report*) and parse the short window of text that
    follows it for a date. We keep whichever dates we find; absence is fine (most entries have one
    or neither, and the record's primary date already covers the common case).

    Args:
        text: the entry's `text` (free body, may be long).

    Returns:
        {'enrolled': dict|None, 'reported': dict|None} where each value is a parse_fragment() dict
        (year may be None — the caller carries the page year forward).
    """
    out = {"enrolled": None, "reported": None}
    if not text:
        return out

    def _date_after(cue_match) -> dict | None:
        # Look at the ~40 characters right after the cue word — long enough to contain "May 3rd
        # 1923" or "Dec. 8th", short enough not to drift into the next clause.
        window = text[cue_match.end(): cue_match.end() + 40]
        frag = parse_fragment(window)
        # Only count it if the window actually held a month or a day; a cue word with no nearby
        # date (common) should not invent one.
        return frag if (frag["month"] or frag["day"]) else None

    em = RE_ENROLL_CUE.search(text)
    if em:
        out["enrolled"] = _date_after(em)
    rm = RE_REPORT_CUE.search(text)
    if rm:
        out["reported"] = _date_after(rm)
    return out


# ==================================================================
# The rule path — reconstruct ONE record's dates (free, deterministic)
# ==================================================================
def reconstruct_record(raw_date: str, body_text: str, ctx: dict) -> dict:
    """Reconstruct a record's full date(s) from its raw date, body, and carried context.

    This is the heart of the free path. It:
      1. parses the raw date string (range-aware),
      2. fills a missing YEAR from the page archival year (ctx['page_year']),
      3. carries forward a missing MONTH from the last entry on the page (ctx['carry_month']) and a
         missing YEAR from the last entry too (ctx['carry_year']) — register reading order,
      4. scans the body for enrollment / report dates and dates them with the same year context,
      5. encodes every result in EDTF with a precision + confidence flag.

    Args:
        raw_date:  the record's raw date string (entry `date` or letter `src_date`).
        body_text: the record's body text (for the bitemporal scan); '' for letters w/o a body.
        ctx:       carry-forward context for THIS page, a dict with keys:
                     'page_year' (int|None)       — year from the page archival field,
                     'page_circa' (bool)          — page-level circa flag,
                     'page_month_year' (dict)     — month->year for year-boundary pages,
                     'carry_month' (int|None)     — last month seen on the page,
                     'carry_year' (int|None)      — last year resolved on the page.

    Returns:
        A record-date dict (see `_record_payload`) carrying raw + edtf + precision + confidence +
        the optional enrollment/report EDTF values, plus the updated carry state for the next entry.
    """
    parsed = parse_date_string(raw_date)
    page_year = ctx.get("page_year")
    page_circa = ctx.get("page_circa", False)
    page_month_year = ctx.get("page_month_year", {})    # month -> year, for year-boundary pages
    carry_month = ctx.get("carry_month")
    carry_year = ctx.get("carry_year")

    # --------------------------------------------------------------
    # Resolve the PRIMARY date (start endpoint) with carry-forward.
    # --------------------------------------------------------------
    start = parsed["start"]
    # Confidence starts high and is debited for each thing we had to *infer* rather than *read*.
    # This is a transparent, rule-based score in [0, 1] — not a model probability.
    confidence = 1.0
    circa = parsed["circa"] or page_circa

    # Resolve the MONTH first, because on a year-boundary page the month tells us which year to pick.
    month = start["month"]
    if month is None and start["day"] is not None:
        # A bare day ("17", "4") — inherit the current month from the previous entry, exactly like
        # a human reading down the page. This is a slightly larger debit (it's a real inference).
        month = carry_month
        if month is not None:
            confidence -= 0.10

    year = start["year"]
    if year is None:
        # No year in the entry — the expected case. Prefer the year the archival field pairs with
        # THIS month (so a January entry on a "1923, December / 1924, January" page becomes 1924,
        # not 1923). Otherwise fall back to the page's primary year, then to the last year resolved
        # on the page. Each inheritance is a small confidence debit.
        if month is not None and month in page_month_year:
            year = page_month_year[month]
        else:
            year = page_year if page_year is not None else carry_year
        if year is not None:
            confidence -= 0.05

    day = start["day"]
    if month is None and day is None and year is not None:
        # We only have a year (e.g. the date string was a feast day or unparseable). That's still a
        # legitimate year-precision date; debit modestly for the lost month/day.
        confidence -= 0.15

    if year is None:
        confidence = min(confidence, 0.2)   # a date with no anchorable year is barely a date

    # --------------------------------------------------------------
    # Resolve the END endpoint if this was a range.
    # --------------------------------------------------------------
    edtf_value = ""
    is_range = parsed["is_range"]
    if is_range and parsed["end"] is not None:
        end = parsed["end"]
        e_year = end["year"] if end["year"] is not None else (page_year or carry_year)
        e_month = end["month"] if end["month"] is not None else month
        e_day = end["day"]
        edtf_value = edtf_range(
            edtf_part(year, month, day),
            edtf_part(e_year, e_month, e_day),
            circa=circa,
        )
        confidence -= 0.05   # ranges are inherently a touch fuzzier
    else:
        edtf_value = edtf_part(year, month, day, circa=circa)

    precision = precision_of(year, month, day, is_range)

    # --------------------------------------------------------------
    # Bitemporal pair from the body text (enrollment + report/outcome).
    # --------------------------------------------------------------
    bit = scan_bitemporal(body_text)

    def _encode_event(frag: dict | None) -> dict | None:
        if not frag:
            return None
        e_year = frag["year"] if frag["year"] is not None else (year or page_year or carry_year)
        e_month = frag["month"] if frag["month"] is not None else month
        e_day = frag["day"]
        ev_edtf = edtf_part(e_year, e_month, e_day, circa=circa and frag["year"] is None)
        return {"edtf": ev_edtf,
                "precision": precision_of(e_year, e_month, e_day, False)}

    enrolled = _encode_event(bit["enrolled"])
    reported = _encode_event(bit["reported"])

    # --------------------------------------------------------------
    # Update the page carry state for the NEXT entry (only with things we actually resolved).
    # --------------------------------------------------------------
    new_carry = {
        "carry_month": month if month is not None else carry_month,
        "carry_year": year if year is not None else carry_year,
    }

    payload = _record_payload(
        raw       = raw_date,
        edtf      = edtf_value,
        precision = precision,
        confidence= round(max(0.0, min(1.0, confidence)), 3),
        circa     = circa,
        method    = "rule",
        enrolled  = enrolled,
        reported  = reported,
    )
    return {"payload": payload, "carry": new_carry}


def _record_payload(raw, edtf, precision, confidence, circa, method,
                    enrolled=None, reported=None) -> dict:
    """Assemble the canonical per-record date payload (one stable shape for the whole corpus).

    Every record in data/dates.json has exactly these keys, so downstream code (temporal retrieval,
    the temporal KG) can rely on the schema. We keep the RAW string next to the EDTF VALUE so the
    reconstruction is always auditable.
    """
    return {
        "raw":        raw or "",          # what the OCR/curator actually wrote
        "edtf":       edtf,               # canonical machine value (ISO 8601-2)
        "precision":  precision,          # unknown | year | month | day | range
        "confidence": confidence,         # rule-based [0,1]; 1.0 == fully read, not inferred
        "circa":      circa,              # True if approximate ('~' in the EDTF)
        "method":     method,             # 'rule' (free) or 'llm' (gated normalizer)
        "enrolled":   enrolled,           # bitemporal: {edtf, precision} or None
        "reported":   reported,           # bitemporal: {edtf, precision} or None
    }


# ==================================================================
# The LLM-assisted normalizer — gated OFF, cost-logged when used
# ==================================================================
# The rule path above handles the overwhelming majority. For the genuinely ambiguous strings (feast
# days like "Feast of the Presentation.", badly garbled OCR, or prose dates spread through a
# sentence) a language model is the right tool. This function is REAL but DESERVES a key and an
# explicit opt-in; `run()` never calls it unless `use_llm=True`. Every call is cost-logged via the
# adapter (lib.providers.llm.generate -> lib.costlog.log).
LLM_SYSTEM = (
    "You are a meticulous archival date normalizer for the historical notebooks of Fr. Solanus "
    "Casey (1920s-1950s). Convert a handwritten date fragment into EDTF (ISO 8601-2). Use these "
    "EDTF conventions exactly: approximate/circa -> trailing '~' (e.g. 1945~); unknown day -> "
    "'-XX' (e.g. 1933-10-XX); unknown month -> stop at the year (e.g. 1929); closed range -> "
    "'start/end' (e.g. 1940-02/1940-10). If the fragment is a feast day, resolve it to its month "
    "and day for the given year when you are confident, otherwise leave the day unknown. Return ONLY "
    "minified JSON with keys: edtf (string), precision (one of unknown|year|month|day|range), "
    "confidence (0..1), circa (boolean)."
)


def llm_normalize_date(raw_date: str, page_year: int | None, model: str | None = None) -> dict:
    """LLM fallback for hard date strings -> EDTF. GATED; cost-logged on every call.

    Args:
        raw_date:  the raw date fragment to normalize (e.g. "Feast of the Presentation.").
        page_year: the page archival year to anchor the answer (passed as context to the model).
        model:     LLM id from config.LLMS; defaults to config.DEFAULTS['llm'].

    Returns:
        A `_record_payload`-shaped dict with method='llm'. On any error (no key, bad JSON) it
        returns a low-confidence 'unknown' payload rather than raising, so a batch never dies on one
        bad row.

    NOTE: This issues a BILLED model call. It is only invoked when a caller passes use_llm=True.
    """
    model = model or config.DEFAULTS["llm"]
    prompt = (
        f"Page archival year (anchor): {page_year if page_year is not None else 'unknown'}\n"
        f"Date fragment to normalize: {raw_date!r}\n"
        "Return the EDTF JSON now."
    )
    try:
        # generate() already routes the call through lib.costlog.log(...), so cost tracking is
        # automatic — we do not double-log here.
        text, _usage = llm.generate(prompt, model=model, system=LLM_SYSTEM, json_mode=True)
        data = json.loads(text)
        return _record_payload(
            raw       = raw_date,
            edtf      = str(data.get("edtf", "")),
            precision = str(data.get("precision", "unknown")),
            confidence= float(data.get("confidence", 0.5)),
            circa     = bool(data.get("circa", "~" in str(data.get("edtf", "")))),
            method    = "llm",
        )
    except Exception as exc:   # never let one ambiguous string sink a batch
        return _record_payload(raw=raw_date, edtf="", precision="unknown",
                               confidence=0.0, circa=False, method="llm",
                               enrolled=None, reported=None)


# ==================================================================
# Corpus walk — reconstruct dates for every letter + every notebook entry
# ==================================================================
def _iter_records(limit: int | None = None):
    """Yield every datable record (letters + notebook entries) with its page context.

    For notebooks we walk pages in their stored order and entries in reading order, so the
    carry-forward of month/year mirrors how Solanus actually filled the register. For letters there
    is one record per document.

    Yields tuples: (rid, kind, raw_date, body_text, page_id, archv_date, page_handle) where
    'page_handle' groups records that should share a carry-forward context (the page id for
    notebooks; a unique id for each letter, since letters don't carry forward between documents).
    """
    # --- letters -------------------------------------------------------
    for d in json.loads(config.DOCUMENTS.read_text()):
        tbl = d.get("text_by_label", {}) or {}
        raw = tbl.get("src_date") or d.get("date") or ""
        archv = tbl.get("archv_date") or ""
        body = tbl.get("src_content") or ""
        yield (d["id"], "letter", raw, body, d["id"], archv, f"letter::{d['id']}")

    # --- notebook entries ---------------------------------------------
    for page in json.loads(config.NOTEBOOKS.read_text()):
        tbl = page.get("text_by_label", {}) or {}
        archv = tbl.get("archv_date") or ""
        page_id = page["id"]
        for e in page.get("entries", []):
            rid = f"{page_id}::{e['rid']}"
            yield (rid, "notebook_entry", e.get("date") or "", e.get("text") or "",
                   page_id, archv, page_id)


def run(limit: int | None = None, use_llm: bool = False,
        llm_max: int = 0, llm_conf_threshold: float = 0.5) -> dict:
    """Reconstruct EDTF dates for the whole corpus and write data/dates.json (keyed by rid).

    The FREE rule path runs for every record. The LLM normalizer is OFF unless `use_llm=True`, and
    even then it is only consulted for the lowest-confidence records, capped at `llm_max` calls — so
    a run can never silently rack up a bill.

    Args:
        limit:              process only a strided sample of N records (smoke tests). None = all.
        use_llm:            if True, escalate hard records to the gated LLM normalizer (BILLED).
        llm_max:            hard cap on LLM calls this run (a budget guardrail). 0 = no LLM calls.
        llm_conf_threshold: only records with rule-path confidence below this are LLM-escalated.

    Returns:
        A summary dict: {records, by_precision, by_method, with_enrolled, with_reported, out_path}.
    """
    config.DATA.mkdir(parents=True, exist_ok=True)

    records = list(_iter_records())
    if limit and limit < len(records):
        # Strided sample so the smoke test spans early/late notebooks + letters, not just the head.
        step = len(records) / limit
        records = [records[int(i * step)] for i in range(limit)]

    # --------------------------------------------------------------
    # Pass 1 — the free rule path, with per-page carry-forward state.
    # --------------------------------------------------------------
    out: dict[str, dict] = {}
    carry_by_page: dict[str, dict] = {}          # page_handle -> {carry_month, carry_year}
    for rid, kind, raw, body, page_id, archv, handle in records:
        page = year_from_archv(archv)
        carry = carry_by_page.get(handle, {"carry_month": None, "carry_year": None})
        ctx = {
            "page_year":       page["year"],
            "page_circa":      page["circa"],
            "page_month_year": page["month_year"],   # year-boundary disambiguation
            "carry_month":     carry["carry_month"],
            "carry_year":      carry["carry_year"],
        }
        res = reconstruct_record(raw, body, ctx)
        carry_by_page[handle] = res["carry"]      # thread carry-forward to the next entry on the page

        rec = res["payload"]
        rec["rid"] = rid
        rec["kind"] = kind
        rec["doc_id"] = page_id
        rec["archv_date"] = archv                 # keep the page anchor for provenance/audit
        out[rid] = rec

    # --------------------------------------------------------------
    # Pass 2 — OPTIONAL gated LLM escalation for the hardest records.
    # --------------------------------------------------------------
    # TODO(deferred, billed): this is the only place that spends money. It is OFF by default
    # (llm_max=0). When David says go, call:  run(use_llm=True, llm_max=<budget>). Each call is
    # cost-logged automatically by lib.providers.llm. We escalate only the lowest-confidence,
    # non-empty records so the spend buys the most correction per dollar.
    llm_calls = 0
    if use_llm and llm_max > 0:
        hard = sorted(
            (r for r in out.values() if r["confidence"] < llm_conf_threshold and r["raw"].strip()),
            key=lambda r: r["confidence"],
        )
        for rec in hard[:llm_max]:
            page_year = year_from_archv(rec["archv_date"])["year"]
            improved = llm_normalize_date(rec["raw"], page_year)
            # Preserve provenance + the bitemporal pair we already found by rule.
            improved.update({"rid": rec["rid"], "kind": rec["kind"], "doc_id": rec["doc_id"],
                             "archv_date": rec["archv_date"],
                             "enrolled": rec["enrolled"], "reported": rec["reported"]})
            out[rec["rid"]] = improved
            llm_calls += 1

    # --------------------------------------------------------------
    # Write the artifact (non-destructive: a brand-new file keyed by rid).
    # --------------------------------------------------------------
    out_path = config.DATA / "dates.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))

    # --------------------------------------------------------------
    # Summary block (the '='*60 SUMMARY the style guide asks every runnable script to print).
    # --------------------------------------------------------------
    by_precision: dict[str, int] = {}
    by_method: dict[str, int] = {}
    n_enrolled = n_reported = 0
    for r in out.values():
        by_precision[r["precision"]] = by_precision.get(r["precision"], 0) + 1
        by_method[r["method"]] = by_method.get(r["method"], 0) + 1
        n_enrolled += 1 if r.get("enrolled") else 0
        n_reported += 1 if r.get("reported") else 0

    summary = {"records": len(out), "by_precision": by_precision, "by_method": by_method,
               "with_enrolled": n_enrolled, "with_reported": n_reported,
               "llm_calls": llm_calls, "out_path": str(out_path)}

    print("=" * 60)
    print("normalize_dates SUMMARY")
    print("=" * 60)
    print(f"  records          : {summary['records']}")
    print(f"  by precision     : {by_precision}")
    print(f"  by method        : {by_method}")
    print(f"  bitemporal pairs : enrolled={n_enrolled}  reported={n_reported}")
    print(f"  llm calls (billed): {llm_calls}")
    print(f"  wrote            : {out_path}")
    print("  (run `python -m lib.costlog` to see logged spend)")
    return summary


# ==================================================================
# CLI — free by default; LLM only with an explicit, capped opt-in
# ==================================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tier-1 A2 date reconstruction -> EDTF (data/dates.json)")
    ap.add_argument("--limit", type=int, default=None,
                    help="process only a strided sample of N records (smoke test)")
    ap.add_argument("--use-llm", action="store_true",
                    help="escalate hard records to the gated LLM normalizer (BILLED)")
    ap.add_argument("--llm-max", type=int, default=0,
                    help="hard cap on LLM calls this run (budget guardrail; 0 = none)")
    ap.add_argument("--print", dest="print_rid", default=None,
                    help="after running, pretty-print one record by rid")
    a = ap.parse_args()

    s = run(limit=a.limit, use_llm=a.use_llm, llm_max=a.llm_max)

    if a.print_rid:
        data = json.loads((config.DATA / "dates.json").read_text())
        print("\n" + json.dumps(data.get(a.print_rid, {"error": f"rid {a.print_rid} not found"}),
                                 ensure_ascii=False, indent=2))
