"""stages/name_authority.py — link canonical PERSON/PLACE entities to global name authorities.

Walkthrough TODO #2 ("TOC / Wikidata name authority"). For each canonical person/place we ask two
FREE, KEYLESS authority services for a ground-truth identifier + spelling, then keep the link only if
it survives a type check and a fuzzy-name check:

  • Wikidata `wbsearchentities` (search) + `wbgetentities` (claims/labels/aliases for type-verification)
      - persons must be P31 = Q5 (human); places must carry coordinates (P625) or a place-class P31.
        This is what rejects "Grace" -> the *given name* item, "Detroit" -> a song, etc.
  • VIAF `AutoSuggest` REST (nametype personal / geographic / corporate).

The curated TABLES OF CONTENTS (step_6/documents.json `recipient` / `parent_doc`) are loaded as
ground-truth recipient spellings: a person that matches a TOC name is flagged `toc_confirmed` and the
TOC spelling is preferred as the lookup string (cleaner than an OCR'd canonical_name).

NON-DESTRUCTIVE: this NEVER touches data/entity_store.json. It writes a NEW side-car artifact,
data/name_authority.json, keyed by entity id — {qid, viaf, authority_name, confidence, confident,...}.
An existing artifact is backed up to data/.backups/ before being overwritten. All API responses are
cached to data/.cache/name_authority_cache.json so re-runs are free + polite (resumable).

This stage spends $0 on LLMs by default (pure difflib fuzzy match + the two free APIs); --max-cost is
accepted for interface parity and the run aborts if it were ever exceeded.

    venv/bin/python stages/name_authority.py --sample 30          # validate on the top entities
    venv/bin/python stages/name_authority.py --min-mentions 2     # full recurring pass
    venv/bin/python stages/name_authority.py --min-mentions 1     # complete set (resumable; slow/polite)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                # noqa: E402
from lib import costlog      # noqa: E402

# ---------------------------------------------------------------- paths / constants
STORE = config.DATA / "entity_store.json"
DOCUMENTS = config.DOCUMENTS                       # step_6/documents.json (curated TOC recipients)
OUT = config.DATA / "name_authority.json"
CACHE = config.DATA / ".cache" / "name_authority_cache.json"
BACKUPS = config.DATA / ".backups"

# Wikimedia asks every client to send an identifying User-Agent with a contact (policy:User-Agent).
_UA = ("solanus-archive-name-authority/1.0 "
       "(https://solanuscasey.org; davidtobiasfalk@gmail.com) python-urllib")
_WD_API = "https://www.wikidata.org/w/api.php"
_VIAF_AUTOSUGGEST = "https://viaf.org/viaf/AutoSuggest"

# Wikidata P31 classes that count as a "place" even when a node has no P625 coordinate of its own.
# (Coordinates are the primary signal; this is a backstop for administrative/holy/abstract places.)
_PLACE_P31 = {
    "Q515", "Q486972", "Q3957", "Q532", "Q15284", "Q1549591",   # city/settlement/town/village/municipality/big city
    "Q5119", "Q1093829", "Q1637706", "Q15978299",               # capital/city in the US/city of millions/etc
    "Q6256", "Q3624078", "Q35657", "Q107390",                   # country/sovereign state/US state/federal state
    "Q34876", "Q13220204", "Q149621", "Q1799794",               # province/county/district/admin-territorial
    "Q23442", "Q40080", "Q82794", "Q2221906",                   # island/protected area/geographic region/geo-location
    "Q4022", "Q23397", "Q8502", "Q46831", "Q54050",             # river/lake/mountain/range/hill
}
# P31 classes that are SELF-IDENTIFYING places (a city named X *is* that city). Institutions
# (a "St. Michael's Church") are NOT — there are thousands — so they need location corroboration.
_SETTLEMENT_P31 = {
    "Q515", "Q486972", "Q3957", "Q532", "Q15284", "Q1549591", "Q5119", "Q1093829", "Q1637706",
    "Q15978299", "Q6256", "Q3624078", "Q35657", "Q107390", "Q34876", "Q13220204", "Q149621",
    "Q1799794", "Q23442", "Q40080", "Q82794", "Q2221906", "Q4022", "Q23397", "Q8502", "Q46831", "Q54050",
}
# words that mark a place as an INSTITUTION/building (ambiguous by name -> require corroboration)
_INSTITUTION_WORDS = {
    "church", "hospital", "monastery", "friary", "chapel", "cathedral", "basilica", "college",
    "university", "school", "seminary", "academy", "asylum", "sanatorium", "sanitarium", "home",
    "institute", "clinic", "parish", "shrine", "rectory", "convent", "abbey", "priory", "infirmary",
    "orphanage", "library", "museum", "hall", "center", "centre", "house",
}
# generic single-word "places" that are not specific enough to authority-link (avoid false pins/links)
_GENERIC_PLACE = {
    "monastery", "friary", "hospital", "church", "chapel", "cathedral", "rectory", "convent",
    "school", "college", "university", "academy", "seminary", "parish", "shrine", "cemetery",
    "home", "house", "heaven", "hell", "purgatory", "earth", "world", "city", "town", "village",
    "downtown", "uptown", "the city", "the monastery", "the hospital", "the church",
}
# honorifics / titles stripped from the head of a personal name before lookup
_HONORIFICS = [
    "very rev. fr.", "rt. rev. msgr.", "very rev.", "rt. rev.", "the most rev.", "most rev.",
    "rev. fr.", "rev. msgr.", "rev. mother", "rev. sr.", "rev. bro.", "reverend",
    "rev.", "msgr.", "monsignor", "father", "fr.", "mother", "m3r.", "mgr.",
    "sister", "sr.", "brother", "bro.", "mrs.", "mr.", "ms.", "miss", "dr.", "prof.",
    "saint", "st.", "blessed", "ven.", "venerable", "pope", "bishop", "archbishop", "cardinal",
]
# trailing post-nominals / order initials stripped before lookup
_POSTNOMINAL_RE = re.compile(
    r"\b(o\.?f\.?m\.?(\s*cap\.?)?|o\.?s\.?b\.?|s\.?j\.?|c\.?ss\.?r\.?|c\.?s\.?c\.?|c\.?p\.?p\.?s\.?|"
    r"o\.?p\.?|o\.?carm\.?|m\.?d\.?|ph\.?d\.?|d\.?d\.?|s\.?t\.?d\.?|m\.?a\.?|esq\.?|jr\.?|sr\.?|"
    r"ii|iii|iv)\b\.?", re.I)
_PAREN_RE = re.compile(r"\([^)]*\)")
_LIFEDATES_RE = re.compile(r",?\s*\(?\d{3,4}\s*[-–]\s*\d{0,4}\)?\.?$")   # "Solanus Casey, 1870-1957"


# ---------------------------------------------------------------- polite HTTP
class RateLimiter:
    """Thread-safe minimum-interval limiter shared across worker threads (global req/s cap)."""

    def __init__(self, rps: float):
        self._min = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
                now = time.monotonic()
            self._next = max(now, self._next) + self._min


_HTTP_LOCK = threading.Lock()
_HTTP_CALLS = 0
_HTTP_429 = 0
_FAIL = object()      # sentinel: HTTP call failed (429/timeout/etc). DISTINCT from a valid empty body.


def _http_json(url: str, limiter: RateLimiter, tries: int = 5):
    """GET JSON with UA + retry/backoff on 429/5xx. Returns parsed JSON on success, or the _FAIL
    sentinel on hard failure (so callers never cache a failure as a real 'no result')."""
    global _HTTP_CALLS, _HTTP_429
    backoff = 2.0
    for attempt in range(tries):
        limiter.wait()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                with _HTTP_LOCK:
                    _HTTP_CALLS += 1
                return json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                with _HTTP_LOCK:
                    _HTTP_429 += 1
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                ra = e.headers.get("Retry-After")
                wait = float(ra) if (ra and str(ra).isdigit()) else backoff
                time.sleep(min(wait, 10.0))                 # cap: never block a worker for a long header value
                backoff = min(backoff * 2, 10.0)
                continue
            return _FAIL
        except Exception:
            if attempt < tries - 1:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            return _FAIL


# ---------------------------------------------------------------- persistent cache (resumable)
_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()
_CACHE_VERSION = 2                          # bump when a cached record's schema changes (auto-invalidates)


def _load_cache() -> None:
    global _CACHE
    if CACHE.exists():
        try:
            _CACHE = json.loads(CACHE.read_text())
        except Exception:
            _CACHE = {}
    if _CACHE.get("_version") != _CACHE_VERSION:
        _CACHE = {"_version": _CACHE_VERSION}      # schema changed -> drop stale records, re-fetch
    _CACHE["_version"] = _CACHE_VERSION
    _CACHE.setdefault("wd_search", {})     # name -> [{id,label,description}]
    _CACHE.setdefault("wd_entity", {})     # qid  -> {p31,coord,lat,lon,sitelinks,label,aliases}
    _CACHE.setdefault("viaf", {})          # name -> [{viafid,term,nametype}]


def _save_cache() -> None:
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_suffix(".tmp")
    with _CACHE_LOCK:
        tmp.write_text(json.dumps(_CACHE, ensure_ascii=False))
    tmp.replace(CACHE)


# ---------------------------------------------------------------- name normalization + similarity
def _strip_honorifics(name: str) -> str:
    s = " " + name.strip().lower() + " "
    changed = True
    while changed:                                            # peel stacked titles ("Very Rev. Fr.")
        changed = False
        for h in _HONORIFICS:
            if s.startswith(" " + h + " "):
                s = " " + s[len(" " + h):].lstrip()
                changed = True
    return s.strip()


def _norm(name: str, is_place: bool = False) -> str:
    """Lowercase, drop parentheticals/post-nominals/life-dates/honorifics, keep letters+digits+spaces."""
    if not name:
        return ""
    s = _PAREN_RE.sub(" ", name)
    s = _LIFEDATES_RE.sub("", s)
    if not is_place:
        s = _strip_honorifics(s)
        s = _POSTNOMINAL_RE.sub(" ", s)
    else:
        s = s.lower()
    s = s.replace("&", " and ").replace("’", "'")
    s = re.sub(r"[^a-z0-9' ]+", " ", s)
    s = re.sub(r"\b(the|of|and|a)\b", " ", s) if is_place else s
    s = re.sub(r"\s+", " ", s).strip(" '")
    return s


def _haversine(a: tuple, b: tuple) -> float:
    """Great-circle distance in km between (lat,lon) pairs."""
    import math
    (la1, lo1), (la2, lo2) = a, b
    r = 6371.0
    p1, p2 = math.radians(la1), math.radians(la2)
    dphi, dlam = math.radians(la2 - la1), math.radians(lo2 - lo1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(h)))


def _sim(a: str, b: str) -> float:
    """Blend of sequence ratio, token-sorted ratio and token Jaccard (0..1). Order-insensitive."""
    if not a or not b:
        return 0.0
    seq = SequenceMatcher(None, a, b).ratio()
    ta, tb = set(a.split()), set(b.split())
    sa, sb = " ".join(sorted(ta)), " ".join(sorted(tb))
    tsr = SequenceMatcher(None, sa, sb).ratio()
    jac = len(ta & tb) / len(ta | tb) if (ta | tb) else 0.0
    contain = 1.0 if (ta and tb and (ta <= tb or tb <= ta)) else 0.0   # full token-subset
    return max(0.6 * max(seq, tsr) + 0.4 * jac, 0.55 + 0.4 * jac if contain else 0.0)


# ---------------------------------------------------------------- Wikidata
def _wd_search(name: str, limit: int, limiter: RateLimiter) -> list:
    key = name.lower()
    with _CACHE_LOCK:
        if key in _CACHE["wd_search"]:
            return _CACHE["wd_search"][key]
    q = urllib.parse.urlencode({"action": "wbsearchentities", "search": name, "language": "en",
                                "uselang": "en", "format": "json", "limit": limit, "type": "item"})
    d = _http_json(_WD_API + "?" + q, limiter)
    if d is _FAIL or not isinstance(d, dict):
        return []                                           # HTTP failure -> don't cache; retry next run
    hits = [{"id": h["id"], "label": h.get("label", ""), "description": h.get("description", "")}
            for h in (d.get("search") or [])]
    with _CACHE_LOCK:
        _CACHE["wd_search"][key] = hits
    return hits


def _wd_entities(qids: list, limiter: RateLimiter) -> dict:
    """Fetch P31/coords/sitelinks/label/aliases for QIDs (per-id cached; missing fetched in batches)."""
    out, need = {}, []
    with _CACHE_LOCK:
        for q in qids:
            if q in _CACHE["wd_entity"]:
                out[q] = _CACHE["wd_entity"][q]
            else:
                need.append(q)
    for i in range(0, len(need), 50):
        batch = need[i:i + 50]
        q = urllib.parse.urlencode({"action": "wbgetentities", "ids": "|".join(batch),
                                    "props": "claims|labels|aliases|sitelinks", "languages": "en",
                                    "format": "json"})
        d = _http_json(_WD_API + "?" + q, limiter)
        if d is _FAIL or not isinstance(d, dict):
            continue                                        # HTTP failure -> leave uncached; retry next run
        ents = d.get("entities", {}) or {}
        for qid in batch:
            ent = ents.get(qid, {}) or {}
            claims = ent.get("claims", {}) or {}
            p31 = [c["mainsnak"]["datavalue"]["value"]["id"]
                   for c in claims.get("P31", []) if c.get("mainsnak", {}).get("datavalue")]
            lat = lon = None
            for c in claims.get("P625", []):
                v = (c.get("mainsnak", {}).get("datavalue") or {}).get("value")
                if v and "latitude" in v:
                    lat, lon = v.get("latitude"), v.get("longitude")
                    break
            label = (ent.get("labels", {}).get("en", {}) or {}).get("value", "")
            aliases = [a.get("value", "") for a in (ent.get("aliases", {}).get("en", []) or [])]
            rec = {"p31": p31, "coord": "P625" in claims, "lat": lat, "lon": lon,
                   "sitelinks": len(ent.get("sitelinks", {}) or {}), "label": label, "aliases": aliases}
            with _CACHE_LOCK:
                _CACHE["wd_entity"][qid] = rec
            out[qid] = rec
    return out


# ---------------------------------------------------------------- VIAF
# VIAF AutoSuggest 429s hard on bursts and then keeps a cool-down window. A circuit breaker stops us
# from hammering a blocked endpoint: after a few consecutive failures we DISABLE VIAF for the rest of
# the run (cached hits still used; the rest marked viaf_failed for a later polite backfill).
_VIAF_DISABLED = False
_VIAF_CONSEC_FAIL = 0
_VIAF_FAIL_LIMIT = 5


def _viaf(name: str, limiter: RateLimiter):
    """VIAF AutoSuggest. Returns a (possibly empty) result list on success, or None if the lookup is
    unavailable (HTTP failure / circuit-breaker disabled) — None is never cached, so a polite re-run
    fills it (no false negatives)."""
    global _VIAF_DISABLED, _VIAF_CONSEC_FAIL
    key = name.lower()
    with _CACHE_LOCK:
        if key in _CACHE["viaf"]:
            return _CACHE["viaf"][key]
    if _VIAF_DISABLED:
        return None                                         # circuit open -> don't touch the blocked API
    d = _http_json(_VIAF_AUTOSUGGEST + "?query=" + urllib.parse.quote(name), limiter, tries=1)
    if d is _FAIL or not isinstance(d, dict):
        with _HTTP_LOCK:
            _VIAF_CONSEC_FAIL += 1
            if _VIAF_CONSEC_FAIL >= _VIAF_FAIL_LIMIT and not _VIAF_DISABLED:
                _VIAF_DISABLED = True
                print(f"  [viaf] DISABLED after {_VIAF_CONSEC_FAIL} consecutive failures (429 cool-down). "
                      f"Finishing with Wikidata + cached VIAF; backfill later: "
                      f"--no-wikidata (cached WD is free) once the block clears.")
        return None                                         # rate-limited / failed -> unknown, don't cache
    with _HTTP_LOCK:
        _VIAF_CONSEC_FAIL = 0                               # a success resets the breaker
    res = [{"viafid": r.get("viafid"), "term": r.get("term", ""), "nametype": r.get("nametype", "")}
           for r in (d.get("result") or [])]
    with _CACHE_LOCK:
        _CACHE["viaf"][key] = res
    return res


# ---------------------------------------------------------------- candidate selection
def _lookup_names(ent: dict) -> list:
    """Ordered, de-duplicated lookup strings for an entity (best first)."""
    is_place = ent.get("type") == "PLACE"
    u = ent.get("understanding") or {}
    cands = []
    for v in (ent.get("toc_name"), u.get("canonical_name"), ent.get("canonical_name")):
        if v and v not in cands:
            cands.append(v)
    if is_place:
        # also try the first comma-segment ("Toledo, Ohio" -> "Toledo") for settlements
        for base in list(cands):
            seg = base.split(",")[0].strip()
            if seg and seg not in cands:
                cands.append(seg)
    return cands


def _eligible(ent: dict) -> tuple[bool, str]:
    """Should we even query this entity? Returns (ok, reason_if_not)."""
    is_place = ent.get("type") == "PLACE"
    raw = (ent.get("understanding") or {}).get("canonical_name") or ent.get("canonical_name") or ""
    n = _norm(raw, is_place=is_place)
    if len(n) < 3:
        return False, "name_too_short"
    if is_place:
        if n in _GENERIC_PLACE:
            return False, "generic_place"
        return True, ""
    # persons: need >= 2 alpha tokens to avoid linking bare given names ("Grace", "Edward")
    toks = [t for t in n.split() if any(c.isalpha() for c in t)]
    if len(toks) < 2:
        return False, "mononym_or_role"
    return True, ""


# ---------------------------------------------------------------- TOC recipient authority
def _clean_toc_name(s: str) -> str:
    """Turn a raw TOC recipient label into a person-name guess; '' if it isn't one."""
    if not s:
        return ""
    t = s.strip()
    t = re.sub(r"^\s*\(?\s*(for|to|card to|cards? for|copy of|copies of)\b[:\s]*", "", t, flags=re.I)
    t = _PAREN_RE.sub(" ", t)
    t = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", t)                 # leading "1. " / "2) "
    t = re.sub(r"^\s*(to\s+(niece|nephew|nephew-in-law|grand-?nephew|grandniece|cousin|sister|brother)[,:]?\s*)",
               "", t, flags=re.I)
    t = re.sub(r"^\s*dear\s+", "", t, flags=re.I)
    t = t.replace("(", " ").replace(")", " ")                # drop any stray unbalanced parens
    t = re.sub(r"\s+", " ", t).strip(" -:.,\"'")
    if not t or "poem" in t.lower() or "photo" in t.lower() or len(t) < 3:
        return ""
    # must look like a name: at least 2 letter-tokens once honorifics are stripped
    toks = [w for w in _norm(t).split() if any(c.isalpha() for c in w)]
    return t if len(toks) >= 2 else ""


def _load_toc_names() -> list:
    if not DOCUMENTS.exists():
        return []
    docs = json.loads(DOCUMENTS.read_text())
    seen, out = set(), []
    for d in docs:
        for field in ("recipient", "parent_doc"):
            cleaned = _clean_toc_name(d.get(field) or "")
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                out.append(cleaned)
    return out


def _best_toc(ent: dict, toc_norm: list) -> tuple[str | None, float]:
    """Best curated TOC spelling for a PERSON entity (name, sim). toc_norm = [(norm, original)]."""
    if ent.get("type") != "PERSON":
        return None, 0.0
    names = [ent.get("canonical_name", "")] + (ent.get("variants") or [])
    best_name, best = None, 0.0
    for nm in names:
        nn = _norm(nm)
        if not nn:
            continue
        for tn, orig in toc_norm:
            s = _sim(nn, tn)
            if s > best:
                best, best_name = s, orig
    return best_name, best


# ---------------------------------------------------------------- matching one entity
def _is_institution(ent: dict) -> bool:
    """A named building/institution (church, hospital, ...) — ambiguous by name, needs corroboration."""
    blob = _norm((ent.get("understanding") or {}).get("canonical_name") or ent.get("canonical_name") or "",
                 is_place=True)
    return bool(set(blob.split()) & _INSTITUTION_WORDS)


def _match_entity(ent: dict, *, wd_limit: int, use_wd: bool, use_viaf: bool,
                  person_min_sitelinks: int, place_corro_km: float, geo_coord: tuple | None,
                  wd_limiter: RateLimiter, viaf_limiter: RateLimiter) -> dict | None:
    is_place = ent.get("type") == "PLACE"
    institution = is_place and _is_institution(ent)
    lookups = _lookup_names(ent)
    primary = _norm(lookups[0], is_place=is_place) if lookups else ""
    # corroboration tokens (place state/country) help disambiguate same-named settlements/institutions
    loc = ((ent.get("understanding") or {}).get("location") or "")
    corro = {t for t in _norm(loc, is_place=True).split() if len(t) > 2}

    wd_best = None
    if use_wd:
        seen_q = []
        for q in lookups:
            for h in _wd_search(q, wd_limit, wd_limiter):
                if h["id"] not in seen_q:
                    seen_q.append(h["id"])
        meta = _wd_entities(seen_q, wd_limiter) if seen_q else {}
        labels = {}                                         # search-cache label/description per qid
        for q in lookups:
            for h in _wd_search(q, wd_limit, wd_limiter):
                labels.setdefault(h["id"], (h.get("label", ""), h.get("description", "")))
        for qid in seen_q:
            m = meta.get(qid, {})
            if is_place:
                type_ok = m.get("coord") or any(p in _PLACE_P31 for p in m.get("p31", []))
            else:
                type_ok = "Q5" in m.get("p31", [])
            if not type_ok:
                continue
            slabel, sdesc = labels.get(qid, (m.get("label", ""), ""))
            names = [slabel, m.get("label", "")] + m.get("aliases", [])
            sim = max((_sim(primary, _norm(x, is_place=is_place)) for x in names if x), default=0.0)
            settlement = is_place and any(p in _SETTLEMENT_P31 for p in m.get("p31", []))
            # ---- location corroboration (places): text tokens OR coordinate proximity ----
            text_corr = False
            if is_place and corro:
                blob = _norm((slabel or "") + " " + (sdesc or ""), is_place=True)
                text_corr = bool(corro & set(blob.split()))
            geo_corr = False
            if is_place and geo_coord and m.get("lat") is not None:
                geo_corr = _haversine(geo_coord, (m["lat"], m["lon"])) <= place_corro_km
            corroborated = text_corr or geo_corr
            conf = sim + (0.06 if (is_place and corroborated) else 0.0)
            conf = min(0.99, conf)
            cand = {"qid": qid, "label": slabel or m.get("label", ""), "description": sdesc,
                    "p31": m.get("p31", []), "sitelinks": m.get("sitelinks", 0),
                    "settlement": settlement, "corroborated": corroborated,
                    "sim": round(sim, 3), "confidence": round(conf, 3)}
            if wd_best is None or conf > wd_best["confidence"]:
                wd_best = cand

    viaf_best = None
    viaf_failed = False
    if use_viaf:
        want = "geographic" if is_place else "personal"
        for q in lookups[:1]:                               # one VIAF call per entity (be gentle)
            res = _viaf(q, viaf_limiter)
            if res is None:                                 # HTTP failure (e.g. 429) -> unknown, not "no match"
                viaf_failed = True
                continue
            for r in res:
                if (r.get("nametype") or "") != want:
                    continue
                term = _LIFEDATES_RE.sub("", r.get("term", ""))
                sim = _sim(primary, _norm(term, is_place=is_place))
                if viaf_best is None or sim > viaf_best["sim"]:
                    viaf_best = {"viafid": r.get("viafid"), "term": r.get("term", ""),
                                 "nametype": r.get("nametype"), "sim": round(sim, 3),
                                 "confidence": round(sim, 3)}

    if not wd_best and not viaf_best:
        return None                                         # nothing useful (viaf_failed is tracked globally)

    confs = [b["confidence"] for b in (wd_best, viaf_best) if b]
    confidence = max(confs)
    if wd_best and viaf_best and min(confs) >= 0.78:        # cross-source agreement boost
        confidence = min(0.99, confidence + 0.04)

    # ---- gate: is this strong enough to *assert* an identity (vs. just record a candidate)? ----
    if is_place:
        # settlements/features self-identify by name+type; institutions must be corroborated.
        eligible = bool(wd_best) and (wd_best["corroborated"] if institution else
                                      (wd_best["settlement"] or wd_best.get("sim", 0) >= 0.9))
        gate_reason = ("institution_uncorroborated" if (institution and wd_best and not wd_best["corroborated"])
                       else ("ok" if eligible else "weak_place"))
    else:
        # persons: only assert when matched to a genuinely NOTABLE Wikidata human (sitelink count).
        # ordinary correspondents who merely share a name with a Wikidata person stay non-confident.
        eligible = bool(wd_best) and wd_best.get("sitelinks", 0) >= person_min_sitelinks
        gate_reason = ("ok" if eligible else
                       ("person_not_notable" if wd_best else "no_wikidata_person"))

    return {
        "entity_id": ent["id"], "type": ent["type"],
        "canonical_name": ent.get("canonical_name"),
        "lookup_name": lookups[0] if lookups else None,
        "is_institution": institution,
        "toc_name": ent.get("toc_name"),
        "toc_confirmed": bool(ent.get("toc_name")),
        "qid": wd_best["qid"] if wd_best else None,
        "viaf": (str(viaf_best["viafid"]) if viaf_best and viaf_best["viafid"] else None),
        "authority_name": (wd_best["label"] if wd_best else (viaf_best["term"] if viaf_best else None)),
        "confidence": round(confidence, 3),
        "gate_eligible": eligible,
        "gate_reason": gate_reason,
        "viaf_failed": viaf_failed,
        "wikidata": wd_best,
        "viaf_match": viaf_best,
    }


# ---------------------------------------------------------------- main
def run(*, sample: int = 0, min_mentions: int = 2, threshold: float = 0.85,
        types: tuple = ("PERSON", "PLACE"), wd_limit: int = 6, rps: float = 5.0, viaf_rps: float = 1.5,
        workers: int = 4, use_wikidata: bool = True, use_viaf: bool = True, person_min_sitelinks: int = 8,
        place_corro_km: float = 60.0, max_cost: float = 5.0,
        out: Path = OUT, save_every: int = 50, verbose: bool = True) -> dict:
    if max_cost < 0:
        raise SystemExit("name_authority: --max-cost must be >= 0")
    global _VIAF_DISABLED, _VIAF_CONSEC_FAIL, _HTTP_429
    _VIAF_DISABLED, _VIAF_CONSEC_FAIL, _HTTP_429 = False, 0, 0     # fresh breaker per run()
    config.DATA.mkdir(parents=True, exist_ok=True)
    BACKUPS.mkdir(parents=True, exist_ok=True)
    _load_cache()

    store = json.loads(STORE.read_text())
    ents_all = store.get("entities", [])
    geocodes = json.loads((config.DATA / "geocodes.json").read_text()) if (config.DATA / "geocodes.json").exists() else {}

    def mc(e):
        return len(e.get("mentions", []))

    cand = [e for e in ents_all if e.get("type") in types and mc(e) >= min_mentions]
    cand.sort(key=mc, reverse=True)
    if sample:                                               # top-N per type (notability proxy)
        per = {}
        picked = []
        for e in cand:
            per.setdefault(e["type"], 0)
            if per[e["type"]] < sample:
                per[e["type"]] += 1
                picked.append(e)
        cand = picked

    # attach curated TOC spelling to person candidates (ground-truth recipient names)
    toc_names = _load_toc_names()
    toc_norm = [(_norm(t), t) for t in toc_names]
    toc_hits = 0
    for e in cand:
        nm, s = _best_toc(e, toc_norm)
        if nm and s >= 0.9:
            e["toc_name"] = nm
            toc_hits += 1

    # eligibility filter
    todo, skipped = [], {}
    for e in cand:
        ok, why = _eligible(e)
        (todo.append(e) if ok else skipped.__setitem__(why, skipped.get(why, 0) + 1))

    if verbose:
        print(f"name_authority: {len(cand)} candidates ({dict_count(cand)}) | "
              f"{len(todo)} eligible to query | skipped {sum(skipped.values())} {skipped}")
        print(f"  TOC recipient names loaded: {len(toc_names)}; matched to {toc_hits} candidates")
        print(f"  APIs: wikidata={use_wikidata}(rps={rps}) viaf={use_viaf}(rps={viaf_rps}) | "
              f"workers={workers} threshold={threshold}")

    # Separate limiters: Wikidata tolerates a brisk rate; VIAF AutoSuggest 429s on bursts, so it gets
    # its own much gentler limiter (a single shared bucket -> effectively serialized + slow).
    wd_limiter = RateLimiter(rps)
    viaf_limiter = RateLimiter(viaf_rps)
    matches: dict = {}
    done = 0

    def _geo(e):
        loc = ((e.get("understanding") or {}).get("location") or "").strip()
        g = geocodes.get(loc) if loc else None
        return (g["lat"], g["lon"]) if (g and g.get("lat") is not None) else None

    def work(e):
        return _match_entity(e, wd_limit=wd_limit, use_wd=use_wikidata, use_viaf=use_viaf,
                             person_min_sitelinks=person_min_sitelinks, place_corro_km=place_corro_km,
                             geo_coord=_geo(e), wd_limiter=wd_limiter, viaf_limiter=viaf_limiter)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(work, e): e for e in todo}
        for fut in _as_completed(futs):
            done += 1
            try:
                r = fut.result()
            except Exception as ex_err:                     # never let one entity kill the run
                r = None
                if verbose:
                    print(f"  [warn] {futs[fut].get('id')}: {str(ex_err)[:80]}")
            if r:
                r["confident"] = bool(r["confidence"] >= threshold and r["gate_eligible"]
                                      and (r["qid"] or r["viaf"]))
                matches[r["entity_id"]] = r
            if done % save_every == 0:
                _save_cache()
                if verbose:
                    nconf = sum(1 for m in matches.values() if m["confident"])
                    print(f"  ... {done}/{len(todo)}  matches={len(matches)} confident={nconf} "
                          f"http_calls={_HTTP_CALLS}")
    _save_cache()

    # ---- assemble + write the side-car artifact (non-destructive) ----
    confident = {k: v for k, v in matches.items() if v["confident"]}
    by_type = lambda pred: {  # noqa: E731
        t: sum(1 for v in confident.values() if v["type"] == t and pred(v)) for t in types}
    summary = {
        "candidates": len(cand),
        "eligible": len(todo),
        "skipped": skipped,
        "with_any_match": len(matches),
        "confident": len(confident),
        "confident_qid": by_type(lambda v: v["qid"]),
        "confident_viaf": by_type(lambda v: v["viaf"]),
        "confident_total_by_type": {t: sum(1 for v in confident.values() if v["type"] == t) for t in types},
        "toc_confirmed": sum(1 for v in matches.values() if v.get("toc_confirmed")),
        "any_qid": sum(1 for v in matches.values() if v.get("qid")),
        "any_viaf": sum(1 for v in matches.values() if v.get("viaf")),
        "viaf_lookups_failed": sum(1 for v in matches.values() if v.get("viaf_failed")),
        "http_429": _HTTP_429,
        "viaf_disabled": _VIAF_DISABLED,
    }
    meta = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "stage": "name_authority",
        "source_store": str(STORE.name),
        "source_store_meta": store.get("_meta", {}),
        "toc_source": str(DOCUMENTS),
        "non_destructive": True,
        "note": "Side-car. entity_store.json is NOT modified. Apply confident links downstream "
                "(entity.authority.wikidata/viaf) at or above the threshold.",
        "params": {"min_mentions": min_mentions, "threshold": threshold, "types": list(types),
                   "wd_limit": wd_limit, "rps": rps, "viaf_rps": viaf_rps, "sample": sample,
                   "person_min_sitelinks": person_min_sitelinks, "place_corro_km": place_corro_km,
                   "use_wikidata": use_wikidata, "use_viaf": use_viaf},
        "apis": ["wikidata:wbsearchentities", "wikidata:wbgetentities", "viaf:AutoSuggest"],
        "user_agent": _UA,
        "http_calls": _HTTP_CALLS,
        "http_429": _HTTP_429,
        "llm_cost_usd": 0.0,
        "summary": summary,
    }
    artifact = {"_meta": meta, "matches": matches}

    out = Path(out)
    if out.exists():                                         # back up before overwriting
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = BACKUPS / f"{out.stem}_{ts}.json"
        bak.write_bytes(out.read_bytes())
        if verbose:
            print(f"  backed up existing {out.name} -> {bak}")
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(artifact, ensure_ascii=False, indent=1))
    tmp.replace(out)

    # this stage uses only free, keyless APIs -> $0 LLM spend; record a zero-cost ledger line
    costlog.log(provider="wikidata+viaf", model="name_authority", op="authority_link",
                items=_HTTP_CALLS, usd=0.0, meta=f"min_mentions={min_mentions};sample={sample}")

    if verbose:
        print(f"name_authority: wrote {out} "
              f"(confident {summary['confident']}/{summary['eligible']}; "
              f"qid {summary['confident_qid']}; viaf {summary['confident_viaf']})")
    return {"out": str(out), "summary": summary, "http_calls": _HTTP_CALLS}


# small helpers kept near the bottom so run() reads top-down
def dict_count(ents: list) -> dict:
    c: dict = {}
    for e in ents:
        c[e.get("type")] = c.get(e.get("type"), 0) + 1
    return c


def _as_completed(futs):
    from concurrent.futures import as_completed
    return as_completed(futs)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Link canonical persons/places to Wikidata QIDs + VIAF IDs (free, keyless).")
    ap.add_argument("--sample", type=int, default=0, help="only the top-N per type (by mentions)")
    ap.add_argument("--min-mentions", type=int, default=2, help="skip entities below this mention count")
    ap.add_argument("--threshold", type=float, default=0.85, help="confidence cutoff for a 'confident' link")
    ap.add_argument("--types", nargs="+", default=["PERSON", "PLACE"])
    ap.add_argument("--wd-limit", type=int, default=6, help="Wikidata search candidates per query")
    ap.add_argument("--rps", type=float, default=5.0, help="Wikidata requests/second cap (politeness)")
    ap.add_argument("--viaf-rps", type=float, default=1.5,
                    help="VIAF requests/second cap — kept LOW; VIAF AutoSuggest 429s on bursts")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--no-wikidata", action="store_true")
    ap.add_argument("--no-viaf", action="store_true")
    ap.add_argument("--person-min-sitelinks", type=int, default=8,
                    help="a PERSON link is 'confident' only if the Wikidata item has >= this many "
                         "sitelinks (notability gate that rejects accidental namesakes)")
    ap.add_argument("--place-corro-km", type=float, default=60.0,
                    help="institution places must sit within this many km of their geocoded location")
    ap.add_argument("--max-cost", type=float, default=5.0, help="USD budget cap (LLM unused -> $0)")
    ap.add_argument("--out", type=str, default=str(OUT))
    a = ap.parse_args()
    run(sample=a.sample, min_mentions=a.min_mentions, threshold=a.threshold, types=tuple(a.types),
        wd_limit=a.wd_limit, rps=a.rps, viaf_rps=a.viaf_rps, workers=a.workers,
        use_wikidata=not a.no_wikidata, use_viaf=not a.no_viaf,
        person_min_sitelinks=a.person_min_sitelinks, place_corro_km=a.place_corro_km,
        max_cost=a.max_cost, out=Path(a.out))
