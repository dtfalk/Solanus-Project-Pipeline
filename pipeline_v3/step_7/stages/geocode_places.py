"""stages/geocode_places.py — geocode PLACE entities to lat/long for the map view (free, Nominatim).

The enrichment gives each place a normalized "City, State" location; this resolves those to coordinates
via OpenStreetMap's Nominatim (no API key). It's FREE but rate-limited, so we cache every lookup to
data/geocodes.json (resumable — re-runs only hit new places) and sleep 1s between calls per Nominatim's
usage policy. build_graph attaches lat/long to place nodes; /api/map + the Map tab plot them.

    from stages import geocode_places as g
    g.run(min_mentions=2)        # geocode the recurring places (bounded; resumable)
"""
from __future__ import annotations
import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402

CACHE = config.DATA / "geocodes.json"
ENTITIES = config.DATA / "entities.json"
ENRICHED = config.DATA / "entities_enriched.json"
_UA = "solanus-archive-tool/1.0 (research; contact via repo)"


def _load_cache() -> dict:
    if CACHE.exists():
        try:
            return json.loads(CACHE.read_text())
        except Exception:
            return {}
    return {}


# Audit P2c: labels that must NEVER become a pin — continents/oceans/multi-state regions/geographic
# features. Nominatim WILL string-match these to some random town (Earth->Earth,TX; Central Europe->
# Central, OK), so we reject them outright rather than map a lie.
_VAGUE_LABELS = {"earth", "world", "africa", "europe", "asia", "north america", "south america",
                 "central america", "central europe", "eastern europe", "western europe", "middle east",
                 "the south", "the west", "the east", "the north", "the midwest", "midwest",
                 "western united states", "southwestern united states", "southwest united states",
                 "midwestern united states", "eastern united states", "the orient", "the pacific",
                 "atlantic ocean", "pacific ocean", "the desert", "heaven", "the holy land"}
_VAGUE_RE = re.compile(r"\b(river|lake|mountains?|ocean|sea|continent|region|coast|valley|desert)\b", re.I)
# common localized country names -> the English Nominatim returns in display_name
_COUNTRY_ALIASES = {"italia": "italy", "deutschland": "germany", "españa": "spain", "espana": "spain",
                    "lëtzebuerg": "luxembourg", "letzebuerg": "luxembourg", "россия": "russia",
                    "schweiz": "switzerland", "österreich": "austria", "osterreich": "austria",
                    "magyarország": "hungary", "polska": "poland", "éire": "ireland", "eire": "ireland",
                    "usa": "united states", "u.s.a.": "united states", "u.s.": "united states",
                    "england": "united kingdom", "scotland": "united kingdom", "wales": "united kingdom"}


def _expected_country(query: str) -> str | None:
    """The country implied by the LAST comma-part of a 'City, State, Country' location, normalized."""
    last = query.split(",")[-1].strip().lower()
    return _COUNTRY_ALIASES.get(last, last) or None


def _vague(query: str) -> bool:
    q = query.strip().lower()
    return q in _VAGUE_LABELS or bool(_VAGUE_RE.search(q))


def _geocode(query: str) -> dict | None:
    """One Nominatim lookup -> {lat, lon, display_name} or None. Rejects vague labels and hits whose
    returned country contradicts the requested country. Caller must rate-limit (1/s)."""
    if _vague(query):
        return None                                        # never pin a continent/ocean/region/feature
    want = _expected_country(query)
    params = {"q": query, "format": "json", "limit": 3, "addressdetails": 1}
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        results = json.load(urllib.request.urlopen(req, timeout=15)) or []
        for hit in results:
            disp = hit.get("display_name", "")
            country = (hit.get("address", {}) or {}).get("country", "") or disp.split(",")[-1]
            # reject a hit whose country contradicts the requested country (Central America->Australia etc.)
            if want and want not in country.strip().lower() and country.strip().lower() not in want:
                continue
            return {"lat": float(hit["lat"]), "lon": float(hit["lon"]), "display_name": disp}
    except Exception as e:
        print(f"  [geocode warn] {query!r}: {str(e)[:80]}")
    return None


def run(min_mentions: int = 2, limit: int | None = None) -> dict:
    """Geocode recurring PLACE entities (those with an enriched location or a usable name)."""
    # Prefer the engine store (merged, reclassified entities) over the raw resolve output.
    STORE = config.DATA / "entity_store.json"
    if STORE.exists():
        ents = json.loads(STORE.read_text()).get("entities", [])
    else:
        ents = json.loads(ENTITIES.read_text()).get("entities", [])
    enriched = json.loads(ENRICHED.read_text()) if ENRICHED.exists() else {}
    cache = _load_cache()

    # Audit P2c: purge previously-cached BAD geocodes so the new rules take effect on a re-run.
    vague_purged, mismatch_purged = 0, 0
    for q, v in list(cache.items()):
        if not v:
            continue
        if _vague(q):
            cache[q] = None                                # never a pin; stay unmapped, don't re-query
            vague_purged += 1
            continue
        want = _expected_country(q)
        disp_country = (v.get("display_name", "").split(",")[-1] or "").strip().lower()
        if want and disp_country and want not in disp_country and disp_country not in want:
            del cache[q]                                   # wrong country -> re-geocode under the new rules
            mismatch_purged += 1
    if vague_purged or mismatch_purged:
        print(f"  purged {vague_purged} vague + {mismatch_purged} wrong-country cached geocodes")

    # build the set of unique location queries — gate PURELY on the engine's is_place flag (which already
    # excludes streets/metaphors and rolls buildings up to their city) + a full location. We no longer
    # filter on the original NER type, since the engine reclassifies (a "PLACE" may be a street; a place
    # may have been typed otherwise). No name+", USA" guessing (that turned "Italy" into Italy, Texas).
    queries: dict = {}                                     # query string -> example place name
    skipped_nonplace = 0
    for e in ents:
        if len(e.get("mentions", [])) < min_mentions:
            continue
        enr = enriched.get(e.get("id")) or {}
        if not enr.get("is_place"):
            skipped_nonplace += 1
            continue
        q = (enr.get("location") or "").strip()
        if len(q) < 3:
            continue                                       # no confident location -> don't map it
        queries.setdefault(q, enr.get("canonical_name") or e.get("canonical_name"))
    if skipped_nonplace:
        print(f"  skipped {skipped_nonplace} non-mappable 'places' (metaphors/buildings w/o location)")

    todo = [q for q in queries if q not in cache]
    if limit:
        todo = todo[:limit]
    print(f"geocode_places: {len(queries)} unique queries, {len(todo)} new (rest cached)")
    for i, q in enumerate(todo, 1):
        cache[q] = _geocode(q)                              # may be None (cache the miss too)
        if i % 20 == 0:
            CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
            print(f"  ... {i}/{len(todo)}")
        time.sleep(1.05)                                   # Nominatim policy: <= 1 req/s
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
    hits = sum(1 for v in cache.values() if v)
    print(f"geocode_places: cache has {len(cache)} queries, {hits} resolved -> {CACHE.name}")
    return {"queries": len(queries), "new": len(todo), "resolved": hits, "out": str(CACHE)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Geocode PLACE entities via Nominatim (free, rate-limited).")
    ap.add_argument("--min-mentions", type=int, default=2)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    run(min_mentions=a.min_mentions, limit=a.limit)
