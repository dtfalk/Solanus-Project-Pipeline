"""stages/geocode_precise.py — upgrade institution/address PLACE pins from CITY centroid to the actual
BUILDING location (free, Nominatim). geocode_places.py deliberately rolls buildings up to their city; this
puts the marker back on the real spot when the full name/address resolves precisely AND lands inside the
right city (a sanity radius guards against Nominatim matching the name to the wrong town).

Writes per-entity coords into data/place_enrichment.json (the side-car the API overlays). Non-destructive:
it never touches the entity store or geocodes.json; the city centroid stays as the fallback.

    venv/bin/python stages/geocode_precise.py            # all institution/address places
    venv/bin/python stages/geocode_precise.py --limit 40 # bounded test run
"""
from __future__ import annotations
import argparse, json, math, re, sys, time, urllib.parse, urllib.request
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402

SIDE_CAR = config.DATA / "place_enrichment.json"
STORE = config.DATA / "entity_store.json"
GEOCODES = config.DATA / "geocodes.json"
ENRICHED = config.DATA / "entities_enriched.json"
_UA = "solanus-archive-tool/1.0 (research; contact via repo)"

# a place whose name is JUST a city/town needs no upgrade — only buildings/institutions/addresses do.
_INST = re.compile(r"\b(friary|monastery|convent|hospital|clinic|sanitarium|sanatorium|asylum|church|"
                   r"cathedral|chapel|shrine|basilica|college|university|seminary|academy|school|"
                   r"orphanage|cemetery|infirmary|home|institute|parish|rectory|hall|sanctuary)\b", re.I)
_ADDR = re.compile(r"^\s*\d{1,5}\s+\S")          # starts with a street number
_ABBR = [(r"\bMt\.?\b", "Mount"), (r"\bSt\.?\b(?=\s)", "Saint"), (r"\bAve\.?\b", "Avenue"),
         (r"\bRd\.?\b", "Road"), (r"\bBlvd\.?\b", "Boulevard"), (r"\bFt\.?\b", "Fort")]


def _haversine_km(a, b):
    (la1, lo1), (la2, lo2) = a, b
    R = 6371.0
    p1, p2 = math.radians(la1), math.radians(la2)
    dp, dl = math.radians(la2 - la1), math.radians(lo2 - lo1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def _nominatim(q):
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(
        {"q": q, "format": "json", "limit": 1, "addressdetails": 1})
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        r = json.load(urllib.request.urlopen(req, timeout=20)) or []
        if r:
            return float(r[0]["lat"]), float(r[0]["lon"]), r[0].get("display_name", "")
    except Exception as e:
        print(f"  [warn] {q!r}: {str(e)[:70]}")
    return None


def _variants(label, loc, address_hint=None):
    """Query strings to try, best first. The label is usually just the institution NAME (no city), so we
    must append the `location` city for Nominatim to resolve it. address_hint (from the research pass) wins."""
    out = []
    if address_hint:
        out.append(address_hint)
    base = label.replace(" — ", ", ").replace("—", ", ").strip(" ,")
    # append the city unless the label already carries it (street addresses already include the city)
    city_tok = loc.split(",")[0].strip().lower()
    combined = base if (city_tok and city_tok in base.lower()) else (base + ", " + loc)
    out.append(combined)
    # abbreviation-expanded ("Mt." -> "Mount", "Ave." -> "Avenue", "St " -> "Saint ")
    exp = combined
    for pat, rep in _ABBR:
        exp = re.sub(pat, rep, exp)
    exp = re.sub(r"\bSt\.?\s", "Saint ", exp)
    if exp != combined:
        out.append(exp)
    seen, uniq = set(), []
    for q in out:
        q = re.sub(r"\s+", " ", q).strip(" ,")
        if q and q.lower() not in seen:
            seen.add(q.lower()); uniq.append(q)
    return uniq


def run(limit=None, radius_km=18.0, only_ids=None, hints_only=False):
    side = json.loads(SIDE_CAR.read_text()) if SIDE_CAR.exists() else {}
    store = json.loads(STORE.read_text()).get("entities", []) if STORE.exists() else []
    enriched = json.loads(ENRICHED.read_text()) if ENRICHED.exists() else {}
    geocodes = json.loads(GEOCODES.read_text()) if GEOCODES.exists() else {}

    targets = []
    for e in store:
        eid = e.get("id")
        enr = enriched.get(eid) or {}
        if not enr.get("is_place"):
            continue
        label = enr.get("canonical_name") or e.get("canonical_name") or ""
        loc = (enr.get("location") or "").strip()
        if not loc or len(loc) < 3:
            continue
        is_building = bool(_INST.search(label) or _ADDR.search(label))
        addr_hint = (side.get(eid) or {}).get("address_hint")   # may be set by the research pass
        if not (is_building or addr_hint):
            continue
        if only_ids and eid not in only_ids:
            continue
        # the city centroid we must stay near (from the existing geocode cache)
        city = geocodes.get(loc)
        if not city:
            continue
        targets.append((eid, label, loc, (city["lat"], city["lon"]), addr_hint))

    # skip ones already precisely geocoded; --hints-only restricts to places the research pass gave an address
    todo = [t for t in targets if not (side.get(t[0]) or {}).get("precise")]
    if hints_only:
        todo = [t for t in todo if (side.get(t[0]) or {}).get("address_hint")]
    if limit:
        todo = todo[:limit]
    print(f"geocode_precise: {len(targets)} building/address places, {len(todo)} to resolve")

    resolved = 0
    for i, (eid, label, loc, city_ll, hint) in enumerate(todo, 1):
        best = None
        for q in _variants(label, loc, hint):
            hit = _nominatim(q)
            time.sleep(1.05)                                   # Nominatim policy: <= 1 req/s
            if not hit:
                continue
            d = _haversine_km(city_ll, (hit[0], hit[1]))
            if d <= radius_km:                                 # must land in the right city
                best = {"lat": hit[0], "lon": hit[1], "precise": True,
                        "address": hit[2], "geocode_query": q, "km_from_city": round(d, 2)}
                break
        rec = side.get(eid, {})
        if best:
            rec.update(best); resolved += 1
            print(f"  [{i}/{len(todo)}] ✓ {label[:46]:46}  ({best['km_from_city']} km)")
        else:
            rec.setdefault("precise", False)
        side[eid] = rec
        if i % 10 == 0:
            SIDE_CAR.write_text(json.dumps(side, ensure_ascii=False, indent=1))
    SIDE_CAR.write_text(json.dumps(side, ensure_ascii=False, indent=1))
    print(f"geocode_precise: {resolved}/{len(todo)} upgraded to building-level -> {SIDE_CAR.name}")
    return {"targets": len(targets), "todo": len(todo), "resolved": resolved}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--radius-km", type=float, default=18.0)
    ap.add_argument("--hints-only", action="store_true", help="only places with a researched address_hint")
    a = ap.parse_args()
    run(limit=a.limit, radius_km=a.radius_km, hints_only=a.hints_only)
