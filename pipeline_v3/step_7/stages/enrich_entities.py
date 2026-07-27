"""stages/enrich_entities.py — give each canonical entity real-world CONTEXT (the graph-quality pass).

Resolution gives us canonical entities, but a node labelled "Mrs. Clairmont" or "S.M.A." tells a
reader nothing, and OCR leaves run-on surfaces ("Mrs. Clairmont (today reported and"). This stage
sends each significant entity's MENTION CONTEXTS (the actual text of the records it appears in) to the
LLM and gets back: a CLEAN canonical name, a one-line description grounded ONLY in those excerpts, the
entity's role, and — crucially for a graph of Solanus's life — its RELATION TO FR. SOLANUS. Places
also get a normalized location for geocoding/mapping.

Design mirrors the NER stage: parallel (latency-bound calls must overlap), resumable (skip already-
enriched ids), every call cost-logged. PAID — gated behind execute=True.

    from stages import enrich_entities as e
    e.run(min_mentions=2, limit=50)                 # dry preview: scope + cost projection ($0)
    e.run(min_mentions=2, execute=True)             # the real pass (billed)

Output: data/entities_enriched.json  ->  {id: {canonical_name, description, role, relation_to_solanus,
location, kind, confidence}}. build_graph reads it to label + connect nodes; never overwrites the
resolver's entities.json.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config              # noqa: E402
from lib import costlog    # noqa: E402
from lib import textclean  # noqa: E402  (de-hyphenate OCR line breaks in the contexts)

ENTITIES = config.DATA / "entities.json"
OUT_PATH = config.DATA / "entities_enriched.json"
ENRICH_MODEL = config.DEFAULTS["llm"]
ENRICH_TYPES = {"PERSON", "PLACE", "ORG"}          # the entities that carry "who/what/where" meaning
MAX_CONTEXTS = 4                                    # representative excerpts per entity (keeps tokens small)
CONTEXT_CHARS = 360


# ==================================================================
# Record-text index — resolve a mention's (doc_id, rid) to its source text
# ==================================================================
def _build_text_index() -> tuple:
    """(letters_text, entry_text): {doc_id -> letter text}, {(page_id, rid) -> entry text}."""
    from lib import chunks as chunks_lib
    letters_text: dict = {}
    for d in json.loads(config.DOCUMENTS.read_text()):
        letters_text[d["id"]] = chunks_lib._letter_text(d)
    entry_text: dict = {}
    for page in json.loads(config.NOTEBOOKS.read_text()):
        for e in page.get("entries", []):
            entry_text[(page["id"], e.get("rid"))] = (e.get("text") or "").strip()
    return letters_text, entry_text


def _contexts_for(ent: dict, letters_text: dict, entry_text: dict) -> list:
    """Up to MAX_CONTEXTS distinct source excerpts where this entity appears (most-informative first)."""
    seen, out = set(), []
    for m in ent.get("mentions", []):
        prov = m.get("provenance", {})
        doc_id, rid = prov.get("doc_id"), prov.get("rid")
        txt = entry_text.get((doc_id, rid)) or letters_text.get(doc_id) or ""
        txt = textclean.clean(txt.strip())
        if not txt or txt in seen:
            continue
        seen.add(txt)
        out.append(txt[:CONTEXT_CHARS])
        if len(out) >= MAX_CONTEXTS:
            break
    return out


# ==================================================================
# The LLM enrichment call (one entity) + structured schema
# ==================================================================
_SYSTEM = (
    "You enrich entities in the Father Solanus Casey archive (1890s-1950s Capuchin friar; his "
    "correspondence and the prayer-favor notebooks of the Seraphic Mass Association). You are given "
    "the entity's name as OCR'd plus excerpts where it appears. Return STRICT JSON, one factual "
    "sentence for the description.\n"
    "You MAY use well-established WORLD KNOWLEDGE about Father Solanus Casey, the Capuchin Order, and "
    "well-known places/people to DISAMBIGUATE and enrich — e.g. that his friaries were St. Bonaventure "
    "(Detroit), St. Felix (Huntington, Indiana), Sacred Heart (Yonkers) and Our Lady of Sorrows "
    "(Brooklyn); that 'S.M.A.' is the Seraphic Mass Association. Use this to give correct full "
    "locations and identify notable figures. BUT for obscure petitioners/people you do not actually "
    "know, stay grounded in the excerpts and use null rather than inventing biography."
)


def enrich_one(ent: dict, contexts: list, model: str | None = None) -> dict:
    """Return the enrichment dict for one entity (fails safe to a minimal record on error)."""
    from lib.providers import llm
    model = model or ENRICH_MODEL
    typ = ent.get("type", "PERSON").lower()
    variants = ", ".join(ent.get("variants", [])[:6])
    ctx = "\n".join(f"- {c}" for c in contexts) or "(no excerpt text available)"
    prompt = (
        f'Entity type: {typ}\nName as OCR\'d: "{ent.get("canonical_name", "")}"\n'
        f'Other spellings seen: {variants or "(none)"}\n\nExcerpts where it appears:\n{ctx}\n\n'
        f'Return JSON: {{'
        f'"canonical_name": "<clean, correctly-spelled display name; drop run-on/OCR noise>", '
        f'"description": "<one grounded factual sentence, or null>", '
        f'"role": "<role/title/kind, e.g. petitioner, priest, doctor, hospital, city, building; or null>", '
        f'"relation_to_solanus": "<correspondent | petitioner | fellow_friar | family | benefactor | '
        f'subject_of_favor | none | unknown>", '
        f'"is_place": <true ONLY if this is a real, mappable geographic location — a city/town/building/'
        f'institution with a real-world location; false for metaphors ("the desert", "heaven"), '
        f'concepts, or generic words>, '
        f'"location": "<the FULL, disambiguated geographic location for mapping, as '
        f'\'City, State/Province, Country\' (e.g. \'Detroit, Michigan, USA\', \'Windsor, Ontario, Canada\', '
        f'\'Rome, Italy\'). For a building/institution give its city: \'Detroit, Michigan, USA\'. Use null '
        f'if not a real place or you cannot confidently place it. NEVER guess a US town for a foreign/'
        f'ambiguous name>", '
        f'"confidence": <0.0-1.0>}}'
    )
    try:
        txt, _ = llm.generate(prompt, model=model, system=_SYSTEM, json_mode=True, temperature=0.1)
        v = json.loads(txt)
        is_place = bool(v.get("is_place")) if v.get("is_place") is not None else (typ == "PLACE")
        loc = v.get("location") if is_place else None
        return {"canonical_name": (v.get("canonical_name") or ent.get("canonical_name") or "").strip(),
                "description": v.get("description"), "role": v.get("role"),
                "relation_to_solanus": v.get("relation_to_solanus"),
                "is_place": is_place, "location": loc,
                "confidence": float(v.get("confidence", 0.0) or 0.0)}
    except Exception as e:
        return {"canonical_name": ent.get("canonical_name"), "description": None, "role": None,
                "relation_to_solanus": "unknown", "is_place": False, "location": None,
                "confidence": 0.0, "error": str(e)[:100]}


# ==================================================================
# Orchestration
# ==================================================================
def _load_done() -> dict:
    if OUT_PATH.exists():
        try:
            return json.loads(OUT_PATH.read_text())
        except Exception:
            return {}
    return {}


# ==================================================================
# Deep research — a richer dossier for the marquee (most-mentioned) entities
# ==================================================================
_DEEP_SYSTEM = (
    "You are a careful historical researcher writing a short dossier on an entity from the Father "
    "Solanus Casey archive. Combine the provided archive excerpts with well-established WORLD KNOWLEDGE "
    "about Father Solanus Casey (1870-1957, Capuchin friar, porter at St. Bonaventure Monastery in "
    "Detroit, co-founder energy of the Seraphic Mass Association, beatified 2017), the Capuchin Order, "
    "and well-known people/places/institutions. Write 2-4 factual sentences. Be specific and accurate; "
    "for obscure petitioners you cannot verify, stay grounded in the excerpts. Never fabricate."
)


def deep_one(ent: dict, contexts: list, model: str | None = None) -> str:
    """A 2-4 sentence researched dossier for one marquee entity (world knowledge + corpus)."""
    from lib.providers import llm
    model = model or ENRICH_MODEL
    typ = (ent.get("type") or "entity").lower()
    ctx = "\n".join(f"- {c}" for c in contexts) or "(no excerpt text)"
    prompt = (f'Write a 2-4 sentence researched dossier on this {typ} from the Solanus Casey archive.\n'
              f'Name: "{ent.get("canonical_name","")}"  (also: {", ".join(ent.get("variants",[])[:5])})\n'
              f'Archive excerpts:\n{ctx}\n\nReturn ONLY the dossier prose (no JSON, no preamble).')
    try:
        txt, _ = llm.generate(prompt, model=model, system=_DEEP_SYSTEM, temperature=0.2)
        return txt.strip()
    except Exception as e:
        return ""


def deep_research(top_n: int = 60, model: str | None = None, workers: int | None = None) -> dict:
    """Upgrade the description of the top-N most-mentioned entities to a researched dossier (PAID)."""
    ents = json.loads(ENTITIES.read_text()).get("entities", [])
    targets = sorted([e for e in ents if e.get("type") in ENRICH_TYPES],
                     key=lambda e: len(e.get("mentions", [])), reverse=True)[:top_n]
    enriched = _load_done()
    letters_text, entry_text = _build_text_index()
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = max(1, workers or int(os.environ.get("ENRICH_WORKERS", "12")))
    print(f"deep_research: dossiers for the top {len(targets)} entities on {workers} workers…")

    def _w(e):
        ctx = _contexts_for(e, letters_text, entry_text)[:8]
        return e["id"], deep_one(e, ctx, model)
    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(_w, e) for e in targets]):
            eid, dossier = fut.result()
            if dossier and eid in enriched:
                enriched[eid]["dossier"] = dossier
                enriched[eid]["description"] = dossier      # the panel shows the richer text
                n += 1
    OUT_PATH.write_text(json.dumps(enriched, ensure_ascii=False, indent=1))
    print(f"deep_research: wrote {n} dossiers")
    return {"dossiers": n}


def run(min_mentions: int = 2, limit: int | None = None, execute: bool = False,
        model: str | None = None, workers: int | None = None) -> dict:
    """Enrich significant entities. min_mentions filters to the recurring figures in his life (the ones
    that make the graph representative); raise it to spend less, lower it to cover the long tail."""
    model = model or ENRICH_MODEL
    ents = json.loads(ENTITIES.read_text()).get("entities", [])
    targets = [e for e in ents if e.get("type") in ENRICH_TYPES and len(e.get("mentions", [])) >= min_mentions]
    targets.sort(key=lambda e: len(e.get("mentions", [])), reverse=True)   # most-central first
    if limit:
        targets = targets[:limit]

    done = _load_done()
    todo = [e for e in targets if e.get("id") not in done]
    print("=" * 60)
    print("enrich_entities — entity context pass")
    print("=" * 60)
    print(f"  entities total        : {len(ents)}")
    print(f"  targets (>= {min_mentions} mentions): {len(targets)}  ({len(todo)} to do, {len(done)} resumed)")
    if not execute:
        approx_in = sum(min(MAX_CONTEXTS, len(e.get('mentions', []))) * (CONTEXT_CHARS // 4) + 120 for e in todo)
        print(f"  PROJECTION (no spend) : ~{approx_in:,} input tokens; run with execute=True to bill it.")
        print("=" * 60)
        return {"targets": len(targets), "todo": len(todo), "executed": False}

    letters_text, entry_text = _build_text_index()
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = max(1, workers or int(os.environ.get("ENRICH_WORKERS", "16")))
    print(f"  enriching {len(todo)} on {workers} workers (model={model})…")

    def _work(e):
        return e["id"], enrich_one(e, _contexts_for(e, letters_text, entry_text), model)

    out = dict(done)
    written = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_work, e): e for e in todo}
        for fut in as_completed(futs):
            try:
                eid, rec = fut.result()
            except Exception as exc:
                print(f"  [warn] {futs[fut].get('id','?')}: {str(exc)[:90]}")
                continue
            out[eid] = rec
            written += 1
            if written % 200 == 0:
                OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1))   # checkpoint
                print(f"  ... {written}/{len(todo)} enriched")
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print(f"  wrote {len(out)} enriched entities -> {OUT_PATH.name}")
    print("=" * 60)
    return {"targets": len(targets), "enriched_now": written, "total_enriched": len(out),
            "executed": True, "out": str(OUT_PATH)}


# ==================================================================
# Consolidation — merge entities that the enrichment gave the SAME clean name
# ==================================================================
# The cautious resolver leaves some duplicates split (e.g. "Fr. Solanus" vs "Fr. Solanus, O.F.M.
# Cap.", or several abbreviations of the Seraphic Mass Association) because the shared token is too
# common to anchor. But the enrichment LLM, reading context, gives those variants ONE clean canonical
# name — which is a strong, cheap merge key. We use it carefully: places/orgs must also share a
# location; PERSON must be a multi-token full name (a lone given name like "Mary" is too risky). Every
# merge is logged to consolidation_audit.json and the pre-merge file is backed up.
_GENERIC_KEY = {"", "unknown", "none", "person", "place", "organization", "the order", "church",
                "hospital", "friary", "monastery", "god", "our lord", "doctor", "physician"}


def _ckey(name: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", (name or "").lower()))


def consolidate(write: bool = True) -> dict:
    """Merge entities sharing an enriched clean name (cautious). Returns a small report."""
    doc = json.loads(ENTITIES.read_text())
    entlist = doc.get("entities", [])
    enriched = _load_done()
    groups: dict = defaultdict(list)
    passthrough: list = []
    for e in entlist:
        enr = enriched.get(e.get("id"))
        clean = (enr or {}).get("canonical_name") or e.get("canonical_name") or ""
        nkey, typ = _ckey(clean), e.get("type")
        loc = _ckey((enr or {}).get("location") or "") if typ in ("PLACE", "ORG") else ""
        mergeable = enr is not None and nkey and nkey not in _GENERIC_KEY
        if typ == "PERSON" and len(nkey.split()) < 2 and "solanus" not in nkey:
            mergeable = False                              # a lone given name is not safe to merge on
        if mergeable:
            groups[(typ, nkey, loc)].append(e)
        else:
            passthrough.append(e)

    merged, audit = [], []
    for key, grp in groups.items():
        if len(grp) == 1:
            merged.append(grp[0])
            continue
        grp.sort(key=lambda e: len(e.get("mentions", [])), reverse=True)
        primary = dict(grp[0])
        allm, allv = [], set()
        for e in grp:
            allm += e.get("mentions", [])
            allv.update(e.get("variants", []) or [])
            if e.get("canonical_name"):
                allv.add(e["canonical_name"])
        enr = enriched.get(primary["id"]) or {}
        primary["canonical_name"] = enr.get("canonical_name") or primary.get("canonical_name")
        primary["mentions"] = allm
        primary["variants"] = sorted(v for v in allv if v and v != primary["canonical_name"])
        primary["consolidated_from"] = [e["id"] for e in grp]
        merged.append(primary)
        audit.append({"type": key[0], "name": primary["canonical_name"], "n": len(grp),
                      "merged": [e.get("canonical_name") for e in grp]})

    result = passthrough + merged
    out = {**doc, "_meta": {**doc.get("_meta", {}), "consolidated": True, "n_entities": len(result)},
           "entities": result}
    if write:
        (config.DATA / "entities_preconsolidation.json").write_text(ENTITIES.read_text())   # backup
        ENTITIES.write_text(json.dumps(out, ensure_ascii=False, indent=1))
        (config.DATA / "consolidation_audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=1))
    print(f"consolidate: {len(entlist)} -> {len(result)} entities ({len(audit)} same-name merges)")
    return {"before": len(entlist), "after": len(result), "merges": len(audit)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Enrich canonical entities with grounded context (PAID).")
    ap.add_argument("--min-mentions", type=int, default=2)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--execute", action="store_true", help="run the paid LLM pass (default: preview)")
    ap.add_argument("--model", type=str, default=None)
    a = ap.parse_args()
    run(min_mentions=a.min_mentions, limit=a.limit, execute=a.execute, model=a.model)
