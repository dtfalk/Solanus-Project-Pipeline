"""NON-DESTRUCTIVE finisher after the TOC-anchored merge:

(1) CLUSTER MOP-UP: merge any PERSON entities whose `toc_name` belongs to the SAME LLM-confirmed
    same-person dedup cluster (from data/entity_engine_toc_clusters.json `map`). This catches residual
    splits the per-entity anchoring missed (e.g. the 5-mention "Msgr. Edward F. Casey" left beside the
    29-mention Edward) — safe because the cluster was already LLM-confirmed to be one person.
(2) SAFE NAME CLEAN (in place, no variant-picking): strip a leading OCR figure-marker like "F 2 " and
    expand whole-token contractions (Edw'd->Edward, Wm->William, Chas->Charles) — surnames untouched.

Backs up entity_store.json + entities_enriched.json first; reports every merge + rename.
"""
import json, re, shutil, time
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
STORE = DATA / "entity_store.json"
ENRICHED = DATA / "entities_enriched.json"
CLUSTERS = DATA / "entity_engine_toc_clusters.json"
BK = DATA / ".backups"; BK.mkdir(exist_ok=True)

_CONTR = {"edwd": "Edward", "edw": "Edward", "wm": "William", "jno": "John", "jn": "John",
          "geo": "George", "chas": "Charles", "thos": "Thomas", "tho": "Thomas", "jas": "James",
          "jos": "Joseph", "robt": "Robert", "richd": "Richard", "danl": "Daniel", "saml": "Samuel",
          "benj": "Benjamin", "matt": "Matthew", "michl": "Michael", "patk": "Patrick",
          "fredk": "Frederick", "alexr": "Alexander", "nathl": "Nathaniel"}
_LEAD_MARK = re.compile(r"^\s*[A-Za-z]\s+\d+[.\):]?\s+")          # "F 2 ", "p 3. " — OCR figure marker
_CONTR_TOK = re.compile(r"(?i)\b(edw'?d|edwd|wm|jn'?o|jno|geo|chas|thos|jas|robt|danl|saml|benj|richd|patk|fredk|alexr|nathl|michl)\b")


def safe_clean(s: str) -> str:
    if not s:
        return s
    s = _LEAD_MARK.sub("", s)
    out = []
    for t in s.split():
        key = re.sub(r"[’'.]", "", t.lower())
        out.append(_CONTR[key] if key in _CONTR else t)        # expand whole tokens only; keep surnames exact
    return re.sub(r"\s+", " ", " ".join(out)).strip()


def main():
    store = json.loads(STORE.read_text())
    ents = store.get("entities", store)
    ents_list = list(ents.values()) if isinstance(ents, dict) else ents
    is_dict = isinstance(ents, dict)
    cmap = {}
    if CLUSTERS.exists():
        cmap = json.loads(CLUSTERS.read_text()).get("map", {}) or {}
    stamp = time.strftime("%Y%m%d_%H%M%S")
    shutil.copy(STORE, BK / f"entity_store_preFINISH_{stamp}.json")

    # (1) cluster mop-up: group persons by their confirmed-cluster canonical
    groups = {}
    for e in ents_list:
        if e.get("type") != "PERSON":
            continue
        toc = e.get("toc_name")
        if not toc:
            continue
        canon = cmap.get(toc, toc)                              # confirmed-cluster canonical (or itself)
        groups.setdefault(canon, []).append(e)

    merged_ids = set()
    merge_log = []
    for canon, members in groups.items():
        if len(members) < 2:
            continue
        members.sort(key=lambda e: len(e.get("mentions", []) or []), reverse=True)
        primary = members[0]
        allm = list(primary.get("mentions", []) or [])
        allv = set(primary.get("variants", []) or [])
        srcs = list(primary.get("merged_from", []) or [primary["id"]])
        for m in members[1:]:
            allm += m.get("mentions", []) or []
            allv |= set(m.get("variants", []) or [])
            allv.add(m.get("canonical_name", ""))
            srcs += (m.get("merged_from", []) or [m["id"]])
            merged_ids.add(m["id"])
        primary["mentions"] = allm
        primary["merged_from"] = sorted(set(srcs))
        disp = safe_clean(canon)                                # clean cluster canonical = display name
        allv.add(primary.get("canonical_name", ""))
        primary["canonical_name"] = disp
        if isinstance(primary.get("understanding"), dict):
            primary["understanding"]["canonical_name"] = disp
        primary["variants"] = sorted(v for v in allv if v and v != disp)
        merge_log.append((disp, len(members), len(allm), [m["id"] for m in members]))

    # drop absorbed entities
    if is_dict:
        for k in list(ents.keys()):
            if (ents[k].get("id") in merged_ids) or (k in merged_ids):
                del ents[k]
        ents_list = list(ents.values())
    else:
        ents_list = [e for e in ents_list if e.get("id") not in merged_ids]
        store["entities"] = ents_list

    # (2) safe in-place name clean for any remaining garbled person canonical
    renamed = 0
    rename_map = {}
    for e in ents_list:
        if e.get("type") != "PERSON":
            continue
        cur = e.get("canonical_name") or ""
        if _LEAD_MARK.search(cur) or _CONTR_TOK.search(cur):
            new = safe_clean(cur)
            if new and new != cur:
                e["canonical_name"] = new
                if isinstance(e.get("understanding"), dict):
                    e["understanding"]["canonical_name"] = new
                vs = set(e.get("variants", []) or []); vs.add(cur); vs.discard(new)
                e["variants"] = sorted(v for v in vs if v)
                rename_map[e.get("id")] = new
                renamed += 1

    if is_dict:
        store["entities"] = ents
    STORE.write_text(json.dumps(store, ensure_ascii=False))

    # propagate names + drops to enriched
    if ENRICHED.exists():
        shutil.copy(ENRICHED, BK / f"entities_enriched_preFINISH_{stamp}.json")
        enr = json.loads(ENRICHED.read_text())
        enr_list = list(enr.values()) if isinstance(enr, dict) else enr
        keep = []
        for r in enr_list:
            if r.get("id") in merged_ids:
                continue
            nm = rename_map.get(r.get("id"))
            # also pick up cluster-merge renames (primary entities)
            if not nm:
                for disp, _, _, ids in merge_log:
                    if r.get("id") in ids and r.get("id") not in merged_ids:
                        nm = disp; break
            if nm:
                r["canonical_name"] = nm
                if isinstance(r.get("understanding"), dict):
                    r["understanding"]["canonical_name"] = nm
            keep.append(r)
        if isinstance(enr, dict):
            enr = {k: v for k, v in enr.items() if v.get("id") not in merged_ids}
            ENRICHED.write_text(json.dumps(enr, ensure_ascii=False))
        else:
            ENRICHED.write_text(json.dumps(keep, ensure_ascii=False))

    print(f"cluster mop-up: {len(merge_log)} multi-entity clusters merged ({len(merged_ids)} absorbed)")
    for disp, nmem, nment, ids in sorted(merge_log, key=lambda x: -x[1])[:12]:
        print(f"  {disp!r}  <- {nmem} entities, {nment} mentions")
    print(f"safe name clean: {renamed} contraction/marker names fixed")
    cur_ents = list(store.get('entities', store).values()) if is_dict else store.get('entities', store)
    edw = [e for e in cur_ents if e.get("type") == "PERSON" and "edward" in (e.get("canonical_name", "")).lower() and "casey" in (e.get("canonical_name", "")).lower()]
    print("Edward Casey now:", [(e.get("id"), e.get("canonical_name"), len(e.get("mentions", []) or [])) for e in edw])
    print("total entities:", len(cur_ents))


if __name__ == "__main__":
    main()
