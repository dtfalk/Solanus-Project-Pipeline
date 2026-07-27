"""solanus_family.py — derive Father Solanus Casey's BIOLOGICAL family from explicit letter greetings.

The hard part: "Brother"/"Sister" in this corpus is religiously overloaded — "Brother Leo" is the friar Leo
Wollenweber, "Sister Lurana" is Mother Lurana of the Atonement, etc. So a sibling is identified by:
  greeting "Dear Brother/Sister <Name>"  AND  the recipient carries the CASEY surname
  (or a documented sister's married name — see MARRIED_NAMES) AND is NOT a religious address.

Output: data/solanus_family.json — an EDITABLE roster (David curates edge cases). Used by the Ask path's
relationship-aware query expansion so "your brothers" resolves to Owen/Patrick/Edward/James/Maurice Casey
instead of the lay brothers.

    python stages/solanus_family.py
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config  # noqa: E402
from lib.safeio import backup, atomic_write_text  # noqa: E402

DOCS = config.REPO / "pipeline_v3" / "step_6" / "documents.json"
OUT = config.DATA / "solanus_family.json"

# documented married names of his sisters (so they're caught despite no "Casey" in the recipient)
MARRIED_NAMES = ("ledoux", "wilhite", "groth", "mccloskey", "mccluskey", "herkenrath")
# religious-order / non-family markers that DISQUALIFY a "Brother/Sister X" greeting
RELIGIOUS = ("o.f.m", "o.m. cap", "o.m.cap", "ofm", "s.a.", "atonement", "wollenweber", "lurana",
             "mother ", "rev. mother", "sister m.", "sister sister", "badalment", "carmel", "constance",
             "dorothy", "convent", "monastery", "provincial")
GREET = re.compile(r"\bdear\s+(brother|sister)\s+([A-Za-z][A-Za-z.]*)", re.I)


def _is_family(recipient: str) -> bool:
    r = recipient.lower()
    if any(x in r for x in RELIGIOUS):
        return False
    return "casey" in r or any(m in r for m in MARRIED_NAMES)


def derive() -> dict:
    docs = json.loads(DOCS.read_text())
    people: dict[str, dict] = {}
    for d in docs:
        tbl = d.get("text_by_label") or {}
        gr = tbl.get("src_greeting") or ""
        rcp = (tbl.get("src_recipient") or d.get("recipient") or "").strip()
        m = GREET.search(gr)
        if not m or not _is_family(rcp):
            continue
        rel = "brother" if m.group(1).lower() == "brother" else "sister"
        first = m.group(2).rstrip(".").title()
        if first.lower() in {"and", "the", "all", "my", "our", "&", "in"} or len(first) < 2:
            continue                                  # parse artifacts ("Dear Brother and Sister …")
        # canonical key by first name (Pat→Patrick handled by merge list below)
        key = {"Pat": "Patrick", "Edw": "Edward", "Maggie": "Margaret"}.get(first, first)
        e = people.setdefault(key, {"name": key, "relation": rel, "recipients": set(), "doc_ids": set()})
        e["recipients"].add(rcp)
        e["doc_ids"].add(d["id"])
    members = []
    for e in people.values():
        members.append({"name": e["name"], "relation": e["relation"],
                        "recipients": sorted(e["recipients"]), "n_letters": len(e["doc_ids"]),
                        "doc_ids": sorted(e["doc_ids"])})
    members.sort(key=lambda x: (-x["n_letters"], x["name"]))
    return {"subject": "Father Solanus Casey",
            "note": "Editable roster — add/remove members or recipient variants as needed (David curates).",
            "brothers": [m["name"] for m in members if m["relation"] == "brother"],
            "sisters": [m["name"] for m in members if m["relation"] == "sister"],
            "members": members}


def main():
    fam = derive()
    backup(OUT, "family")   # the roster is David-curated — never overwrite it without a recovery point
    atomic_write_text(OUT, json.dumps(fam, ensure_ascii=False, indent=2))
    print(f"brothers: {fam['brothers']}")
    print(f"sisters:  {fam['sisters']}")
    print(f"-> {OUT} ({len(fam['members'])} members)")


if __name__ == "__main__":
    main()
