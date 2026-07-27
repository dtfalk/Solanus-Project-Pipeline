"""verify_kinship.py — LLM-grounded adjudication of Solanus's family relationships.

The expensive, high-fidelity pass: for each CANDIDATE relative (from the book+web+corpus research), assemble
the ACTUAL letters Solanus exchanged with / wrote about them (greetings + recipient + the passages where he
names the relationship), hand the LLM that evidence ALONGSIDE the biographical research, and have it rule on
the TRUE relationship — distinguishing a blood/marital relative from a Capuchin friar ("Brother X"), a nun
("Sister Y"), an in-law, or an unrelated correspondent — with quoted evidence + a confidence score.

This resolves exactly the ambiguities heuristics can't: Emma (sister vs sister-in-law), "Brother Leo"
(friar, not kin), Mary McCloskey (his sister, mother of the 1945-recording nephew), etc.

Inputs (written from the kinship research workflow):
  data/solanus_family_candidates.json : {"members":[{canonical_name, relation, aliases:[...]}], "research": "..."}
Output:
  data/solanus_family_verified.json   : per-member adjudication + the final roster

    python stages/verify_kinship.py            # parallel LLM adjudication (PAID)
    python stages/verify_kinship.py --model gemini-2.5-pro
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402
from lib.providers import llm       # noqa: E402

DOCS = config.REPO / "pipeline_v3" / "step_6" / "documents.json"
CAND = config.DATA / "solanus_family_candidates.json"
OUT = config.DATA / "solanus_family_verified.json"
MAX_LETTERS = 8                      # cap evidence per candidate to bound tokens
EXCERPT = 700


def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", " ", (s or "").lower())


def gather_evidence(member: dict, docs: list) -> list[dict]:
    """Letters whose recipient/greeting/content names this candidate (by canonical first name or any alias)."""
    keys = {member["canonical_name"].split()[0].lower()}
    for a in member.get("aliases", []):
        for tok in re.findall(r"[A-Za-z]{3,}", a):
            keys.add(tok.lower())
    keys -= {"mrs", "mr", "rev", "father", "letters", "casey", "the", "and", "msgr"}  # too generic alone
    ev = []
    for d in docs:
        tbl = d.get("text_by_label") or {}
        hay = _norm(" ".join(str(tbl.get(k) or "") for k in ("src_greeting", "src_recipient")) +
                    " " + (d.get("recipient") or ""))
        body = tbl.get("src_content") or ""
        named = any(f" {k} " in f" {hay} " for k in keys)
        # also catch in-body relationship mentions ("my brother Owen", "nephew", "Maurice and I")
        rel_sent = ""
        if member["canonical_name"].split()[0].lower() in _norm(body):
            for s in re.split(r"(?<=[.!?])\s+", body):
                if member["canonical_name"].split()[0].lower() in _norm(s) and \
                   re.search(r"brother|sister|nephew|niece|family|father|mother|cousin", s, re.I):
                    rel_sent = s.strip()[:300]; break
        if named or rel_sent:
            ev.append({"doc_id": d.get("id"), "greeting": tbl.get("src_greeting"),
                       "recipient": tbl.get("src_recipient") or d.get("recipient"),
                       "excerpt": (rel_sent or body[:EXCERPT])})
        if len(ev) >= MAX_LETTERS:
            break
    return ev


def adjudicate(member: dict, research: str, model: str) -> dict:
    ev = gather_evidence(member, adjudicate.docs)
    letters = "\n\n".join(f"[{i+1}] to «{e['recipient']}» — greeting: «{e['greeting']}»\n{e['excerpt']}"
                          for i, e in enumerate(ev)) or "(no letters located for this name)"
    prompt = (
        "You are establishing Father Solanus Casey's (Bernard Francis Casey, 1870-1957, Capuchin friar) "
        "true family. Decide whether the CANDIDATE below is his blood or marital FAMILY, and the EXACT "
        "relation. Critically distinguish a relative from a Capuchin FRIAR (addressed 'Brother X'), a NUN "
        "('Sister Y'), an IN-LAW, or an unrelated correspondent. Use ONLY the research + letters provided; "
        "quote the evidence; if unsure, say so and lower confidence.\n\n"
        f"RESEARCH (biography + genealogy):\n{research[:3500]}\n\n"
        f"CANDIDATE: {member['canonical_name']}  (research-proposed relation: {member.get('relation','?')})\n"
        f"Addressed/known as: {', '.join(member.get('aliases', [])) or member['canonical_name']}\n\n"
        f"LETTERS (Solanus's own words):\n{letters}\n\n"
        "Return JSON: {\"is_family\": bool, \"relation\": \"brother|sister|father|mother|nephew|niece|"
        "in-law|cousin|grandnephew|grandniece|friar|nun|unrelated|unknown\", \"confidence\": 0.0-1.0, "
        "\"evidence\": [\"short quote\", ...], \"reasoning\": \"one or two sentences\"}")
    try:
        txt, _ = llm.generate(prompt, model=model, json_mode=True, temperature=0.1)
        obj = json.loads(re.sub(r"^```(json)?|```$", "", txt.strip(), flags=re.I | re.M).strip())
    except Exception as e:
        obj = {"is_family": None, "relation": "unknown", "confidence": 0.0,
               "evidence": [], "reasoning": f"adjudication error: {str(e)[:80]}"}
    obj.update({"canonical_name": member["canonical_name"], "n_evidence": len(ev),
                "research_relation": member.get("relation")})
    return obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    cand = json.loads(CAND.read_text())
    members = cand.get("members", [])
    research = cand.get("research", "")
    adjudicate.docs = json.loads(DOCS.read_text())
    print(f"adjudicating {len(members)} candidates with {a.model} ...")
    results = []
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = {ex.submit(adjudicate, m, research, a.model): m for m in members}
        for fut in as_completed(futs):
            r = fut.result(); results.append(r)
            print(f"  {r['canonical_name']:28} -> {r['relation']:11} conf={r['confidence']} "
                  f"({r['n_evidence']} letters)")
    fam = [r for r in results if r.get("is_family") and r["relation"] not in ("friar", "nun", "unrelated")]
    OUT.write_text(json.dumps({"verified": results,
                               "brothers": [r["canonical_name"] for r in fam if r["relation"] == "brother"],
                               "sisters": [r["canonical_name"] for r in fam if r["relation"] == "sister"],
                               "other_family": [f"{r['canonical_name']} ({r['relation']})" for r in fam
                                                if r["relation"] not in ("brother", "sister")]},
                              ensure_ascii=False, indent=2))
    print(f"\n{len(fam)}/{len(results)} confirmed family -> {OUT}")


if __name__ == "__main__":
    main()
