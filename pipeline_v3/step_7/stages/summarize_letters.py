"""summarize_letters.py — one-line summaries for the 570 correspondence letters.

The Reading Room and His Life pages list letters by date, but a reader can only tell what a letter is
about by opening it. This writes a single grounded sentence per letter ("what this letter is about"),
so the list/timeline is scannable. Non-destructive side-car; overlaid by /api/letters + /api/letter.

  data/letter_summaries.json   {letter_id: {summary, recipient, edtf, date}}

Parallel LLM (Gemini), cost-capped, resumable (skips letters already summarized). An existing artifact
is backed up to data/.backups/ before overwrite.

    venv/bin/python stages/summarize_letters.py                 # all 570 (resumable)
    venv/bin/python stages/summarize_letters.py --limit 20      # smoke test
"""
from __future__ import annotations
import argparse
import json
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                                    # noqa: E402
from lib import costlog                          # noqa: E402
from lib.providers import llm                    # noqa: E402

GRAPH = config.DATA / "graph.json"
DOCS = config.STEP6 / "documents.json"
OUT = config.DATA / "letter_summaries.json"


def summarize(node, docs, model):
    doc_id = node.get("doc_id")
    rec = docs.get(doc_id, {})
    tbl = rec.get("text_by_label", {})
    text = (tbl.get("src_content") or node.get("text") or "").strip()
    if not text:
        return {"id": node["id"], "summary": None, "skip": "no text"}
    recipient = rec.get("recipient") or (node.get("label") or "").replace("Letter to ", "")
    prompt = (
        "This is a letter written by Father Solanus Casey (Capuchin friar, 1870-1957). In ONE plain, "
        "concrete sentence of at most 20 words, say what the letter is about — its purpose or main news. "
        "No preamble, no 'This letter', no quotation marks; just the sentence. Ground it only in the text.\n\n"
        f"TO: {recipient}\n\nLETTER:\n{text[:2600]}")
    try:
        txt, _ = llm.generate(prompt, model=model, temperature=0.2)
        s = (txt or "").strip().strip('"').split("\n")[0].strip()
        return {"id": node["id"], "summary": s, "recipient": recipient,
                "edtf": node.get("edtf"), "date": node.get("date_raw")}
    except Exception as e:
        return {"id": node["id"], "summary": None, "error": str(e)[:100]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-cost", type=float, default=5.0)
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()

    g = json.loads(GRAPH.read_text())
    letters = [n for n in g["nodes"] if n.get("kind") == "letter"]
    docs = {d["id"]: d for d in json.loads(DOCS.read_text())}
    if a.limit:
        letters = sorted(letters, key=lambda n: n.get("edtf") or "")[:a.limit]

    done = json.loads(OUT.read_text()) if OUT.exists() else {}
    todo = [n for n in letters if n["id"] not in done]
    print(f"summarize_letters: {len(letters)} letters, {len(todo)} to do (resume skipped {len(letters) - len(todo)})")

    c0 = costlog.snapshot()
    n_done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(summarize, n, docs, a.model) for n in todo]
        for fut in as_completed(futs):
            r = fut.result()
            if r.get("summary"):
                done[r["id"]] = {"summary": r["summary"], "recipient": r.get("recipient"),
                                 "edtf": r.get("edtf"), "date": r.get("date")}
            n_done += 1
            run_usd = costlog.delta(c0, costlog.snapshot()).get("usd", 0.0)
            if n_done % 50 == 0:
                OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
                print(f"  {n_done}/{len(todo)} summarized; this run ${run_usd:.3f}")
            if a.max_cost and run_usd > a.max_cost:
                print(f"  cost cap ${a.max_cost} reached at {n_done} (resumable)"); break

    if OUT.exists():
        bkp = config.DATA / ".backups"
        bkp.mkdir(exist_ok=True)
        shutil.copy(OUT, bkp / f"letter_summaries.{datetime.now():%Y%m%d_%H%M%S}.json")
    OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
    print(f"\nDONE. {len(done)} letter summaries -> {OUT.name}")


if __name__ == "__main__":
    main()
