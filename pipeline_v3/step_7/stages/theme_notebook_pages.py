"""theme_notebook_pages.py — a theme + one-line gloss for each notebook page.

The notebook reading room browses ~8,773 short petition fragments; individually they're near-meaningless,
but each sits on a page with a coherent character (a day's reported favors, spiritual notes, Latin
excerpts, newspaper clippings, a specific event). This writes a short theme + gloss + tags per PAGE so an
entry can show the context it belongs to. Non-destructive side-car; overlaid by /api/notebook_entries.

  data/notebook_page_themes.json   {doc_id: {theme, gloss, tags[]}}

Parallel LLM (Gemini), cost-capped, resumable. Backs up an existing artifact before overwrite.

    venv/bin/python stages/theme_notebook_pages.py                # all pages (resumable)
    venv/bin/python stages/theme_notebook_pages.py --limit 8      # smoke test
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

NOTEBOOKS = config.STEP6 / "notebooks.json"
OUT = config.DATA / "notebook_page_themes.json"

SCHEMA_HINT = ('Return ONLY compact JSON: {"theme": "<=6-word title", '
               '"gloss": "one sentence, <=22 words", "tags": ["3-6 short topic tags"]}')


def theme_page(page, model):
    doc_id = page.get("id")
    text = ((page.get("text_by_label") or {}).get("src_content") or "").strip()
    if len(text) < 40:
        return {"id": doc_id, "theme": None, "skip": "too short"}
    prompt = (
        "This is one page from the petition/notes notebooks of Father Solanus Casey (Capuchin friar, "
        "1870-1957), who logged favors reported through the Seraphic Mass Association. Characterize THIS "
        "PAGE for a reader browsing the notebooks. " + SCHEMA_HINT + "\n\nPAGE:\n" + text[:3000])
    try:
        txt, _ = llm.generate(prompt, model=model, temperature=0.2, json_mode=True)
        d = json.loads(txt)
        return {"id": doc_id, "theme": (d.get("theme") or "").strip(),
                "gloss": (d.get("gloss") or "").strip(), "tags": d.get("tags") or []}
    except Exception as e:
        return {"id": doc_id, "theme": None, "error": str(e)[:100]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-cost", type=float, default=5.0)
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()

    pages = json.loads(NOTEBOOKS.read_text())
    if a.limit:
        pages = pages[:a.limit]
    done = json.loads(OUT.read_text()) if OUT.exists() else {}
    todo = [p for p in pages if p.get("id") not in done]
    print(f"theme_notebook_pages: {len(pages)} pages, {len(todo)} to do (resume skipped {len(pages) - len(todo)})")

    c0 = costlog.snapshot()
    n_done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(theme_page, p, a.model) for p in todo]
        for fut in as_completed(futs):
            r = fut.result()
            if r.get("theme"):
                done[r["id"]] = {"theme": r["theme"], "gloss": r.get("gloss"), "tags": r.get("tags") or []}
            n_done += 1
            run_usd = costlog.delta(c0, costlog.snapshot()).get("usd", 0.0)
            if n_done % 50 == 0:
                OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
                print(f"  {n_done}/{len(todo)} themed; this run ${run_usd:.3f}")
            if a.max_cost and run_usd > a.max_cost:
                print(f"  cost cap ${a.max_cost} reached at {n_done} (resumable)"); break

    if OUT.exists():
        bkp = config.DATA / ".backups"
        bkp.mkdir(exist_ok=True)
        shutil.copy(OUT, bkp / f"notebook_page_themes.{datetime.now():%Y%m%d_%H%M%S}.json")
    OUT.write_text(json.dumps(done, ensure_ascii=False, indent=1))
    print(f"\nDONE. {len(done)} page themes -> {OUT.name}")


if __name__ == "__main__":
    main()
