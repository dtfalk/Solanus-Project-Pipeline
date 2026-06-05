#!/usr/bin/env python3
"""Evaluate the TUNED model (no few-shot, raw output — no snap/backstop scaffolding).

1) VAL: the 9 held-out val pages (A1/A3, never trained on) -> write outputs ->
   panoptic ink-IoU score vs gold, per volume.
2) COLD: Appendix_2 (completely held out: not in training, not in the few-shot
   pool, no gold yet) -> write outputs -> gold-free signals: schema validity +
   disagreement vs the production 3.5-flash run. Gold scoring happens after the
   user reviews A2.

Usage:
    ./venv/bin/python rerun_compare/tuned_eval.py val
    ./venv/bin/python rerun_compare/tuned_eval.py a2 [--max N]
    ./venv/bin/python rerun_compare/tuned_eval.py score     # panoptic on val outputs
"""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import auto_labeler as AL  # noqa: E402
from google import genai  # noqa: E402
from google.genai import types  # noqa: E402

OUT = HERE / "ab" / "tuned-v1"
STATE = json.load(open(ROOT / "tuning_data" / "tuning_job.json"))
SYS = None  # lazy


def _sys_prompt():
    global SYS
    if SYS is None:
        spec = importlib.util.spec_from_file_location("etd", ROOT / "export_tuning_data.py")
        etd = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(etd)
        SYS = etd._tuning_system_prompt()
    return SYS


def _client():
    return genai.Client(vertexai=True, project="solanus-project", location="us-central1")


def _val_pages():
    """(vol, page_name) for the val split, parsed from the uploaded JSONL."""
    out = []
    for line in open(ROOT / "tuning_data" / "vertex_val.jsonl"):
        d = json.loads(line)
        for part in d["contents"][0]["parts"]:
            if "fileData" in part:
                stem = part["fileData"]["fileUri"].split("/")[-1].removesuffix(".png")
                vol, page = stem.split("__")
                out.append((vol, page))
    return out


def _label_page(c, endpoint, vol, page):
    pdf = AL.POLYGON_PDFS_DIR / vol / "pages" / f"{page}.pdf"
    img, _rw, _rh, sw, sh = AL.render_page(pdf, 768)
    buf = io.BytesIO(); img.convert("RGB").save(buf, format="PNG")
    r = c.models.generate_content(
        model=endpoint,
        contents=[types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"),
                  "Label this page in the schema."],
        config=types.GenerateContentConfig(system_instruction=_sys_prompt(),
                                           temperature=0.0,
                                           response_mime_type="application/json"))
    parsed = json.loads((r.text or "").strip())
    # Tuned model outputs vertices in [0,1000] (matches the prompt + the normalized
    # training target) -> scale back to SOURCE PIXELS so panoptic_eval (gold is in
    # source pixels) compares apples-to-apples. Mirrors production's
    # _scale_response_to_original.
    docs = parsed.get("documents", {}) or {}
    for doc in docs.values():
        if isinstance(doc, dict):
            for polys in doc.values():
                if isinstance(polys, list):
                    for b in polys:
                        for v in (b.get("vertices") or []):
                            v["x"] = round(v["x"] / 1000 * sw)
                            v["y"] = round(v["y"] / 1000 * sh)
    full = {"page_number": int(page.split("_")[1]), "page_width": sw, "page_height": sh,
            "render_dpi": AL.RENDER_DPI, "num_documents": parsed.get("num_documents", 1),
            "documents": docs}
    od = OUT / vol / page
    od.mkdir(parents=True, exist_ok=True)
    json.dump(full, open(od / f"{page}.json", "w"), indent=2)
    if not (od / f"{page}.pdf").exists():
        shutil.copy2(pdf, od / f"{page}.pdf")
    return full


def cmd_val(_args):
    c = _client(); ep = STATE["endpoint"]
    print(f"tuned endpoint: {ep}")
    ok = 0
    pages = _val_pages()
    for vol, page in pages:
        try:
            full = _label_page(c, ep, vol, page)
            print(f"  {vol}/{page}: docs={full['num_documents']} "
                  f"boxes={sum(len(p) for d in full['documents'].values() for p in d.values() if isinstance(p, list))}")
            ok += 1
        except Exception as e:
            print(f"  {vol}/{page}: FAILED {type(e).__name__}: {str(e)[:120]}")
    print(f"val inference: {ok}/{len(pages)} ok")


def cmd_a2(args):
    c = _client(); ep = STATE["endpoint"]
    pages = sorted(p for (_d, p, _f) in
                   [(d, f"page_{n:03d}", f) for (d, n, f) in AL.discover_target_pages("Appendix_2")])
    if args.max:
        pages = pages[:args.max]
    ok = 0
    for page in pages:
        out = OUT / "Appendix_2" / page / f"{page}.json"
        if out.exists():
            ok += 1; continue
        try:
            _label_page(c, ep, "Appendix_2", page); ok += 1
        except Exception as e:
            print(f"  {page}: FAILED {type(e).__name__}: {str(e)[:100]}")
        if ok % 10 == 0:
            print(f"  ... {ok}/{len(pages)}", flush=True)
    print(f"A2 cold inference: {ok}/{len(pages)} ok")
    # gold-free signal: disagreement vs the production 3.5-flash run
    spec = importlib.util.spec_from_file_location("triage", ROOT / "triage.py")
    T = importlib.util.module_from_spec(spec); spec.loader.exec_module(T)
    rows = []
    for page in pages:
        a = AL.AUTO_LABELED_DIR / "Appendix_2" / page / f"{page}.json"
        b = OUT / "Appendix_2" / page / f"{page}.json"
        if a.exists() and b.exists():
            rows.append((T._disagreement(a, b), page))
    rows.sort(reverse=True)
    print(f"disagreement vs 3.5-flash: median={sorted(d for d,_ in rows)[len(rows)//2] if rows else '-'} "
          f"top-5={rows[:5]} zero={sum(1 for d,_ in rows if d==0)}/{len(rows)}")


def cmd_score(_args):
    spec = importlib.util.spec_from_file_location("panoptic_eval", ROOT / "panoptic_eval.py")
    PE = importlib.util.module_from_spec(spec); spec.loader.exec_module(PE)
    print(f"{'volume':12} {'PQ':>6} {'RQ':>6} {'SQ':>6} {'TP':>4} {'FP':>4} {'FN':>4}")
    for vol in ("Appendix_1", "Appendix_3"):
        if (OUT / vol).exists():
            r = PE.eval_volume(vol, OUT, ROOT / "reviewed", 1000, False)
            print(f"{vol:12} {r['PQ']:6.3f} {r['RQ']:6.3f} {r['SQ']:6.3f} "
                  f"{r['TP']:4d} {r['FP']:4d} {r['FN']:4d}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("val")
    p = sub.add_parser("a2"); p.add_argument("--max", type=int, default=0)
    sub.add_parser("score")
    args = ap.parse_args()
    {"val": cmd_val, "a2": cmd_a2, "score": cmd_score}[args.cmd](args)
