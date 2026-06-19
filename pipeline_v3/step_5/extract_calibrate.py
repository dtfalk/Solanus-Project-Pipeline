#!/usr/bin/env python3
"""STEP_5 Phase 2 — dual text-extraction CALIBRATION (Gemini / google.genai).

For each sample page, run TWO passes over the already-masked image and compare:
  A. full-page structured — one call over the (downsampled) masked page; the model
     is given every labeled region's id+category+bbox and returns text per id.
  B. per-polygon zoom    — one call per region over a tight native-res crop.

Writes extract_calib.json into each page folder and prints an agreement/cost summary,
so we can decide engine + blanket-vs-targeted before scaling to all 1,408 pages.

Reuses step_4 conventions: google.genai client, GEMINI_API_KEY from step_4/.env,
gemini-3.5-flash, response_schema (pydantic), temperature 0.

Usage (paths relative to 2_src_organized/, or absolute):
  ./venv/bin/python extract_calibrate.py Volume_2/1_source_pages/page_232 \
                                         Volume_3/1_source_pages/page_054
  ../step_4/venv/bin/python extract_calibrate.py <page_dir> --model gemini-3.5-flash
"""
from __future__ import annotations
import os, json, argparse
from pathlib import Path
from PIL import Image
from pydantic import BaseModel
from dotenv import load_dotenv
from google import genai
from google.genai import types

Image.MAX_IMAGE_PIXELS = None
HERE  = Path(__file__).resolve().parent
STEP4 = HERE.parent / "step_4"
ROOT  = HERE / "2_src_organized"
load_dotenv(STEP4 / ".env")


class RegionText(BaseModel):
    rid: str
    text: str

class PageOut(BaseModel):
    regions: list[RegionText]

class OneText(BaseModel):
    text: str


FULL_PROMPT = """This is a masked scan of one page from the Father Solanus Casey archival
papers (typed transcriptions, handwritten letters, Seraphic Mass enrollment cards, and
notebook/ledger pages). Everything OUTSIDE the labeled regions is whited out. Below are the
labeled regions, each with an id, a category, and a bounding box [x0,y0,x1,y1] in PIXEL
coordinates of THIS image. Transcribe the text inside each region VERBATIM — keep original
spelling, punctuation, capitalization, and line breaks. If a region is illegible, give your
best reading. Return exactly one entry per id.

Regions:
{regions}
"""

POLY_PROMPT = """This is a tight crop of ONE region (category: {category}) from an archival
document (typed or handwritten, often early 1900s). Transcribe its text VERBATIM — keep
spelling, punctuation, and line breaks. If illegible, give your best reading."""


def make_client():
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])

def bbox(poly):
    xs = [v["x"] for v in poly["vertices"]]; ys = [v["y"] for v in poly["vertices"]]
    return min(xs), min(ys), max(xs), max(ys)

def collect_regions(meta):
    out = []
    for dk, doc in (meta.get("documents") or {}).items():
        for cat, polys in (doc or {}).items():
            for i, p in enumerate(polys or []):
                out.append((f"{dk}.{cat}.{i}", dk, cat, bbox(p)))
    return out

def usage(resp):
    u = getattr(resp, "usage_metadata", None)
    return (int(getattr(u, "prompt_token_count", 0) or 0),
            int(getattr(u, "candidates_token_count", 0) or 0))

def norm(s):
    return " ".join((s or "").split()).lower()


def full_page_pass(cl, model, small_img, regions, scale):
    lines = [f"{rid} | {cat} | [{int(x0*scale)},{int(y0*scale)},{int(x1*scale)},{int(y1*scale)}]"
             for (rid, dk, cat, (x0, y0, x1, y1)) in regions]
    resp = cl.models.generate_content(
        model=model, contents=[FULL_PROMPT.format(regions="\n".join(lines)), small_img],
        config=types.GenerateContentConfig(response_mime_type="application/json",
                                           response_schema=PageOut, temperature=0.0))
    out = PageOut.model_validate_json((resp.text or "").strip())
    return {r.rid: r.text for r in out.regions}, usage(resp)

def poly_pass(cl, model, full_img, regions, pad=8):
    res, tin, tout = {}, 0, 0
    for (rid, dk, cat, (x0, y0, x1, y1)) in regions:
        crop = full_img.crop((max(0, x0 - pad), max(0, y0 - pad), x1 + pad, y1 + pad))
        resp = cl.models.generate_content(
            model=model, contents=[POLY_PROMPT.format(category=cat), crop],
            config=types.GenerateContentConfig(response_mime_type="application/json",
                                               response_schema=OneText, temperature=0.0))
        res[rid] = OneText.model_validate_json((resp.text or "").strip()).text
        a, b = usage(resp); tin += a; tout += b
    return res, (tin, tout)


def run_page(cl, model, page_dir, full_width):
    js = next(page_dir.glob("page_*.json"))
    meta = json.load(open(js))
    png = page_dir / f"{js.stem}.masked.png"
    full = Image.open(png).convert("RGB")
    pw = int(meta["page_width"])
    W = min(full_width, full.width); H = int(full.height * W / full.width)
    small = full.resize((W, H), Image.LANCZOS)
    scale = W / pw                                   # vertices live in page_width pixel space
    regions = collect_regions(meta)

    a_text, a_use = full_page_pass(cl, model, small, regions, scale)
    b_text, b_use = poly_pass(cl, model, full, regions)

    rows = []
    for (rid, dk, cat, _bb) in regions:
        ta, tb = a_text.get(rid, ""), b_text.get(rid, "")
        rows.append({"rid": rid, "doc": dk, "category": cat,
                     "full_page": ta, "per_polygon": tb, "agree": norm(ta) == norm(tb)})
    agree = sum(r["agree"] for r in rows)
    out = {"page": str(page_dir.relative_to(ROOT)), "model": model, "n_regions": len(regions),
           "agree": agree, "full_page_tokens": a_use, "per_polygon_tokens": b_use, "regions": rows}
    (page_dir / "extract_calib.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"  {out['page']}: {len(regions)} regions | agree {agree}/{len(regions)} | "
          f"full-page tok {a_use} | poly tok {b_use}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pages", nargs="+", help="page dirs (relative to 2_src_organized/ or absolute)")
    ap.add_argument("--model", default="gemini-3.5-flash")
    ap.add_argument("--full-width", type=int, default=1536, help="downsample width for the full-page pass")
    a = ap.parse_args()
    cl = make_client()
    print(f"model={a.model} full_width={a.full_width}")
    for p in a.pages:
        pd = Path(p) if Path(p).is_absolute() else (ROOT / p)
        if not pd.is_dir() or not list(pd.glob("page_*.masked.png")):
            print(f"  SKIP {p} (no masked page)"); continue
        try:
            run_page(cl, a.model, pd, a.full_width)
        except Exception as exc:
            print(f"  ERROR {p}: {exc}")


if __name__ == "__main__":
    main()
