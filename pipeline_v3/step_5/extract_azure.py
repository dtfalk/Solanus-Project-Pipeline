#!/usr/bin/env python3
"""STEP_5 Phase 2 — faithful text extraction with Azure Document Intelligence (prebuilt-read).

Pure OCR (no language-model normalization), so the engine's default is to transcribe
literally — preserving the typewritten typos the archivists kept. We already have the
layout from the gold polygons, so we use the READ model only (no layout model).

DOUBLE PASS per page, over the masked image:
  A. full-page Read  — one call; map each returned word to its gold polygon by geometry,
     concatenate per region in reading order, record OCR confidence.
  B. per-polygon Read — one call per region over a tight native-res crop.

Disagreements between the passes, and low-confidence words, become the human-review queue.
Per page folder it writes the cleaned extract_azure.json AND extract_azure.raw.json (the complete
unprocessed Azure responses for both passes). Gold geometry is never touched.

Creds (copy .env.template -> .env in this folder, then fill in):
  AZURE_DI_ENDPOINT=https://<resource>.cognitiveservices.azure.com/
  AZURE_DI_KEY=<key>

Usage:
  venv/bin/python extract_azure.py Volume_2/1_source_pages/page_242        # smoke test (1 page)
  venv/bin/python extract_azure.py Volume_1 Volume_2 ...                    # (see --help)
"""
from __future__ import annotations
import os, io, json, argparse
from datetime import datetime
from pathlib import Path
from PIL import Image
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest

Image.MAX_IMAGE_PIXELS = None
HERE  = Path(__file__).resolve().parent
ROOT  = HERE / "3_enriched"                       # dataset root (override with --root)
load_dotenv(HERE / ".env")                       # step_5/.env (see .env.template)

USD_PER_PAGE = 0.0015                             # Azure DI Read, S0 tier: $1.50 / 1000 pages
USAGE_CSV    = HERE / "usage_azure.csv"


def log_usage(page, full_calls, poly_calls):
    """Azure DI bills per analyzed page (= per analyze call), flat rate. Append a row to
    usage_azure.csv (step_4 convention) and return this page's cost in USD."""
    calls = full_calls + poly_calls
    cost  = calls * USD_PER_PAGE
    new = not USAGE_CSV.exists()
    with open(USAGE_CSV, "a") as f:
        if new:
            f.write("timestamp,page,full_page_calls,poly_calls,total_calls,cost_usd\n")
        f.write(f"{datetime.now().isoformat(timespec='seconds')},{page},"
                f"{full_calls},{poly_calls},{calls},{cost:.4f}\n")
    return cost


def make_client():
    ep = os.environ["AZURE_DI_ENDPOINT"].rstrip("/")
    key = os.environ["AZURE_DI_KEY"]
    return DocumentIntelligenceClient(ep, AzureKeyCredential(key))

def png_bytes(img: Image.Image) -> bytes:
    b = io.BytesIO(); img.save(b, "PNG"); return b.getvalue()

def analyze(client, img: Image.Image):
    poller = client.begin_analyze_document(
        "prebuilt-read", AnalyzeDocumentRequest(bytes_source=png_bytes(img)))
    return poller.result()

def pts(poly):                                  # gold polygon -> [(x,y),...]
    return [(v["x"], v["y"]) for v in poly["vertices"]]

def bbox(p):
    xs = [x for x, _ in p]; ys = [y for _, y in p]
    return min(xs), min(ys), max(xs), max(ys)

def in_poly(x, y, poly):                        # ray-cast point-in-polygon
    inside = False; n = len(poly); j = n - 1
    for i in range(n):
        xi, yi = poly[i]; xj, yj = poly[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-9) + xi):
            inside = not inside
        j = i
    return inside

def collect_regions(meta):
    out = []
    for dk, doc in (meta.get("documents") or {}).items():
        for cat, polys in (doc or {}).items():
            for i, p in enumerate(polys or []):
                pp = pts(p); out.append((f"{dk}.{cat}.{i}", dk, cat, pp, bbox(pp)))
    return out

def word_centroid(w):
    pg = w.polygon                              # flat [x1,y1,...,x4,y4]
    xs = pg[0::2]; ys = pg[1::2]
    return sum(xs) / len(xs), sum(ys) / len(ys)


def full_page_pass(client, full_img, regions):
    res = analyze(client, full_img)
    page = res.pages[0] if res.pages else None
    pwords = {rid: [] for (rid, *_) in regions}     # positioned words per region (page-pixel coords)
    for w in (getattr(page, "words", []) or []):    # reading order
        cx, cy = word_centroid(w)
        hit = next((rid for (rid, dk, cat, poly, bb) in regions if in_poly(cx, cy, poly)), None)
        if hit is None:                             # fallback: nearest region center
            hit = min(regions, key=lambda r: (cx-(r[4][0]+r[4][2])/2)**2 + (cy-(r[4][1]+r[4][3])/2)**2)[0]
        pg = w.polygon                              # flat [x1,y1,...,x4,y4]
        box = [round(min(pg[0::2]),1), round(min(pg[1::2]),1), round(max(pg[0::2]),1), round(max(pg[1::2]),1)]
        pwords[hit].append({"t": w.content,
                            "c": round(float(getattr(w, "confidence", 1.0) or 1.0), 3), "box": box})
    text    = {rid: " ".join(x["t"] for x in pwords[rid]) for rid in pwords}
    minconf = {rid: min((x["c"] for x in pwords[rid]), default=None) for rid in pwords}
    return text, minconf, res.as_dict(), pwords     # + the full raw response + positioned words

def poly_pass(client, full_img, regions, pad=10):
    text, minconf, words, raw = {}, {}, {}, {}
    for (rid, dk, cat, poly, (x0, y0, x1, y1)) in regions:
        crop = full_img.crop((max(0, x0-pad), max(0, y0-pad), x1+pad, y1+pad))
        try:
            res = analyze(client, crop)
            raw[rid] = res.as_dict()                 # full raw Azure response for this crop
            ws = [{"t": w.content, "c": round(float(getattr(w, "confidence", 1.0) or 1.0), 3)}
                  for pg in (res.pages or []) for w in (getattr(pg, "words", []) or [])]
            words[rid] = ws
            text[rid] = " ".join(w["t"] for w in ws) if ws else (res.content or "").replace("\n", " ").strip()
            minconf[rid] = min((w["c"] for w in ws), default=None)
        except Exception as exc:
            text[rid] = ""; minconf[rid] = None; words[rid] = []; raw[rid] = None
    return text, minconf, words, raw

def norm(s):
    return " ".join((s or "").split()).lower()


def run_page(client, page_dir):
    js = next(page_dir.glob("page_*.json")); meta = json.load(open(js))
    full = Image.open(page_dir / f"{js.stem}.masked.png").convert("RGB")
    regions = collect_regions(meta)
    a_text, a_conf, a_raw, a_words = full_page_pass(client, full, regions)
    b_text, b_conf, b_words, b_raw = poly_pass(client, full, regions)
    rows, agree, lowconf = [], 0, 0
    for (rid, dk, cat, poly, bb) in regions:
        ta, tb = a_text.get(rid, ""), b_text.get(rid, "")
        ag = norm(ta) == norm(tb); agree += ag
        mc = min([c for c in (a_conf.get(rid), b_conf.get(rid)) if c is not None], default=None)
        if mc is not None and mc < 0.90: lowconf += 1
        rows.append({"rid": rid, "doc": dk, "category": cat, "full_page": ta,
                     "per_polygon": tb, "agree": ag, "min_conf": mc,
                     "words": a_words.get(rid, [])})            # positioned words [{t, c, box}] for 1:1 render
    cost = log_usage(str(page_dir.relative_to(ROOT)), 1, len(regions))
    low_words = sum(1 for ws in a_words.values() for w in ws if w["c"] < 0.90)
    out = {"page": str(page_dir.relative_to(ROOT)), "engine": "azure-prebuilt-read",
           "n_regions": len(regions), "agree": agree,
           "low_conf_words": low_words, "low_conf_regions": lowconf,
           "azure_pages": 1 + len(regions), "cost_usd": round(cost, 4), "regions": rows}
    (page_dir / "extract_azure.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    # full, unprocessed Azure responses (both passes) — never thrown away, in case we need them later
    (page_dir / "extract_azure.raw.json").write_text(json.dumps(
        {"engine": "azure-prebuilt-read", "full_page": a_raw, "per_polygon": b_raw},
        indent=2, ensure_ascii=False, default=str))
    print(f"  {out['page']}: {len(regions)} regions | agree {agree}/{len(regions)} | "
          f"<0.90 conf: {low_words} words in {lowconf} regions | {out['azure_pages']} OCR pages | ${cost:.4f}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pages", nargs="+", help="page dirs (relative to --root or absolute)")
    ap.add_argument("--root", default="3_enriched", help="dataset root to read from / write into (default: 3_enriched)")
    a = ap.parse_args()
    global ROOT
    ROOT = (HERE / a.root).resolve()
    client = make_client()
    total = 0.0
    for p in a.pages:
        pd = Path(p) if Path(p).is_absolute() else (ROOT / p)
        if not pd.is_dir() or not list(pd.glob("page_*.masked.png")):
            print(f"  SKIP {p} (no masked page)"); continue
        try:
            total += run_page(client, pd).get("cost_usd", 0.0)
        except Exception as exc:
            print(f"  ERROR {p}: {type(exc).__name__}: {exc}")
    print(f"\nthis run: ${total:.4f}  |  cumulative log: {USAGE_CSV.name}")


if __name__ == "__main__":
    main()
