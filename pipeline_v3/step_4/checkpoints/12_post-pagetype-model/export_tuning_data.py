#!/usr/bin/env python3
"""Export the human-corrected gold (reviewed/) as Gemini supervised-fine-tuning
JSONL, so the tuning dataset accumulates NOW and is ready to train once the corpus
is big enough (~300-500 pages; Google recommends >=100 examples to start).

WHY tune later: today every inference page ships ~12 few-shot images + a ~5k-token
prompt (~50k input tokens/page). A tuned model BAKES IN the schema + segmentation
conventions, so you can drop the few-shot images and shrink the prompt — cheaper
per page AND more consistent granularity (the exact thing that cost the review hour).
Tuned-endpoint inference is ~1.5x base; a tuned flash-lite would still be far cheaper
than few-shot-pro while internalizing the conventions.

This emits the Vertex-AI SFT envelope (systemInstruction + contents[user image,
model json]) with inline base64 images. VERIFY the exact field names against the
current Vertex `gemini-supervised-tuning-prepare` docs before you submit a job —
the wrapper occasionally changes; the (image -> json) PAYLOAD here is what matters
and is stable.

Usage:
    ./venv/bin/python export_tuning_data.py                 # all reviewed volumes
    ./venv/bin/python export_tuning_data.py --volumes Appendix_1 Appendix_3
    ./venv/bin/python export_tuning_data.py --val-frac 0.1  # held-out split
Outputs: tuning_data/train.jsonl, tuning_data/val.jsonl, tuning_data/STATS.txt
"""
from __future__ import annotations

import argparse
import base64
import json
import random
from collections import Counter
from io import BytesIO
from pathlib import Path

from auto_labeler import (
    render_page, SYSTEM_PROMPT, CATEGORIES, CATEGORY_DESCRIPTIONS,
    infer_page_type_from_labels, resolve_image_width,
)

SCRIPT_DIR = Path(__file__).resolve().parent
REVIEW_ROOT = SCRIPT_DIR / "reviewed"
PDF_ROOT = SCRIPT_DIR / "polygon_cropped_pdfs"   # source-of-truth page PDFs
AUTO_ROOT = SCRIPT_DIR / "auto_labeled"          # fallback (also holds page PDFs)
OUT_DIR = SCRIPT_DIR / "tuning_data"


def _find_pdf(vol: str, page_name: str) -> Path | None:
    for cand in (PDF_ROOT / vol / "pages" / f"{page_name}.pdf",
                 AUTO_ROOT / vol / page_name / f"{page_name}.pdf"):
        if cand.exists():
            return cand
    return None

# A slim system prompt for the tuned model: the schema + rules, WITHOUT the
# "Below are several example pages..." few-shot trailer (tuning replaces few-shot).
def _tuning_system_prompt() -> str:
    cats = "\n".join(f"  - {c}: {CATEGORY_DESCRIPTIONS[c]}" for c in CATEGORIES)
    body = SYSTEM_PROMPT.format(categories=cats)
    trailer = "Below are several example pages"
    return body.split(trailer)[0].rstrip() + "\n\nReturn ONLY the labels JSON for the page image."


def _canonical_target(data: dict) -> str:
    """The model's training target: the documents JSON, stripped to what the
    response schema emits (num_documents + documents)."""
    return json.dumps({
        "num_documents": data.get("num_documents", len(data.get("documents", {}))),
        "documents": data.get("documents", {}),
    }, separators=(",", ":"))


def _img_b64(pdf_path: Path, image_width: int | None) -> tuple[str, str]:
    img = render_page(pdf_path, image_width)[0].convert("RGB")
    buf = BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii"), "image/png"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--volumes", nargs="*", default=None,
                    help="Reviewed volumes to export (default: all under reviewed/).")
    ap.add_argument("--val-frac", type=float, default=0.1)
    ap.add_argument("--image-width", default="1024")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    image_width = resolve_image_width(args.image_width)

    vols = args.volumes or sorted(p.name for p in REVIEW_ROOT.iterdir() if p.is_dir())
    sys_prompt = _tuning_system_prompt()

    rows: list[tuple[str, str, dict]] = []   # (key, jsonl_line, meta)
    for vol in vols:
        vdir = REVIEW_ROOT / vol
        if not vdir.is_dir():
            print(f"  skip {vol} (not under reviewed/)"); continue
        for pd in sorted(vdir.glob("page_*")):
            jp = pd / f"{pd.name}.json"
            pdf = _find_pdf(vol, pd.name)
            if not (jp.exists() and pdf):
                continue
            data = json.load(open(jp))
            b64, mime = _img_b64(pdf, image_width)
            line = json.dumps({
                "systemInstruction": {"role": "system", "parts": [{"text": sys_prompt}]},
                "contents": [
                    {"role": "user", "parts": [
                        {"inlineData": {"mimeType": mime, "data": b64}},
                        {"text": "Label this page in the schema."},
                    ]},
                    {"role": "model", "parts": [{"text": _canonical_target(data)}]},
                ],
            })
            rows.append((f"{vol}/{pd.name}", line, {"type": infer_page_type_from_labels(data)}))

    # stratified-ish train/val split (shuffle, hold out val-frac)
    rng = random.Random(args.seed)
    rng.shuffle(rows)
    n_val = int(len(rows) * args.val_frac)
    val, train = rows[:n_val], rows[n_val:]

    OUT_DIR.mkdir(exist_ok=True)
    for name, subset in (("train", train), ("val", val)):
        with open(OUT_DIR / f"{name}.jsonl", "w") as f:
            for _k, line, _m in subset:
                f.write(line + "\n")

    by_type = Counter(m["type"] for _k, _l, m in rows)
    approx_img_tokens = 258  # ~per 1024px image tile (Gemini); body json adds more
    stats = [
        f"tuning dataset exported to {OUT_DIR}",
        f"  volumes: {', '.join(vols)}",
        f"  total examples: {len(rows)}  (train {len(train)} / val {len(val)})",
        f"  by page type: {dict(by_type)}",
        f"  system prompt chars: {len(sys_prompt)}",
        f"  NOTE: Google recommends >=100 examples; you have {len(rows)}.",
        "  NEXT: when corpus ~300-500 pages and conventions are stable, tune a flash model;",
        "        compare tuned-flash vs few-shot-pro on a held-out volume before committing.",
    ]
    (OUT_DIR / "STATS.txt").write_text("\n".join(stats) + "\n")
    print("\n".join(stats))


if __name__ == "__main__":
    main()
