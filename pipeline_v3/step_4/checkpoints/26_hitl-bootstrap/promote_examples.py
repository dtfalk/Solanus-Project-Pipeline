#!/usr/bin/env python3
"""Promote hand-reviewed gold pages into the few-shot pool (HITL_BOOTSTRAP.md PHASE 4).

Copies reviewed/<Vol>/page_NNN/page_NNN.json (gold labels) plus
polygon_cropped_pdfs/<Vol>/pages/page_NNN.pdf (reviewed/ holds no PDFs) into
labeled_examples/<Vol>/page_NNN/. Audit-gated by design: requires an explicit
--pages list, refuses to overwrite an existing pool page unless --force, warns
when a page is on EXCLUDED_EXAMPLES, and never deletes anything.

With --upload, uploads ONLY the pages promoted in this run to the Gemini Files
API and merges them into file_uris.json — no full-pool re-upload. (Files expire
48h after THEIR upload; the map's top-level uploaded_at still reflects the last
FULL upload_examples.py run.)

Usage:
    ./venv/bin/python promote_examples.py Volume_2 --pages 3,17,42-45 --upload
"""
from __future__ import annotations

import argparse
import io
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from auto_labeler import ENV_PATH, EXCLUDED_EXAMPLES, parse_pages_arg

SCRIPT_DIR        = Path(__file__).resolve().parent
REVIEW_ROOT       = SCRIPT_DIR / "reviewed"
POOL_ROOT         = SCRIPT_DIR / "labeled_examples"
POLYGON_PDFS_ROOT = SCRIPT_DIR / "polygon_cropped_pdfs"
URI_MAP_PATH      = SCRIPT_DIR / "file_uris.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("volume", help="Volume the pages belong to (e.g. Volume_2).")
    p.add_argument("--pages", type=str, required=True,
                   help="Comma-separated page numbers/ranges to promote (e.g. '3,17,42-45'). "
                        "Explicit by design — promotion is the gold gate.")
    p.add_argument("--force", action="store_true",
                   help="Overwrite pages already in the pool (e.g. after re-reviewing them).")
    p.add_argument("--upload", action="store_true",
                   help="Also upload the newly promoted pages to the Gemini Files API and "
                        "merge into file_uris.json.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    nums = sorted(parse_pages_arg(args.pages))

    promoted: list[str] = []     # "Vol/page_NNN" keys copied this run
    skipped:  list[str] = []
    errors:   list[str] = []
    for num in nums:
        name = f"page_{num:03d}"
        key  = f"{args.volume}/{name}"
        src_json = REVIEW_ROOT / args.volume / name / f"{name}.json"
        src_pdf  = POLYGON_PDFS_ROOT / args.volume / "pages" / f"{name}.pdf"
        dest     = POOL_ROOT / args.volume / name
        if not src_json.exists():
            errors.append(f"{key}: no gold at {src_json.relative_to(SCRIPT_DIR)} — review it first")
            continue
        if not src_pdf.exists():
            errors.append(f"{key}: no PDF at {src_pdf.relative_to(SCRIPT_DIR)}")
            continue
        if dest.exists() and not args.force:
            skipped.append(f"{key}: already in pool (use --force to refresh from reviewed/)")
            continue
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_json, dest / f"{name}.json")
        shutil.copy2(src_pdf, dest / f"{name}.pdf")
        promoted.append(key)
        if key in EXCLUDED_EXAMPLES:
            print(f"  WARNING: {key} is on EXCLUDED_EXAMPLES in auto_labeler.py — it stays "
                  f"OUT of few-shot selection until removed from that list.")

    for line in errors:
        print(f"  ERROR  {line}")
    for line in skipped:
        print(f"  skip   {line}")
    print(f"Promoted {len(promoted)} page(s) into {POOL_ROOT.name}/{args.volume}/ "
          f"({len(skipped)} skipped, {len(errors)} errors).")

    if args.upload and promoted:
        if not URI_MAP_PATH.exists():
            raise SystemExit("file_uris.json missing — run upload_examples.py once for a full upload.")
        uri_map = json.loads(URI_MAP_PATH.read_text())
        width_arg = str(uri_map.get("image_width", "1024"))

        from dotenv import load_dotenv
        from google import genai
        from google.genai import types
        from upload_examples import render_to_png_bytes, resolve_width
        load_dotenv(ENV_PATH)
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise SystemExit("No GEMINI_API_KEY in .env")
        client = genai.Client(api_key=api_key)

        width = resolve_width(width_arg)
        print(f"Uploading {len(promoted)} new example(s) at width={width_arg}...")
        for i, key in enumerate(promoted, 1):
            vol, name = key.split("/", 1)
            pdf_path = POOL_ROOT / vol / name / f"{name}.pdf"
            uploaded = client.files.upload(
                file   = io.BytesIO(render_to_png_bytes(pdf_path, width)),
                config = types.UploadFileConfig(mime_type="image/png", display_name=key),
            )
            uri_map["uris"][key] = uploaded.name
            print(f"  [{i}/{len(promoted)}] {key} -> {uploaded.name}")
        uri_map["last_incremental_upload"] = datetime.now().isoformat(timespec="seconds")
        URI_MAP_PATH.write_text(json.dumps(uri_map, indent=2))
        print(f"Merged into {URI_MAP_PATH.name}.")
        full_at = uri_map.get("uploaded_at", "?")
        print(f"NOTE: the rest of the pool was last fully uploaded {full_at}; Files-API "
              f"entries expire 48h after upload — if that is stale, re-run upload_examples.py.")

    if promoted:
        nums_str = ",".join(k.split("page_")[1].lstrip("0") or "0" for k in promoted)
        pin_str  = ",".join(promoted)
        print(f"\nNext (HITL_BOOTSTRAP.md):")
        print(f"  draft the volume note:  ./venv/bin/python review_diff.py {args.volume} --draft-note")
        print(f"  label the rest:         ./venv/bin/python auto_labeler.py --volume {args.volume} "
              f"--pin-examples '{pin_str}'")
        print(f"  (promoted page numbers: {nums_str})")


if __name__ == "__main__":
    main()
