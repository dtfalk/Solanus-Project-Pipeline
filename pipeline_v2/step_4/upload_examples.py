"""
Upload every labeled_examples page to the Gemini Files API at a chosen
image width. Writes the resulting URI map to file_uris.json so the labeler
can reference uploaded files instead of inlining images on every request.

Files expire after 48 hours — re-run this script when they do.

Usage:
    ./venv/bin/python upload_examples.py                  # default 1024 width
    ./venv/bin/python upload_examples.py --image-width 2048
    ./venv/bin/python upload_examples.py --image-width full
"""
from __future__ import annotations

import argparse
import io
import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pdf2image import convert_from_path
from google import genai
from google.genai import types

SCRIPT_DIR           = Path(__file__).resolve().parent
LABELED_EXAMPLES_DIR = SCRIPT_DIR / "labeled_examples"
ENV_PATH             = SCRIPT_DIR / ".env"
URI_MAP_PATH         = SCRIPT_DIR / "file_uris.json"
RENDER_DPI           = 150


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--image-width", default="1024",
                   help="Width to render images at (px) or 'full'. Default: 1024.")
    return p.parse_args()


def resolve_width(arg: str) -> int | None:
    if arg.strip().lower() == "full":
        return None
    return int(arg)


def render_to_png_bytes(pdf_path: Path, image_width: int | None) -> bytes:
    """Render the first page of pdf_path at given width and return PNG bytes."""
    page = convert_from_path(pdf_path, dpi=RENDER_DPI, first_page=1, last_page=1)[0]
    if image_width is not None and image_width < page.width:
        new_h = int(round(page.height * (image_width / page.width)))
        page = page.resize((image_width, new_h))
    buf = io.BytesIO()
    page.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()


def main() -> None:
    args = parse_args()
    image_width = resolve_width(args.image_width)

    load_dotenv(ENV_PATH)
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise SystemExit("No GEMINI_API_KEY in .env")
    client = genai.Client(api_key=api_key)

    page_dirs = sorted(
        p for doc in sorted(LABELED_EXAMPLES_DIR.iterdir()) if doc.is_dir()
        for p in sorted(doc.glob("page_*"))
        if (p / f"{p.name}.pdf").exists()
    )
    print(f"Uploading {len(page_dirs)} examples at width={args.image_width}…")

    uris: dict[str, str] = {}
    for i, page_dir in enumerate(page_dirs, 1):
        key      = f"{page_dir.parent.name}/{page_dir.name}"
        pdf_path = page_dir / f"{page_dir.name}.pdf"

        png_bytes = render_to_png_bytes(pdf_path, image_width)
        uploaded  = client.files.upload(
            file   = io.BytesIO(png_bytes),
            config = types.UploadFileConfig(
                mime_type    = "image/png",
                display_name = key,
            ),
        )
        uris[key] = uploaded.name  # e.g. "files/abc123xyz"
        print(f"  [{i:3d}/{len(page_dirs)}] {key} -> {uploaded.name}")

    URI_MAP_PATH.write_text(json.dumps({
        "image_width": args.image_width,
        "uploaded_at": datetime.now().isoformat(timespec="seconds"),
        "uris":        uris,
    }, indent=2))
    print(f"\nWrote {len(uris)} URIs to {URI_MAP_PATH.name}")
    print("Note: URIs expire after 48 hours; re-run this script to refresh.")


if __name__ == "__main__":
    main()
