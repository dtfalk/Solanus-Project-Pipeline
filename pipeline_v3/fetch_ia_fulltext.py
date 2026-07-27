"""fetch_ia_fulltext.py — try to pull the PUBLIC OCR full-text (_djvu.txt) for the IA Solanus books.

For each Internet Archive item we found, look up its full-text file via metadata (format "DjVuTXT") and try
to download it. Controlled-digital-lending books often gate this too — we validate that what comes back is
real OCR text (not an HTML "borrow" stub) before keeping it. Saves to public_media/text_fulltext/.

    python fetch_ia_fulltext.py
"""
from __future__ import annotations
import re
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "public_media" / "text_fulltext"
OUT.mkdir(parents=True, exist_ok=True)
S = requests.Session()
S.headers.update({"User-Agent": "SolanusArchiveBot/1.0 (research; davidtobiasfalk@gmail.com)"})

# IA identifiers for the Solanus biographies (from the items already located)
IDENTS = [
    "fathersolanussto0000odel", "fathersolanussto0000odel_i5m6",
    "porterofsaintbon0000deru", "porterofsaintbon0000jame",
    "solanuscaseyoffi0000unse", "solanuscaseyoffi00cros",
    "thankgodaheadoft0000cros", "thankgodaheadoft0000cros_x7o8",
]


def looks_like_text(body: bytes) -> bool:
    """Real OCR text, not an HTML access-restricted page."""
    head = body[:400].lstrip().lower()
    if head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<head" in head:
        return False
    return len(body) > 2000          # a real book OCR is large


def fetch(ident: str):
    try:
        meta = S.get(f"https://archive.org/metadata/{ident}", timeout=40).json()
    except Exception as e:
        return ident, f"metadata error: {str(e)[:50]}"
    title = (meta.get("metadata", {}) or {}).get("title", ident)
    # find the full-text file (DjVuTXT or *_djvu.txt / *.txt)
    txt_files = [f["name"] for f in meta.get("files", [])
                 if f.get("format") == "DjVuTXT" or f.get("name", "").endswith("_djvu.txt")
                 or (f.get("name", "").endswith(".txt") and "meta" not in f.get("name", ""))]
    if not txt_files:
        return ident, f"no full-text file listed ({title[:40]})"
    for name in txt_files:
        url = f"https://archive.org/download/{ident}/{name}"
        try:
            r = S.get(url, timeout=60)
            if r.status_code == 200 and looks_like_text(r.content):
                dest = OUT / f"{ident}.txt"
                dest.write_bytes(r.content)
                words = len(re.findall(r"\w+", r.content.decode("utf-8", "ignore")))
                return ident, f"OK -> {dest.name} ({words:,} words) [{title[:40]}]"
            else:
                return ident, f"gated/empty (HTTP {r.status_code}) [{title[:40]}]"
        except Exception as e:
            return ident, f"download error: {str(e)[:50]}"
    return ident, "no usable text"


def main():
    print("Trying public OCR full-text for the IA Solanus books...\n")
    ok = 0
    for ident in IDENTS:
        i, msg = fetch(ident)
        print(f"  {i:32} {msg}")
        if msg.startswith("OK"):
            ok += 1
    print(f"\n{ok}/{len(IDENTS)} books had public full-text -> {OUT}")


if __name__ == "__main__":
    main()
