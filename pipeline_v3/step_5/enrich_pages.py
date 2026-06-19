#!/usr/bin/env python3
"""Build an enriched mirror of 2_src_organized/ with explicit, self-consistent page numbering.

Reads 2_src_organized/ (NEVER modifies it) and writes a NEW folder (default 3_enriched/).

NUMBERING MODEL — two numbers per page:
  pdf_page_number      : the page's physical position in the scanned PDF (contiguous, unique)
  page_number_in_type  : the page's number within its type
                           - source pages       : the document's own printed page number (may skip)
                           - table of contents  : contiguous 1..N
                           - post pages         : contiguous 1-based index per section

FILESYSTEM IDENTITY (Option A): folders AND files are named by pdf_page_number — the stable,
gapless physical key. Files are already pdf-named, so only the source-page *folders* are renamed
in the output (e.g. 1_source_pages/page_242 -> page_252). page_number_in_type lives in the JSON
field + the per-section manifest, NOT in the filenames.

Each gold page JSON is rewritten general -> specific: `source_file` first (reduced to the bare
document name, e.g. "Volume_1"), then the page numbering
(`page_number` renamed to `pdf_page_number`, plus `page_type` and `page_number_in_type`), then the
page specifics (width/height/dpi/num_documents), ending with `documents` (the polygons). No value
is lost — page_number's value lives on as pdf_page_number.
Each document root gets a page_index.json mapping the two schemes for every page.
All other files (PDFs, masked PNGs/PDFs, OCR jsons, .backups, ...) are mirrored unchanged —
hardlinked when possible (≈ no extra disk), copied across filesystems.

Stdlib only; repeatable / cross-machine. Output write modes:
  (default)        overwrite every file in the output
  --no-overwrite   skip files already present (only fill gaps)
  --fresh          delete the entire output folder first, then rebuild from scratch
  --dry-run        report only; write/delete nothing

Usage:
  python3 enrich_pages.py                 # build/refresh 3_enriched/ from 2_src_organized/
  python3 enrich_pages.py --fresh         # wipe 3_enriched/ and rebuild clean
  python3 enrich_pages.py --dry-run       # preview, write nothing
  python3 enrich_pages.py --no-overwrite  # only fill in missing files
"""
from __future__ import annotations
import os, re, json, shutil, argparse
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent

TYPE_MAP = {                                          # subfolder -> page_type
    "0_table_of_contents": "table_of_contents",
    "1_source_pages":      "source_page",
    "2_post_pages":        "post_page",
}
TYPE_SUB  = {v: k for k, v in TYPE_MAP.items()}       # page_type -> subfolder
TYPE_ORD  = {"table_of_contents": 0, "source_page": 1, "post_page": 2}
GOLD_RE    = re.compile(r"page_(\d+)\.json")          # gold page json: page_252.json
PAGEDIR_RE = re.compile(r"page_(\d+)")                # page folder:    page_242


def gold_json(folder: Path):
    for f in folder.iterdir():
        if f.is_file() and GOLD_RE.fullmatch(f.name):
            return f
    return None


def scan_pages(SRC: Path) -> dict:
    """resolved page-folder Path -> {section, page_type, pdf, in_type, out_name}."""
    page_meta = {}
    for sub, ptype in TYPE_MAP.items():
        for type_dir in sorted(SRC.glob(f"*/{sub}")):
            section = type_dir.parent.name
            info = []                                  # (folder, pdf_page_number)
            for d in type_dir.iterdir():
                if d.is_dir() and PAGEDIR_RE.fullmatch(d.name):
                    g = gold_json(d)
                    if g:
                        info.append((d, int(GOLD_RE.fullmatch(g.name).group(1))))
            info.sort(key=lambda t: t[1])              # by pdf page order
            for i, (d, pdf) in enumerate(info, 1):
                # within-type number: post = fresh 1-based index; source/toc = the folder number
                in_type = i if ptype == "post_page" else int(PAGEDIR_RE.fullmatch(d.name).group(1))
                page_meta[d.resolve()] = {
                    "section": section, "page_type": ptype, "pdf": pdf,
                    "in_type": in_type, "out_name": f"page_{pdf:03d}",   # filesystem = pdf (Option A)
                }
    return page_meta


def remap(rel: Path, SRC: Path, page_meta: dict) -> Path:
    """Output rel path: rename the page-folder component to its pdf-numbered name."""
    parts = list(rel.parts)
    for i, p in enumerate(parts):
        if p in TYPE_MAP and i + 1 < len(parts) and PAGEDIR_RE.fullmatch(parts[i + 1]):
            m = page_meta.get(SRC.joinpath(*parts[:i + 2]).resolve())
            if m:
                parts[i + 1] = m["out_name"]
            break
    return Path(*parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="2_src_organized")
    ap.add_argument("--out", default="3_enriched")
    ap.add_argument("--no-overwrite", action="store_true",
                    help="skip files already present in the output (default: overwrite everything)")
    ap.add_argument("--fresh", action="store_true",
                    help="delete the ENTIRE output folder first, then rebuild from scratch")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing")
    a = ap.parse_args()

    SRC = (HERE / a.src).resolve()
    OUT = (HERE / a.out).resolve()
    if not SRC.is_dir():
        raise SystemExit(f"no source dir: {SRC}")
    if OUT == SRC or SRC in OUT.parents:
        raise SystemExit("--out must be a separate folder, outside --src")

    if a.fresh and OUT.exists():
        if HERE not in OUT.parents:                  # safety: only ever delete inside step_5/
            raise SystemExit(f"refusing --fresh on {OUT} (not under {HERE})")
        if a.dry_run:
            print(f"[--fresh] would delete existing output: {OUT}")
        else:
            shutil.rmtree(OUT)
            print(f"[--fresh] deleted existing output: {OUT}")

    page_meta = scan_pages(SRC)

    enriched = linked = copied = skipped = renamed = 0
    samples = {}
    for src in sorted(SRC.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(SRC)
        out_rel = remap(rel, SRC, page_meta)
        dst = OUT / out_rel
        m = page_meta.get(src.parent.resolve())
        if m and GOLD_RE.fullmatch(src.name) and ".backups" not in src.parts:   # gold page JSON -> enrich
            orig = json.loads(src.read_text())
            # order general -> specific: source document, then page numbering (page_number renamed
            # to pdf_page_number), then page specifics, ending with the documents/polygons.
            meta = {}
            if "source_file" in orig:
                meta["source_file"] = Path(orig["source_file"]).stem   # ".../Volume_1.pdf" -> "Volume_1"
            meta["pdf_page_number"] = m["pdf"]
            meta["page_type"] = m["page_type"]
            meta["page_number_in_type"] = m["in_type"]
            drop = set(meta) | {"source_file", "page_number", "documents"}
            for k, v in orig.items():
                if k not in drop:
                    meta[k] = v
            if "documents" in orig:
                meta["documents"] = orig["documents"]
            enriched += 1
            if out_rel != rel:
                renamed += 1
            if m["page_type"] not in samples:
                arrow = f"  ->  {out_rel}" if out_rel != rel else "   (folder unchanged)"
                samples[m["page_type"]] = (f"{rel}{arrow}",
                                           {"page_type": m["page_type"], "pdf_page_number": m["pdf"],
                                            "page_number_in_type": m["in_type"]})
            if not a.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
        else:                                                                   # mirror unchanged
            if dst.exists() and a.no_overwrite:
                skipped += 1
                continue
            linked += 1
            if not a.dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    dst.unlink()
                try:
                    os.link(src, dst)
                except OSError:
                    shutil.copy2(src, dst); linked -= 1; copied += 1

    # per-section manifest (page_index.json) — the two-scheme conversion table
    bysec = defaultdict(list)
    for m in page_meta.values():
        bysec[m["section"]].append(m)
    for section, ms in bysec.items():
        ms.sort(key=lambda m: (TYPE_ORD[m["page_type"]], m["pdf"]))
        manifest = {"section": section, "pages": [
            {"page_type": m["page_type"], "pdf_page_number": m["pdf"],
             "page_number_in_type": m["in_type"], "folder": f"{TYPE_SUB[m['page_type']]}/{m['out_name']}"}
            for m in ms]}
        if not a.dry_run:
            (OUT / section).mkdir(parents=True, exist_ok=True)
            (OUT / section / "page_index.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    head = "DRY-RUN (wrote nothing)" if a.dry_run else "DONE"
    print(f"{head}: {enriched} gold JSONs enriched ({renamed} folders renamed to pdf number) | "
          f"{linked} mirrored | {copied} copied | {skipped} skipped | {len(bysec)} page_index.json manifests")
    print(f"  source (untouched): {SRC}")
    print(f"  output            : {OUT}")
    for rel, f in samples.values():
        print(f"  {rel}\n      -> {f}")


if __name__ == "__main__":
    main()
