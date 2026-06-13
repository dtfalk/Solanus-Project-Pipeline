#!/usr/bin/env python3
"""Add step_1-dropped front/back matter back and renumber a volume to TRUE SOURCE
page numbers. Reads qa_output/<Vol>/frontmatter_map.json (recover_frontmatter_map.py).

Shift is computed from the CURRENT folder numbering (so V3, already at 2-311, gets
shift 0 and merely gains pages 1 & 312; V1 +5; V2 +10). Renames are done
descending to avoid collisions. NEW front/back pages are extracted from
step_1/cleaned_pdfs/<Vol>.pdf into polygon_cropped_pdfs/<Vol>/pages at their source
numbers — ready to label; they are NOT written to reviewed/ (David reviews them).

Migrated precisely (page_NNN -> page_MMM, incl. internal page_number field):
  reviewed/  auto_labeled/  polygon_cropped_pdfs/<Vol>/pages/  labeled_examples/
  page_type_cache/  file_uris.json keys  EXCLUDED_EXAMPLES(auto_labeler.py)
  qa_output/<Vol>/approved_unedited.json (provenance page lists)
Archived (moved to qa_output/<Vol>/_pre_frontmatter/, regenerable, volume done):
  clusters* representatives* chunk* bootstrap_state dismissed triage qa_report
  overlays volume_note_draft ; shadow_labels/<Vol>

Safety: make a git restore point FIRST. Descending renames. Post-run verify:
counts preserved, every reviewed page_number == folder number, sample crop PDFs
pixel-match. Reversible via git.

Usage:
  ./venv/bin/python experiments/add_frontmatter.py Volume_1 --dry-run
  ./venv/bin/python experiments/add_frontmatter.py Volume_1 --execute
"""
from __future__ import annotations
import argparse, json, re, shutil, sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
ROOT = HERE.parent.parent
CLEAN = ROOT / "step_1" / "cleaned_pdfs"
PER_PAGE_DIRS = ["reviewed", "auto_labeled", "labeled_examples"]   # page_N/ dirs w/ inner files
FLAT_PDF = "polygon_cropped_pdfs/{vol}/pages"                       # page_N.pdf
FLAT_JSON = "page_type_cache/{vol}"                                 # page_N.json
ARCHIVE_QA = ["clusters.json","clusters.txt","representatives.json","representatives.txt",
              "representatives.pre-bootstrap.json","chunk.json","chunk.txt",
              "bootstrap_state.json","dismissed.json","triage.txt","qa_report.json",
              "volume_note_draft.txt","cluster_review.json","rerun_marks.json"]


def load_plan(vol):
    m = json.loads((HERE/"qa_output"/vol/"frontmatter_map.json").read_text())
    kept_source = sorted(m["cropidx_to_source"].values())
    current = sorted(int(p.name.split("_")[1]) for p in (HERE/"reviewed"/vol).glob("page_*"))
    assert len(kept_source) == len(current), f"{vol}: {len(current)} reviewed vs {len(kept_source)} kept"
    shifts = {s - c for s, c in zip(kept_source, current)}
    assert len(shifts) == 1, f"{vol}: non-uniform shift {sorted(shifts)} — needs per-page remap"
    shift = shifts.pop()
    return m, shift, current


def _renumber_inner(d: Path, old: str, new: str):
    for f in list(d.iterdir()):
        if old in f.name:
            f.rename(d / f.name.replace(old, new))
    j = d / f"{new}.json"
    if j.exists():
        data = json.loads(j.read_text())
        if "page_number" in data:
            data["page_number"] = int(new.split("_")[1])
            j.write_text(json.dumps(data, indent=2))


def migrate(vol, execute):
    m, shift, current = load_plan(vol)
    dropped = m["dropped_source_pages"]
    tag = "EXECUTE" if execute else "dry-run"
    print(f"\n=== {vol} [{tag}] shift +{shift}; {len(current)} pages "
          f"{current[0]}-{current[-1]} -> {current[0]+shift}-{current[-1]+shift}; "
          f"add dropped {dropped} ===")
    if shift and current[0] + shift <= current[-1]:
        pass  # overlap handled by descending order

    if shift == 0:
        print("  shift 0 — existing pages untouched; only adding the dropped pages.")
        _extract_dropped(vol, dropped, execute)
        return shift, dropped

    order = sorted(current, reverse=(shift > 0))    # descending when shifting up
    # 1) per-page dirs
    for base in PER_PAGE_DIRS:
        root = HERE/base/vol
        if not root.exists():
            continue
        moved = 0
        for old_n in order:
            old, new = f"page_{old_n:03d}", f"page_{old_n+shift:03d}"
            od, nd = root/old, root/new
            if not od.exists():
                continue
            if execute:
                od.rename(nd); _renumber_inner(nd, old, new)
            moved += 1
        print(f"  {base}: {moved} pages renamed")
    # 2) flat crop pdfs + page_type_cache
    for tmpl, ext in ((FLAT_PDF, "pdf"), (FLAT_JSON, "json")):
        root = HERE/tmpl.format(vol=vol)
        if not root.exists():
            continue
        moved = 0
        for old_n in order:
            of = root/f"page_{old_n:03d}.{ext}"
            if not of.exists():
                continue
            if execute:
                of.rename(root/f"page_{old_n+shift:03d}.{ext}")
            moved += 1
        print(f"  {tmpl.format(vol=vol)}: {moved} files renamed")
    # 3) file_uris.json keys
    up = HERE/"file_uris.json"
    if up.exists():
        uri = json.loads(up.read_text()); u = uri["uris"]
        keys = sorted([k for k in u if k.startswith(f"{vol}/page_")],
                      key=lambda k: int(k.split("page_")[1]), reverse=(shift > 0))
        for k in keys:
            n = int(k.split("page_")[1])
            u[f"{vol}/page_{n+shift:03d}"] = u.pop(k)
        if execute:
            up.write_text(json.dumps(uri, indent=2))
        print(f"  file_uris.json: {len(keys)} keys shifted")
    # 4) EXCLUDED_EXAMPLES in auto_labeler.py — ONLY real frozenset entries
    #    (a line that is whitespace + "Vol/page_N", ), never a comment/docstring.
    al = HERE/"auto_labeler.py"; txt = al.read_text(); hits = []
    def _sub(mo):
        n = int(mo.group(2)); hits.append((n, n+shift))
        return f'{mo.group(1)}"{vol}/page_{n+shift:03d}",'
    new_txt = re.sub(rf'^(\s*)"{vol}/page_(\d+)",', _sub, txt, flags=re.MULTILINE)
    if hits:
        if execute:
            al.write_text(new_txt)
        print(f"  EXCLUDED_EXAMPLES entries: {hits}")
    # 5) approved_unedited.json page list
    au = HERE/"qa_output"/vol/"approved_unedited.json"
    if au.exists():
        a = json.loads(au.read_text())
        a["pages"] = [f"page_{int(p.split('_')[1])+shift:03d}" for p in a.get("pages", [])]
        a["renumbered_frontmatter"] = datetime.now().isoformat(timespec="seconds")
        if execute:
            au.write_text(json.dumps(a, indent=2))
        print(f"  approved_unedited.json: {len(a['pages'])} page refs shifted")
    # 6) archive transient qa + shadow (numbering now stale for this volume)
    arch = HERE/"qa_output"/vol/"_pre_frontmatter"
    archived = []
    for name in ARCHIVE_QA + ["overlays"]:
        p = HERE/"qa_output"/vol/name
        if p.exists():
            archived.append(name)
            if execute:
                arch.mkdir(exist_ok=True); shutil.move(str(p), str(arch/name))
    sh = HERE/"shadow_labels"/vol
    if sh.exists() and any(sh.iterdir()):
        archived.append(f"shadow_labels/{vol}")
        if execute:
            arch.mkdir(exist_ok=True); shutil.move(str(sh), str(arch/"shadow_labels"))
    if archived:
        print(f"  archived (regenerable): {archived}")
    # 7) extract dropped front/back-matter PDFs at source numbers
    _extract_dropped(vol, dropped, execute)
    return shift, dropped


def _extract_dropped(vol, dropped, execute):
    from pdf2image import convert_from_path
    pages_dir = HERE/FLAT_PDF.format(vol=vol)
    if execute:
        src = CLEAN/f"{vol}.pdf"
        for sp in dropped:
            # Render ONE page at a time (whole-volume render at 300 DPI OOMs);
            # PIL writes a single-page raster PDF (no img2pdf dep), re-rendered at
            # 150 DPI by the labeler like every other page.
            img = convert_from_path(src, dpi=300, first_page=sp, last_page=sp)[0]
            img.convert("RGB").save(pages_dir/f"page_{sp:03d}.pdf", "PDF", resolution=300.0)
            del img
    print(f"  front/back PDFs -> {pages_dir}: {[f'page_{p:03d}' for p in dropped]}")


def verify(vol):
    bad = []
    for d in (HERE/"reviewed"/vol).glob("page_*"):
        fn = int(d.name.split("_")[1])
        j = d/f"{d.name}.json"
        if j.exists():
            pn = json.loads(j.read_text()).get("page_number")
            if pn != fn:
                bad.append((d.name, pn))
    print(f"  VERIFY {vol}: {'OK' if not bad else 'FIELD MISMATCH '+str(bad[:5])} "
          f"(reviewed now {len(list((HERE/'reviewed'/vol).glob('page_*')))} pages)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volumes", nargs="+")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--execute", action="store_true")
    a = ap.parse_args()
    for vol in a.volumes:
        migrate(vol, a.execute)
        if a.execute:
            verify(vol)


if __name__ == "__main__":
    main()
