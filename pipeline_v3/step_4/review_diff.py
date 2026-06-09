#!/usr/bin/env python3
"""Compare auto_labeled/ (model output) against reviewed/ (human-corrected) to
learn the systematic corrections a human made — so the prompt / snap can be
tuned to produce them more reliably.

The step_4 editor has no "change category" action, so a recategorization appears
as delete-old-box + draw-new-box at the same spot. This tool reconstructs intent:
  - RESIZE         same polygon id, moved/resized vertices (geometry)
  - RECATEGORIZE   a removed box + an added box overlap >IOU, different category
  - ADD            an added box overlapping nothing removed (recall miss)
  - REMOVE         a removed box overlapping nothing added (spurious box)
  - DOC-COUNT      num_documents changed

Usage:
    python review_diff.py Appendix_1            # one volume
    python review_diff.py --all
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

SCRIPT_DIR  = Path(__file__).resolve().parent
SOURCE_ROOT = SCRIPT_DIR / "auto_labeled"
REVIEW_ROOT = SCRIPT_DIR / "reviewed"

IOU_MATCH = 0.5      # overlap above which a removed+added pair is the "same region"
RESIZE_IOU = 0.97    # same-id boxes below this IoU count as a meaningful resize


def _bbox(vertices):
    xs = [v["x"] for v in vertices]; ys = [v["y"] for v in vertices]
    return (min(xs), min(ys), max(xs), max(ys))


def _iou(a, b):
    ix = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    ua = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def _load_boxes(path):
    """Return (boxes, num_documents). boxes: list of {id, doc, cat, bbox}."""
    data = json.load(open(path, encoding="utf-8"))
    boxes = []
    for doc_id, doc in data.get("documents", {}).items():
        if not isinstance(doc, dict):
            continue
        for cat, polys in doc.items():
            if not isinstance(polys, list):
                continue
            for b in polys:
                if not b.get("vertices"):
                    continue
                boxes.append({"id": b.get("id"), "doc": doc_id, "cat": cat,
                              "bbox": _bbox(b["vertices"])})
    return boxes, data.get("num_documents")


def diff_page(src_path, rev_path):
    src, src_nd = _load_boxes(src_path)
    rev, rev_nd = _load_boxes(rev_path)
    src_by_id = {b["id"]: b for b in src if b["id"]}
    rev_by_id = {b["id"]: b for b in rev if b["id"]}

    resized, recat, added, removed = [], [], [], []

    # same id in both -> possible resize
    for bid, sb in src_by_id.items():
        rb = rev_by_id.get(bid)
        if rb and _iou(sb["bbox"], rb["bbox"]) < RESIZE_IOU:
            resized.append((sb, rb))

    # id only on one side
    only_removed = [b for bid, b in src_by_id.items() if bid not in rev_by_id]
    only_added   = [b for bid, b in rev_by_id.items() if bid not in src_by_id]

    # match removed<->added spatially: same region -> recategorize or redraw
    used_add = set()
    for rb in only_removed:
        best, best_iou = None, IOU_MATCH
        for i, ab in enumerate(only_added):
            if i in used_add:
                continue
            v = _iou(rb["bbox"], ab["bbox"])
            if v >= best_iou:
                best, best_iou = i, v
        if best is not None:
            used_add.add(best)
            ab = only_added[best]
            if ab["cat"] != rb["cat"]:
                recat.append((rb, ab))
            else:
                resized.append((rb, ab))      # same category, redrawn = geometry
        else:
            removed.append(rb)
    added = [ab for i, ab in enumerate(only_added) if i not in used_add]

    return {
        "resized": resized, "recat": recat, "added": added, "removed": removed,
        "doc_count": (src_nd, rev_nd) if src_nd != rev_nd else None,
    }


def analyze_volume(volume):
    src_dir = SOURCE_ROOT / volume
    rev_dir = REVIEW_ROOT / volume
    pages = sorted(p for p in rev_dir.iterdir() if p.is_dir() and p.name.startswith("page_"))

    agg = {"resized": 0, "recat": Counter(), "added": Counter(), "removed": Counter(),
           "doc_count_changes": 0, "pages_changed": 0, "pages_compared": 0}
    grew = shrank = 0
    edge_out = Counter()   # which edges the human pushed OUTWARD (model clipped there)
    per_page = []

    for pd in pages:
        name = pd.name
        src_path = src_dir / name / f"{name}.json"
        rev_path = pd / f"{name}.json"
        if not src_path.exists() or not rev_path.exists():
            continue
        agg["pages_compared"] += 1
        d = diff_page(src_path, rev_path)
        n_changes = len(d["resized"]) + len(d["recat"]) + len(d["added"]) + len(d["removed"]) \
            + (1 if d["doc_count"] else 0)
        if n_changes:
            agg["pages_changed"] += 1
            per_page.append((name, d))

        for sb, rb in d["resized"]:
            agg["resized"] += 1
            sa = (sb["bbox"][2]-sb["bbox"][0]) * (sb["bbox"][3]-sb["bbox"][1])
            ra = (rb["bbox"][2]-rb["bbox"][0]) * (rb["bbox"][3]-rb["bbox"][1])
            grew  += ra > sa * 1.02
            shrank += ra < sa * 0.98
            for k, edge in zip((0, 1, 2, 3), ("left", "top", "right", "bottom")):
                d_out = (sb["bbox"][k] - rb["bbox"][k]) if k < 2 else (rb["bbox"][k] - sb["bbox"][k])
                if d_out > 8:   # human moved this edge outward by >8px
                    edge_out[edge] += 1
        for rb, ab in d["recat"]:
            agg["recat"][f'{rb["cat"]} -> {ab["cat"]}'] += 1
        for ab in d["added"]:
            agg["added"][ab["cat"]] += 1
        for rb in d["removed"]:
            agg["removed"][rb["cat"]] += 1
        if d["doc_count"]:
            agg["doc_count_changes"] += 1

    return agg, grew, shrank, edge_out, per_page


def _print(volume, agg, grew, shrank, edge_out, per_page):
    print(f"\n========== {volume}: {agg['pages_changed']} page(s) changed ==========")
    tot_recat = sum(agg["recat"].values())
    tot_add   = sum(agg["added"].values())
    tot_rem   = sum(agg["removed"].values())
    print(f"  RECATEGORIZE: {tot_recat}   ADD: {tot_add}   REMOVE: {tot_rem}   "
          f"RESIZE: {agg['resized']}   DOC-COUNT: {agg['doc_count_changes']}")

    if agg["recat"]:
        print("\n  -- Recategorizations (model label -> human label), by frequency --")
        for k, n in agg["recat"].most_common():
            print(f"     {n:>3}  {k}")
    if agg["added"]:
        print("\n  -- Boxes the human ADDED (model recall misses), by category --")
        for k, n in agg["added"].most_common():
            print(f"     {n:>3}  {k}")
    if agg["removed"]:
        print("\n  -- Boxes the human REMOVED (model over-produced), by category --")
        for k, n in agg["removed"].most_common():
            print(f"     {n:>3}  {k}")
    if agg["resized"]:
        print(f"\n  -- Resizes: {agg['resized']} (grew {grew}, shrank {shrank}); "
              f"edges pushed OUTWARD: {dict(edge_out)} --")


def draft_note(volume, agg, grew, shrank, edge_out):
    """Render the correction aggregates as a DRAFT VOLUME_PROMPT_NOTES entry
    (HITL_BOOTSTRAP.md PHASE 4). Deterministic; the human supplies the WHY."""
    MIN = 2   # systematic = seen at least twice; singletons go in the footer
    bullets, singles = [], []
    for k, n in agg["recat"].most_common():
        if n >= MIN:
            frm, to = k.split(" -> ")
            bullets.append(f"- Regions the model labels '{frm}' here are usually '{to}' "
                           f"(reviewer relabeled {n}).")
        else:
            singles.append(f"recat {k}")
    for k, n in agg["added"].most_common():
        if n >= MIN:
            bullets.append(f"- The model MISSES '{k}' regions on these pages (reviewer "
                           f"added {n}) — look for them explicitly.")
        else:
            singles.append(f"add {k}")
    for k, n in agg["removed"].most_common():
        if n >= MIN:
            bullets.append(f"- The model over-produces '{k}' (reviewer removed {n}).")
        else:
            singles.append(f"remove {k}")
    if agg["resized"] >= 3:
        bias = ("too SMALL — extend them" if grew > shrank * 1.5 else
                "too LARGE — tighten them" if shrank > grew * 1.5 else "inconsistent")
        bullets.append(f"- Box extents run {bias} (reviewer resized {agg['resized']}: "
                       f"grew {grew}, shrank {shrank}; edges pushed outward: "
                       f"{dict(edge_out) or 'none'}).")
    if agg["doc_count_changes"]:
        bullets.append(f"- The document COUNT was corrected on {agg['doc_count_changes']} "
                       f"page(s) — re-check how many documents this page really holds.")

    head = (f"DRAFT VOLUME_PROMPT_NOTES entry for {volume} — from review_diff over "
            f"{agg['pages_compared']} reviewed page(s), {agg['pages_changed']} changed.\n"
            f"EDIT BEFORE USE: bullets say WHAT the reviewer changed; rewrite into the\n"
            f"convention WHY (see VOLUME_PROMPT_NOTES['Volume_4'] in auto_labeler.py for\n"
            f"the target voice), then paste into VOLUME_PROMPT_NOTES[{volume!r}].\n\n"
            f"VOLUME-SPECIFIC OVERRIDE (this page is from {volume}):\n")
    body = "\n".join(bullets) if bullets else "- (no systematic correction pattern found)"
    foot = (f"\n\n(below-threshold singletons, for your judgment: {'; '.join(singles)})"
            if singles else "")
    return head + body + foot


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("volumes", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--pages", action="store_true", help="also print per-page detail")
    ap.add_argument("--draft-note", action="store_true",
                    help="also write a DRAFT VOLUME_PROMPT_NOTES entry distilled from the "
                         "corrections to qa_output/<vol>/volume_note_draft.txt")
    args = ap.parse_args()
    volumes = (sorted(p.name for p in REVIEW_ROOT.iterdir() if p.is_dir())
               if args.all else args.volumes)
    if not volumes:
        ap.error("give a volume name or --all")
    for vol in volumes:
        agg, grew, shrank, edge_out, per_page = analyze_volume(vol)
        _print(vol, agg, grew, shrank, edge_out, per_page)
        if args.draft_note:
            note = draft_note(vol, agg, grew, shrank, edge_out)
            out = SCRIPT_DIR / "qa_output" / vol / "volume_note_draft.txt"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(note + "\n")
            print(f"\n{note}\n\n  -> wrote {out.relative_to(SCRIPT_DIR)}")
        if args.pages:
            for name, d in per_page:
                bits = []
                if d["recat"]:   bits.append("recat=" + ",".join(f'{r["cat"]}>{a["cat"]}' for r, a in d["recat"]))
                if d["added"]:   bits.append("add=" + ",".join(a["cat"] for a in d["added"]))
                if d["removed"]: bits.append("rem=" + ",".join(r["cat"] for r in d["removed"]))
                if d["resized"]: bits.append(f"resize={len(d['resized'])}")
                if d["doc_count"]: bits.append(f"docs={d['doc_count'][0]}>{d['doc_count'][1]}")
                print(f"     {name}: " + " | ".join(bits))


if __name__ == "__main__":
    main()
