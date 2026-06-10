#!/usr/bin/env python3
"""Pick the STAGE-3 chunk for the staged HITL flow (DESKTOP_PROMPT §3.4).

After the representatives are gold and promoted (stages 1-2), the flow labels a
SOLID CHUNK (~20-30%) of the volume — cluster-stratified, diversity-sampled — so
a convention mistake is caught at chunk cost, never whole-volume cost. David
reviews a sample of the chunk; only when it passes does stage 4 (the remainder)
run.

Selection: per cluster (clusters.json from cluster_pages.py; falls back to one
stratum without it), proportional allocation of round(frac * volume), then
farthest-point sampling on the layout fingerprints — excluding pages already in
reviewed/ (gold) and the stage-1 representatives.

Outputs qa_output/<Vol>/chunk.txt + chunk.json with the paste-ready --pages
string for auto_labeler.

Usage:
    ./venv/bin/python pick_chunk.py Volume_2                # 25% chunk
    ./venv/bin/python pick_chunk.py Volume_2 --frac 0.3
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from auto_labeler import _layout_descriptor, discover_target_pages
from pick_representatives import allocate, farthest_point_sample

SCRIPT_DIR = Path(__file__).resolve().parent
QA_OUTPUT_DIR = SCRIPT_DIR / "qa_output"
REVIEW_ROOT = SCRIPT_DIR / "reviewed"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("volume")
    p.add_argument("--frac", type=float, default=0.25,
                   help="Fraction of the volume to put in the chunk (default 0.25).")
    p.add_argument("--count", type=int, default=0,
                   help="Absolute number of pages instead of --frac (bootstrap "
                        "--label-more uses this).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    pages = discover_target_pages(args.volume)
    if not pages:
        raise SystemExit(f"No pages for {args.volume!r}.")

    out_dir = QA_OUTPUT_DIR / args.volume
    cluster_map: dict[str, int] = {}
    cpath = out_dir / "clusters.json"
    if cpath.exists():
        cluster_map = json.loads(cpath.read_text())["page_to_cluster"]
    else:
        print(f"NOTE: {cpath} missing — sampling as a single stratum "
              f"(run cluster_pages.py first for stratified coverage).")

    reps: set[str] = set()
    rpath = out_dir / "representatives.json"
    if rpath.exists():
        reps = {r["page"] for r in json.loads(rpath.read_text())["representatives"]}

    reviewed = {p.name for p in (REVIEW_ROOT / args.volume).glob("page_*")} \
        if (REVIEW_ROOT / args.volume).is_dir() else set()

    # Eligible = not gold, not a stage-1 rep, fingerprintable.
    info = []                       # (num, name, stratum, desc)
    for _doc, num, pdf in pages:
        name = f"page_{num:03d}"
        if name in reps or name in reviewed:
            continue
        desc = _layout_descriptor(pdf)
        if desc is None:
            continue
        c = cluster_map.get(name)
        info.append((num, name, str(c) if c is not None else "all", desc))
    if not info:
        raise SystemExit("No eligible pages (everything is gold or a representative?).")

    target = args.count if args.count else max(1, round(args.frac * len(pages)))
    strata: dict[str, list[int]] = {}
    for idx, (_n, _na, s, _d) in enumerate(info):
        strata.setdefault(s, []).append(idx)
    alloc = allocate({s: len(v) for s, v in strata.items()}, min(target, len(info)))

    chosen: list[int] = []
    for s in sorted(strata):
        members = strata[s]
        picks = farthest_point_sample([info[i][3] for i in members], alloc.get(s, 0))
        chosen.extend(members[p] for p in picks)
    chosen = sorted(chosen, key=lambda i: info[i][0])

    pages_arg = ",".join(str(info[i][0]) for i in chosen)
    payload = {
        "volume": args.volume, "frac": args.frac, "count": args.count,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "chunk_size": len(chosen), "volume_pages": len(pages),
        "excluded_reviewed": len(reviewed), "excluded_reps": len(reps),
        "allocation": alloc, "pages_arg": pages_arg,
        "pages": [info[i][1] for i in chosen],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "chunk.json").write_text(json.dumps(payload, indent=2) + "\n")
    lines = [
        f"# stage-3 chunk for {args.volume}: {len(chosen)} of {len(pages)} pages "
        f"(frac {args.frac}), generated {payload['generated_at']}",
        f"# strata allocation: {alloc}",
        f"# excluded: {len(reviewed)} reviewed (gold) + {len(reps)} representatives",
        *[info[i][1] for i in chosen],
        "#",
        f"# STAGE 3:  ./venv/bin/python auto_labeler.py --volume {args.volume} "
        f"--pages {pages_arg}",
        "# David reviews a sample of the chunk; fix note/pool and re-run THE CHUNK",
        "# ONLY if conventions are off. Stage 4 (the rest) runs only after his pass.",
    ]
    (out_dir / "chunk.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:3]))
    print(f"Wrote {out_dir/'chunk.txt'} and chunk.json ({len(chosen)} pages).")


if __name__ == "__main__":
    main()
