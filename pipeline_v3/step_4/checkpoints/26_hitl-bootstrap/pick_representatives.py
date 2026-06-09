#!/usr/bin/env python3
"""Pick a volume's representative pages for the HITL bootstrap (HITL_BOOTSTRAP.md PHASE 1).

Stratifies the volume by VLM page type (the proven router — RESEARCH_AND_PLAN §1),
then farthest-point-samples the 192-dim ink-grid layout descriptors within each
stratum (k-center greedy seeded at the stratum medoid — deterministic, no RNG).
Writes qa_output/<Vol>/representatives.{txt,json}, including paste-ready
strings for `auto_labeler.py --pages` (PHASE 2) and `--pin-examples` (PHASE 5).

Cost: one cheap cached VLM call per page for typing (pennies per volume; re-runs
are free) + a 256px render per page for fingerprints. --no-api skips typing and
diversifies on layout alone (a single stratum).

Usage:
    ./venv/bin/python pick_representatives.py Volume_2              # k=12
    ./venv/bin/python pick_representatives.py Volume_2 --k 15
    ./venv/bin/python pick_representatives.py Volume_2 --no-api
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from auto_labeler import (
    ENV_PATH,
    PAGE_TYPE_CACHE_DIR,
    _layout_descriptor,
    classify_page_type,
    discover_target_pages,
)

SCRIPT_DIR = Path(__file__).resolve().parent
QA_OUTPUT_DIR = SCRIPT_DIR / "qa_output"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("volume", help="Volume to sample (e.g. Volume_2).")
    p.add_argument("--k", type=int, default=12,
                   help="Total representatives to pick (default: 12 — one full "
                        "same-volume few-shot set; see HITL_BOOTSTRAP.md §1).")
    p.add_argument("--no-api", action="store_true",
                   help="Skip VLM page-typing (no API key needed); diversify on "
                        "layout alone as a single stratum.")
    p.add_argument("--page-type-model", type=str, default="gemini-3.1-flash-lite",
                   help="Model for the cheap page-type call (default: gemini-3.1-flash-lite; "
                        "cached to page_type_cache/ — same cache auto_labeler uses).")
    return p.parse_args()


def allocate(counts: dict[str, int], k: int) -> dict[str, int]:
    """Largest-remainder allocation of k reps across strata, min 1 per stratum,
    capped at stratum size. Deterministic (ties break by size then name)."""
    types = sorted(counts, key=lambda t: (-counts[t], t))
    if len(types) >= k:
        return {t: (1 if i < k else 0) for i, t in enumerate(types)}
    alloc = {t: 1 for t in types}
    rem = k - len(types)
    total = sum(counts.values())
    quota = {t: rem * counts[t] / total for t in types}
    for t in types:
        take = min(int(quota[t]), counts[t] - alloc[t])
        alloc[t] += take
        rem -= take
    by_frac = sorted(types, key=lambda t: (-(quota[t] - int(quota[t])), -counts[t], t))
    while rem > 0:
        moved = False
        for t in by_frac:
            if rem <= 0:
                break
            if alloc[t] < counts[t]:
                alloc[t] += 1
                rem -= 1
                moved = True
        if not moved:        # k exceeds the number of fingerprintable pages
            break
    return alloc


def farthest_point_sample(vecs: list, n: int) -> list[int]:
    """k-center greedy over unit vectors (cosine distance), seeded at the medoid.
    Returns indices into vecs. Deterministic; ties break by lowest index."""
    import numpy as np
    if n <= 0 or not vecs:
        return []
    arr = np.stack(vecs)                       # (m, 192), rows unit-norm
    centroid = arr.mean(axis=0)
    cn = float((centroid * centroid).sum()) ** 0.5
    if cn > 0:
        centroid = centroid / cn
    chosen = [int((arr @ centroid).argmax())]  # medoid = most central page
    min_dist = 1.0 - arr @ arr[chosen[0]]      # cosine distance to the chosen set
    while len(chosen) < min(n, len(vecs)):
        nxt = int(min_dist.argmax())
        chosen.append(nxt)
        min_dist = np.minimum(min_dist, 1.0 - arr @ arr[nxt])
    return chosen


def main() -> None:
    args = parse_args()
    pages = discover_target_pages(args.volume)
    if not pages:
        raise SystemExit(f"No pages found for volume {args.volume!r} under polygon_cropped_pdfs/.")
    print(f"{args.volume}: {len(pages)} pages. Fingerprinting + typing...")

    client = None
    if not args.no_api:
        from dotenv import load_dotenv
        from google import genai
        load_dotenv(ENV_PATH)
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise SystemExit("No GEMINI_API_KEY in .env — use --no-api for layout-only sampling.")
        client = genai.Client(api_key=api_key)

    # Per page: layout fingerprint (local render) + page type (cached VLM call).
    info = []          # (page_num, page_name, type, desc)
    no_desc = []
    for i, (_doc, num, pdf) in enumerate(pages, 1):
        desc = _layout_descriptor(pdf)
        if client is not None:
            ptype, _, _ = classify_page_type(client, args.page_type_model, pdf,
                                             cache_dir=PAGE_TYPE_CACHE_DIR / args.volume)
        else:
            ptype = "all"
        name = f"page_{num:03d}"
        if desc is None:
            no_desc.append(name)
        else:
            info.append((num, name, ptype, desc))
        if i % 25 == 0 or i == len(pages):
            print(f"  [{i}/{len(pages)}]")
    if not info:
        raise SystemExit("No page could be fingerprinted — check poppler / the PDFs.")

    strata: dict[str, list[int]] = {}              # type -> indices into info
    for idx, (_n, _na, t, _d) in enumerate(info):
        strata.setdefault(t, []).append(idx)
    counts = {t: len(v) for t, v in strata.items()}
    alloc = allocate(counts, args.k)
    print(f"Type distribution: {counts}  ->  allocation: {alloc}")

    rep_idx: list[int] = []
    for t in sorted(strata):
        members = strata[t]
        picks = farthest_point_sample([info[i][3] for i in members], alloc.get(t, 0))
        rep_idx.extend(members[p] for p in picks)

    # Coverage: assign every page to its nearest rep (cosine distance, global).
    import numpy as np
    reps = sorted(rep_idx, key=lambda i: info[i][0])
    rep_mat = np.stack([info[i][3] for i in reps])
    covers = {i: [] for i in reps}                  # rep idx -> [(dist, page idx)]
    for idx, (_n, _na, _t, d) in enumerate(info):
        dists = 1.0 - rep_mat @ d
        j = int(dists.argmin())
        covers[reps[j]].append((float(dists[j]), idx))
    all_d = [d for lst in covers.values() for d, _ in lst]

    pages_arg = ",".join(str(info[i][0]) for i in reps)
    pin_arg = ",".join(f"{args.volume}/{info[i][1]}" for i in reps)

    out_dir = QA_OUTPUT_DIR / args.volume
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# representatives for {args.volume} — k={len(reps)} of {len(pages)} pages, "
        f"generated {datetime.now().isoformat(timespec='seconds')}",
        f"# type distribution: {counts}   allocation: {alloc}",
    ]
    for i in reps:
        num, name, t, _ = info[i]
        ds = sorted(d for d, _ in covers[i])
        lines.append(f"{name}  type={t:<9}  covers {len(ds):3d} pages  "
                     f"(farthest at cos-dist {ds[-1]:.3f})")
    lines += [
        f"# coverage: mean nearest-rep dist {sum(all_d)/len(all_d):.3f}, "
        f"max {max(all_d):.3f}",
        f"# unfingerprintable (review manually): {', '.join(no_desc) if no_desc else 'none'}",
        "#",
        f"# PHASE 2:  ./venv/bin/python auto_labeler.py --volume {args.volume} --pages {pages_arg}",
        f"# PHASE 3:  EDITOR_DOCUMENT={args.volume} ./venv/bin/python normalized_editor.py",
        f"# PHASE 4:  ./venv/bin/python promote_examples.py {args.volume} --pages {pages_arg} --upload",
        f"#           ./venv/bin/python review_diff.py {args.volume} --draft-note",
        f"# PHASE 5:  ./venv/bin/python auto_labeler.py --volume {args.volume} "
        f"--pin-examples '{pin_arg}'",
    ]
    (out_dir / "representatives.txt").write_text("\n".join(lines) + "\n")
    (out_dir / "representatives.json").write_text(json.dumps({
        "volume": args.volume, "k": len(reps), "total_pages": len(pages),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "type_distribution": counts, "allocation": alloc,
        "pages_arg": pages_arg, "pin_arg": pin_arg,
        "representatives": [
            {"page": info[i][1], "page_number": info[i][0], "type": info[i][2],
             "covers": len(covers[i]),
             "max_covered_dist": max(d for d, _ in covers[i])}
            for i in reps],
        "unfingerprintable": no_desc,
    }, indent=2) + "\n")

    print("\n".join(lines))
    print(f"\nWrote {out_dir / 'representatives.txt'} and .json")


if __name__ == "__main__":
    main()
