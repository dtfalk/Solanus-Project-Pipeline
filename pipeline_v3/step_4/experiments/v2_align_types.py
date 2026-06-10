#!/usr/bin/env python3
"""Map local (desktop, 360-page) Volume_2 numbering onto the laptop's 372-page set
by aligning the two page-TYPE sequences (the laptop's types are committed in
page_type_cache/Volume_2.lapnum-372; local types were freshly classified).

VERDICT (2026-06-10, synthetic validation BEFORE use): NOT VIABLE for V2.
With ~84% of pages typed "letter", insertions slide freely inside same-type
runs — a simulated 12-insertion case recovered only 2/12 true positions. Type
sequences are too uniform to identify page IDENTITY here. V2 therefore runs in
desktop numbering (provisional-canonical: all 12 pool seeds verify at offset 0,
git's upstream polygon data matches); the exact map gets derived by PIXEL
correlation when the laptop's 372-page crops are synced, then migrated like
A3 (+2) and V3 (+1) were. Kept for the record + the reusable NW aligner.

Model: the laptop set = local set + inserted pages (recrop added covers/dividers).
Needleman-Wunsch with laptop-side gaps free-ish; full two-sided alignment as a
fallback detector for local-only pages (which would mean DROPPED content — red flag).

Writes qa_output/Volume_2/numbering_map.json:
  { "local_to_laptop": {"1": 2, ...}, "laptop_only": [...], "local_only": [...],
    "aligned_mismatch_rate": 0.04 }
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
LAP = HERE / "page_type_cache" / "Volume_2.lapnum-372"
LOC = HERE / "page_type_cache" / "Volume_2"
OUT = HERE / "qa_output" / "Volume_2" / "numbering_map.json"


def seq(d: Path) -> dict[int, str]:
    out = {}
    for f in sorted(d.glob("page_*.json")):
        out[int(f.stem.split("_")[1])] = json.loads(f.read_text())["page_type"]
    return out


def align(L: list[str], P: list[str], gap_l: float, gap_p: float):
    """NW alignment; returns list of (i|None, j|None) pairs (0-based)."""
    n, m = len(L), len(P)
    NEG = float("-inf")
    score = [[NEG] * (m + 1) for _ in range(n + 1)]
    back = [[None] * (m + 1) for _ in range(n + 1)]
    score[0][0] = 0.0
    for i in range(n + 1):
        for j in range(m + 1):
            s = score[i][j]
            if s == NEG:
                continue
            if i < n and j < m:
                ns = s + (1.0 if L[i] == P[j] else -1.0)
                if ns > score[i + 1][j + 1]:
                    score[i + 1][j + 1] = ns
                    back[i + 1][j + 1] = "d"
            if j < m:                       # laptop-only page (gap in local)
                ns = s - gap_p
                if ns > score[i][j + 1]:
                    score[i][j + 1] = ns
                    back[i][j + 1] = "p"
            if i < n:                       # local-only page (gap in laptop)
                ns = s - gap_l
                if ns > score[i + 1][j]:
                    score[i + 1][j] = ns
                    back[i + 1][j] = "l"
    pairs = []
    i, j = n, m
    while i or j:
        b = back[i][j]
        if b == "d":
            pairs.append((i - 1, j - 1)); i, j = i - 1, j - 1
        elif b == "p":
            pairs.append((None, j - 1)); j -= 1
        else:
            pairs.append((i - 1, None)); i -= 1
    return list(reversed(pairs)), score[n][m]


def main() -> None:
    lap = seq(LAP)
    loc = seq(LOC)
    lap_nums, loc_nums = sorted(lap), sorted(loc)
    P = [lap[k] for k in lap_nums]
    L = [loc[k] for k in loc_nums]
    print(f"laptop {len(P)} pages, local {len(L)} pages")

    # local-only gaps heavily penalized: the recrop should only ADD pages.
    pairs, s = align(L, P, gap_l=6.0, gap_p=0.4)

    local_to_laptop, laptop_only, local_only = {}, [], []
    mism = both = 0
    for i, j in pairs:
        if i is not None and j is not None:
            local_to_laptop[str(loc_nums[i])] = lap_nums[j]
            both += 1
            mism += L[i] != P[j]
        elif j is not None:
            laptop_only.append({"laptop_page": lap_nums[j], "type": P[j]})
        else:
            local_only.append(loc_nums[i])

    rate = mism / max(1, both)
    # offset profile: how the shift grows through the volume
    offsets = sorted({lap - int(loc) for loc, lap in local_to_laptop.items()})
    report = {
        "generated_from": "type-sequence alignment (NW), laptop cache vs fresh local typing",
        "local_pages": len(L), "laptop_pages": len(P),
        "aligned": both, "aligned_mismatch_rate": round(rate, 4),
        "laptop_only": laptop_only, "local_only": local_only,
        "offset_values_seen": offsets,
        "local_to_laptop": local_to_laptop,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2) + "\n")
    print(f"aligned {both}, type-mismatch rate {rate:.1%} (classifier noise expected ~5-10%)")
    print(f"laptop-only pages ({len(laptop_only)}): "
          + ", ".join(f"{e['laptop_page']}({e['type']})" for e in laptop_only))
    if local_only:
        print(f"⚠ LOCAL-ONLY pages (laptop DROPPED these?): {local_only}")
    print(f"offsets seen: {offsets}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
