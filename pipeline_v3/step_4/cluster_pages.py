#!/usr/bin/env python3
"""Cluster a volume's pages by PAGE ARCHITECTURE for the staged HITL flow (TASK B).

Membership is GEOMETRIC ONLY — measurable layout/ink features, no VLM, no labels,
no semantic judgment (assistant semantic judgment is unreliable on this corpus —
see EPISTEMIC_AUDIT.md / DESKTOP_PROMPT §3.1). The output is meant for DAVID's
eyes: a contact sheet per cluster (page thumbnails, most-central first) that he
confirms / renames / merges. Cluster names from --vlm-names are display-only
decoration and never load-bearing.

Features per page (z-scored per block, blocks variance-balanced):
  A. 12x16 ink-density grid (192 dims) — the validated layout fingerprint
     (similarity A/B: PQ 0.639->0.681, experiments/fewshot_ab.py).
  B. Structural signals (12 dims) from the binarized page: text-line count,
     text-row fraction, largest vertical gap, top-3 gap sizes, left-margin ink
     share, margin-gutter blankness, ink center of mass (x,y), ink density,
     line-start alignment spread (numbered-list/outline proxy, measurable).

k is chosen by max mean silhouette over --k-range (default 3..8), overridable
with --k. Deterministic: fixed seeds, sorted inputs, restart-best by inertia.

Outputs (per volume):
  qa_output/<Vol>/clusters.json                page -> cluster, centroids meta
  qa_output/<Vol>/clusters.txt                 human summary + next commands
  label_review/contact_sheets/<Vol>/cluster_N.png   thumbnail grids for David

Usage:
    ./venv/bin/python cluster_pages.py Volume_1
    ./venv/bin/python cluster_pages.py Volume_2 --k 6
    ./venv/bin/python cluster_pages.py Volume_2 --vlm-names   # display-only names
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

from auto_labeler import (
    ENV_PATH,
    PAGE_TYPE_CACHE_DIR,
    _layout_descriptor,
    _otsu_threshold,
    classify_page_type,
    discover_target_pages,
    render_page,
)

SCRIPT_DIR = Path(__file__).resolve().parent
QA_OUTPUT_DIR = SCRIPT_DIR / "qa_output"
SHEETS_DIR = SCRIPT_DIR / "label_review" / "contact_sheets"

THUMB_W = 150          # contact-sheet thumbnail width
SHEET_COLS = 8
SHEET_MAX_ROWS = 12    # pages per sheet capped at COLS*MAX_ROWS; overflow -> _b sheet


MIN_SPLIT = 12          # a type stratum smaller than this stays one cluster
MIN_SUB_SIL = 0.05      # sub-split a type only when silhouette clears this


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("volume", help="Volume to cluster (e.g. Volume_2).")
    p.add_argument("--k", type=int, default=0,
                   help="(--no-api mode) force this many clusters.")
    p.add_argument("--k-range", type=str, default="3-8",
                   help="(--no-api mode) k range searched (default 3-8).")
    p.add_argument("--no-api", action="store_true",
                   help="Skip VLM page typing: cluster on geometry alone "
                        "(numeric cluster ids). Default is BY TYPE: pages are "
                        "first typed (letter/mass_card/notebook/other — cached, "
                        "pennies), then sub-clustered geometrically within each "
                        "type (ids like 'notebook_2'), so the clusters David "
                        "confirms are type-coherent.")
    p.add_argument("--vlm-names", action="store_true",
                   help="Ask the VLM for a one-line DISPLAY-ONLY name per cluster "
                        "(pennies; membership is unaffected).")
    p.add_argument("--page-type-model", type=str, default="gemini-3.1-flash-lite")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def structural_features(pdf: Path) -> np.ndarray | None:
    """12 measurable structure signals from the binarized 384px render."""
    try:
        img = render_page(pdf, 384)[0].convert("L")
    except Exception:
        return None
    a = np.asarray(img)
    H, W = a.shape
    thr = _otsu_threshold(img.histogram()[:256])
    ink = (a < thr).astype(np.float32)
    total = float(ink.sum()) or 1.0

    rowsum = ink.sum(axis=1)
    colsum = ink.sum(axis=0)
    texty = rowsum > 0.04 * W
    transitions = int(np.count_nonzero(texty[1:] != texty[:-1]))
    n_lines = transitions / 2.0
    # vertical gap structure: runs of blank rows
    gaps, cur = [], 0
    for t in texty:
        if not t:
            cur += 1
        elif cur:
            gaps.append(cur); cur = 0
    if cur:
        gaps.append(cur)
    top3 = sorted(gaps, reverse=True)[:3] + [0, 0, 0]
    # line-start alignment spread: std of each text line's first-ink column.
    # Prose blocks share a left edge (low spread); outlines / numbered lists /
    # ragged ledgers indent variably (higher spread). Measurable proxy only.
    starts = []
    y = 0
    while y < H:
        if texty[y]:
            y2 = y
            while y2 < H and texty[y2]:
                y2 += 1
            band = ink[y:y2]
            cols = np.flatnonzero(band.sum(axis=0) > 0)
            if cols.size:
                starts.append(int(cols[0]))
            y = y2
        else:
            y += 1
    start_spread = float(np.std(starts)) / W if len(starts) >= 2 else 0.0

    cmx = float((colsum * np.arange(W)).sum()) / total / W
    cmy = float((rowsum * np.arange(H)).sum()) / total / H
    left_share = float(ink[:, : int(0.13 * W)].sum()) / total
    band = colsum[int(0.13 * W): int(0.20 * W)]
    gutter_blank = float((band < 0.04 * W).mean()) if band.size else 0.0

    return np.array([
        n_lines / 40.0, float(texty.mean()),
        top3[0] / H, top3[1] / H, top3[2] / H,
        left_share, gutter_blank,
        cmx, cmy, total / (W * H),
        start_spread, len(gaps) / 40.0,
    ], dtype=np.float32)


def build_matrix(pages) -> tuple[list, np.ndarray]:
    """Per page: validated 192-dim grid + 12 structural dims, blocks balanced."""
    keep, grids, structs = [], [], []
    for _doc, num, pdf in pages:
        g = _layout_descriptor(pdf)
        s = structural_features(pdf)
        if g is None or s is None:
            continue
        keep.append((num, f"page_{num:03d}", pdf))
        grids.append(np.asarray(g, dtype=np.float32))
        structs.append(s)
    G = np.stack(grids)
    S = np.stack(structs)

    def zscore(M):
        mu, sd = M.mean(axis=0), M.std(axis=0)
        sd[sd == 0] = 1.0
        return (M - mu) / sd

    # Balance the blocks: equal TOTAL variance per block, so 192 grid dims don't
    # drown the 12 structural dims (each block scaled by 1/sqrt(n_dims)).
    G = zscore(G) / np.sqrt(G.shape[1])
    S = zscore(S) / np.sqrt(S.shape[1])
    return keep, np.hstack([G, S])


def kmeans(X: np.ndarray, k: int, seed: int, restarts: int = 8, iters: int = 100):
    best = None
    n = X.shape[0]
    for rs in range(restarts):
        rng = np.random.RandomState(seed * 100 + rs)
        cent = X[rng.choice(n, k, replace=False)].copy()
        asg = np.zeros(n, dtype=int)
        for _ in range(iters):
            d = ((X[:, None, :] - cent[None, :, :]) ** 2).sum(axis=2)
            new_asg = d.argmin(axis=1)
            if (new_asg == asg).all() and _ > 0:
                break
            asg = new_asg
            for c in range(k):
                m = X[asg == c]
                if len(m):
                    cent[c] = m.mean(axis=0)
        inertia = float(((X - cent[asg]) ** 2).sum())
        if best is None or inertia < best[0]:
            best = (inertia, asg.copy(), cent.copy())
    return best[1], best[2]


def mean_silhouette(X: np.ndarray, asg: np.ndarray) -> float:
    n = len(asg)
    D = np.sqrt(((X[:, None, :] - X[None, :, :]) ** 2).sum(axis=2))
    sil = np.zeros(n)
    for i in range(n):
        own = asg[i]
        same = asg == own
        a = D[i][same & (np.arange(n) != i)].mean() if same.sum() > 1 else 0.0
        bs = [D[i][asg == c].mean() for c in np.unique(asg) if c != own]
        b = min(bs) if bs else 0.0
        sil[i] = 0.0 if max(a, b) == 0 else (b - a) / max(a, b)
    return float(sil.mean())


def contact_sheets(volume: str, members: dict[int, list], dist_to_cent) -> list[Path]:
    """One PNG grid per cluster, thumbnails ordered most-central first."""
    out_dir = SHEETS_DIR / volume
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("cluster_*.png"):
        old.unlink()
    written = []
    for c, items in sorted(members.items()):
        items = sorted(items, key=lambda it: dist_to_cent[it[3]])
        chunks = [items[i:i + SHEET_COLS * SHEET_MAX_ROWS]
                  for i in range(0, len(items), SHEET_COLS * SHEET_MAX_ROWS)]
        for part, chunk in enumerate(chunks):
            thumbs = []
            for num, name, pdf, _idx in chunk:
                try:
                    t = render_page(pdf, THUMB_W)[0].convert("RGB")
                except Exception:
                    t = Image.new("RGB", (THUMB_W, int(THUMB_W * 1.3)), "gray")
                thumbs.append((name, t))
            th = max(t.height for _n, t in thumbs)
            cols = min(SHEET_COLS, len(thumbs))
            rows = (len(thumbs) + cols - 1) // cols
            pad, cap = 4, 14
            sheet = Image.new("RGB", (cols * (THUMB_W + pad) + pad,
                                      rows * (th + cap + pad) + pad), "white")
            draw = ImageDraw.Draw(sheet)
            for i, (name, t) in enumerate(thumbs):
                x = pad + (i % cols) * (THUMB_W + pad)
                y = pad + (i // cols) * (th + cap + pad)
                sheet.paste(t, (x, y))
                draw.text((x + 2, y + th + 1), name.replace("page_", "p"), fill="black")
            suffix = f"_{chr(ord('b') + part - 1)}" if part else ""
            path = out_dir / f"cluster_{c}{suffix}.png"
            sheet.save(path)
            written.append(path)
    return written


def vlm_cluster_names(volume: str, members: dict[int, list]) -> dict[int, str]:
    """DISPLAY-ONLY one-line cluster descriptions from the 3 most-central pages."""
    from dotenv import load_dotenv
    from google import genai
    load_dotenv(ENV_PATH)
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        print("  (--vlm-names skipped: no GEMINI_API_KEY)")
        return {}
    client = genai.Client(api_key=key)
    names = {}
    for c, items in sorted(members.items()):
        imgs = [render_page(pdf, 512)[0] for _n, _na, pdf, _i in items[:3]]
        try:
            r = client.models.generate_content(
                model="gemini-3.1-flash-lite",
                contents=["These pages are from one visual cluster of an archival "
                          "volume. In ONE short line (<=10 words), describe the "
                          "shared PAGE ARCHITECTURE (layout, not content meaning):",
                          *imgs],
            )
            names[c] = (r.text or "").strip().splitlines()[0][:90]
        except Exception as exc:
            names[c] = f"(naming failed: {exc})"
    return names


def sub_cluster(X, idxs: list[int], type_name: str, seed: int):
    """Geometric sub-clusters WITHIN one page type. Splits only when the
    silhouette earns it; small strata stay whole. Returns {page_idx: label}."""
    if len(idxs) < MIN_SPLIT:
        return {i: type_name for i in idxs}, None
    sub = X[idxs]
    best = None
    for k_try in range(2, min(5, len(idxs) // 8 + 1) + 1):
        if k_try >= len(idxs):
            break
        asg, _cent = kmeans(sub, k_try, seed)
        s = mean_silhouette(sub, asg)
        if best is None or s > best[0]:
            best = (s, k_try, asg)
    if best is None or best[0] < MIN_SUB_SIL:
        return {i: type_name for i in idxs}, (best[0] if best else None)
    s, k, asg = best
    print(f"  {type_name}: split into {k} (silhouette {s:.3f})")
    return {idx: f"{type_name}_{int(a) + 1}" for idx, a in zip(idxs, asg)}, s


def main() -> None:
    args = parse_args()
    pages = discover_target_pages(args.volume)
    if not pages:
        raise SystemExit(f"No pages for {args.volume!r} under polygon_cropped_pdfs/.")
    print(f"{args.volume}: {len(pages)} pages. Extracting features...")
    keep, X = build_matrix(pages)
    print(f"  {len(keep)} pages fingerprinted ({len(pages) - len(keep)} unrenderable).")

    labels: dict[int, str] = {}
    if args.no_api:
        # pure-geometry mode (numeric ids) — the original behavior
        if args.k:
            k, (asg, _c) = args.k, kmeans(X, args.k, args.seed)
            print(f"k={k} (forced): silhouette {mean_silhouette(X, asg):.3f}")
        else:
            lo, hi = (int(s) for s in args.k_range.split("-"))
            scored = []
            for k_try in range(lo, min(hi, len(keep) - 1) + 1):
                asg_t, _ = kmeans(X, k_try, args.seed)
                s = mean_silhouette(X, asg_t)
                scored.append((s, k_try, asg_t))
                print(f"  k={k_try}: silhouette {s:.3f}")
            s, k, asg = max(scored, key=lambda t: (t[0], -t[1]))
            print(f"chosen k={k} (silhouette {s:.3f})")
        labels = {i: str(int(a)) for i, a in enumerate(asg)}
        mode = "geometric"
    else:
        # BY TYPE (default): VLM page type strata (cached), geometric sub-clusters
        from dotenv import load_dotenv
        from google import genai
        load_dotenv(ENV_PATH)
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise SystemExit("No GEMINI_API_KEY — use --no-api for geometry-only clustering.")
        client = genai.Client(api_key=api_key)
        cache = PAGE_TYPE_CACHE_DIR / args.volume
        types: list[str] = []
        for j, (num, name, pdf) in enumerate(keep, 1):
            t, _, _ = classify_page_type(client, args.page_type_model, pdf, cache_dir=cache)
            types.append(t)
            if j % 50 == 0 or j == len(keep):
                print(f"  typed [{j}/{len(keep)}]")
        by_type: dict[str, list[int]] = {}
        for i, t in enumerate(types):
            by_type.setdefault(t, []).append(i)
        print(f"Type distribution: {({t: len(v) for t, v in sorted(by_type.items())})}")
        for t in sorted(by_type):
            sub_labels, _sil = sub_cluster(X, by_type[t], t, args.seed)
            labels.update(sub_labels)
        mode = "by-type"

    members: dict[str, list] = {}
    for i, (num, name, pdf) in enumerate(keep):
        members.setdefault(labels[i], []).append((num, name, pdf, i))

    centroids = {c: X[[it[3] for it in items]].mean(axis=0)
                 for c, items in members.items()}
    dist = np.zeros(len(keep))
    for c, items in members.items():
        for *_rest, i in items:
            d = X[i] - centroids[c]
            dist[i] = float((d * d).sum() ** 0.5)

    sheets = contact_sheets(args.volume, members, dist)
    names = vlm_cluster_names(args.volume, members) if args.vlm_names else {}

    out_dir = QA_OUTPUT_DIR / args.volume
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "volume": args.volume,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "k": int(len(members)),
        "seed": args.seed,
        "mode": mode,
        "note": "membership = VLM page type + geometric sub-structure (by-type "
                "mode) or layout features only (geometric mode); display names "
                "are decoration, never load-bearing",
        "clusters": {
            str(c): {
                "display_name": names.get(c, ""),
                "size": len(items),
                "pages": [it[1] for it in sorted(items)],
                "most_central": [it[1] for it in
                                 sorted(items, key=lambda it: dist[it[3]])[:3]],
            } for c, items in sorted(members.items())
        },
        "page_to_cluster": {it[1]: str(c) for c, items in members.items()
                            for it in items},
    }
    (out_dir / "clusters.json").write_text(json.dumps(payload, indent=2) + "\n")

    lines = [f"# {args.volume} clusters ({mode}) — k={len(members)}, "
             f"{len(keep)} pages, generated {payload['generated_at']}",
             "# David confirms/merges via the contact sheets:",
             *[f"#   {p}" for p in sheets[:16]]]
    for c, items in sorted(members.items()):
        nm = f"  '{names[c]}'" if c in names else ""
        central = ", ".join(it[1] for it in sorted(items, key=lambda it: dist[it[3]])[:3])
        lines.append(f"cluster {c}: {len(items):3d} pages{nm}  (central: {central})")
    lines += ["#",
              f"# NEXT: ./venv/bin/python bootstrap.py {args.volume} "
              f"--confirm-clusters [--merge A+B]",
              "# (David reviews the sheets BEFORE anything is labeled.)"]
    (out_dir / "clusters.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nWrote {out_dir/'clusters.json'}, clusters.txt, {len(sheets)} contact sheet(s).")


if __name__ == "__main__":
    main()
