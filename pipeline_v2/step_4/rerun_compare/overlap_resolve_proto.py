"""Prototype: SAFE cross-role overlap resolution by trimming PADDING to text.

A cross-role full-word overlap is one box's padding crossing into a different box's
text. Resolve by snapping each box's edge to its own text along the separating
blank strip — tried both vertically (stacked: content over farewell) and
horizontally (side-by-side: date beside content). A revert-net recomputes
uncovered_ink and undoes any trim that would expose ink, so it can never cut text.
Genuinely interleaved overlaps (no separating strip) are left flagged.
"""
import copy
import json
import sys
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from auto_labeler import render_page, _bbox_of
from qa_report import (_ink_mask, _inter_over_min, check_uncovered_ink,
                       check_overlap_loose, OVERLAP_THRESH)

ROOT = Path(__file__).resolve().parent.parent


def _boxes(documents):
    for did, doc in documents.items():
        if isinstance(doc, dict):
            for cat, polys in doc.items():
                if isinstance(polys, list):
                    for p in polys:
                        if p.get("vertices"):
                            yield did, cat, p, _bbox_of(p["vertices"])


def _ink_extent(ink, x0, x1, y0, y1, axis):
    """First/last inked index along `axis` ('y' rows or 'x' cols) in the region."""
    x0, x1, y0, y1 = int(x0), int(x1), int(y0), int(y1)
    if x1 <= x0 or y1 <= y0:
        return None, None
    if axis == "y":
        prof = [ink.crop((x0, y0, x1, y0 + 1)).resize((1, 1), Image.BOX).getpixel((0, 0))
                for y0 in range(y0, y1)]
        base = int(max(0, y0 - len(prof)))  # placeholder; recompute below
    # robust version via one resize
    if axis == "y":
        col = ink.crop((x0, y0, x1, y1)).resize((1, y1 - y0), Image.BOX)
        vals = [col.getpixel((0, r)) for r in range(y1 - y0)]
        base = y0
    else:
        row = ink.crop((x0, y0, x1, y1)).resize((x1 - x0, 1), Image.BOX)
        vals = [row.getpixel((c, 0)) for c in range(x1 - x0)]
        base = x0
    first = next((base + i for i, v in enumerate(vals) if v >= 1), None)
    last = next((base + i for i in range(len(vals) - 1, -1, -1) if vals[i] >= 1), None)
    return first, last


def _set_box(p, x0, y0, x1, y1):
    p["vertices"] = [{"x": int(x0), "y": int(y0)}, {"x": int(x1), "y": int(y0)},
                     {"x": int(x1), "y": int(y1)}, {"x": int(x0), "y": int(y1)}]


def _apply(documents, ink, W, H, edits):
    """edits: list of (poly, bbox, edge, val). Apply; revert all if uncovered ink rises."""
    snaps = [(p, copy.deepcopy(p["vertices"])) for p, _, _, _ in edits]
    before = len(check_uncovered_ink(documents, ink, W, H))
    for p, bb, edge, val in edits:
        x0, y0, x1, y1 = bb
        if edge == "bottom": y1 = val
        elif edge == "top": y0 = val
        elif edge == "right": x1 = val
        elif edge == "left": x0 = val
        if x1 - x0 < 2 or y1 - y0 < 2:
            for p2, v in snaps:
                p2["vertices"] = v
            return False
        _set_box(p, x0, y0, x1, y1)
    if len(check_uncovered_ink(documents, ink, W, H)) > before:
        for p, v in snaps:
            p["vertices"] = v
        return False
    return True


def _try_resolve(documents, ink, W, H, pa, A, pb, B):
    # vertical: upper / lower text separated by a blank row strip
    (pu, U), (pl, L) = ((pa, A), (pb, B)) if (A[1] + A[3]) < (B[1] + B[3]) else ((pb, B), (pa, A))
    x0, x1 = max(U[0], L[0]), min(U[2], L[2])
    l_top, _ = _ink_extent(ink, x0, x1, L[1], L[3], "y")
    if l_top is not None:
        _, u_bot = _ink_extent(ink, x0, x1, U[1], l_top, "y")
        if u_bot is not None and u_bot < l_top:
            if _apply(documents, ink, W, H,
                      [(pu, U, "bottom", u_bot + 1), (pl, L, "top", l_top - 1)]):
                return True
    # horizontal: left / right text separated by a blank column strip
    (pl2, Lx), (pr2, Rx) = ((pa, A), (pb, B)) if (A[0] + A[2]) < (B[0] + B[2]) else ((pb, B), (pa, A))
    y0, y1 = max(Lx[1], Rx[1]), min(Lx[3], Rx[3])
    r_left, _ = _ink_extent(ink, Rx[0], Rx[2], y0, y1, "x")
    if r_left is not None:
        _, l_right = _ink_extent(ink, Lx[0], r_left, y0, y1, "x")
        if l_right is not None and l_right < r_left:
            if _apply(documents, ink, W, H,
                      [(pl2, Lx, "right", l_right + 1), (pr2, Rx, "left", r_left - 1)]):
                return True
    return False


def _pairs(documents, ink, line_h):
    items = list(_boxes(documents))
    out = []
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            da, ca, pa, A = items[i]; db, cb, pb, B = items[j]
            if (da, ca) == (db, cb) or _inter_over_min(A, B) <= OVERLAP_THRESH:
                continue
            ix0, iy0, ix1, iy1 = int(max(A[0], B[0])), int(max(A[1], B[1])), int(min(A[2], B[2])), int(min(A[3], B[3]))
            if ix1 <= ix0 or iy1 <= iy0:
                continue
            ib = ink.crop((ix0, iy0, ix1, iy1)).getbbox()
            if ib and (ib[3] - ib[1]) >= 0.5 * line_h and (ib[2] - ib[0]) >= 0.5 * line_h:
                out.append((pa, A, pb, B))
    return out


def resolve(documents, ink, W, H):
    hs = sorted(b[3] - b[1] for _d, _c, _p, b in _boxes(documents))
    line_h = hs[len(hs) // 2] if hs else max(1, int(0.015 * H))
    resolved = skipped = 0
    for pa, A, pb, B in _pairs(documents, ink, line_h):
        if _try_resolve(documents, ink, W, H, pa, A, pb, B):
            resolved += 1
        else:
            skipped += 1
    return resolved, skipped


def _n_over(documents, ink, W, H):
    return sum(1 for f in check_overlap_loose(documents, ink, W, H) if f["type"] == "overlap")


def main():
    tb = ta = res = skip = new_unc = 0
    for pd in sorted((ROOT / "auto_labeled" / "Appendix_1").glob("page_*")):
        jp = pd / f"{pd.name}.json"
        if not jp.exists():
            continue
        data = json.load(open(jp)); docs = data["documents"]
        ink = _ink_mask(render_page(pd / f"{pd.name}.pdf", None)[0].convert("L"))
        W, H = data["page_width"], data["page_height"]
        b = _n_over(docs, ink, W, H); ub = len(check_uncovered_ink(docs, ink, W, H))
        r, s = resolve(docs, ink, W, H)
        a = _n_over(docs, ink, W, H); ua = len(check_uncovered_ink(docs, ink, W, H))
        if b or r:
            print(f"{pd.name}: overlap {b}->{a} | resolved {r} skipped {s} | uncovered {ub}->{ua}")
        tb += b; ta += a; res += r; skip += s; new_unc += max(0, ua - ub)
    print(f"\nTOTAL overlap {tb}->{ta} | resolved {res} skipped {skip} | NEW uncovered ink: {new_unc} (must be 0)")


if __name__ == "__main__":
    main()
