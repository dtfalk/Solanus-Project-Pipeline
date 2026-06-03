"""Prototype + tune the extend-vs-new recognizer against the 16 known misses
(checkpoint 02's no-backstop output + its qa flags). Expected: ~14 extend, ~2 new."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parent / "checkpoints/02_pre-coverage-backstop/auto_labeled_Appendix_1_no_backstop"
QA = HERE.parent / "checkpoints/02_pre-coverage-backstop/qa_no_backstop.json"


def bbox_of(vs):
    xs = [v["x"] for v in vs]; ys = [v["y"] for v in vs]
    return (min(xs), min(ys), max(xs), max(ys))


def classify_uncovered_region(bbox, documents, line_h=None):
    """EXTEND an existing box (too short/narrow, this ink continues it) vs NEW box.
    Scale-free: a continuation sits within ~one text line of a host and overlaps it
    strongly along the shared edge. Ambiguous/distant -> NEW (safer: a wrong extend
    mislabels; a wrong new only fragments). Returns ('extend', {...}) | ('new', None)."""
    rx0, ry0, rx1, ry1 = bbox
    rw, rh = max(rx1 - rx0, 1), max(ry1 - ry0, 1)
    near = line_h or rh
    best = None
    for did, doc in documents.items():
        for cat, polys in doc.items():
            for i, b in enumerate(polys):
                bx0, by0, bx1, by1 = bbox_of(b["vertices"])
                ix = max(0, min(rx1, bx1) - max(rx0, bx0))
                iy = max(0, min(ry1, by1) - max(ry0, by0))
                hov = ix / min(rw, bx1 - bx0) if min(rw, bx1 - bx0) > 0 else 0
                vov = iy / min(rh, by1 - by0) if min(rh, by1 - by0) > 0 else 0
                cands = []
                if hov >= 0.6:  # shared column -> vertical continuation
                    if (ry0 - by1) >= -0.5 * near and (ry0 - by1) < 1.2 * near:
                        cands.append((hov - max(ry0 - by1, 0) / near, "down"))
                    if (by0 - ry1) >= -0.5 * near and (by0 - ry1) < 1.2 * near:
                        cands.append((hov - max(by0 - ry1, 0) / near, "up"))
                if vov >= 0.6:  # shared line -> horizontal continuation
                    if (rx0 - bx1) >= -0.5 * near and (rx0 - bx1) < 1.5 * near:
                        cands.append((vov - max(rx0 - bx1, 0) / near, "right"))
                    if (bx0 - rx1) >= -0.5 * near and (bx0 - rx1) < 1.5 * near:
                        cands.append((vov - max(bx0 - rx1, 0) / near, "left"))
                for score, d in cands:
                    if best is None or score > best[0]:
                        best = (score, did, cat, i, d)
    if best:
        return "extend", {"doc": best[1], "cat": best[2], "idx": best[3], "dir": best[4]}
    return "new", None


def main():
    qa = json.load(open(QA))
    ext = new = 0
    for p in qa["pages"]:
        flags = [f for f in p["flags"] if f["type"] == "uncovered_ink"]
        if not flags:
            continue
        pg = f"page_{p['page']:03d}"
        d = json.load(open(OLD / pg / f"{pg}.json"))
        for f in flags:
            dec, tgt = classify_uncovered_region(tuple(f["bbox"]), d["documents"])
            if dec == "extend":
                ext += 1
                print(f"  {pg}: EXTEND {tgt['cat']} ({tgt['dir']})")
            else:
                new += 1
                print(f"  {pg}: NEW")
    print(f"\nclassifier: {ext} extend, {new} new   (expected ~14 extend, ~2 new)")


if __name__ == "__main__":
    main()
