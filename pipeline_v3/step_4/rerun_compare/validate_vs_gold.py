"""Validate the extend-vs-new recognizer against GOLD: for each of the 16 misses,
does the human gold group that region into a LARGER box (=> extend was right, and
to which category) or give it its own ~same-size box (=> new was right)?"""
import json
from pathlib import Path
from classify_proto import classify_uncovered_region, bbox_of

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OLD = ROOT / "checkpoints/02_pre-coverage-backstop/auto_labeled_Appendix_1_no_backstop"
GOLD = ROOT / "reviewed/Appendix_1"
QA = ROOT / "checkpoints/02_pre-coverage-backstop/qa_no_backstop.json"


def cover(region, box):
    rx0, ry0, rx1, ry1 = region
    bx0, by0, bx1, by1 = box
    ix = max(0, min(rx1, bx1) - max(rx0, bx0))
    iy = max(0, min(ry1, by1) - max(ry0, by0))
    ra = (rx1 - rx0) * (ry1 - ry0)
    return (ix * iy) / ra if ra > 0 else 0


def main():
    qa = json.load(open(QA))
    agree = disagree = 0
    for p in qa["pages"]:
        flags = [f for f in p["flags"] if f["type"] == "uncovered_ink"]
        if not flags:
            continue
        pg = f"page_{p['page']:03d}"
        model = json.load(open(OLD / pg / f"{pg}.json"))
        gold = json.load(open(GOLD / pg / f"{pg}.json"))
        gboxes = [(c, bbox_of(b["vertices"]))
                  for doc in gold["documents"].values()
                  for c, ps in doc.items() for b in ps]
        for f in flags:
            R = tuple(f["bbox"])
            ra = (R[2] - R[0]) * (R[3] - R[1])
            dec, tgt = classify_uncovered_region(R, model["documents"])
            # gold box covering the region best
            best = max(gboxes, key=lambda cb: cover(R, cb[1]), default=None)
            gcat, gbb = best
            gcov = cover(R, gbb)
            garea = (gbb[2] - gbb[0]) * (gbb[3] - gbb[1])
            ratio = garea / ra if ra else 0
            gold_dec = "extend" if (gcov > 0.6 and ratio > 2.5) else "new" if gcov > 0.6 else "uncovered-in-gold-too"
            cat_ok = (dec == "new") or (tgt and tgt["cat"] == gcat)
            ok = (dec == gold_dec) and cat_ok
            agree += ok
            disagree += not ok
            mark = "ok " if ok else "XX "
            tc = tgt["cat"] if tgt else "-"
            print(f"  {mark}{pg}: clf={dec}/{tc:16} | gold box={gcat:16} cov={gcov:.2f} ratio={ratio:.1f} -> gold={gold_dec}")
    print(f"\nagreement with gold: {agree}/{agree+disagree}")


if __name__ == "__main__":
    main()
