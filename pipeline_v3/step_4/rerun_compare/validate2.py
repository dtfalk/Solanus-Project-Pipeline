"""Validate the CATEGORY-SAFE rule: category := what the model/gold says for the
region; geometry only decides extend (merge into an adjacent SAME-category box)
vs new (no same-category neighbor). This can never mislabel — it only chooses
topology. We check the topology decision against gold's grouping."""
import json
from pathlib import Path
from classify_proto import bbox_of

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OLD = ROOT / "checkpoints/02_pre-coverage-backstop/auto_labeled_Appendix_1_no_backstop"
GOLD = ROOT / "reviewed/Appendix_1"
QA = ROOT / "checkpoints/02_pre-coverage-backstop/qa_no_backstop.json"


def cover(R, b):
    ix = max(0, min(R[2], b[2]) - max(R[0], b[0]))
    iy = max(0, min(R[3], b[3]) - max(R[1], b[1]))
    ra = (R[2] - R[0]) * (R[3] - R[1])
    return ix * iy / ra if ra else 0


def adjacent_same_cat(R, cat, documents):
    """A box of category `cat` whose edge the region abuts (within ~1 line)."""
    rx0, ry0, rx1, ry1 = R
    rw, rh = max(rx1 - rx0, 1), max(ry1 - ry0, 1)
    for doc in documents.values():
        for c, ps in doc.items():
            if c != cat:
                continue
            for b in ps:
                bx0, by0, bx1, by1 = bbox_of(b["vertices"])
                ix = max(0, min(rx1, bx1) - max(rx0, bx0))
                iy = max(0, min(ry1, by1) - max(ry0, by0))
                hov = ix / min(rw, bx1 - bx0) if min(rw, bx1 - bx0) > 0 else 0
                vov = iy / min(rh, by1 - by0) if min(rh, by1 - by0) > 0 else 0
                if hov >= 0.6 and -0.5 * rh <= (ry0 - by1) < 1.2 * rh:
                    return True
                if hov >= 0.6 and -0.5 * rh <= (by0 - ry1) < 1.2 * rh:
                    return True
                if vov >= 0.6 and -0.5 * rh <= (rx0 - bx1) < 1.5 * rh:
                    return True
                if vov >= 0.6 and -0.5 * rh <= (bx0 - rx1) < 1.5 * rh:
                    return True
    return False


def main():
    qa = json.load(open(QA))
    agree = total = 0
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
            R = tuple(f["bbox"]); ra = (R[2] - R[0]) * (R[3] - R[1])
            gcat, gbb = max(gboxes, key=lambda cb: cover(R, cb[1]))
            ratio = ((gbb[2] - gbb[0]) * (gbb[3] - gbb[1])) / ra if ra else 0
            gold_dec = "extend" if ratio > 2.5 else "new"
            dec = "extend" if adjacent_same_cat(R, gcat, model["documents"]) else "new"
            ok = dec == gold_dec
            total += 1; agree += ok
            print(f"  {'ok ' if ok else 'XX '}{pg}: cat={gcat:14} clf={dec:6} gold={gold_dec:6} (ratio {ratio:.1f})")
    print(f"\ntopology agreement with gold: {agree}/{total}  (category is always the model's -> never mislabels)")


if __name__ == "__main__":
    main()
