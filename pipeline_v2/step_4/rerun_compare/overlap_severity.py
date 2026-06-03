"""Classify each overlapping box-pair by the INK in the shared region:
  whitespace  - shared region essentially blank (cropping captures nothing extra)
  partial     - ink present but a thin band (clipped char tops/bottoms; a text
                extraction prompt can weed it out)
  fullword    - the shared region holds >= ~half a text line of ink (cropping the
                box would capture a full extra word) -> the only case worth flagging
"""
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from auto_labeler import render_page
from qa_report import _ink_mask, _iter_boxes, _inter_over_min, OVERLAP_THRESH

ROOT = Path(__file__).resolve().parent.parent
VOL = "Appendix_1"


def main():
    counts = {"whitespace": 0, "partial": 0, "fullword": 0}
    fullword = []
    polarity_checked = False
    for pd in sorted((ROOT / "auto_labeled" / VOL).glob("page_*")):
        jp = pd / f"{pd.name}.json"; pp = pd / f"{pd.name}.pdf"
        if not jp.exists():
            continue
        docs = json.load(open(jp))["documents"]
        boxes = [(cat, bb) for _d, cat, _b, bb in _iter_boxes(docs)]
        pairs = [(boxes[i], boxes[j]) for i in range(len(boxes)) for j in range(i + 1, len(boxes))
                 if _inter_over_min(boxes[i][1], boxes[j][1]) > OVERLAP_THRESH]
        if not pairs:
            continue
        ink = _ink_mask(render_page(pp, None)[0].convert("L"))
        if not polarity_checked:
            tot = sum(1 for p in ink.getdata() if p > 0) / (ink.size[0] * ink.size[1])
            print(f"[sanity] page ink fraction = {tot:.1%} (typed page should be small => ink=nonzero)")
            polarity_checked = True
        heights = sorted(b[3] - b[1] for _c, b in boxes)
        line_h = heights[len(heights) // 2]
        for (ci, bi), (cj, bj) in pairs:
            ix0, iy0 = max(bi[0], bj[0]), max(bi[1], bj[1])
            ix1, iy1 = min(bi[2], bj[2]), min(bi[3], bj[3])
            if ix1 <= ix0 or iy1 <= iy0:
                continue
            crop = ink.crop((int(ix0), int(iy0), int(ix1), int(iy1)))
            area = (ix1 - ix0) * (iy1 - iy0)
            bb_ink = crop.getbbox()
            frac = (sum(1 for p in crop.getdata() if p > 0) / area) if area else 0
            if bb_ink is None or frac < 0.01:
                cls = "whitespace"
            else:
                h_ink = bb_ink[3] - bb_ink[1]
                if h_ink < 0.5 * line_h:
                    cls = "partial"
                else:
                    cls = "fullword"
                    fullword.append((pd.name, f"{ci}+{cj}", f"ink {h_ink/line_h:.1f} lines tall, {bb_ink[2]-bb_ink[0]}px wide"))
            counts[cls] += 1
    print("\noverlap severity across Appendix_1:", counts)
    print("FULL-WORD overlaps (the only ones worth flagging):")
    for e in fullword:
        print("  ", e)


if __name__ == "__main__":
    main()
