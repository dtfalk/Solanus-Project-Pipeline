#!/usr/bin/env python3
"""STEP_5 — fix common OCR substitution patterns in the merged text (NON-DESTRUCTIVE).

CUMULATIVE + curatable. Each --apply layers onto the previous result: the source for a page is
extract_cleaned.json if it already exists, else extract_merged.json. So you can commit the safe
rules first and review the rest with the clutter gone:

  1. APPLY the known-good rule(s):
       venv/bin/python clean_ocr.py --apply --rules ist
  2. DRY-RUN again -> the report now only lists what's LEFT (ist already fixed, no longer matches):
       venv/bin/python clean_ocr.py
  3. CURATE clean_report.tsv -> delete rows you DON'T want, then layer them on:
       venv/bin/python clean_ocr.py --apply --edits clean_report.tsv

extract_merged.json / extract_azure.json(.raw) are NEVER modified. extract_cleaned.json keeps the
original merged_text, the running cleaned_text, and a cumulative edit log. --from-merged ignores any
existing extract_cleaned.json and restarts from extract_merged.json. All rules are length-preserving,
so positions / row ids stay valid across layers.

Rules (all on by default; subset with --rules):
  ist        \\b[Il]st\\b -> 1st          "for the Ist part" -> "1st"
  zero_word  0 inside an otherwise-alphabetic token -> O   "0gg"->"Ogg", "0ct"->"Oct" (numbers safe)
  lone_o     standalone 0 / 0. -> O / O.   interjection "O" and "Portland, O."(=Oregon); REVIEW these
"""
from __future__ import annotations
import json, re, csv, argparse
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALL_RULES = ("ist", "zero_word", "lone_o")
COLS = ("rule", "page", "rid", "category", "before", "after", "context", "id")

RX = {
    "ist":       (re.compile(r"\b[Il]st\b"),                                          lambda m: "1st"),
    "zero_word": (re.compile(r"\b(?=[A-Za-z0]*[A-Za-z])(?=[A-Za-z0]*0)[A-Za-z0]+\b"), lambda m: m.group(0).replace("0", "O")),
    "lone_o":    (re.compile(r"(?<![\w])0(\.?)(?![\w])"),                             lambda m: "O" + m.group(1)),
}


def candidates(text: str, rules, page: str, rid: str):
    """All proposed edits for a region, on the text as given. The three rules match disjoint spans,
    so they don't interact; ids are stable because every substitution is length-preserving."""
    out = []
    for name in ALL_RULES:
        if name not in rules:
            continue
        rx, repl = RX[name]
        for m in rx.finditer(text):
            out.append({"id": f"{page}|{rid}|{m.start()}|{name}", "rule": name,
                        "before": m.group(0), "after": repl(m), "pos": m.start()})
    return out


def apply_edits(text: str, edits):
    for e in sorted(edits, key=lambda x: x["pos"], reverse=True):
        text = text[:e["pos"]] + e["after"] + text[e["pos"] + len(e["before"]):]
    return text


def read_source(merged_path: Path, from_merged: bool):
    """Per page -> (page, [(rid, category, working_text, original_merged, prior_log)], chained?).
    Chains off extract_cleaned.json when present so layers accumulate."""
    cleaned = merged_path.parent / "extract_cleaned.json"
    if cleaned.exists() and not from_merged:
        d = json.loads(cleaned.read_text())
        regs = [(r.get("rid", ""), r.get("category", ""), r.get("cleaned_text", ""),
                 r.get("merged_text", ""), list(r.get("edits", []))) for r in d.get("regions", [])]
        return d["page"], regs, True
    d = json.loads(merged_path.read_text())
    regs = [(r.get("rid", ""), r.get("category", ""), r.get("merged_text", ""),
             r.get("merged_text", ""), []) for r in d.get("regions", [])]
    return d["page"], regs, False


def load_approved(path: Path):
    with open(path, newline="") as f:
        return {row["id"] for row in csv.DictReader(f, delimiter="\t") if row.get("id")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="3_enriched")
    ap.add_argument("--rules", default=",".join(ALL_RULES), help=f"comma list of {ALL_RULES}")
    ap.add_argument("--toc-only", action="store_true", help="only table_of_contents pages")
    ap.add_argument("--apply", action="store_true", help="write extract_cleaned.json per page (default: dry-run)")
    ap.add_argument("--edits", help="an edited clean_report.tsv; apply ONLY the rows it still contains")
    ap.add_argument("--from-merged", action="store_true", help="ignore existing extract_cleaned.json; restart from merged")
    a = ap.parse_args()
    root = (HERE / a.root).resolve()
    rules = {x.strip() for x in a.rules.split(",") if x.strip()}
    bad = rules - set(ALL_RULES)
    if bad:
        raise SystemExit(f"unknown rule(s): {bad}; valid: {ALL_RULES}")
    approved = load_approved(Path(a.edits)) if a.edits else None

    counts = {r: 0 for r in ALL_RULES}
    rows, npages, nchanged, applied, skipped = [], 0, 0, 0, 0
    for f in sorted(root.rglob("extract_merged.json")):
        if a.toc_only and "0_table_of_contents" not in f.parts:
            continue
        page, regs, chained = read_source(f, a.from_merged)
        npages += 1
        out_regions, this_page_new = [], 0
        for (rid, cat, working, orig_merged, prior_log) in regs:
            cands = candidates(working, rules, page, rid)
            preview = apply_edits(working, cands)
            for c in cands:
                counts[c["rule"]] += 1
                ctx = preview[max(0, c["pos"] - 25): c["pos"] + len(c["after"]) + 20].replace("\t", " ").replace("\n", " ")
                rows.append({"rule": c["rule"], "page": page, "rid": rid, "category": cat,
                             "before": c["before"], "after": c["after"], "context": ctx, "id": c["id"]})
            to_apply = cands if approved is None else [c for c in cands if c["id"] in approved]
            applied += len(to_apply); skipped += len(cands) - len(to_apply); this_page_new += len(to_apply)
            new_log = prior_log + [{k: c[k] for k in ("rule", "before", "after")} for c in to_apply]
            out_regions.append({"rid": rid, "category": cat,
                                "cleaned_text": apply_edits(working, to_apply),
                                "merged_text": orig_merged, "edits": new_log})
        if a.apply and (chained or this_page_new):                # persist layer (and preserve prior)
            if any(rr["edits"] for rr in out_regions):
                nchanged += 1
                (f.parent / "extract_cleaned.json").write_text(json.dumps(
                    {"page": page, "source": "extract_cleaned.json(chained)" if chained else "extract_merged.json",
                     "rules_run": sorted(rules), "n_edits": sum(len(rr["edits"]) for rr in out_regions),
                     "regions": out_regions}, indent=2, ensure_ascii=False))

    if not a.apply:
        stem = "clean_report.toc" if a.toc_only else "clean_report"
        with open(HERE / f"{stem}.tsv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=COLS, delimiter="\t"); w.writeheader(); w.writerows(rows)
        print(f"DRY-RUN: {len(rows)} remaining candidate(s) "
              f"(" + ", ".join(f"{k}={counts[k]}" for k in ALL_RULES) + f") over {npages} pages")
        for k in ALL_RULES:
            ex = next((r for r in rows if r["rule"] == k), None)
            if ex:
                print(f"  e.g. {k:9}: {ex['before']!r} -> {ex['after']!r}   …{ex['context']}…")
        print(f"\nedit -> delete rows you DON'T want, then:  clean_ocr.py --apply --edits {stem}.tsv")
        print(f"report: {HERE / (stem + '.tsv')}")
    else:
        mode = f"curated from {a.edits}" if a.edits else f"rules={sorted(rules)}"
        print(f"APPLIED ({mode}): {applied} substitutions this run, {skipped} skipped, {nchanged} pages written")
        print("layered into extract_cleaned.json per page (merged/raw untouched)")


if __name__ == "__main__":
    main()
