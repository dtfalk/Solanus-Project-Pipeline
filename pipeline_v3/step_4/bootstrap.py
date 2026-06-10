#!/usr/bin/env python3
"""THE per-volume bootstrap service: one command that drives the staged HITL flow.

Run it repeatedly; it inspects the filesystem, performs the next MACHINE step,
and stops at exactly the HUMAN gates with the one next action printed. David's
flow (2026-06-10): cluster pages by type -> he checks the contact sheets and
confirms -> sample pages are labeled with the most-similar generic few-shots ->
he corrects them in the editor -> his corrections become the volume's demos ->
he decides: label MORE samples to check, or run the whole remainder.

    ./venv/bin/python bootstrap.py Volume_3                  # do/print next step
    ./venv/bin/python bootstrap.py Volume_3 --status         # report only
    ./venv/bin/python bootstrap.py Volume_3 --confirm-clusters [--merge A+B ...]
    ./venv/bin/python bootstrap.py Volume_3 --bless-unchanged
    ./venv/bin/python bootstrap.py Volume_3 --label-more 20
    ./venv/bin/python bootstrap.py Volume_3 --run-rest

Stages (derived from artifacts, never trusted from memory):
  A  no clusters.json          -> run cluster_pages.py (by type + geometry)
  B  clusters unconfirmed      -> WAIT for --confirm-clusters (David eyeballs
                                  contact sheets; --merge A+B folds clusters)
  C  no representatives        -> pick reps across clusters, label them with the
                                  existing cross-volume pool (layout-similarity
                                  picks the most relevant demos), then WAIT for
                                  David's corrections in the editor
  D  loop                      -> promote his corrected pages into the pool
                                  (+ --bless-unchanged for sampled pages he
                                  deemed already-correct), then WAIT for his
                                  --label-more N or --run-rest decision
  E  --run-rest                -> relabel the full remainder with HIS demos
                                  pinned + the volume note, then qa_report +
                                  triage -> review-ready

Safety: reviewed/ is David-only (never written here); crops are fingerprinted at
first run and re-verified every run (machine-vintage drift killed two volumes —
see RESUME_HERE 2026-06-09); every API step prints its cost estimate first.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
QA_DIR     = SCRIPT_DIR / "qa_output"
REVIEW_DIR = SCRIPT_DIR / "reviewed"
POOL_DIR   = SCRIPT_DIR / "labeled_examples"
AUTO_DIR   = SCRIPT_DIR / "auto_labeled"
CROPS_DIR  = SCRIPT_DIR / "polygon_cropped_pdfs"
NOTES_DIR  = SCRIPT_DIR / "volume_notes"
PY         = str(SCRIPT_DIR / "venv" / "bin" / "python")

COST_PER_PAGE = 0.09          # 3.5-flash, observed (usage.csv)
MAX_PINS      = 12            # more demos measurably hurt (c20 < c12, 2026-06-08)


# ── small pure helpers (unit-tested in run_all_tests F10) ─────────────────────

def apply_merges(clusters: dict, merges: list[list[str]]) -> dict:
    """Fold cluster ids per David's --merge groups; first id in a group survives.
    Returns the rewritten clusters.json payload (pure; caller writes it)."""
    alias = {}
    for group in merges:
        keep = group[0]
        for other in group[1:]:
            alias[other] = keep
    p2c = {p: alias.get(str(c), str(c)) for p, c in clusters["page_to_cluster"].items()}
    merged: dict[str, dict] = {}
    for cid, entry in clusters["clusters"].items():
        tgt = alias.get(cid, cid)
        if tgt not in merged:
            merged[tgt] = {"display_name": entry.get("display_name", ""),
                           "size": 0, "pages": [], "most_central": []}
        merged[tgt]["size"] += entry["size"]
        merged[tgt]["pages"] = sorted(merged[tgt]["pages"] + entry["pages"])
        merged[tgt]["most_central"] = (merged[tgt]["most_central"]
                                       + entry.get("most_central", []))[:3]
    out = dict(clusters)
    out["clusters"] = merged
    out["page_to_cluster"] = p2c
    out["k"] = len(merged)
    out["merged_at"] = datetime.now().isoformat(timespec="seconds")
    return out


def pick_pins(vol: str, pool_pages: list[str], page_to_cluster: dict,
              max_pins: int = MAX_PINS) -> list[str]:
    """Choose <=max_pins of the volume's pool pages, round-robin across clusters
    so every confirmed cluster keeps a demo in the pinned set."""
    if len(pool_pages) <= max_pins:
        return sorted(pool_pages)
    by_cluster: dict[str, list[str]] = {}
    for p in sorted(pool_pages):
        by_cluster.setdefault(str(page_to_cluster.get(p, "?")), []).append(p)
    pins, queues = [], sorted(by_cluster.values(), key=len, reverse=True)
    while len(pins) < max_pins and any(queues):
        for q in queues:
            if q and len(pins) < max_pins:
                pins.append(q.pop(0))
    return sorted(pins)


def crops_fingerprint(pages_dir: Path) -> dict:
    """Cheap identity of the crop set: count + md5 of first/middle/last PDFs.
    Guards against the machine-vintage drift that corrupted A3 and V4."""
    pdfs = sorted(pages_dir.glob("page_*.pdf"))
    if not pdfs:
        return {"count": 0}
    picks = [pdfs[0], pdfs[len(pdfs) // 2], pdfs[-1]]
    return {
        "count": len(pdfs),
        "samples": {p.name: hashlib.md5(p.read_bytes()).hexdigest() for p in picks},
    }


def derive_stage(vol: str, state: dict, paths: dict) -> str:
    """A/B/C/D from the filesystem. Pure given the path snapshot dict:
    keys: has_pages, has_clusters, confirmed, has_reps."""
    if not paths["has_pages"]:
        return "BLOCKED"
    if not paths["has_clusters"]:
        return "A"
    if not state.get("clusters_confirmed"):
        return "B"
    if not paths["has_reps"]:
        return "C"
    return "D"


# ── filesystem plumbing ───────────────────────────────────────────────────────

def state_path(vol: str) -> Path:
    return QA_DIR / vol / "bootstrap_state.json"


def load_state(vol: str) -> dict:
    p = state_path(vol)
    return json.loads(p.read_text()) if p.exists() else {}


def save_state(vol: str, state: dict) -> None:
    state_path(vol).parent.mkdir(parents=True, exist_ok=True)
    state_path(vol).write_text(json.dumps(state, indent=2) + "\n")


def run(cmd: list[str], why: str) -> None:
    print(f"\n── {why}\n   $ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=SCRIPT_DIR)
    if r.returncode != 0:
        raise SystemExit(f"step failed (exit {r.returncode}): {why}")


def vol_pool_pages(vol: str) -> list[str]:
    d = POOL_DIR / vol
    return sorted(p.name for p in d.glob("page_*")) if d.is_dir() else []


def vol_reviewed_pages(vol: str) -> list[str]:
    d = REVIEW_DIR / vol
    return sorted(p.name for p in d.glob("page_*")) if d.is_dir() else []


def nums_arg(pages: list[str]) -> str:
    return ",".join(str(int(p.split("_")[1])) for p in pages)


def upload_pool_pages(vol: str, pages: list[str]) -> None:
    """Incremental Files-API upload for pool pages (merged into file_uris.json).
    Same mechanics as promote_examples --upload, usable for blessed pages too."""
    uri_path = SCRIPT_DIR / "file_uris.json"
    if not uri_path.exists():
        print("   WARNING: file_uris.json missing — demos will inline (slow). "
              "Run upload_examples.py for a full upload.")
        return
    from dotenv import load_dotenv
    from google import genai
    from google.genai import types
    from upload_examples import render_to_png_bytes, resolve_width
    load_dotenv(SCRIPT_DIR / ".env")
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    uri_map = json.loads(uri_path.read_text())
    width = resolve_width(str(uri_map.get("image_width", "1024")))
    for i, name in enumerate(pages, 1):
        key = f"{vol}/{name}"
        pdf = POOL_DIR / vol / name / f"{name}.pdf"
        up = client.files.upload(
            file=io.BytesIO(render_to_png_bytes(pdf, width)),
            config=types.UploadFileConfig(mime_type="image/png", display_name=key))
        uri_map["uris"][key] = up.name
        print(f"   uploaded [{i}/{len(pages)}] {key}")
    uri_map["last_incremental_upload"] = datetime.now().isoformat(timespec="seconds")
    uri_path.write_text(json.dumps(uri_map, indent=2))


def bless_pages(vol: str, pages: list[str]) -> None:
    """David approved these sampled pages AS LABELED (no edits needed): copy the
    auto labels + PDFs into the demo pool. reviewed/ is NOT touched — it stays
    exclusively the pages he actually edited."""
    for name in pages:
        src = AUTO_DIR / vol / name
        dest = POOL_DIR / vol / name
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src / f"{name}.json", dest / f"{name}.json")
        shutil.copy2(src / f"{name}.pdf", dest / f"{name}.pdf")
    upload_pool_pages(vol, pages)


# ── the driver ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("volume")
    p.add_argument("--status", action="store_true", help="Report state, change nothing.")
    p.add_argument("--confirm-clusters", action="store_true",
                   help="Record David's approval of the contact sheets (stage B gate).")
    p.add_argument("--merge", action="append", default=[], metavar="A+B",
                   help="While confirming: fold cluster B (and C...) into A. Repeatable.")
    p.add_argument("--bless-unchanged", action="store_true",
                   help="Sampled pages David did NOT edit were already correct: "
                        "promote their auto labels to the demo pool (reviewed/ untouched).")
    p.add_argument("--label-more", type=int, metavar="N",
                   help="Next round: label N more diverse sample pages for checking.")
    p.add_argument("--run-rest", action="store_true",
                   help="Label the entire remainder with David's demos pinned, then qa + triage.")
    p.add_argument("--refingerprint", action="store_true",
                   help="Accept a changed crop set after verifying alignment yourself.")
    p.add_argument("--open-sheets", action="store_true",
                   help="Open this volume's cluster contact sheets in your image "
                        "viewer (THIS is how you review clusters before confirming).")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    vol = a.volume
    state = load_state(vol)
    pages_dir = CROPS_DIR / vol / "pages"
    n_pages = len(list(pages_dir.glob("page_*.pdf"))) if pages_dir.is_dir() else 0

    # crop-vintage guard
    fp = crops_fingerprint(pages_dir) if n_pages else {"count": 0}
    if state.get("crops_fingerprint") and not a.refingerprint:
        if fp != state["crops_fingerprint"]:
            print(f"⚠ {vol}: the crop set CHANGED since this flow started "
                  f"(was {state['crops_fingerprint'].get('count')} pages, now {fp.get('count')}).\n"
                  f"  Machine-vintage drift corrupted A3 and V4 — verify page-numbering "
                  f"alignment (pixel-correlate a few pages), then rerun with --refingerprint.")
            return
    if n_pages and fp != state.get("crops_fingerprint"):
        state["crops_fingerprint"] = fp
        save_state(vol, state)

    # representatives are only valid if generated AFTER the current clusters —
    # an older file (e.g. a pre-bootstrap pick) must not skip stage C.
    has_reps = False
    cpath = QA_DIR / vol / "clusters.json"
    rpath = QA_DIR / vol / "representatives.json"
    if rpath.exists() and cpath.exists():
        try:
            r_at = json.loads(rpath.read_text()).get("generated_at", "")
            c_at = json.loads(cpath.read_text()).get("generated_at", "")
            has_reps = r_at >= c_at
            if not has_reps:
                stale = rpath.with_suffix(".pre-bootstrap.json")
                if not stale.exists():
                    shutil.copy2(rpath, stale)
                    print(f"  (archived stale pre-cluster representatives -> {stale.name})")
        except Exception:
            has_reps = False

    paths = {
        "has_pages": n_pages > 0,
        "has_clusters": cpath.exists(),
        "has_reps": has_reps,
    }
    stage = derive_stage(vol, state, paths)

    if a.open_sheets:
        sheets = sorted((SCRIPT_DIR / "label_review" / "contact_sheets" / vol).glob("*.png"))
        if not sheets:
            print(f"no contact sheets yet — run bootstrap.py {vol} first (stage A).")
            return
        summary = QA_DIR / vol / "clusters.txt"
        if summary.exists():
            print(summary.read_text())
        for s in sheets:
            subprocess.Popen(["xdg-open", str(s)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"opened {len(sheets)} sheet(s) in your image viewer. Each thumbnail "
              f"is one page (most-central pages first).\nIf the groups make sense:  "
              f"./venv/bin/python bootstrap.py {vol} --confirm-clusters"
              f"\nTo fold two groups:       ... --confirm-clusters --merge A+B")
        return

    if a.status:
        report_status(vol, stage, state)
        return

    if stage == "BLOCKED":
        print(f"✗ {vol}: no pages under {pages_dir} — sync the crops first "
              f"(see RESUME_HERE machine-vintage note), then rerun.")
        return

    if stage == "A":
        has_key = (SCRIPT_DIR / ".env").exists()
        cmd = [PY, "cluster_pages.py", vol] + ([] if has_key else ["--no-api"])
        run(cmd, f"stage A — cluster {vol} by page type + architecture ({n_pages} pages)")
        sheets = sorted((SCRIPT_DIR / "label_review" / "contact_sheets" / vol).glob("*.png"))
        print(f"\n■ YOUR TURN — check the clusters ({len(sheets)} contact sheets):")
        for s in sheets:
            print(f"    {s}")
        print(f"  then:  ./venv/bin/python bootstrap.py {vol} --confirm-clusters"
              f"   (add --merge A+B to fold clusters)")
        return

    if stage == "B":
        if not a.confirm_clusters:
            print(f"■ waiting on you: review the contact sheets in "
                  f"label_review/contact_sheets/{vol}/, then\n"
                  f"  ./venv/bin/python bootstrap.py {vol} --confirm-clusters [--merge A+B]")
            return
        cpath = QA_DIR / vol / "clusters.json"
        clusters = json.loads(cpath.read_text())
        if a.merge:
            groups = [g.split("+") for g in a.merge]
            clusters = apply_merges(clusters, groups)
            cpath.write_text(json.dumps(clusters, indent=2) + "\n")
            state["merges"] = a.merge
            print(f"  merged: {a.merge} -> {clusters['k']} clusters")
        state["clusters_confirmed"] = datetime.now().isoformat(timespec="seconds")
        save_state(vol, state)
        print(f"✓ clusters confirmed ({json.loads(cpath.read_text())['k']})")
        stage = "C"   # fall through — keep moving

    if stage == "C":
        run([PY, "pick_representatives.py", vol, "--clusters"],
            "stage C — pick representatives across your confirmed clusters")
        reps = json.loads((QA_DIR / vol / "representatives.json").read_text())
        rep_pages = [r["page"] for r in reps["representatives"]]
        est = len(rep_pages) * COST_PER_PAGE
        run([PY, "auto_labeler.py", "--volume", vol, "--pages", reps["pages_arg"]],
            f"stage C — label the {len(rep_pages)} representatives with the "
            f"most-similar existing demos (~${est:.2f})")
        state["sampled"] = sorted(set(state.get("sampled", [])) | set(rep_pages))
        save_state(vol, state)
        print(f"\n■ YOUR TURN — correct the samples in the editor:")
        print(f"    EDITOR_DOCUMENT={vol} ./venv/bin/python normalized_editor.py")
        print(f"    pages: {nums_arg(rep_pages)}")
        print(f"  then:  ./venv/bin/python bootstrap.py {vol}")
        return

    # stage D — the promote/decide loop
    sampled  = set(state.get("sampled", []))
    reviewed = set(vol_reviewed_pages(vol))
    pool     = set(vol_pool_pages(vol))

    fresh   = sorted((reviewed & sampled) - pool)
    refresh = sorted(p for p in (reviewed & sampled & pool)
                     if (REVIEW_DIR / vol / p / f"{p}.json").stat().st_mtime
                     > (POOL_DIR / vol / p / f"{p}.json").stat().st_mtime)
    if fresh:
        run([PY, "promote_examples.py", vol, "--pages", nums_arg(fresh), "--upload"],
            f"promoting {len(fresh)} corrected page(s) into the demo pool")
    if refresh:
        run([PY, "promote_examples.py", vol, "--pages", nums_arg(refresh), "--upload", "--force"],
            f"refreshing {len(refresh)} re-corrected page(s) in the pool")
    if fresh or refresh:
        run([PY, "review_diff.py", vol, "--draft-note"],
            "drafting the volume convention note from your corrections")
        note = NOTES_DIR / f"{vol}.md"
        print(f"  draft at qa_output/{vol}/volume_note_draft.txt — edit the real "
              f"convention into {note.relative_to(SCRIPT_DIR)} (plain text, yours).")

    unedited = sorted(sampled - reviewed - pool)
    if a.bless_unchanged and unedited:
        print(f"── blessing {len(unedited)} unedited sample(s) as correct-as-labeled")
        bless_pages(vol, unedited)
        unedited = []

    pool = set(vol_pool_pages(vol))   # refresh after promotions

    if a.label_more:
        if not pool:
            print("✗ no demos from this volume yet — correct (or --bless-unchanged) "
                  "some samples first.")
            return
        run([PY, "pick_chunk.py", vol, "--count", str(a.label_more)],
            f"picking {a.label_more} more diverse sample pages")
        chunk = json.loads((QA_DIR / vol / "chunk.json").read_text())
        pins = pick_pins(vol, sorted(pool),
                         json.loads((QA_DIR / vol / "clusters.json").read_text())["page_to_cluster"])
        pin_arg = ",".join(f"{vol}/{p}" for p in pins)
        est = chunk["chunk_size"] * COST_PER_PAGE
        run([PY, "auto_labeler.py", "--volume", vol, "--pages", chunk["pages_arg"],
             "--pin-examples", pin_arg],
            f"labeling {chunk['chunk_size']} sample pages with YOUR {len(pins)} demos pinned (~${est:.2f})")
        state["sampled"] = sorted(sampled | set(chunk["pages"]))
        save_state(vol, state)
        print(f"\n■ YOUR TURN — check/correct in the editor (pages {chunk['pages_arg']}), "
              f"then rerun bootstrap.py {vol} (decide --label-more N or --run-rest).")
        return

    if a.run_rest:
        if not pool:
            print("✗ no demos from this volume yet — correct (or --bless-unchanged) "
                  "some samples first.")
            return
        pins = pick_pins(vol, sorted(pool),
                         json.loads((QA_DIR / vol / "clusters.json").read_text())["page_to_cluster"])
        pin_arg = ",".join(f"{vol}/{p}" for p in pins)
        remainder = n_pages - len(vol_reviewed_pages(vol))
        est = remainder * COST_PER_PAGE
        note = NOTES_DIR / f"{vol}.md"
        if not note.exists():
            print(f"  NOTE: no {note.name} volume note — running on demos alone. "
                  f"(Write one any time; it loads automatically.)")
        run([PY, "auto_labeler.py", "--volume", vol, "--overwrite",
             "--pin-examples", pin_arg],
            f"stage E — labeling the remainder (~{remainder} pages, ~${est:.2f}) "
            f"with your {len(pins)} pinned demos")
        run([PY, "qa_report.py", vol], "qa_report")
        run([PY, "triage.py", vol], "triage (worst-first worklist)")
        state["completed"] = datetime.now().isoformat(timespec="seconds")
        save_state(vol, state)
        print(f"\n✓ {vol} COMPLETE — review with:\n"
              f"    EDITOR_DOCUMENT={vol} ./venv/bin/python normalized_editor.py\n"
              f"  worst-first list: qa_output/{vol}/triage.txt")
        return

    report_status(vol, stage, state, sampled, reviewed, pool, unedited)


def report_status(vol, stage, state, sampled=None, reviewed=None, pool=None, unedited=None):
    print(f"{vol}: stage {stage}")
    if state.get("clusters_confirmed"):
        print(f"  clusters confirmed {state['clusters_confirmed']}"
              + (f", merges {state['merges']}" if state.get("merges") else ""))
    if sampled is not None:
        corrected = sorted(reviewed & sampled)
        print(f"  sampled {len(sampled)} | corrected by you {len(corrected)} | "
              f"in pool {len(pool)} | unedited samples {len(unedited or [])}")
        if unedited:
            print(f"  unedited: {nums_arg(sorted(unedited))} — rerun with "
                  f"--bless-unchanged if these were already correct")
        print(f"  next: --label-more N (another check round) or --run-rest (finish the volume)")
    if state.get("completed"):
        print(f"  completed {state['completed']}")


if __name__ == "__main__":
    main()
