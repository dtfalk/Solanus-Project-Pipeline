"""extract_corpus.py — pull Father Solanus Casey's OWN prose out of the archive.

Reads pipeline_v3/step_6/documents.json (570 records), keeps the `letter` records he authored, and
writes one clean row per letter to data/solanus_corpus.jsonl:

    {id, date, recipient, sender_location, greeting, farewell, signature, body, words}

`body` is text_by_label.src_content (the letter itself, in his voice). Greeting/farewell/recipient/date
are carried as metadata for instruction-pair prompts and RAG filters. FREE, offline, deterministic.

    python extract_corpus.py            # writes data/solanus_corpus.jsonl
"""
from __future__ import annotations
import json
import re

import mimic_config as config


def _is_his(tbl: dict) -> bool:
    sig = " ".join(str(tbl.get(k) or "") for k in ("src_signature", "src_farewell")).lower()
    if any(h in sig for h in config.SIGNATURE_HINTS):
        return True
    return config.KEEP_UNSIGNED and not sig.strip()      # unsigned kept by default (his-letters volumes)


def extract() -> list[dict]:
    docs = json.loads(config.STEP6_DOCS.read_text())
    rows = []
    for d in docs:
        if d.get("type") != "letter":
            continue
        tbl = d.get("text_by_label") or {}
        body = (tbl.get("src_content") or "").strip()
        if len(body.split()) < config.MIN_LETTER_WORDS or not _is_his(tbl):
            continue
        rows.append({
            "id": d.get("id"),
            "date": tbl.get("src_date") or d.get("date"),
            "recipient": tbl.get("src_recipient") or d.get("recipient"),
            "sender_location": tbl.get("src_location_sender"),
            "recipient_location": tbl.get("src_location_recipient"),
            "greeting": tbl.get("src_greeting"),
            "farewell": tbl.get("src_farewell"),
            "signature": tbl.get("src_signature"),
            "body": re.sub(r"\s+", " ", body).strip(),
            "words": len(body.split()),
        })
    return rows


def main():
    rows = extract()
    with config.F_CORPUS.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    total = sum(r["words"] for r in rows)
    dated = sum(1 for r in rows if r["date"])
    print(f"extracted {len(rows)} letters by Solanus, {total:,} words (~{total/1000:.0f}k) "
          f"-> {config.F_CORPUS.name}")
    print(f"  with date: {dated} | with recipient: {sum(1 for r in rows if r['recipient'])}")
    print(f"  sample recipients: {[r['recipient'] for r in rows[:4]]}")


if __name__ == "__main__":
    main()
