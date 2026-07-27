"""stages/ingest_book.py — chunk + index an external book about Solanus for retrieval/enrichment.

David supplied Crosby's *Thank God Ahead of Time* (a public biography) as a PDF. We extract its text,
chunk it with a simple sliding word-window (the "dumb auto-chunk" — robust + good enough for prose),
de-hyphenate the OCR/justification line breaks, and embed it into its OWN vector partition
(``book-tgat@<dim>``) so it never mixes with the archive corpus. A toggleable ``book_search`` tool
(app/tools/book_search.py) then lets the agent consult the biography, and answers can cite it by page.

    from stages import ingest_book as b
    b.run()                      # extract + chunk (free); also embeds (gemini embed ~ a couple cents)
    b.run(embed=False)           # just chunk + write book_chunks.jsonl

Non-destructive: writes data/book_chunks.jsonl + data/vectors/book-tgat@1536/. Cost-logged.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config              # noqa: E402
from lib import textclean  # noqa: E402

BOOK_PDF = config.REPO / "Thank-God-Ahead-of-Time.pdf"
OUT_PATH = config.DATA / "book_chunks.jsonl"
SPACE = "book-tgat"                                         # vector-store partition prefix
WORDS_PER_CHUNK = 280
OVERLAP_WORDS = 45
BOOK_META = {"book_id": "tgat", "title": "Thank God Ahead of Time",
             "author": "Michael Crosby", "subject": "Father Solanus Casey"}


def _extract_pages(pdf: Path) -> list:
    """[(page_number, text)] — page_number is 1-based for human-facing citations."""
    try:
        from pypdf import PdfReader
    except Exception:
        from PyPDF2 import PdfReader
    reader = PdfReader(str(pdf))
    return [(i + 1, textclean.clean((p.extract_text() or "").strip())) for i, p in enumerate(reader.pages)]


def _chunk(pages: list) -> list:
    """Sliding word-window chunks that carry the page span they came from (for citations)."""
    # flatten to (word, page) so a chunk can report the page range it covers
    words = []
    for pg, txt in pages:
        for w in txt.split():
            words.append((w, pg))
    chunks, i, n = [], 0, len(words)
    step = WORDS_PER_CHUNK - OVERLAP_WORDS
    while i < n:
        window = words[i:i + WORDS_PER_CHUNK]
        if not window:
            break
        text = " ".join(w for w, _ in window)
        pgs = [p for _, p in window]
        if len(text) > 40:                                 # skip front-matter scraps
            chunks.append({"id": f"tgat::w{i}", "kind": "book", "text": text,
                           "meta": {**BOOK_META, "page_start": pgs[0], "page_end": pgs[-1],
                                    "page": pgs[len(pgs) // 2]}})
        i += step
    return chunks


def run(embed: bool = True, dim: int = 1536, model: str = "gemini-embedding-001") -> dict:
    if not BOOK_PDF.exists():
        raise FileNotFoundError(f"{BOOK_PDF} not found (place the book PDF at the repo root)")
    config.DATA.mkdir(parents=True, exist_ok=True)
    pages = _extract_pages(BOOK_PDF)
    chunks = _chunk(pages)
    with open(OUT_PATH, "w") as f:
        for c in chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"ingest_book: {len(pages)} pages -> {len(chunks)} chunks -> {OUT_PATH.name}")

    if embed:
        from lib.providers import embed as emb
        from lib import vectorstore
        texts = [c["text"] for c in chunks]
        ids = [c["id"] for c in chunks]
        metas = [{**c["meta"], "kind": c["kind"], "text": c["text"][:600]} for c in chunks]
        vecs, _ = emb.embed_texts(texts, model, dim)
        space = f"{SPACE}@{dim}"
        vectorstore.write(space, ids, vecs, metas)
        print(f"ingest_book: embedded {len(texts)} chunks -> {space} (dim {vecs.shape[1]})")
        return {"pages": len(pages), "chunks": len(chunks), "space": space, "embedded": True}
    return {"pages": len(pages), "chunks": len(chunks), "embedded": False}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-embed", action="store_true", help="chunk only (no embedding spend)")
    a = ap.parse_args()
    run(embed=not a.no_embed)
