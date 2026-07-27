"""stages/contextualize_chunks.py — Anthropic-style Contextual Retrieval (the highest-leverage upgrade).

The single biggest retrieval problem in this corpus is that a chunk, on its own, is *context-starved*.
A notebook entry might read in full:

    "22  Cured."

Embed that, and a search for "cancer cure reported November 1933" will sail right past it — the
chunk shares almost no words or meaning with the query even though it is exactly the answer. The
date, the notebook, the page, the favor being reported all live *around* the entry (on the page, in
the running register), not *in* it. Dense vectors and BM25 both only see the words that are present,
so terse entries become nearly invisible.

Anthropic's **Contextual Retrieval** fixes this directly: before we embed (and before we build the
BM25 index), we ask an LLM to write a short 1-2 sentence situating preamble for each chunk —

    "This is a Nov 1933 entry in Fr. Solanus Notebook No. 6 reporting a favor: a cancer condition
     described as cured through the Seraphic Mass Association."

— and we PREPEND that to the chunk text. Now the same vector/BM25 index carries the date, the
notebook, and the topic, so the query and the chunk finally meet in the same neighborhood.
Anthropic reports this cuts retrieval failures by up to ~67% when combined with contextual BM25 and
a reranker. For our register-style notebooks — short, date-less, continuation-laden — it is the
most direct win available, and it reuses *exactly* the structural context that the stitch/date
stages (A1/A2) recover elsewhere in the plan.

What this stage produces (NON-DESTRUCTIVE — a new artifact only):
    data/contextualized_chunks.jsonl   # one row per chunk: {id, kind, context, text, meta}

Downstream, embed_corpus.py (and a future BM25 stage) embeds `context + "\n\n" + text` instead of
`text` — the chunk id and provenance meta are untouched, so citations still point at the exact
region. The raw `text` is preserved alongside `context` so we can always show the original and
re-generate context if we change the prompt.

⚠️ COST: this is a paid LLM pass over the corpus (~9.3k chunks). It is wired but NOT run here —
`run(limit=None)` is ready to go the moment David says so. Two cost levers are built in:

  - **Prompt caching of the shared instruction.** The long, identical task instruction is sent once
    as a cached system prefix; per-chunk we only pay for the (short) chunk + its page context. On
    Gemini this is implicit context caching (repeated prefixes are auto-discounted) — see the cache
    note in `_build_system()` below.
  - **Batch API for the full run.** The full pass should go through the provider's async Batch API
    (~-50% vs interactive) since contextualization is an offline, embarrassingly-parallel job with
    no latency requirement. See the `# TODO(batch)` block in `run()`.

    python stages/contextualize_chunks.py --limit 12 --dry-run   # build prompts, NO API calls
    python stages/contextualize_chunks.py --limit 12             # tiny billed smoke test
    python stages/contextualize_chunks.py                        # FULL paid pass (only when told)
"""
from __future__ import annotations

# ------------------------------------------------------------------ Core Python Imports
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

# ------------------------------------------------------------------ Local File Imports
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                         # noqa: E402  (paths + model registry)
from lib import chunks as chunks_lib  # noqa: E402  (the canonical chunk builder — same data shapes)
from lib import costlog               # noqa: E402  (every model call is priced)
from lib.providers import llm         # noqa: E402  (model-agnostic generation adapter)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("contextualize")

# The output artifact. New file only — we never touch documents.json / notebooks.json / chunks.
OUT_PATH = config.DATA / "contextualized_chunks.jsonl"


# ==================================================================
# The shared instruction (cached once) — this is the "situate it" prompt
# ==================================================================
# This block of text is IDENTICAL for every chunk, which is precisely why it belongs in the cache:
# we pay to process it once, then reuse it across all ~9.3k calls. Keep it stable — changing a
# single character invalidates the cache and re-bills the prefix. (That's also why the per-chunk
# material is injected separately in _build_prompt, never spliced into this constant.)
SYSTEM_INSTRUCTION = """\
You are an archival assistant indexing the personal correspondence and prayer-favor notebooks of
Fr. Solanus Casey (1870-1957), a Capuchin friar. Your job is to write a SHORT situating context
for a single chunk so a search engine can find it later.

You will be given:
  - DOCUMENT CONTEXT: where this chunk sits (notebook or letter, date, page, neighbours).
  - CHUNK: the exact text to be embedded.

Write 1-2 plain sentences that situate the CHUNK within the whole document, surfacing facts that the
chunk's own words omit but a searcher would use: the document type (letter vs notebook favor entry),
the notebook name and/or recipient, the year/month/day (combine the entry's day with the page's
archival year when the entry alone is partial), the place if named, and the topic in a few words
(e.g. the medical/spiritual condition and the reported outcome — "enrolled", "cured", "improved").

Rules:
  - Output ONLY the situating context. No preamble, no quotes, no "This chunk...". Just the sentences.
  - Be specific and faithful. Use ONLY facts present in the inputs; never invent a date, name, or outcome.
  - If a fact is genuinely unknown, omit it rather than guess.
  - Keep it to roughly 15-50 words. This text is prepended to the chunk before embedding, so density
    of searchable facts matters more than prose."""


# ==================================================================
# Harvesting the page/letter context that a bare chunk can't see
# ==================================================================
# chunks_lib.build_chunks() is our single source of truth for *what* gets indexed, but its `meta`
# is deliberately lean. The situating signals we want for notebooks — the page's archival year
# (text_by_label.archv_date / src_date), the entry's page_label ("Page 178 Cont."), and the
# neighbouring entries on the same page — live in the raw step_6 records. We load those once and key
# them by doc_id so each chunk can borrow its surrounding context cheaply, WITHOUT changing chunks.py
# (non-destructive: we only read).
def _load_source_index() -> tuple[dict, dict]:
    """Index the raw step_6 sources by id for fast context lookup.

    Returns:
        (letters_by_id, pages_by_id): two dicts mapping the source `id` to its full raw record.
            Letters come from documents.json; notebook pages from notebooks.json. We keep the whole
            record so _document_context can pull whatever situating fields it needs.
    """
    letters_by_id = {d["id"]: d for d in json.loads(config.DOCUMENTS.read_text())}
    pages_by_id = {p["id"]: p for p in json.loads(config.NOTEBOOKS.read_text())}
    return letters_by_id, pages_by_id


def _page_year(page: dict) -> str:
    """Best-effort archival year/month for a notebook page.

    The year for a notebook entry usually isn't on the entry — it's on the PAGE, in the archival
    metadata (`archv_date`, e.g. "1933, October") or failing that the page's `src_date` header. The
    LLM needs this to turn an entry's bare "Mar. 30" into a real date, so we surface it explicitly.

    Args:
        page: a raw notebook page record from notebooks.json.

    Returns:
        A short year/month string ("1933, October") or "" if the page carries no archival date.
    """
    tbl = page.get("text_by_label", {}) or {}
    # archv_date is the curated archival date for the page; src_date is the page's own date header.
    return (tbl.get("archv_date") or tbl.get("src_date") or "").strip()


def _neighbours(page: dict, rid: str, window: int = 1) -> str:
    """A tiny excerpt of the entries immediately above/below this one on the same page.

    Continuation is rampant in these registers ("Page N Cont."), so an entry's real subject often
    lives in its neighbour. We hand the LLM a small window (1 before, 1 after by default) so it can
    situate a fragment that, alone, says only "22  Cured." Kept short on purpose — neighbours are a
    hint, not the payload, and we don't want to inflate the per-chunk (uncached) token bill.

    Args:
        page: the raw notebook page record holding `entries`.
        rid:  the region id of the target entry (so we can find its position).
        window: how many entries on each side to include.

    Returns:
        A newline-joined "(prev) ...text... / (next) ...text..." snippet, or "" if none.
    """
    entries = page.get("entries", []) or []
    idx = next((i for i, e in enumerate(entries) if e.get("rid") == rid), None)
    if idx is None:
        return ""
    bits = []
    for j in range(max(0, idx - window), min(len(entries), idx + window + 1)):
        if j == idx:
            continue
        snippet = (entries[j].get("text") or "").strip().replace("\n", " ")
        if snippet:
            tag = "prev" if j < idx else "next"
            bits.append(f"({tag}) {snippet[:160]}")
    return "\n".join(bits)


def _document_context(chunk: dict, letters_by_id: dict, pages_by_id: dict) -> str:
    """Build the DOCUMENT CONTEXT block handed to the LLM alongside the chunk.

    This is the situating scaffold — the surrounding facts the chunk text itself lacks. We assemble
    it from the cheap, already-known structured fields (so the LLM mostly *phrases* known facts
    rather than inventing them), which keeps the task reliable and faithful.

    Args:
        chunk: one dict from chunks_lib.build_chunks ({id, kind, text, meta}).
        letters_by_id: index from _load_source_index.
        pages_by_id: index from _load_source_index.

    Returns:
        A short labelled, newline-separated context block (a string), or a minimal fallback if the
        source record can't be found (so the stage never crashes on an unexpected id).
    """
    meta = chunk.get("meta", {})
    lines = []
    if chunk["kind"] == "letter":
        # ----- Letters already separate recipient / date / place — harvest them directly.
        d = letters_by_id.get(meta.get("doc_id"), {})
        lines.append("Document type: a letter in the Solanus Casey correspondence.")
        if d.get("recipient") or meta.get("recipient"):
            lines.append(f"Recipient: {d.get('recipient') or meta.get('recipient')}")
        if d.get("date") or meta.get("date"):
            lines.append(f"Date: {d.get('date') or meta.get('date')}")
        tbl = d.get("text_by_label", {}) or {}
        if tbl.get("src_location_sender"):
            lines.append(f"Sent from: {tbl['src_location_sender']}")
        if tbl.get("src_location_recipient"):
            lines.append(f"Sent to (place): {tbl['src_location_recipient']}")
        if meta.get("page") is not None:
            lines.append(f"Page in volume: {meta.get('page')}")
    else:
        # ----- Notebook favor entry: the rich context lives on the PAGE, not the entry.
        page = pages_by_id.get(meta.get("doc_id"), {})
        lines.append("Document type: a prayer-favor entry in a Fr. Solanus notebook register.")
        if meta.get("notebook"):
            lines.append(f"Notebook: {meta['notebook']}")
        year = _page_year(page)
        if year:
            lines.append(f"Page archival date (year/month for entries on this page): {year}")
        if meta.get("date"):
            lines.append(f"Entry date as written (may be partial — combine with the page year): {meta['date']}")
        # page_label carries continuation signals like "Page 178 Cont."
        rid = meta.get("rid")
        entry = next((e for e in page.get("entries", []) if e.get("rid") == rid), {})
        if entry.get("page_label"):
            lines.append(f"Page label: {entry['page_label']}")
        nb = _neighbours(page, rid)
        if nb:
            lines.append(f"Neighbouring entries on the same page (for continuation context only):\n{nb}")
    return "\n".join(lines) if lines else "(no additional document context available)"


# ==================================================================
# Per-chunk prompt assembly
# ==================================================================
def _build_system() -> str:
    """The cached system instruction.

    Returns:
        The shared SYSTEM_INSTRUCTION string.

    Note on caching: the llm.generate adapter passes this through Gemini's `system_instruction`. On
    Gemini, *implicit* context caching automatically discounts repeated leading content (our
    instruction is byte-identical every call), so we get the caching win without managing a cache
    handle. If/when we move to explicit caching (`client.caches.create(...)`) or to Claude
    (`cache_control: {"type": "ephemeral"}` on this block), this is the exact span to cache — it is
    the large, invariant prefix; everything chunk-specific is kept out of it on purpose.
    """
    return SYSTEM_INSTRUCTION


def _build_prompt(chunk: dict, doc_context: str) -> str:
    """Assemble the per-chunk user prompt (the part that legitimately varies).

    Args:
        chunk: one chunk dict.
        doc_context: the situating block from _document_context.

    Returns:
        The user-turn prompt string. Deliberately small: only this is uncached/re-billed per chunk,
        so we keep it tight (context block + the chunk text + a one-line ask).
    """
    return (
        "DOCUMENT CONTEXT:\n"
        f"{doc_context}\n\n"
        "CHUNK (the exact text to be embedded):\n"
        f"\"\"\"\n{chunk['text']}\n\"\"\"\n\n"
        "Write the 1-2 sentence situating context now."
    )


# ==================================================================
# The stage entry point
# ==================================================================
def run(limit: int | None = None, model: str | None = None, dry_run: bool = False) -> Path:
    """Generate situating context for each chunk and write data/contextualized_chunks.jsonl.

    Args:
        limit: cap the number of chunks (a strided sample across letters + notebook entries, via
            chunks_lib.build_chunks). None = the FULL corpus (the paid pass — only when David says go).
        model: which LLM to use; defaults to config.DEFAULTS["llm"] (a cheap Gemini Flash variant —
            contextualization is a small, well-scoped task, so the lite/flash tier is the right fit).
        dry_run: if True, build every prompt and write rows with context="" but make ZERO API calls.
            This lets us inspect the prompts and project token cost before spending a cent.

    Returns:
        The path to the written JSONL artifact.

    Side effects:
        Writes OUT_PATH (non-destructive — a new file). Every real LLM call is cost-logged inside
        lib.providers.llm.generate, so costs/usage.csv reflects this pass automatically.
    """
    model = model or config.DEFAULTS["llm"]
    config.DATA.mkdir(parents=True, exist_ok=True)

    # ----- Gather inputs: the chunks to contextualize + the raw sources to situate them against.
    cs = chunks_lib.build_chunks(limit=limit)
    letters_by_id, pages_by_id = _load_source_index()
    system = _build_system()
    log.info("contextualizing %d chunk(s) with model=%s%s", len(cs), model,
             "  [DRY RUN — no API calls]" if dry_run else "")

    # ----- TODO(batch): for the FULL run, route through the provider's async Batch API (~-50%).
    #   Contextualization is offline and embarrassingly parallel, so latency doesn't matter and
    #   batch is strictly cheaper. Sketch:
    #       1. emit one request per chunk (system=cached instruction, user=_build_prompt(...))
    #          into a JSONL batch file, each tagged with the chunk id;
    #       2. submit via the Batch API and poll (it returns within ~24h);
    #       3. join results back to chunk ids, cost-log the aggregate usage, write the same JSONL.
    #   We keep the interactive path below as the reference + smoke-test implementation. The output
    #   schema is identical either way, so downstream stages don't care which path produced it.

    t0 = time.time()

    # ----- DRY RUN: build every prompt, write empty-context rows, spend nothing. -----
    if dry_run:
        rows = [{"id": c["id"], "kind": c["kind"], "context": "",
                 "text": c["text"], "meta": c["meta"]} for c in cs]
        with open(OUT_PATH, "w") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        log.info("wrote %d row(s) -> %s", len(rows), OUT_PATH)
        _print_summary(rows, model, dry_run)
        return OUT_PATH

    # ----- REAL paid pass: RESUME + PARALLEL + INCREMENTAL write. -----
    # Resume: prior rows with a NON-empty context are real results we keep (a committed dry-run file
    # has only empty contexts, so it never blocks a fresh real pass).
    rows_done = {}
    if OUT_PATH.exists():
        for line in OUT_PATH.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("context"):
                rows_done[r["id"]] = r

    todo = [c for c in cs if c["id"] not in rows_done]

    # Each chunk is an independent ~9s LLM round-trip, so we overlap them with a thread pool (same
    # rationale + safety model as the NER stage: workers only CALL the model + shape rows; THIS loop is
    # the sole writer, draining as_completed, so writes stay append-only and a crash leaves a valid
    # prefix to resume from). The shared system instruction is prompt-cached by the provider.
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _work(c):
        prompt = _build_prompt(c, _document_context(c, letters_by_id, pages_by_id))
        context, _usage = llm.generate(prompt, model=model, system=system, temperature=0.0)
        return {"id": c["id"], "kind": c["kind"], "context": context.strip(),
                "text": c["text"], "meta": c["meta"]}

    max_workers = max(1, int(os.environ.get("CTX_WORKERS", "16")))
    log.info("  %d chunk(s) to do (resume skipped %d) on %d workers",
             len(todo), len(rows_done), max_workers)

    rows = list(rows_done.values())                          # for the end-of-run summary
    written = 0
    with open(OUT_PATH, "w") as f:
        for r in rows:                                       # re-emit prior real results first
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        f.flush()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_work, c): c for c in todo}
            for fut in as_completed(futs):
                try:
                    r = fut.result()
                except Exception as e:                       # one chunk failing must not kill the run
                    log.warning("  chunk %s failed: %s", futs[fut].get("id", "?"), str(e)[:140])
                    continue
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                rows.append(r)
                written += 1
                if written % 200 == 0:                       # periodic flush + heartbeat
                    f.flush()
                    log.info("  %d/%d done (%.1fs elapsed)", written, len(todo), time.time() - t0)
    log.info("wrote %d new + %d resumed = %d row(s) -> %s",
             written, len(rows_done), len(rows), OUT_PATH)

    _print_summary(rows, model, dry_run)
    return OUT_PATH


def _print_summary(rows: list, model: str, dry_run: bool) -> None:
    """Print the end-of-run SUMMARY block (style-guide convention for runnable scripts).

    Args:
        rows: the rows just written.
        model: the model used.
        dry_run: whether this was a no-spend pass.
    """
    kinds = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print("=" * 60)
    print("CONTEXTUALIZE CHUNKS — SUMMARY")
    print("=" * 60)
    print(f"  model         : {model}")
    print(f"  mode          : {'DRY RUN (no API calls)' if dry_run else 'billed'}")
    print(f"  chunks        : {len(rows)}  {kinds}")
    print(f"  output        : {OUT_PATH}")
    if not dry_run:
        # Pull this pass's spend from the always-on cost log so the number is real, not guessed.
        agg = costlog.summary().get(model, {})
        if agg:
            print(f"  cost so far   : ${agg.get('usd', 0.0):.4f}  "
                  f"({agg.get('input_tokens', 0)} in / {agg.get('output_tokens', 0)} out tokens, "
                  f"{agg.get('calls', 0)} calls — cumulative for this model)")
    else:
        print("  next          : drop --dry-run for a tiny billed smoke test, then run with no")
        print("                  --limit for the full pass (prefer the Batch API — see TODO in run()).")
    print("=" * 60)


# ==================================================================
# CLI
# ==================================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None,
                    help="cap chunk count (strided sample). Omit for the FULL paid pass.")
    ap.add_argument("--model", type=str, default=None,
                    help=f"LLM id from config.LLMS (default: {config.DEFAULTS['llm']})")
    ap.add_argument("--dry-run", action="store_true",
                    help="build prompts and write rows with empty context — ZERO API calls.")
    a = ap.parse_args()
    run(limit=a.limit, model=a.model, dry_run=a.dry_run)
