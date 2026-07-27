#!/usr/bin/env python3
"""step_7/run.py — the stage DAG + CLI for the archival-tool build pipeline.

This file DECLARES the pipeline (stages, dependencies, inputs, outputs) and provides the
diff-and-rerun CLI. Stage bodies live in `stages/` and are filled in per milestone; until then
they raise NotImplementedError, so the DAG is fully inspectable but nothing executes.

    python run.py status                 # show the diff (what's stale and why)  [read-only]
    python run.py run                    # run all stale stages + everything downstream
    python run.py run --only embed_corpus
    python run.py run --dry              # report what WOULD run, execute nothing
    python run.py graph                  # print the dependency DAG

Nothing runs on import. David runs stages explicitly when ready.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parent
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))

import config                                   # noqa: E402
from lib.pipeline import Pipeline, Stage        # noqa: E402
# Every stage below is a REAL, implemented module. The DAG wires each stage's FREE/default path so a
# blanket `run` never spends money or downloads heavy models; the paid/heavy passes (full LLM NER,
# contextualization, ColPali/RAPTOR builds) are opt-in via the stage's own flags, run explicitly.
from stages import embed_corpus                 # noqa: E402
from stages import normalize_dates              # noqa: E402  (Tier-1 A2)
from stages import stitch_notebooks             # noqa: E402  (Tier-1 A1 — cross-page continuation)
from stages import extract_entities             # noqa: E402  (Tier-1 NER)
from stages import resolve_entities             # noqa: E402  (Tier-1 A3 — seed clustering)
from stages import enrich_entities              # noqa: E402  (legacy; superseded by entity_engine, kept for rollback)
from stages import entity_engine                # noqa: E402  (context-aware iterative entity engine — REPLACES resolve-final+enrich)
from stages import geocode_places               # noqa: E402  (place -> lat/lon for the map; free Nominatim, rate-limited)
from stages import name_authority                # noqa: E402  (persons/places -> Wikidata QID + VIAF; free/keyless, rate-limited)
from stages import build_graph                  # noqa: E402  (Tier-2 KG)
from stages import build_kinship                 # noqa: E402  (FREE post-pass: Casey-node merge + FAMILY edges)
from stages import reapply_layers                # noqa: E402  (FREE post-pass: re-apply verdicts + descriptions from side-cars)
from stages import graph_analysis               # noqa: E402  (Louvain communities + Mapper overview)
from stages import index_sources                # noqa: E402  (Tier-2/4 FTS + IIIF + searchable PDFs)
from stages import contextualize_chunks         # noqa: E402  (Tier-0 contextual retrieval — PAID)
from stages import colpali_index                # noqa: E402  (Tier-3 multimodal — scaffold)
from stages import raptor                       # noqa: E402  (Tier-3 hierarchical summaries — scaffold)

DOCS = str(config.DOCUMENTS)
NB = str(config.NOTEBOOKS)
ENRICHED = str(config.ENRICHED / "**" / "*.json")
DATA = config.DATA


def build_pipeline() -> Pipeline:
    p = Pipeline(config_fingerprint=config.fingerprint())
    ST = lambda name: str(STEP7 / "stages" / f"{name}.py")   # a stage's own source = an input (code changes -> stale)

    # --- Tier-1 A1: cross-page continuation (notebook entries that span page breaks) ---
    p.add(Stage("stitch_notebooks", stitch_notebooks.run,
                inputs=[NB, ST("stitch_notebooks")],
                outputs=[str(DATA / "stitched_notebooks.json")],
                note="A1: detect '... Cont.' / truncation across page breaks -> logical multi-page entries; "
                     "free rule path (gated LLM tie-breaker for the ambiguous band)"))

    # --- Tier-1 A2: structural truth — date reconstruction ---
    p.add(Stage("normalize_dates", normalize_dates.run,
                inputs=[DOCS, NB, ST("normalize_dates")],
                outputs=[str(DATA / "dates.json")],
                note="A2: combine entry month/day + page archival year + carry-forward -> EDTF; "
                     "bitemporal enrolled/reported pairs; free rule path (gated LLM normalizer)"))

    # --- Tier-1 NER -> Tier-1 A3 resolution -> Tier-2 knowledge graph ---
    # extract_entities.run() defaults to execute=False: the FREE structured-field harvest only. The paid
    # LLM NER pass is opt-in (`run(execute=True)`), so a blanket `run` stays free.
    p.add(Stage("extract_entities", extract_entities.run,
                inputs=[DOCS, NB, ST("extract_entities")],
                outputs=[str(DATA / "entities_raw.jsonl")],
                note="NER: structured-field harvest (free) + opt-in LLM extraction; "
                     "every mention cites doc/rid/page/vertices"))
    p.add(Stage("resolve_entities", resolve_entities.run,
                deps=["extract_entities"],
                inputs=[str(DATA / "entities_raw.jsonl"), ST("resolve_entities")],
                outputs=[str(DATA / "entities.json")],
                note="normalize + block + cluster (free); opt-in embedding/LLM clustering + authority "
                     "reconciliation (Wikidata/VIAF/GeoNames/Getty TGN)"))
    # entity_engine REPLACES resolve's final clustering + all of enrich_entities: a context-aware,
    # iterative engine that reads each entity in its source context, disambiguates it, and merges. PAID
    # (one LLM "think" per entity), so the DAG runs its FREE dry-run (scope + cost projection); the real
    # pass is opt-in (`entity_engine.run(execute=True)`). It seeds from resolve's entities.json and emits
    # the canonical store + a gated entities_enriched.json view (build_graph + geocode read these).
    p.add(Stage("entity_engine", lambda: entity_engine.run(execute=False),
                deps=["resolve_entities"],
                inputs=[str(DATA / "entities.json"), ST("entity_engine"), ST("enrich_entities")],
                outputs=[str(DATA / "entity_store.json"), str(DATA / "entities_enriched.json")],
                note="context-aware understanding + identity merge -> central store; incremental "
                     "(content-hash), rollback-safe, loop-capable (PAID; DAG previews scope, pass opt-in)"))
    # geocode is a free (Nominatim) but network-bound, RATE-LIMITED (1 req/s) + resumable side-effect.
    p.add(Stage("geocode_places", lambda: geocode_places.run(min_mentions=1),
                deps=["entity_engine"],
                inputs=[str(DATA / "entity_store.json"), str(DATA / "entities_enriched.json"), ST("geocode_places")],
                outputs=[str(DATA / "geocodes.json")],
                note="resolve mappable place locations -> lat/lon for the map (cached + resumable)"))
    # TOC / Wikidata+VIAF name authority (walkthrough TODO #2): link persons/places to global IDs.
    # FREE + keyless (Wikidata API + VIAF AutoSuggest), rate-limited + cached/resumable. NON-DESTRUCTIVE:
    # writes a side-car (data/name_authority.json) keyed by entity id; entity_store.json is NOT mutated.
    # Persons gated on Wikidata notability (sitelinks); institution places gated on location corroboration
    # (geocode coords) so accidental namesakes / wrong-city matches stay candidates, not assertions.
    p.add(Stage("name_authority", lambda: name_authority.run(min_mentions=2),
                deps=["entity_engine", "geocode_places"],
                inputs=[str(DATA / "entity_store.json"), str(DATA / "geocodes.json"), DOCS,
                        ST("name_authority")],
                outputs=[str(DATA / "name_authority.json")],
                note="persons/places -> Wikidata QID + VIAF (free/keyless, rate-limited, cached); curated "
                     "TOC recipient spellings cross-checked; non-destructive side-car (store untouched)"))
    p.add(Stage("build_graph", build_graph.run,
                deps=["entity_engine", "normalize_dates", "geocode_places"],
                inputs=[str(DATA / "entity_store.json"), str(DATA / "dates.json"),
                        str(DATA / "entities_enriched.json"), str(DATA / "geocodes.json"),
                        DOCS, NB, ST("build_graph")],
                outputs=[str(DATA / "graph.json"), str(DATA / "graph.ttl")],
                note="temporal knowledge graph aligned to RiC-O; reads the engine store (drops noise, "
                     "trusts engine kinds); JSON + RDF/TTL exports"))
    # FREE post-passes that used to run OUTSIDE the DAG (so a build_graph rerun silently erased them).
    # Now registered downstream so every rebuild re-layers automatically. Neither declares graph.json as an
    # INPUT (only build_graph does) — they mutate it in place, keyed off the build_graph dep for ordering.
    p.add(Stage("build_kinship", build_kinship.main,
                deps=["build_graph"],
                inputs=[str(DATA / "kinship_merges.json"), str(DATA / "solanus_family.json"),
                        ST("build_kinship")],
                outputs=[str(DATA / "graph.json")],
                note="FREE: collapse fragmented Casey nodes + add Solanus--FAMILY-->relative edges "
                     "(from solanus_family.json + kinship_merges.json); in-place on graph.json"))
    p.add(Stage("reapply_layers", reapply_layers.run,
                deps=["build_kinship"],
                inputs=[str(DATA / "connection_verdicts.json"), str(DATA / "entity_descriptions.json"),
                        ST("reapply_layers"), ST("verify_connections")],
                outputs=[str(DATA / "graph.json")],
                note="FREE: re-apply the PAID connection verdicts + entity descriptions from their side-cars "
                     "onto graph.json (no model calls), so a rebuild never loses the paid layers"))
    # structure summaries of the graph: Louvain communities + a Mapper-style overview (free, local).
    # Runs LAST, on the fully re-layered graph, so communities reflect the merged/verified graph.
    p.add(Stage("graph_analysis", graph_analysis.run,
                deps=["reapply_layers"],
                inputs=[str(DATA / "graph.json"), ST("graph_analysis")],
                outputs=[str(DATA / "communities.json"), str(DATA / "mapper.json")],
                note="Louvain communities (colour-by-community + cluster browsing) + Mapper-style "
                     "community-quotient overview minimap of the whole graph"))

    # --- M1/M2: retrieval surfaces ---
    p.add(Stage("embed_corpus", embed_corpus.run,
                inputs=[DOCS, NB, ST("embed_corpus"),
                        str(STEP7 / "lib" / "providers" / "embed.py"), str(STEP7 / "lib" / "chunks.py")],
                outputs=[str(DATA / "vectors")],
                note="chunk -> multi-space embeddings (one partition per provider/model/dim)"))
    p.add(Stage("index_sources", index_sources.run,
                inputs=[DOCS, NB, ENRICHED, ST("index_sources")],
                outputs=[str(DATA / "fts.sqlite"), str(config.SEARCHABLE_PDFS)],
                note="sqlite FTS over page text + IIIF manifests + text-layer searchable PDFs"))

    # --- Tier-0 / Tier-3 enrichments — PAID or HEAVY, so wired to their SAFE/default path here ---
    # contextualize_chunks: full run is paid (per-chunk LLM context blurb). The DAG node runs the FREE
    # dry-run PLAN (counts + cost estimate, no calls); the real pass is `contextualize_chunks.run()`.
    p.add(Stage("contextualize_chunks", lambda: contextualize_chunks.run(dry_run=True),
                inputs=[DOCS, NB, ST("contextualize_chunks"), str(STEP7 / "lib" / "chunks.py")],
                outputs=[str(DATA / "contextualized_chunks.jsonl")],
                note="Anthropic Contextual Retrieval: prepend an LLM-written situating blurb per chunk "
                     "before embedding (PAID). DAG runs the free dry-run plan; real pass is opt-in"))
    # ColPali (vision retrieval) + RAPTOR (hierarchical summaries): heavy model download / paid summaries.
    # The DAG runs each scaffold's plan-only path (no build); the real build is opt-in via its flags.
    p.add(Stage("colpali_index", lambda: colpali_index.run(do_build=False),
                inputs=[ST("colpali_index")],
                outputs=[str(DATA / "colpali")],
                note="Tier-3 multimodal: page-image (visual) retrieval via ColPali. Scaffold; "
                     "real index build is opt-in (`do_build=True`, heavy model download)"))
    p.add(Stage("raptor", lambda: raptor.run(execute=False),
                deps=["embed_corpus"],
                inputs=[ST("raptor")],
                outputs=[str(DATA / "raptor_tree.json")],
                note="Tier-3 RAPTOR: recursive cluster->summarize tree over chunks for multi-hop recall. "
                     "Scaffold; real build is opt-in (`execute=True`, paid summaries)"))
    return p


def main():
    ap = argparse.ArgumentParser(description="step_7 build pipeline (diff-and-rerun)")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("status")
    sub.add_parser("graph")
    sub.add_parser("seed")
    r = sub.add_parser("run")
    r.add_argument("--only", nargs="*", default=None)
    r.add_argument("--dry", action="store_true")
    r.add_argument("--force", action="store_true")
    a = ap.parse_args()
    p = build_pipeline()

    if a.cmd == "graph" or a.cmd is None:
        print("DAG (stage <- deps):")
        for name in p.topo():
            st = p.stages[name]
            print(f"  {name:18} <- {st.deps or '(root)'}\n      {st.note}")
        if a.cmd is None:
            print("\n(use `status` to see staleness, `run` to execute)")
        return
    if a.cmd == "status":
        for name, info in p.status().items():
            tag = "STALE" if info["stale"] else "fresh"
            why = (" — " + "; ".join(info["reasons"])) if info["reasons"] else ""
            print(f"  [{tag:5}] {name}{why}")
        return
    if a.cmd == "seed":
        seeded = p.seed()
        print(f"seeded {len(seeded)} stages as up-to-date (outputs present): {', '.join(seeded)}")
        print("A subsequent `run` now executes only stages whose inputs/code change or whose outputs are "
              "missing — it will NOT rebuild everything just because the state file was absent.")
        return
    if a.cmd == "run":
        ran = p.run(only=a.only, dry=a.dry, force=a.force)
        print(("WOULD run: " if a.dry else "ran: ") + (", ".join(ran) if ran else "(nothing stale)"))


if __name__ == "__main__":
    main()
