"""stages/entity_engine.py — the context-aware, iterative entity engine (a central store).

This REPLACES the final clustering of resolve_entities + all of enrich_entities with a single
context-aware engine that, per entity, READS its source excerpts (what the document is about, what is
being discussed), reasons about WHAT IT ACTUALLY IS, researches/disambiguates it (e.g. "Colorado" in a
Detroit address is a STREET, not the state), and emits a canonical "disambiguated identity". A cheap,
deterministic merge then groups everything that resolves to the SAME identity — so variants the crude
resolver split get united and OCR noise gets dropped — and the loop can REPEAT (re-think the entities
whose mention set grew) until the store stabilizes.

Design contract (David's spec — see memory entity-engine-direction):
  • Central store: data/entity_store.json is the new source of truth (build_graph + geocode read it).
  • Non-destructive + rollback: never deletes; backs up the prior store to data/.backups/ with a stamp.
  • Reproducible + INCREMENTAL: a content hash per entity means a re-run (cleaner OCR / more docs) only
    re-thinks what changed; unchanged entities reuse their stored understanding (no spend).
  • Loop-capable but cheap: one converge pass by default; `passes`/`max_cost_usd` open the door to more.
  • Bulk model = gemini-2.5-flash-lite (validated to nail the hard cases at ~$6 for the whole corpus);
    `uncertain_model` (flash) can re-think low-confidence entities on later passes.
  • Cost-logged through lib.providers.llm; thread-pool + single writer + resume (the bulk-loop pattern).

Run:
    python stages/entity_engine.py --execute                 # one converge pass over all entities
    python stages/entity_engine.py --execute --passes 3 --max-cost 25
    python stages/entity_engine.py                           # dry-run (counts + cost estimate only)
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                                              # noqa: E402
from lib import costlog                                   # noqa: E402
from lib import textclean                                 # noqa: E402
from stages.enrich_entities import _build_text_index, _contexts_for  # noqa: E402  (reuse the context fetch)
from stages.name_authority import _load_toc_names, _clean_toc_name  # noqa: E402  (reuse curated TOC authority)

ENTITIES = config.DATA / "entities.json"                  # seed clustering (resolve output)
STORE_PATH = config.DATA / "entity_store.json"            # the new canonical store
THOUGHT_PATH = config.DATA / "entity_thought.json"        # pre-merge per-entity understanding (re-mergeable)
BACKUP_DIR = config.DATA / ".backups"
AUDIT_PATH = config.DATA / "entity_engine_audit.json"
TOC_AUDIT_PATH = config.DATA / "entity_engine_toc_audit.json"  # TOC-anchored canonicalization + merge audit
TOC_CLUSTERS_PATH = config.DATA / "entity_engine_toc_clusters.json"  # cached TOC same-person dedup decisions

BULK_MODEL = "gemini-2.5-flash-lite"                      # validated on the hard cases; ~$6 / full corpus
UNCERTAIN_MODEL = "gemini-2.5-flash"                      # escalation for low-confidence re-thinks
CHECKPOINT_EVERY = 1500          # checkpoint cadence; store JSON is large, so don't rewrite it too often

# entity_kind taxonomy the engine assigns (what the thing ACTUALLY is, regardless of NER's guess)
PLACE_KINDS = {"city", "town", "state", "country", "region", "neighborhood", "street",
               "institution", "building"}
# only these place kinds get a real map pin; a bare street/building maps to its PARENT city instead
MAPPABLE_PLACE_KINDS = {"city", "town", "state", "country", "region", "neighborhood"}


# ==================================================================
# The "think" call — one entity, read in context, disambiguated
# ==================================================================
_SYSTEM = (
    "You are the entity-understanding engine for the Father Solanus Casey archive (Detroit Capuchin "
    "friar 1870-1957; his correspondence + the Seraphic Mass Association (S.M.A.) prayer-favor "
    "notebooks; his friaries: St. Bonaventure (Detroit, Michigan), St. Felix (Huntington, Indiana), "
    "Sacred Heart (Yonkers, NY), Our Lady of Sorrows (Brooklyn, NY)). You read a candidate entity AS "
    "OCR'd plus the EXACT source excerpts where it appears, and decide WHAT IT ACTUALLY IS — grounded "
    "in the excerpts, using well-established world knowledge ONLY to disambiguate, never to invent "
    "biography. For obscure petitioners you cannot verify, stay grounded in the excerpts and use null. "
    "Return STRICT JSON.\n"
    "CRITICAL place rule: a bare ambiguous token (Colorado, Lincoln, Washington, Clinton, Jefferson, "
    "Lawton, Lafayette) is OFTEN a DETROIT STREET, not the US state/city — if the excerpts show a "
    "street/address context (Ave, St, Blvd, Rd, a house number) or a Detroit/Michigan context, classify "
    "it as a STREET with parent_place 'Detroit, Michigan, USA' and is_mappable_place=false. NEVER map a "
    "bare street as a state."
)


def _think_prompt(ent: dict, contexts: list) -> str:
    typ = ent.get("type", "?")
    variants = ", ".join(ent.get("variants", [])[:6])
    ctx = "\n".join(f"- {c}" for c in contexts) or "(no excerpt text available)"
    return (
        f'OCR type: {typ}\nName as OCR\'d: "{ent.get("canonical_name","")}"\n'
        f'Other spellings: {variants or "(none)"}\n\nSource excerpts where it appears:\n{ctx}\n\n'
        'Return JSON: {'
        '"is_real_entity": <bool; false for OCR noise / generic words like "enrolled","amount","favor">, '
        '"entity_kind": "<person|place|organization|condition|favor|outcome|role|event|date|religious_term|other|noise>", '
        '"what_it_is": "<one grounded sentence: who/what this specifically is>", '
        '"disambiguated_identity": "<canonical real-world identity for de-duplication, e.g. '
        '\'Father Solanus Casey (Capuchin friar)\', \'Colorado Avenue (street in Detroit, Michigan)\', '
        '\'cancer (medical condition)\'>", '
        '"canonical_name": "<clean, correctly-spelled display name; drop OCR noise>", '
        '"place_type": "<city|town|state|country|region|neighborhood|street|institution|building or null>", '
        '"is_mappable_place": <bool; true ONLY for a real, pin-able geographic place>, '
        '"location": "<the FULL mappable location as City, State/Province, Country, or null>", '
        '"parent_place": "<for a street/building/institution, the city it is in as City, State, Country; else null>", '
        '"role": "<role/title/kind, e.g. petitioner, priest, doctor, hospital; or null>", '
        '"relation_to_solanus": "<correspondent|petitioner|fellow_friar|family|benefactor|subject_of_favor|none|unknown>", '
        '"description": "<one grounded factual sentence, or null>", '
        '"confidence": <0.0-1.0>}'
    )


def _coerce(v):
    """Normalize the model's stringy nulls ('null','none','') to real None."""
    if isinstance(v, str) and v.strip().lower() in ("null", "none", "n/a", ""):
        return None
    return v


def think_one(ent: dict, contexts: list, model: str = BULK_MODEL) -> dict:
    """Return the structured 'understanding' for one entity (fails safe to a minimal record)."""
    from lib.providers import llm
    try:
        txt, _ = llm.generate(_think_prompt(ent, contexts), model=model, system=_SYSTEM,
                              json_mode=True, temperature=0.0)
        v = json.loads(txt)
        kind = (_coerce(v.get("entity_kind")) or "other").lower()
        is_place = (kind == "place")                       # entity_kind is the TOP level; sub-type is place_type
        ptype = _coerce(v.get("place_type"))
        mappable = bool(v.get("is_mappable_place")) and is_place
        return {
            "is_real_entity": bool(v.get("is_real_entity", True)),
            "entity_kind": kind,
            "what_it_is": _coerce(v.get("what_it_is")),
            "disambiguated_identity": _coerce(v.get("disambiguated_identity")),
            "canonical_name": (_coerce(v.get("canonical_name")) or ent.get("canonical_name") or "").strip(),
            "place_type": ptype,
            "is_mappable_place": mappable,
            "location": _coerce(v.get("location")),
            "parent_place": _coerce(v.get("parent_place")),
            "role": _coerce(v.get("role")),
            "relation_to_solanus": _coerce(v.get("relation_to_solanus")) or "unknown",
            "description": _coerce(v.get("description")),
            "confidence": float(v.get("confidence", 0.0) or 0.0),
            "model": model,
        }
    except Exception as e:                                  # never let one entity kill the run
        return {"is_real_entity": True, "entity_kind": (ent.get("type") or "other").lower(),
                "canonical_name": ent.get("canonical_name"), "disambiguated_identity": None,
                "confidence": 0.0, "relation_to_solanus": "unknown", "is_mappable_place": False,
                "error": str(e)[:120], "model": model}


# ==================================================================
# Incrementality — a content hash over the inputs that drive understanding
# ==================================================================
def _content_hash(ent: dict) -> str:
    mids = sorted(m.get("mention_id", "") for m in ent.get("mentions", []))
    basis = (ent.get("canonical_name", "") + "|" + (ent.get("type") or "") + "|" + "|".join(mids))
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


def _load_store() -> dict:
    if STORE_PATH.exists():
        try:
            return json.loads(STORE_PATH.read_text())
        except Exception:
            return {}
    return {}


# ==================================================================
# Merge — group entities that resolve to the SAME disambiguated identity
# ==================================================================
def _norm_identity(s: str | None) -> str:
    """Normalize a disambiguated identity for grouping: lowercase, drop the parenthetical gloss, then a
    leading 'the' + any STACKED leading honorific/saint titles so 'Saint Francis of Assisi' == 'Francis
    of Assisi', 'The Catholic Church' == 'Catholic Church', and 'Rev. Fr. Fardy' == 'Father Fardy'
    (audit-driven: a single strip left 'father fardy' != 'fardy'). GUARDS: keep >=1 name token, never
    strip Mr/Mrs/Ms/Miss (keeps spouse pairs distinct), and never strip a relational title across an
    'of' ('Mother of Edward Hickey')."""
    if not s:
        return ""
    import re
    s = re.sub(r"\(.*?\)", " ", s)                          # drop "(street in Detroit, Michigan)" gloss
    # FIRST collapse apostrophes/periods (NOT to spaces) so a contracted given name survives as ONE token:
    # "Edw'd"->"edwd", "Wm."->"wm", "Jn'o"->"jno", "Geo."->"geo" — then expand via _CONTRACTIONS below.
    s = re.sub(r"[’'.]", "", s.lower())
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    toks = re.sub(r"\s+", " ", s).strip().split()
    # deterministic 19th-c. contraction expansion (free, no LLM): "edwd"->"edward", "wm"->"william", so
    # the OCR'd contracted form groups with its expanded sibling. BEFORE the 'the'/honorific strip so the
    # expanded token (e.g. "william") is also counted by the >=2-name-token guard.
    toks = [_CONTRACTIONS.get(t, t) for t in toks]
    if toks and toks[0] == "the":
        toks = toks[1:]
    # strip STACKED leading honorifics (e.g. "V. Rev. Fr. Fardy" -> "fardy", matching "Father Fardy"),
    # keeping >=1 name token and never stripping a relational title across 'of'.
    while len(toks) >= 2 and toks[0] in _HONORIFICS and toks[1] != "of":
        toks = toks[1:]
    return " ".join(toks)


# honorific/saint titles stripped from the FRONT of an identity (so variants unify). Deliberately EXCLUDES
# mr/mrs/ms/miss — those distinguish a wife from her husband (19 spouse pairs in the store).
_HONORIFICS = {"st", "saint", "ste", "pope", "fr", "father", "rev", "reverend", "sr", "sister", "br",
               "brother", "mother", "dr", "bl", "blessed", "ven", "venerable", "cardinal", "card",
               "bishop", "archbishop", "msgr", "monsignor", "servant"}

# deterministic 19th-/early-20th-c. given-name CONTRACTIONS (apostrophes/periods already stripped, so
# "edw'd"->"edwd", "wm."->"wm" arrive here as single tokens). Expanded in _norm_identity so a contracted
# OCR form groups with its full spelling (the "Edw'd Casey" == "Edward Casey" fix).
_CONTRACTIONS = {
    "edwd": "edward", "edw": "edward",
    "wm": "william",
    "jno": "john", "jn": "john",
    "geo": "george",
    "chas": "charles",
    "thos": "thomas", "tho": "thomas",
    "jas": "james",
    "jos": "joseph",
    "robt": "robert",
    "richd": "richard", "rich": "richard",
    "danl": "daniel",
    "saml": "samuel",
    "benj": "benjamin",
    "matt": "matthew",
    "michl": "michael",
    "patk": "patrick",
    "fredk": "frederick",
    "alexr": "alexander",
    "nathl": "nathaniel",
    "chris": "christopher",
}


def _is_bare_honorific(name: str) -> bool:
    """True when a name has NO real name token left after stripping honorifics — e.g. 'Fr.', 'Sister',
    'Rt. Rev.' — so it must never be used as a merged person's DISPLAY name."""
    toks = re.sub(r"[^a-z ]+", " ", (name or "").lower()).split()
    real = [t for t in toks if t not in _HONORIFICS]
    return len(real) < 1


def _name_token_count(name: str) -> int:
    """Count of real name tokens after dropping honorifics + generic stop-words — used to prefer a real
    multi-token person name (>=2) over a bare honorific when choosing a merged record's display name."""
    toks = re.sub(r"[^a-z ]+", " ", (name or "").lower()).split()
    return sum(1 for t in toks if t not in _HONORIFICS and t not in _GENERIC_PROPER_STOP)

# place NAMES too vague/large to pin meaningfully (a continent, a country-as-vague, a geographic feature)
_VAGUE_PLACE_NAMES = {"america", "u.s.", "u.s.a.", "us", "usa", "the states", "earth", "world",
                      "europe", "asia", "africa", "the orient", "the west", "the east", "heaven"}
_VAGUE_PLACE_RE = re.compile(r"\b(river|lake|ocean|sea|mountains?|continent|gulf|bay|valley)\b", re.I)


def _vague_place_name(name: str) -> bool:
    n = (name or "").strip().lower()
    nd = n.replace(".", "").replace(" ", "")               # "U.S." -> "us", "U. S." -> "us"
    return n in _VAGUE_PLACE_NAMES or nd in _VAGUE_PLACE_NAMES or bool(_VAGUE_PLACE_RE.search(n))


_GENERIC_IDENTITY = {"unknown", "a person", "person", "place", "a place", "petitioner", "the writer",
                     "god", "jesus", "our lord", "the lord", "a man", "a woman", "the patient"}


def _orig_cat(e: dict) -> str:
    """Coarse category of an entity's ORIGINAL NER type — for the cross-category merge guard."""
    t = (e.get("type") or "").upper()
    if t in ("PLACE", "LOC", "LOCATION", "GPE"):
        return "place"
    if t == "PERSON":
        return "person"
    return "other"


# generic words that, even Title-cased, don't make something a real proper-noun entity
_GENERIC_PROPER_STOP = {"mother", "father", "doctor", "priest", "sister", "brother", "company", "bank",
                        "court", "office", "heaven", "monastery", "convent", "church", "hospital",
                        "school", "home", "house", "family", "city", "town", "county", "state", "river",
                        "lake", "saint", "god", "lord", "jesus", "christ", "the", "of", "and", "mass",
                        "two", "children", "baby", "wife", "husband", "son", "daughter"}


def _recoverable_propernoun(e: dict) -> bool:
    """Audit P1a: an is_real_entity=false drop that is ACTUALLY a real named PERSON/PLACE/ORG — a proper
    noun the noise judge mis-killed (Pope Leo XIII, Guam, Red Wing, B&O Depot, Pennsylvania RR). True when
    the orig NER type is a name-bearing type and the name has a non-generic Title-case token (<=4 tokens)."""
    if (e.get("type") or "").upper() not in ("PERSON", "PLACE", "ORG", "ORGANIZATION", "LOC", "LOCATION", "GPE"):
        return False
    toks = [t for t in re.split(r"[^A-Za-z]+", e.get("canonical_name") or "") if t]
    if not toks or len(toks) > 4:
        return False
    return any(t[:1].isupper() and len(t) > 1 and t.lower() not in _GENERIC_PROPER_STOP for t in toks)


def _engine_contexts(ent: dict, letters_text: dict, entry_text: dict, max_ctx: int = 4,
                     window: int = 420) -> list:
    """Like enrich's _contexts_for but CENTERS each excerpt on the mention's surface, so the entity's
    name is actually IN the excerpt. (Root cause of the noise false-drops: a truncated head-of-letter
    snippet that never contained the name, so the judge said 'not present → not real'.)"""
    seen, out = set(), []
    fallback = (ent.get("canonical_name") or "").lower()
    for m in ent.get("mentions", []):
        prov = m.get("provenance", {})
        doc_id, rid = prov.get("doc_id"), prov.get("rid")
        txt = textclean.clean((entry_text.get((doc_id, rid)) or letters_text.get(doc_id) or "").strip())
        if not txt:
            continue
        needle = (m.get("surface") or fallback or "").lower()
        i = txt.lower().find(needle) if needle else -1
        excerpt = txt[max(0, i - window // 3): max(0, i - window // 3) + window] if (i >= 0 and len(txt) > window) else txt[:window]
        if excerpt in seen:
            continue
        seen.add(excerpt)
        out.append(excerpt)
        if len(out) >= max_ctx:
            break
    return out


def reflect_and_merge(thought: list) -> tuple:
    """Group thought-through entities by (entity_kind, normalized identity); merge each group into one
    canonical entity. CAUTIOUS: only entities the engine judged the SAME specific real-world thing
    (identical normalized identity, same kind, non-generic, non-noise) are united. Returns (entities, audit)."""
    groups: dict = {}
    singles: list = []
    for ent in thought:
        u = ent.get("understanding", {})
        ident = _norm_identity(u.get("disambiguated_identity"))
        kind = u.get("entity_kind", "other")
        # don't merge on a missing/generic identity, or noise — keep them as their own records
        if (not u.get("is_real_entity", True)) or (not ident) or ident in _GENERIC_IDENTITY \
                or len(ident) < 3 or kind == "noise":
            singles.append(ent)
            continue
        groups.setdefault((kind, ident), []).append(ent)

    out, audit = [], []
    for (kind, ident), members in groups.items():
        if len(members) == 1:
            out.append(members[0])
            continue
        members.sort(key=lambda e: len(e.get("mentions", [])), reverse=True)
        primary = members[0]
        # CROSS-CATEGORY GUARD (audit-driven): never absorb a member whose ORIGINAL NER type contradicts
        # the group — a PLACE must not fold into a PERSON group, nor vice versa (the "David Dion = Idaho"
        # bug). Mismatched members drop back to singles.
        pcat = _orig_cat(primary)
        kept, rejected = [primary], []
        for m in members[1:]:
            mc = _orig_cat(m)
            if {pcat, mc} == {"person", "place"}:
                rejected.append(m)
            else:
                kept.append(m)
        singles.extend(rejected)
        members = kept
        if len(members) == 1:
            out.append(primary)
            continue
        # DISPLAY-NAME PICK (bare-honorific fix): keep the most-mentions member as the canonical RECORD
        # (preserving the mention-count signal), but the SHOWN name must be a real name, not "Fr." —
        # prefer (1) any member's TOC name, then (2) a real multi-token name from the highest-mention
        # member that has one, else (3) primary's own name.
        display_name = None
        for m in members:
            if m.get("toc_name"):
                display_name = m["toc_name"]
                break
        if not display_name:
            for m in members:                              # members are sorted by mention count, desc
                cand = (m.get("understanding") or {}).get("canonical_name") or m.get("canonical_name")
                if _name_token_count(cand) >= 2:
                    display_name = cand
                    break
        if not display_name:
            display_name = (primary.get("understanding") or {}).get("canonical_name") or primary.get("canonical_name")
        merged = dict(primary)
        merged["canonical_name"] = display_name
        # copy understanding so we don't mutate the shared source thought dict
        merged["understanding"] = {**(primary.get("understanding") or {}), "canonical_name": display_name}
        allm = list(primary.get("mentions", []))
        allv = set(primary.get("variants", []))
        allv.add(primary.get("canonical_name", ""))        # keep primary's original name as a variant
        srcs = [primary["id"]]
        for m in members[1:]:
            allm += m.get("mentions", [])
            allv |= set(m.get("variants", []))
            allv.add(m.get("canonical_name", ""))
            srcs.append(m["id"])
        merged["mentions"] = allm
        merged["variants"] = sorted(v for v in allv if v and v != display_name)
        merged["merged_from"] = srcs
        out.append(merged)
        audit.append({"identity": ident, "kind": kind, "canonical": merged.get("canonical_name"),
                      "merged": [{"id": m["id"], "name": m.get("canonical_name")} for m in members],
                      "n_mentions": len(allm)})
    out.extend(singles)
    # sanity log: the biggest merge groups (large groups for Solanus / common conditions are EXPECTED
    # and good; a large PERSON group on a common surname would be the thing to eyeball in the audit).
    big = sorted(audit, key=lambda a: len(a["merged"]), reverse=True)[:6]
    if big:
        print("  largest merges: " + "; ".join(
            f"{a['kind']}:{(a['canonical'] or '?')[:24]}×{len(a['merged'])}" for a in big), flush=True)
    return out, audit


# ==================================================================
# TOC-anchored canonicalization + merge — use the curated TOC recipient
# names as the NAME AUTHORITY, and let the model decide identity from
# mention context (unifies OCR-fragmented people, e.g. the 5 Edward Caseys)
# ==================================================================
_TOC_SYSTEM = (
    "You are the recipient-name authority matcher for the Father Solanus Casey archive (Detroit Capuchin "
    "friar 1870-1957; his correspondence + the Seraphic Mass Association prayer-favor notebooks). You are "
    "given ONE candidate PERSON entity (as OCR'd, with the EXACT source excerpts where it appears) and a "
    "shortlist of CURATED, authoritative recipient names from the archive's table of contents. Decide "
    "whether the entity is the SAME real person as one of the curated names; if so, return that curated "
    "name EXACTLY as written in the shortlist — the curated spelling is the authority, prefer it over the "
    "OCR. If SEVERAL shortlisted names refer to the same person (differing only by honorific, middle "
    "initials, or wording), choose the single most complete/formal one and use it CONSISTENTLY. Be strict: "
    "a shared SURNAME alone is NOT a match — the given name, role and excerpt context must agree. If none "
    "is clearly the same person, return null. Return STRICT JSON only."
)

# strip TOC label cruft ("Letters to ...", "Letter to ...", "Excerpt from Letter to ...") for a clean
# display + merge key, on top of name_authority._clean_toc_name (which keeps those prefixes).
_TOC_PREFIX_RE = re.compile(r"^\s*(excerpt\s+from\s+)?(letters?\s+to|letter\s+to)\s+", re.I)


def _toc_display(s: str) -> str:
    return re.sub(r"\s+", " ", _TOC_PREFIX_RE.sub("", s or "")).strip(" -:.,\"'")


def _surnames(name: str) -> set:
    """The surname (last non-honorific name token) of a name — for surname shortlisting."""
    toks = [t.lower() for t in re.split(r"[^A-Za-z]+", name or "") if len(t) > 1]
    toks = [t for t in toks if t not in _HONORIFICS]
    return {toks[-1]} if toks else set()


def _ent_surnames(ent: dict) -> set:
    sns: set = set()
    for nm in ([ent.get("canonical_name", ""), (ent.get("understanding") or {}).get("canonical_name", "")]
               + (ent.get("variants") or [])):
        sns |= _surnames(nm)
    return sns


def _toc_clean_names() -> list:
    """Cleaned + deduped curated TOC recipient display names (the authority spelling pool)."""
    seen, out = set(), []
    for n in _load_toc_names():
        d = _toc_display(n)
        if d and d.lower() not in seen:
            seen.add(d.lower())
            out.append(d)
    return out


# ==================================================================
# TOC-AUTHORITY DEDUP — collapse same-person variants (middle initials /
# honorifics / OCR spelling) of the SAME curated recipient into ONE canonical
# string, so e.g. every "Edward [F./F.J./J.R./J.F.] Casey" form resolves to one
# entity. Multi-string clusters are adjudicated by ONE Gemini call each and the
# decisions are CACHED to TOC_CLUSTERS_PATH (lazy + resumable + free on re-run).
# NOTE: the LLM adjudication is the ONLY new spend; it is invoked LAZILY the
# first time --toc-anchor runs (never at import / never here).
# ==================================================================
# title/prefix tokens dropped when reducing a TOC label to a (given, surname) key. Broader than the global
# _HONORIFICS (adds rt/right/very/most/mgr/mr/mrs/...) ON PURPOSE: this set is used ONLY for the coarse
# clustering key, so over-grouping (e.g. "Mrs. James Casey" with "James Casey") is SAFE — the per-cluster
# LLM call splits genuinely-distinct people back apart (it is told the "Mrs. <husband>" = wife rule).
_CLUSTER_TITLES = _HONORIFICS | {"rt", "right", "very", "most", "mgr", "mr", "mrs", "ms", "miss", "the", "v"}
_CLUSTER_DROP = {"with", "and", "of", "for", "associates", "girls", "camp", "fire", "jr", "sr", "esq",
                 "md", "phd", "om", "cap", "sj", "osb", "ofm", "letters", "letter", "to", "excerpt", "from"}

_CLUSTER_SYSTEM = (
    "You adjudicate whether a set of archival recipient-name labels (from the Father Solanus Casey "
    "correspondence table of contents) refer to ONE single real person or to MULTIPLE distinct people. "
    "The labels already share a given name and surname; they differ only in middle initials, honorifics, "
    "or OCR spelling. Middle-initial, honorific, and spelling differences almost always indicate the SAME "
    "person (OCR / curation noise). BUT keep people SEPARATE when the labels denote different individuals: "
    "in particular 'Mrs. <a man's full name>' denotes that man's WIFE (a distinct person from the man), and "
    "a clearly different role can mean a different person. Be conservative: only declare ONE person when the "
    "labels are plainly the same individual. Return STRICT JSON only."
)


def _toc_name_key(name: str):
    """Reduce a cleaned TOC label to a coarse (first-given-name, surname) clustering key — ignoring
    honorifics/titles, middle initials, digits and order-suffixes, with contraction expansion applied.
    Returns None when fewer than 2 real name tokens survive (so it will not cluster)."""
    s = re.sub(r"[’'.]", "", _toc_display(name).lower())
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    toks = [_CONTRACTIONS.get(t, t) for t in s.split()]
    real = [t for t in toks if len(t) >= 2 and not t.isdigit()
            and t not in _CLUSTER_TITLES and t not in _CLUSTER_DROP]
    if len(real) < 2:
        return None
    return (real[0], real[-1])


def _pick_canonical(strings: list) -> str:
    """Deterministic 'most complete spelling' fallback: most real name tokens, then has a middle initial,
    then has a clerical honorific, then shortest (cleanest) — avoids picking an OCR-garbled long label."""
    def score(s: str):
        low = re.sub(r"[’'.]", "", s.lower())
        toks = re.sub(r"[^a-z ]+", " ", low).split()
        real = [t for t in toks if len(t) >= 2 and t not in _CLUSTER_TITLES and t not in _CLUSTER_DROP]
        has_initial = any(len(t) == 1 for t in toks)
        has_title = any(t in _HONORIFICS for t in toks)
        return (len(real), has_initial, has_title, -len(s))
    return max(strings, key=score)


def _resolve_cluster(strings: list, model: str = UNCERTAIN_MODEL) -> dict:
    """ONE Gemini call deciding same-single-person vs multiple-distinct-people for a (given,surname)
    cluster of >=2 distinct curated labels. Returns {same_person, canonical, confidence, reason};
    fail-safe to 'multiple' (keep separate) on any error. THE ONLY new spend in the dedup."""
    from lib.providers import llm
    listing = "\n".join(f"{i + 1}. {s}" for i, s in enumerate(strings))
    prompt = (
        "These archival recipient-name labels share a given name and surname but differ in middle "
        "initials, honorifics, or spelling:\n" + listing + "\n\n"
        "Do they all refer to ONE single real person, or to MULTIPLE distinct people? "
        'Return JSON: {"same_person": <bool>, '
        '"canonical": "<the exact label copied from the list above that is the single most complete and '
        'correct spelling, when same_person; else null>", '
        '"confidence": <0.0-1.0>, "reason": "<one sentence>"}'
    )
    try:
        txt, _ = llm.generate(prompt, model=model, system=_CLUSTER_SYSTEM, json_mode=True, temperature=0.0)
        v = json.loads(txt)
        same = bool(v.get("same_person"))
        canon = _coerce(v.get("canonical"))
        low = {s.lower(): s for s in strings}
        if canon and canon.strip().lower() in low:
            canon = low[canon.strip().lower()]             # snap to an exact provided label
        elif same:
            canon = _pick_canonical(strings)               # model gave a paraphrase -> deterministic pick
        else:
            canon = None
        return {"same_person": same, "canonical": canon,
                "confidence": float(v.get("confidence", 0.0) or 0.0), "reason": _coerce(v.get("reason"))}
    except Exception as e:                                  # never let one cluster kill the build
        return {"same_person": False, "canonical": None, "confidence": 0.0, "reason": f"error: {str(e)[:80]}"}


def _build_toc_clusters(toc_clean: list, model: str = UNCERTAIN_MODEL, force: bool = False,
                        max_cost_usd: float = 5.0) -> dict:
    """Cluster cleaned TOC names by (given, surname); resolve each MULTI-string cluster with ONE LLM call
    (same-person -> one canonical). Cached to TOC_CLUSTERS_PATH and RESUMABLE (unchanged clusters reuse the
    cached decision -> free). Returns {cleaned_label: canonical_label} remap for same-person clusters only.

    LAZY: this is called from toc_anchor_and_merge during a --toc-anchor run, NOT at import. It is the only
    function here that spends; it is cheap because only multi-string clusters call the model and decisions
    are cached. Cost-capped via the existing costlog pattern."""
    cache = {}
    if TOC_CLUSTERS_PATH.exists() and not force:
        try:
            cache = json.loads(TOC_CLUSTERS_PATH.read_text())
        except Exception:
            cache = {}
    decided = {tuple(c["key"]): c for c in cache.get("clusters", []) if c.get("key")}

    clusters: dict = {}
    for s in toc_clean:
        k = _toc_name_key(s)
        if k is None:
            continue
        clusters.setdefault(k, [])
        if s not in clusters[k]:
            clusters[k].append(s)
    multi = {k: sorted(v) for k, v in clusters.items() if len(v) >= 2}

    cost0 = costlog.snapshot()
    out_clusters, n_calls = [], 0
    for k, strings in sorted(multi.items()):
        prev = decided.get(k)
        if prev and sorted(prev.get("strings", [])) == strings:
            out_clusters.append(prev)                      # reuse cached decision (free, resumable)
            continue
        if costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0) >= max_cost_usd:
            # cap reached: leave the rest unresolved (they stay separate); a re-run resumes them
            out_clusters.append({"key": list(k), "strings": strings, "same_person": False,
                                 "canonical": None, "confidence": 0.0, "reason": "deferred (cost cap)"})
            continue
        res = {"key": list(k), "strings": strings, **_resolve_cluster(strings, model)}
        n_calls += 1
        out_clusters.append(res)

    remap = {}
    for c in out_clusters:
        if c.get("same_person") and c.get("canonical"):
            for s in c["strings"]:
                remap[s] = c["canonical"]
    spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
    payload = {"_meta": {"model": model, "built": time.strftime("%Y-%m-%dT%H:%M:%S"),
                         "n_multi_clusters": len(multi), "llm_calls": n_calls,
                         "same_person_clusters": sum(1 for c in out_clusters if c.get("same_person")),
                         "spent_usd": round(spent, 4)},
               "map": remap, "clusters": out_clusters}
    config.DATA.mkdir(parents=True, exist_ok=True)
    TOC_CLUSTERS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    print(f"  toc-clusters: {len(multi)} multi-string clusters; {n_calls} new LLM calls; "
          f"{len(remap)} labels -> canonical | ${spent:.2f}", flush=True)
    return remap


def toc_match_one(ent: dict, contexts: list, toc_shortlist: list, model: str = BULK_MODEL) -> dict:
    """Ask the model whether a PERSON entity is one of the curated TOC names. Returns
    {"toc_name", "is_same_person", "confidence", "reason"}; fails safe to null. PERSON entities only."""
    if ent.get("type") != "PERSON":
        return {"toc_name": None, "is_same_person": False, "confidence": 0.0, "reason": "not a person"}
    if not toc_shortlist:
        return {"toc_name": None, "is_same_person": False, "confidence": 0.0, "reason": "no candidates"}
    from lib.providers import llm
    u = ent.get("understanding") or {}
    variants = ", ".join((ent.get("variants") or [])[:8])
    ctx = "\n".join(f"- {c}" for c in contexts) or "(no excerpt text available)"
    cand = "\n".join(f"- {c}" for c in toc_shortlist)
    prompt = (
        'Candidate entity (as OCR\'d):\n'
        f'  name: "{u.get("canonical_name") or ent.get("canonical_name", "")}"\n'
        f'  disambiguated_identity: {u.get("disambiguated_identity") or "null"}\n'
        f'  role: {u.get("role") or "null"}\n'
        f'  other spellings: {variants or "(none)"}\n\n'
        f'Source excerpts where it appears:\n{ctx}\n\n'
        f'Curated authoritative recipient names (the shortlist):\n{cand}\n\n'
        'Return JSON: {"toc_name": "<exact curated name copied from the shortlist, or null>", '
        '"is_same_person": <bool>, "confidence": <0.0-1.0>, "reason": "<one sentence>"}'
    )
    try:
        txt, _ = llm.generate(prompt, model=model, system=_TOC_SYSTEM, json_mode=True, temperature=0.0)
        v = json.loads(txt)
        tn = _coerce(v.get("toc_name"))
        if tn:                                             # snap to the shortlist's exact casing/spelling
            low = {s.lower(): s for s in toc_shortlist}
            tn = low.get(tn.strip().lower(), tn.strip())
        return {"toc_name": tn, "is_same_person": bool(v.get("is_same_person")),
                "confidence": float(v.get("confidence", 0.0) or 0.0), "reason": _coerce(v.get("reason"))}
    except Exception as e:                                  # never let one entity kill the run
        return {"toc_name": None, "is_same_person": False, "confidence": 0.0,
                "reason": f"error: {str(e)[:80]}"}


def toc_anchor_and_merge(entities: list, lt: dict, et: dict, model: str = BULK_MODEL,
                         max_cost_usd: float = 2.0, limit: int | None = None,
                         workers: int | None = None, rebuild_clusters: bool = False) -> tuple:
    """Thread-pool toc_match_one over PERSON entities with a surname-shortlisted candidate list; for
    confident matches (is_same_person & confidence>=0.80) set the TOC name as the canonical display name
    (mapped through the TOC-authority dedup so same-person variants share ONE canonical), then MERGE all
    entities resolved to the SAME canonical (exact union logic from reflect_and_merge, incl. the
    person/place cross-category guard). Returns (entities, audit). Cost-capped + non-destructive."""
    from concurrent.futures import ThreadPoolExecutor
    # cleaned + deduped curated TOC display names, with a surname -> [names] index for shortlisting
    toc_clean = _toc_clean_names()
    surn_index: dict = {}
    for d in toc_clean:
        for sn in _surnames(d):
            surn_index.setdefault(sn, []).append(d)
    # TOC-AUTHORITY DEDUP (lazy + cached): collapse same-person middle-initial/honorific variants so every
    # such curated label resolves to ONE canonical (e.g. all Edward Casey forms -> one). Built/cached on
    # the FIRST --toc-anchor run; reused free thereafter.
    cluster_map = _build_toc_clusters(toc_clean, model=model, force=rebuild_clusters)

    def _shortlist(ent: dict) -> list:
        out, seenc = [], set()
        for sn in _ent_surnames(ent):
            for d in surn_index.get(sn, []):
                if d.lower() not in seenc:
                    seenc.add(d.lower())
                    out.append(d)
        return sorted(out)[:25]

    persons = [e for e in entities if e.get("type") == "PERSON"]
    eligible = [(e, _shortlist(e)) for e in persons]
    eligible = [(e, sl) for (e, sl) in eligible if sl]     # only persons with a TOC surname match spend
    if limit:
        eligible = eligible[:limit]
    print(f"  toc-anchor: {len(persons)} persons; {len(eligible)} with a TOC surname shortlist "
          f"(limit={limit}) | cap=${max_cost_usd}", flush=True)
    if not eligible:
        return entities, []

    cost0 = costlog.snapshot()
    try:
        from lib.providers import llm as _llm
        _llm.generate("ok", model=model)                   # warm the shared client before the burst
    except Exception:
        pass
    byid = {e["id"]: e for e in entities}
    matched: dict = {}                                     # id -> (toc_name, confidence, reason)
    ex = ThreadPoolExecutor(max_workers=workers or 16)
    futs = {ex.submit(toc_match_one, e, _engine_contexts(e, lt, et), sl, model): e["id"]
            for (e, sl) in eligible}

    def _handle(fut, _f=futs):
        eid = _f[fut]
        try:
            r = fut.result()
        except Exception:
            return False
        if r.get("toc_name") and r.get("is_same_person") and float(r.get("confidence") or 0.0) >= 0.80:
            tn = cluster_map.get(r["toc_name"], r["toc_name"])  # collapse same-person variants to canonical
            matched[eid] = (tn, float(r["confidence"]), r.get("reason"))
        return costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0) >= max_cost_usd

    capped = _drain_futures(ex, list(futs), _handle, label="toc-match calls")
    spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
    print(f"  toc-anchor: {len(matched)} confident matches (>=0.80) | spent ${spent:.2f}"
          + ("  [CAPPED]" if capped else ""), flush=True)

    # apply the curated spelling as the canonical display name on EVERY confident match
    for eid, (tn, conf, _reason) in matched.items():
        e = byid[eid]
        e["canonical_name"] = tn
        e["understanding"] = {**(e.get("understanding") or {}), "canonical_name": tn}
        e["toc_name"] = tn
        e["toc_confidence"] = conf

    # MERGE entities that resolved to the SAME toc_name (exact reflect_and_merge union logic)
    groups: dict = {}
    for eid in matched:
        groups.setdefault(matched[eid][0], []).append(byid[eid])
    absorbed, merged_records, audit = set(), [], []
    for tn, members in groups.items():
        if len(members) < 2:
            continue
        members.sort(key=lambda e: len(e.get("mentions", [])), reverse=True)
        primary = members[0]
        pcat = _orig_cat(primary)
        kept = [primary]
        for m in members[1:]:                              # cross-category guard: a PLACE never folds in
            if {pcat, _orig_cat(m)} != {"person", "place"}:
                kept.append(m)
        if len(kept) < 2:
            continue
        merged = dict(primary)
        allm = list(primary.get("mentions", []))
        allv = set(primary.get("variants", []))
        allv.add(primary.get("canonical_name", ""))
        srcs = [primary["id"]]
        absorbed.add(primary["id"])
        for m in kept[1:]:
            allm += m.get("mentions", [])
            allv |= set(m.get("variants", []))
            allv.add(m.get("canonical_name", ""))
            srcs.append(m["id"])
            absorbed.add(m["id"])
        merged["mentions"] = allm
        merged["variants"] = sorted(v for v in allv if v and v != tn)
        merged["merged_from"] = srcs
        merged["canonical_name"] = tn
        merged["understanding"] = {**(primary.get("understanding") or {}), "canonical_name": tn}
        merged["toc_name"] = tn
        merged["toc_confidence"] = max(matched[m["id"]][1] for m in kept)
        merged_records.append(merged)
        audit.append({"toc_name": tn, "confidence": merged["toc_confidence"],
                      "reason": matched[primary["id"]][2],
                      "merged": [{"id": m["id"], "name": m.get("canonical_name")} for m in kept],
                      "n_mentions": len(allm)})
    final = [e for e in entities if e["id"] not in absorbed] + merged_records
    big = sorted(audit, key=lambda a: len(a["merged"]), reverse=True)[:6]
    if big:
        print("  largest TOC merges: " + "; ".join(
            f"{(a['toc_name'] or '?')[:28]}×{len(a['merged'])}" for a in big), flush=True)
    return final, audit


def toc_anchor(workers: int | None = None, model: str = BULK_MODEL, max_cost_usd: float = 2.0,
               limit: int | None = None, rebuild_clusters: bool = False) -> dict:
    """Standalone driver: run TOC-anchored canonicalization + merge against the EXISTING store, back it
    up, write the TOC audit, and re-emit the enriched view. Mirrors rethink_subset/remerge. The TOC-
    authority dedup clusters are built+cached lazily here on the first run (re-used free thereafter;
    pass rebuild_clusters=True to force a rebuild)."""
    store = _load_store()
    ents = store.get("entities", [])
    if not ents:
        print("toc_anchor: empty store — run the engine first")
        return {}
    lt, et = _build_text_index()
    n0 = len(ents)
    entities, audit = toc_anchor_and_merge(ents, lt, et, model=model, max_cost_usd=max_cost_usd,
                                           limit=limit, workers=workers, rebuild_clusters=rebuild_clusters)
    _write_store(entities, passes_done=store.get("_meta", {}).get("passes_done", 1), backup=True)
    TOC_AUDIT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=1))
    emit_enriched()
    print(f"toc_anchor: {n0} → {len(entities)} entities ({len(audit)} TOC merges) | store → {STORE_PATH.name}")
    return {"before": n0, "after": len(entities), "toc_merges": len(audit)}


def toc_rebuild_clusters(model: str = UNCERTAIN_MODEL, max_cost_usd: float = 5.0) -> dict:
    """Convenience: (re)build + cache ONLY the TOC-authority same-person dedup clusters (no store touch).
    Spends one cheap LLM call per multi-string cluster; cached to TOC_CLUSTERS_PATH. Normally unnecessary —
    --toc-anchor builds them lazily — but handy to precompute or after editing the TOC."""
    remap = _build_toc_clusters(_toc_clean_names(), model=model, force=True, max_cost_usd=max_cost_usd)
    print(f"toc_rebuild_clusters: {len(remap)} labels remap to a canonical -> {TOC_CLUSTERS_PATH.name}")
    return {"remapped_labels": len(remap)}


# ==================================================================
# Orchestration — think (parallel, resumable, cost-capped) → merge → write
# ==================================================================
def _estimate(n: int) -> float:
    # ~0.00017 USD/entity on flash-lite (measured: ~750 in + 150 out tokens).
    return round(n * 0.00017, 2)


_STALL_SECS = 130       # no-progress window; > the 90s server deadline so a straggler can retry-recover
                        # before we give up on it (abandoned ones still resume on the next run)


def _drain_futures(ex, futures, handle, stall_secs: int = _STALL_SECS, label: str = "calls") -> bool:
    """Process futures as they finish, with a NO-PROGRESS WATCHDOG. ``handle(fut)`` processes one
    finished future and returns True to STOP early (e.g. cost cap). If nothing finishes for
    ``stall_secs`` we conclude the remaining calls are wedged, abandon them (resumable), and return.
    Always shuts the executor down (cancelling not-yet-started futures). Returns True iff stopped via
    ``handle`` (capped)."""
    from concurrent.futures import wait, FIRST_COMPLETED
    pending = set(futures)
    last = time.time()
    stopped = False
    while pending and not stopped:
        finished, pending = wait(pending, timeout=20, return_when=FIRST_COMPLETED)
        if not finished:
            if time.time() - last > stall_secs:
                print(f"  !! no progress for {stall_secs}s — abandoning {len(pending)} wedged "
                      f"{label} (resumable on re-run)", flush=True)
                break
            continue
        last = time.time()
        for fut in finished:
            if handle(fut):
                stopped = True
                break
    ex.shutdown(wait=False, cancel_futures=True)
    return stopped


def run(execute: bool = False, workers: int | None = None, limit: int | None = None,
        passes: int = 1, max_cost_usd: float = 25.0, model: str = BULK_MODEL) -> dict:
    """Build/refresh the central entity store. One converge pass by default.

    execute=False → dry-run (counts + cost estimate). passes>1 → re-think entities whose mention set
    grew after a merge, repeating until stable or max_cost_usd is hit.
    """
    workers = workers or int(os.environ.get("ENGINE_WORKERS", "16"))
    seed = json.loads(ENTITIES.read_text()).get("entities", [])
    if limit:
        seed = seed[:limit]
    print(f"entity_engine: {len(seed)} seed entities  | model={model} workers={workers} "
          f"passes={passes} cap=${max_cost_usd}")
    print(f"  estimated cost for a full think pass: ~${_estimate(len(seed))}")
    if not execute:
        print("  (dry-run — pass --execute to spend)")
        return {"seed": len(seed), "estimate_usd": _estimate(len(seed)), "executed": False}

    lt, et = _build_text_index()
    store = _load_store()
    prior = {e["id"]: e for e in store.get("entities", [])} if store else {}

    from concurrent.futures import ThreadPoolExecutor
    cost0 = costlog.snapshot()
    thought: dict = {}
    reused = 0

    # seed reuse: unchanged entities keep their stored understanding (incrementality → no spend)
    todo = []
    for e in seed:
        h = _content_hash(e)
        p = prior.get(e["id"])
        if p and p.get("content_hash") == h and p.get("understanding"):
            thought[e["id"]] = {**e, "understanding": p["understanding"], "content_hash": h}
            reused += 1
        else:
            todo.append((e, h))
    print(f"  reuse (unchanged): {reused}  | to think: {len(todo)}")

    def _w(item):
        e, h = item
        u = think_one(e, _contexts_for(e, lt, et), model)
        return e, h, u

    # NO-PROGRESS WATCHDOG drain (see _drain_futures): ~1-2% of provider calls intermittently wedge
    # (the SDK's timeout is a server deadline, not a client read timeout, so it cannot abort them).
    # Completions stream continuously during healthy work; if NOTHING completes for STALL_SECS we
    # abandon the wedged stragglers (they re-think on the next incremental run) and move on.
    # Warm the shared HTTP client BEFORE the concurrent burst: a cold connection pool hit by N threads
    # at once is what intermittently wedges the first calls (serial diag showed only entity[0] stalling).
    try:
        from lib.providers import llm as _llm
        _llm.generate("ok", model=model)
    except Exception:
        pass

    ex = ThreadPoolExecutor(max_workers=workers)
    futs = [ex.submit(_w, it) for it in todo]
    state = {"done": 0}

    def _handle(fut):
        try:
            e, h, u = fut.result()
        except Exception:
            return False                                   # failed call → skip (resumable)
        thought[e["id"]] = {**e, "understanding": u, "content_hash": h}
        state["done"] += 1
        if state["done"] % CHECKPOINT_EVERY == 0:
            spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
            print(f"    thought {state['done']}/{len(todo)}  (${spent:.2f})", flush=True)
            _write_store(list(thought.values()), passes_done=0)   # checkpoint (resume)
            if spent >= max_cost_usd:
                return True
        return False

    capped = _drain_futures(ex, futs, _handle, label="think calls")
    if capped:
        print(f"  !! cost cap ${max_cost_usd} reached — stopping think early (partial pass saved)")

    # persist the PRE-MERGE thought so merge rules can be re-tuned later WITHOUT re-spending (--remerge)
    THOUGHT_PATH.write_text(json.dumps(list(thought.values()), ensure_ascii=False))
    entities, audit = reflect_and_merge(list(thought.values()))
    spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
    print(f"  pass 1: {len(thought)} thought → {len(entities)} after merge "
          f"({len(audit)} merges) | spent ${spent:.2f}")

    # ---- converge: re-think entities whose mentions grew via a merge, until stable / cap ----
    pass_no = 1
    while passes > pass_no and not capped and spent < max_cost_usd:
        pass_no += 1
        grew = [e for e in entities if e.get("merged_from")]
        if not grew:
            print(f"  pass {pass_no}: store stable (no merges to re-examine) — done")
            break
        print(f"  pass {pass_no}: re-thinking {len(grew)} merged entities for richer identities…")
        ex = ThreadPoolExecutor(max_workers=workers)
        futs = {ex.submit(think_one, e, _contexts_for(e, lt, et), model): e for e in grew}

        def _handle_rethink(fut, _futs=futs):
            e = _futs[fut]
            try:
                e["understanding"] = fut.result()
            except Exception:
                return False
            e.pop("merged_from", None)
            return costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0) >= max_cost_usd

        capped = _drain_futures(ex, list(futs), _handle_rethink, label="re-think calls")
        entities, audit2 = reflect_and_merge(entities)
        audit += audit2
        spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
        print(f"  pass {pass_no}: → {len(entities)} entities ({len(audit2)} new merges) | spent ${spent:.2f}")
        if not audit2:
            print("  store converged (no new merges) — done")
            break

    _write_store(entities, passes_done=pass_no, backup=True)
    AUDIT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=1))
    emit_enriched()                                        # derive the legacy view build_graph/geocode read
    real = sum(1 for e in entities if e.get("understanding", {}).get("is_real_entity", True))
    mappable = sum(1 for e in entities if e.get("understanding", {}).get("is_mappable_place"))
    print(f"entity_engine DONE: {len(entities)} canonical entities "
          f"({real} real, {len(entities)-real} noise/flagged, {mappable} mappable places) | "
          f"spent ${spent:.2f} | store → {STORE_PATH.name}")
    return {"entities": len(entities), "merges": len(audit), "real": real, "mappable_places": mappable,
            "spent_usd": round(spent, 4), "executed": True}


def rethink_subset(workers: int | None = None, model: str = BULK_MODEL, max_cost_usd: float = 6.0) -> dict:
    """Audit P4 + P1a: re-think the entities the first pass under-served — those NEVER classified
    (confidence 0 + no what_it_is) and recoverable proper-noun false-drops — using mention-centered
    context. Updates the store, saves thought, then re-merges + re-emits. Cheap (~$0.3 for ~1.8k)."""
    from concurrent.futures import ThreadPoolExecutor
    store = _load_store()
    ents = store.get("entities", [])
    if not ents:
        print("rethink_subset: empty store — run the engine first")
        return {}

    def needs(e):
        u = e.get("understanding") or {}
        if u.get("what_it_is") is None and not (u.get("confidence") or 0):
            return True                                    # never classified (errored/abandoned first pass)
        if u.get("is_real_entity") is False and _recoverable_propernoun(e):
            return True                                    # proper-noun false-drop
        return False

    targets = [e for e in ents if needs(e)]
    print(f"rethink_subset: re-thinking {len(targets)} of {len(ents)} entities (est ~${_estimate(len(targets))})",
          flush=True)
    if not targets:
        return {"rethought": 0}
    lt, et = _build_text_index()
    byid = {e["id"]: e for e in ents}
    cost0 = costlog.snapshot()
    try:
        from lib.providers import llm as _llm
        _llm.generate("ok", model=model)                  # warm the client
    except Exception:
        pass
    ex = ThreadPoolExecutor(max_workers=workers or 16)
    futs = {ex.submit(think_one, e, _engine_contexts(e, lt, et), model): e["id"] for e in targets}

    def _handle(fut, _f=futs):
        eid = _f[fut]
        try:
            byid[eid]["understanding"] = fut.result()
        except Exception:
            return False
        return costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0) >= max_cost_usd

    _drain_futures(ex, list(futs), _handle, label="re-think calls")
    spent = costlog.delta(cost0, costlog.snapshot()).get("usd", 0.0)
    print(f"rethink_subset: done re-thinking ({spent:.2f}) → saving thought + re-merging", flush=True)
    THOUGHT_PATH.write_text(json.dumps(ents, ensure_ascii=False))   # store is now the up-to-date thought
    return remerge()


def remerge() -> dict:
    """Re-run the deterministic merge on the saved pre-merge thought (data/entity_thought.json) and
    rewrite the store + enriched. FREE (no LLM) — for tuning merge rules after inspecting the audit."""
    if not THOUGHT_PATH.exists():
        print("remerge: no entity_thought.json — run the engine once first")
        return {}
    thought = json.loads(THOUGHT_PATH.read_text())
    entities, audit = reflect_and_merge(thought)
    _write_store(entities, passes_done=1, backup=True)
    AUDIT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=1))
    emit_enriched()
    print(f"remerge: {len(thought)} thought → {len(entities)} entities ({len(audit)} merges)")
    return {"entities": len(entities), "merges": len(audit)}


def emit_enriched() -> dict:
    """Derive the legacy ``entities_enriched.json`` view from the store so build_graph + geocode read
    the engine's results unchanged. THIS is where the place-mapping gate lives: a street/building/
    institution maps to its PARENT CITY (never a bare name → wrong state), and only real, located
    places get is_place=True. Backs up the prior enriched file (rollback)."""
    store = _load_store()
    ents = store.get("entities", [])
    if not ents:
        print("emit_enriched: store empty — run the engine first")
        return {}
    enr_path = config.DATA / "entities_enriched.json"
    if enr_path.exists():
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        (BACKUP_DIR / f"entities_enriched_{time.strftime('%Y%m%d_%H%M%S')}.json").write_text(enr_path.read_text())
    out = {}
    mapped = 0
    for e in ents:
        u = e.get("understanding", {})
        kind = u.get("entity_kind", "other")
        ptype = u.get("place_type") if kind == "place" else None         # null stale place_type on non-places
        is_place = (kind == "place") and u.get("is_real_entity", True)   # entity_kind=="place"; place_type is sub-type
        # A bare STREET is NOT its own map pin (that is the Colorado-Ave→Colorado-state bug); drop it.
        # A building/institution (a friary, a hospital) DOES map — to its parent CITY, a real landmark.
        # Cities/states/countries/regions/neighborhoods map to their own location.
        if ptype == "street" or kind == "street":
            is_place, loc = False, None
        elif ptype in ("building", "institution") or kind in ("building", "institution"):
            loc = u.get("parent_place") or u.get("location")
        else:
            loc = u.get("location")
        # don't pin geographic FEATURES / continents / whole countries-as-vague (America, U.S., Detroit
        # River, St. Louis River) — they geocode to a meaningless centroid (audit P2c, name side).
        if is_place and _vague_place_name(u.get("canonical_name") or e.get("canonical_name") or ""):
            is_place, loc = False, None
        is_place = bool(is_place and loc)
        if is_place:
            mapped += 1
        rec = {"canonical_name": u.get("canonical_name") or e.get("canonical_name"),
               "description": u.get("description") or u.get("what_it_is"),
               "role": u.get("role"), "relation_to_solanus": u.get("relation_to_solanus"),
               "is_place": is_place, "location": loc if is_place else None,
               "entity_kind": kind, "place_type": ptype,
               "what_it_is": u.get("what_it_is"),
               "disambiguated_identity": u.get("disambiguated_identity"),
               "is_real_entity": u.get("is_real_entity", True),
               "confidence": u.get("confidence", 0.0)}
        out[e["id"]] = rec
    enr_path.write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print(f"emit_enriched: wrote {len(out)} enriched records ({mapped} mappable places) -> {enr_path.name}")
    return {"records": len(out), "mappable": mapped}


def _write_store(entities: list, passes_done: int, backup: bool = False) -> None:
    if backup and STORE_PATH.exists():
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        (BACKUP_DIR / f"entity_store_{stamp}.json").write_text(STORE_PATH.read_text())
    payload = {"_meta": {"passes_done": passes_done, "n_entities": len(entities),
                         "config_fingerprint": config.fingerprint()},
               "entities": entities}
    STORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--execute", action="store_true", help="actually spend (default: dry-run)")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None, help="only the first N seed entities (smoke test)")
    ap.add_argument("--passes", type=int, default=1, help="max converge passes (1 = single pass)")
    ap.add_argument("--max-cost", type=float, default=25.0, help="USD cap for this run")
    ap.add_argument("--model", default=BULK_MODEL)
    ap.add_argument("--emit-only", action="store_true",
                    help="just re-derive entities_enriched.json from the existing store (no spend)")
    ap.add_argument("--remerge", action="store_true",
                    help="re-merge from saved thought (no spend) — for tuning merge rules")
    ap.add_argument("--rethink-subset", action="store_true",
                    help="re-think only the under-served entities (unclassified + proper-noun false-drops), then re-merge")
    ap.add_argument("--toc-anchor", action="store_true",
                    help="TOC-anchored canonicalization + merge: use curated TOC recipient names as the "
                         "name authority to unify OCR-fragmented people (LLM; respects --max-cost/--limit/--model)")
    ap.add_argument("--toc-rebuild-clusters", action="store_true",
                    help="(re)build + cache ONLY the TOC same-person dedup clusters (forces rebuild when "
                         "combined with --toc-anchor; standalone otherwise). Normally built lazily on first --toc-anchor")
    a = ap.parse_args()
    if a.emit_only:
        emit_enriched()
    elif a.remerge:
        remerge()
    elif a.rethink_subset:
        rethink_subset(workers=a.workers, model=a.model, max_cost_usd=a.max_cost)
    elif a.toc_anchor:
        toc_anchor(workers=a.workers, model=a.model, max_cost_usd=a.max_cost, limit=a.limit,
                   rebuild_clusters=a.toc_rebuild_clusters)
    elif a.toc_rebuild_clusters:
        toc_rebuild_clusters(model=a.model, max_cost_usd=a.max_cost)
    else:
        run(execute=a.execute, workers=a.workers, limit=a.limit, passes=a.passes,
            max_cost_usd=a.max_cost, model=a.model)
    # Hard exit: a wedged provider call leaves a non-daemon worker thread alive, which would otherwise
    # block interpreter shutdown forever. All outputs are already flushed, so force-terminate.
    sys.stdout.flush()
    os._exit(0)
