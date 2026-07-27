"""stages/resolve_entities.py — Tier-1 A3 entity resolution ("grouping").

Many names, one person. The notebooks and letters spell the *same* human a dozen ways — "Grace.",
"Grace Panyard", "Mrs. Grace Panyard"; OCR turns "O'Donnell" into "ODonnell"/"O Donnell"; a place
is "Detroit" here and "Detroit, Mich." there. Entity resolution (ER) is the craft of deciding which
of those surface *mentions* point at the same real-world *entity*, then minting one tidy authority
record per entity. This stage implements the classic ER pipeline end-to-end:

    normalize  ->  block  ->  match  ->  cluster  ->  canonicalize  ->  (authority reconcile)

WHY this shape (a quick mental model). Comparing every mention against every other is O(n^2) — at a
few thousand mentions that is millions of comparisons, most of them obviously-not-a-match ("Grace"
vs "Detroit"). So we **block**: bucket mentions by a cheap key (a phonetic code + coarse
location/date) so that only plausibly-equal mentions ever get compared. Inside a block we **match**
pairs with a small ensemble of signals (phonetic equality, edit distance, token overlap, and —
optionally — embedding cosine), then **cluster** the agreeing pairs into groups (transitive closure
/ connected components). Each cluster becomes one **canonical** record carrying every variant and
every mention's provenance, so downstream retrieval can still cite the exact region.

Three things are deliberately GATED (wired, documented, but never executed here — no surprise bills
and no large downloads):
  - the **embedding** match signal (lib.providers.embed; free local path offered, paid path gated),
  - the **LLM adjudication** of ambiguous clusters (lib.providers.llm — the 2025-SOTA tiebreaker),
  - **authority reconciliation** to Wikidata/VIAF (persons) and GeoNames/Getty TGN (places).
Each is a real function with a `gated=True` guard and a clear TODO; the rule-based core runs offline
with zero dependencies beyond numpy/stdlib, so the stage is testable today.

Inputs / outputs (non-destructive — we only write new files):
  IN   data/entities_raw.jsonl   one raw mention per line (the NER stage's output; see SCHEMA below)
  OUT  data/entities.json        canonical records: id,type,canonical_name,variants[],attrs,
                                  mentions[+provenance],confidence
  OUT  data/entities_merge_audit.jsonl   one row per merge decision (who/why/score) — the paper
                                  trail David needs to trust or overrule any grouping.

Scale-up note: for a *much* larger corpus, swap the in-process matcher for **Splink** (probabilistic
record linkage, Fellegi-Sunter; scales to millions on a laptop). The blocking keys and comparison
features here map directly onto Splink's `blocking_rules` and `comparisons` — see `splink_plan()`.
"""
from __future__ import annotations

# ==================================================================
# Imports — grouped under little headers, matching the house style
# ==================================================================
# Core Python Imports
import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

# Third-party (numpy is already a project dependency via the vector store)
import numpy as np

# Local File Imports — same sys.path bootstrap every step_7 module uses
STEP7 = Path(__file__).resolve().parents[1]
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))
import config                       # noqa: E402  (paths + model registry)
from lib import costlog            # noqa: E402  (route EVERY model/API call through here)
from lib import chunks as chunks_lib  # noqa: E402  (reuse the corpus reader for the demo fallback)
# embed + llm are imported lazily inside the gated functions so importing this
# module never reaches for a heavy/paid dependency.


# ==================================================================
# Paths + schema — where we read/write, and the shape of a "mention"
# ==================================================================
RAW_PATH = config.DATA / "entities_raw.jsonl"          # input  (from the NER stage in Part B)
OUT_PATH = config.DATA / "entities.json"               # output (canonical authority records)
AUDIT_PATH = config.DATA / "entities_merge_audit.jsonl"  # output (merge decision log)

# A raw mention (one JSON object per line in entities_raw.jsonl). The NER stage emits these; we only
# *consume* them here. Keeping the contract explicit means the two stages can evolve independently.
#   {
#     "mention_id": "doc_12::doc_12.src_content.0::m3",  # globally unique
#     "type":       "PERSON",                            # PERSON | PLACE | ORG | ... (see TYPES)
#     "surface":    "Mrs. Grace Panyard",                # the text exactly as written
#     "attrs":      {"role": "petitioner", "city": "Detroit"},   # optional fine-grained fields
#     "provenance": {                                    # so a canonical record can cite the region
#         "doc_id":   "doc_12",
#         "rid":      "doc_12.src_content.0",            # region id -> vertices live in the record
#         "page":     3,
#         "pdf_page": 41,
#         "date":     "1933-10-XX",                      # EDTF if the date stage ran, else raw
#         "vertices": [[x,y],[x,y],[x,y],[x,y]],         # IIIF region for deep-zoom citation
#         "min_conf": 0.948                              # OCR confidence of the source region
#     }
#   }
TYPES = {"PERSON", "PLACE", "ORG", "CONDITION", "FAVOR", "ROLE", "DATE", "MISC"}

# Honorifics / titles we strip before comparing names. "Mrs. Grace Panyard" and "Grace Panyard"
# should land in the same bucket, so the title must not pollute the phonetic key.
TITLES = {
    "mr", "mrs", "ms", "miss", "mister", "dr", "doctor", "prof", "professor",
    "rev", "reverend", "fr", "father", "br", "brother", "sr", "sister", "mother",
    "fr.", "st", "saint", "msgr", "monsignor", "bp", "bishop", "card", "cardinal",
    "capt", "captain", "col", "colonel", "sgt", "sergeant", "lt", "lieutenant",
    "hon", "honorable", "gen", "general", "maj", "major",
}

# Common abbreviations -> canonical expansion. This corpus is full of them: "St." for Saint, "Co."
# for County, "Mich." for Michigan, and the order initials ("O.F.M. Cap." — Capuchins). Expanding
# *before* phonetics means "St. Bonaventure" and "Saint Bonaventure" key identically.
ABBREVIATIONS = {
    "st": "saint", "ste": "saint", "co": "county", "cty": "county",
    "mt": "mount", "ft": "fort", "ave": "avenue", "av": "avenue", "blvd": "boulevard",
    "rd": "road", "ln": "lane", "pl": "place", "sq": "square",
    "hosp": "hospital", "hospl": "hospital", "univ": "university", "coll": "college", "par": "parish",
    "ofm": "order of friars minor", "cap": "capuchin", "sma": "seraphic mass association",
    # institutional / religious shorthand this corpus leans on heavily
    "ser": "seraphic", "seraph": "seraphic", "assoc": "association", "assn": "association",
    "asn": "association", "ass": "association", "prov": "providence", "cath": "catholic",
    "rev": "reverend", "ch": "church", "mon": "monastery", "monast": "monastery",
    "soc": "society", "cong": "congregation", "sem": "seminary", "conv": "convent",
    # common medical / condition shorthand (CONDITION type)
    "tb": "tuberculosis", "tbc": "tuberculosis", "op": "operation", "oper": "operation",
    "rhum": "rheumatism", "rheu": "rheumatism", "rheum": "rheumatism", "rheumatic": "rheumatism",
    "pneu": "pneumonia", "appen": "appendicitis", "appendix": "appendicitis",
    # U.S. state abbreviations that show up in addresses (a non-exhaustive starter set):
    "mich": "michigan", "wis": "wisconsin", "wisc": "wisconsin", "ind": "indiana",
    "ill": "illinois", "ohio": "ohio", "ny": "new york", "pa": "pennsylvania",
    "minn": "minnesota", "calif": "california", "cal": "california", "tex": "texas",
    "mo": "missouri", "ky": "kentucky", "tenn": "tennessee", "ga": "georgia",
    "fla": "florida", "md": "maryland",
    # NB: deliberately NOT mapping "mass"->massachusetts or "la"->louisiana — in this Capuchin corpus
    # "Mass" is overwhelmingly the religious Mass and "la" a Spanish/French article, so those state
    # expansions would corrupt far more than they fix.
}

# Multi-word alias PHRASES applied to the whole normalized string BEFORE tokenizing, so an initialism
# and its spelled-out form converge to ONE canonical phrase. (Single-token expansions live in
# ABBREVIATIONS; these need phrase context.) Keys are already accent-folded/lower/space-separated.
ALIAS_PHRASES = {
    "s m a": "seraphic mass association",
    "o f m cap": "capuchin", "o f m capuchin": "capuchin", "ofm cap": "capuchin",
    "o f m": "order of friars minor",
    "seraphic mass assn": "seraphic mass association",
    "t b": "tuberculosis", "t b c": "tuberculosis",         # T.B. -> tuberculosis (dotted initialism)
    "p stroke": "paralytic stroke",
}


# ==================================================================
# STEP 1 — Normalize: turn a messy surface form into comparison keys
# ==================================================================
# The goal of normalization is NOT to "fix" the text — we never overwrite the original surface — but
# to derive *stable keys* a computer can compare. We produce three things from each surface string:
#   - a cleaned token list (titles stripped, abbreviations expanded, punctuation removed),
#   - a normalized display string (those tokens, lower-cased), and
#   - a phonetic code (double-metaphone) so spelling/OCR variants of the same *sound* collide.

def _strip_accents(s: str) -> str:
    """Fold accented letters to ASCII (é -> e). NFKD splits a char into base + combining marks; we
    then drop the marks. Historical OCR is inconsistent about diacritics, so we remove that variable
    entirely before comparing."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


from functools import lru_cache   # noqa: E402  (memoize the pure normalizers — see note below)

# PERFORMANCE: normalize/phonetic are pure functions of the surface string, but pair scoring calls
# them millions of times (once per mention per pairwise comparison). A surface in a 1,400-member block
# would otherwise be re-tokenized + re-metaphoned ~1,400×. Memoizing collapses that to one computation
# per DISTINCT surface — turning an ~10-minute clustering pass into seconds. Results are immutable
# strings (or a list we only ever read), so caching is safe. maxsize=None: the surface vocabulary is
# bounded by the corpus and small relative to the pair count.
@lru_cache(maxsize=None)
def normalize_tokens(surface: str) -> list:
    """Clean a surface form into comparable lower-case tokens.

    Pipeline (each step removes one source of spurious *difference*):
      1. fold accents + lower-case,
      2. split possessives/punctuation into spaces ("O'Donnell" -> "o donnell"),
      3. drop honorific titles ("mrs", "fr"),
      4. expand known abbreviations ("st" -> "saint", "mich" -> "michigan"),
      5. drop now-empty tokens.

    Args:
        surface: The mention text exactly as written in the source.

    Returns:
        A list of normalized tokens (possibly empty for junk input).
    """
    s = _strip_accents(surface).lower()
    # Rejoin OCR line-break hyphenation BEFORE we collapse punctuation: "Hus- band" -> "husband",
    # "Diabe- tis" -> "diabetis", "En- rolled" -> "enrolled". (Only hyphen-then-space; a real hyphen
    # like "heart-trouble" stays two tokens, which the joined-phonetic key still bridges.)
    s = re.sub(r"([a-z])-\s+([a-z])", r"\1\2", s)
    s = re.sub(r"^\s*\d{1,2}[.)]\s+", " ", s)              # strip a leading list-ordinal "1. " / "2) "
    # Apostrophe-contracted abbreviations must expand BEFORE punctuation is collapsed, else "Ass'n" ->
    # "ass n" (two tokens) never expands. Catches the Seraphic Mass Association's many shorthands.
    s = re.sub(r"\bass['’`]?n\b|\bas['’`]n\b|\bassn\b|\bassoc['’`]?n?\b", " association ", s)
    s = re.sub(r"\bcong['’`]?n\b", " congregation ", s)
    # Replace any run of non-alphanumerics with a single space. This is what collapses the many ways
    # OCR renders an apostrophe-name: "O'Donnell" / "O`Donnell" / "ODonnell" / "O Donnell" all
    # become the token pair ["o", "donnell"].
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    # Phrase-level institutional aliases (initialism <-> spelled-out), applied before tokenizing so
    # "S.M.A." ("s m a") and "Seraphic Mass Assoc'n" both converge to "seraphic mass association".
    for k, v in ALIAS_PHRASES.items():
        s = re.sub(rf"\b{re.escape(k)}\b", v, s)
    toks = [t for t in s.split() if t and t not in TITLES]   # step 3: drop honorifics
    expanded = []                                            # step 4: expand (multi-word-safe)
    for t in toks:
        expanded.extend(ABBREVIATIONS.get(t, t).split())
    return [t for t in expanded if t]


@lru_cache(maxsize=None)
def normalized_name(surface: str) -> str:
    """The cleaned tokens joined back into one comparable string ("mrs grace panyard" -> "grace
    panyard"). Used as a display normalization and for token-set comparisons."""
    return " ".join(normalize_tokens(surface))


# ------------------------------------------------------------------
# Double Metaphone — phonetic encoding (vendored, dependency-free)
# ------------------------------------------------------------------
# Phonetic algorithms map a word to a code representing how it *sounds*, so that spellings differing
# only in silent letters or OCR slips ("Panyard"/"Panyord", "Smith"/"Smyth") share a code and thus
# share a block. Double Metaphone (Lawrence Philips, 2000) is the workhorse for English names and,
# unlike Soundex, returns a primary AND an alternate code to capture genuinely ambiguous
# pronunciations (e.g. "Cs" can sound like "K" or "S").
#
# A full Double Metaphone is ~250 lines of consonant-cluster rules. To keep this stage runnable with
# ZERO third-party installs, we ship a compact, well-commented implementation that captures the
# high-value rules (silent letters, common digraphs, the C/G softening, vowel-only-at-start). It is
# good enough to drive blocking; for production you may prefer the `metaphone` / `jellyfish`
# packages — see `_phonetic()` for the drop-in swap (also cost-logged at $0, being local).
_VOWELS = set("AEIOUY")


def double_metaphone(word: str) -> tuple:
    """A compact Double Metaphone. Returns (primary, alternate) codes.

    This is intentionally a *pragmatic* subset of Philips' algorithm — it handles the rules that
    matter most for blocking English surnames and place names (silent initial clusters, digraphs
    like PH/SH/TH/CH, soft vs hard C/G, doubled letters, trailing silent E). It is deterministic and
    fast. When the real package is installed, `_phonetic()` prefers it automatically.

    Args:
        word: A single token (letters only is ideal; digits are dropped).

    Returns:
        (primary_code, alternate_code) — alternate may equal primary.
    """
    w = "".join(ch for ch in word.upper() if ch.isalpha())
    if not w:
        return ("", "")
    primary: list = []
    alternate: list = []
    n = len(w)
    i = 0

    # Silent leading clusters: GN, KN, PN, WR, PS all begin with an unpronounced letter.
    if w[:2] in ("GN", "KN", "PN", "WR", "PS"):
        i = 1
    # A leading X sounds like S ("Xavier" -> S...).
    if w[0] == "X":
        primary.append("S")
        alternate.append("S")
        i = 1

    def add(p: str, a: str | None = None):
        primary.append(p)
        alternate.append(a if a is not None else p)

    while i < n and len(primary) < 8:        # cap code length (metaphone keys are short by design)
        c = w[i]
        nxt = w[i + 1] if i + 1 < n else ""
        prv = w[i - 1] if i > 0 else ""

        if c in _VOWELS:
            # Vowels only count at the very start of the word (metaphone is consonant-skeleton-ish).
            if i == 0:
                add("A")
            i += 1
            continue

        if c == prv and c not in ("C",):     # collapse doubled consonants ("LL" -> one "L")
            i += 1
            continue

        if c == "B":
            add("P") if not (i == n - 1 and prv == "M") else None   # silent B in "...MB" (dumb)
            i += 1
        elif c == "C":
            if w[i:i + 2] == "CH":
                add("X")                      # CH -> "X" (church); "K" alternate for Greek-ish
                if len(alternate) == len(primary):
                    alternate[-1] = "K"
                i += 2
            elif w[i:i + 2] == "CK":
                add("K"); i += 2
            elif nxt in ("I", "E", "Y"):
                add("S"); i += 1              # soft C before front vowels (cent, city)
            else:
                add("K"); i += 1             # hard C otherwise (cat)
        elif c == "D":
            if w[i:i + 3] in ("DGE", "DGI", "DGY"):
                add("J"); i += 3             # "edge" -> J
            else:
                add("T"); i += 1
        elif c == "G":
            if nxt == "H":
                # GH is usually silent ("night") unless it leads a syllable; we drop it (common case).
                i += 2
            elif nxt in ("I", "E", "Y"):
                add("J", "K"); i += 1        # soft G (gem) primary J, hard G alternate
            else:
                add("K"); i += 1
        elif c == "H":
            # H is pronounced only between two vowels or at a word start; otherwise silent.
            if (prv in _VOWELS or i == 0) and nxt in _VOWELS:
                add("H")
            i += 1
        elif c == "J":
            add("J"); i += 1
        elif c in ("K", "Q"):
            add("K"); i += 1
        elif c == "L":
            add("L"); i += 1
        elif c == "M":
            add("M"); i += 1
        elif c == "N":
            add("N"); i += 1
        elif c == "P":
            if nxt == "H":
                add("F"); i += 2            # PH -> F (Philip)
            else:
                add("P"); i += 1
        elif c == "R":
            add("R"); i += 1
        elif c == "S":
            if w[i:i + 2] == "SH":
                add("X"); i += 2           # SH -> "X" (sound)
            elif w[i:i + 3] in ("SIO", "SIA"):
                add("X"); i += 1
            else:
                add("S"); i += 1
        elif c == "T":
            if w[i:i + 2] == "TH":
                add("0"); i += 2           # TH -> "0" (theta), the metaphone convention
            elif w[i:i + 3] in ("TIO", "TIA"):
                add("X"); i += 1
            else:
                add("T"); i += 1
        elif c == "V":
            add("F"); i += 1
        elif c == "W":
            if nxt in _VOWELS:
                add("W")                   # W only voiced before a vowel
            i += 1
        elif c == "X":
            add("KS"); i += 1
        elif c == "Z":
            add("S"); i += 1
        else:
            i += 1                          # anything unexpected: skip

    return ("".join(primary), "".join(alternate))


def _phonetic(token: str) -> tuple:
    """Phonetic code for one token, preferring a real library if present (drop-in upgrade path).

    Tries `metaphone.doublemetaphone` then `jellyfish.metaphone`; falls back to our vendored
    `double_metaphone`. This is pure-local CPU work called in a tight inner loop, so it is NOT
    cost-logged (it never bills, and per-token logging would just flood usage.csv) — only the
    embedding/LLM/authority *services* route through costlog, which is the point of that ledger.
    """
    try:
        from metaphone import doublemetaphone                  # type: ignore
        return doublemetaphone(token)
    except Exception:
        pass
    try:
        import jellyfish                                        # type: ignore
        code = jellyfish.metaphone(token)
        return (code, code)
    except Exception:
        pass
    return double_metaphone(token)


@lru_cache(maxsize=None)
def phonetic_key(surface: str) -> str:
    """A whole-mention phonetic key: primary double-metaphone of each normalized token, joined.

    Example: "Mrs. Grace Panyard" -> tokens ["grace","panyard"] -> "KRS|PNRT". Two surfaces that
    *sound* the same get the same key and therefore the same block, which is exactly the candidate
    generation we want for OCR/spelling variants.
    """
    toks = normalize_tokens(surface)
    return "|".join(_phonetic(t)[0] for t in toks if t)


@lru_cache(maxsize=None)
def phonetic_key_joined(surface: str) -> str:
    """A SECOND phonetic key over the tokens *concatenated*, then encoded as one word.

    Why we need both: `phonetic_key` encodes token-by-token, so a name that OCR splits differently —
    "O'Donnell" (one written token) vs "O Donnell" (two) — produces different per-token keys and
    would NOT co-block. Joining first ("odonnell") and encoding the whole thing yields the SAME code
    for every split, so these genuinely-equal surfaces always share a block. We key on both so a
    match survives either tokenization.
    """
    joined = "".join(normalize_tokens(surface))
    return _phonetic(joined)[0] if joined else ""


# ==================================================================
# STEP 2 — Block: cheap candidate generation (avoid O(n^2))
# ==================================================================
# A "block" is a bucket of mentions that *might* be the same entity, keyed so we only ever compare
# within a bucket. We key on (type, phonetic, coarse-location, coarse-date) — but to avoid being so
# strict that real matches fall into different buckets, we emit SEVERAL keys per mention (multi-pass
# blocking / "canopy" style) and union the candidate pairs. A mention with a city joins both a
# "phonetic+city" block and a looser "phonetic-only" block, so a match survives even if one record
# lacks the city.

def _coarse_date(prov: dict) -> str:
    """Year bucket from a provenance date (EDTF or raw). People recur across years, so we bucket on
    the YEAR only — tight enough to separate unrelated same-name people in different decades, loose
    enough not to split one person's mentions across adjacent entries."""
    d = str(prov.get("date") or "")
    m = re.search(r"(1[89]\d\d|20\d\d)", d)               # any 18xx/19xx/20xx year
    return m.group(1) if m else ""


def _coarse_location(attrs: dict, prov: dict) -> str:
    """A normalized city/place token for blocking (first token of any city-ish attr)."""
    loc = attrs.get("city") or attrs.get("location") or attrs.get("place") or prov.get("location") or ""
    toks = normalize_tokens(str(loc))
    return toks[0] if toks else ""


def blocking_keys(m: dict) -> list:
    """All blocking keys for a mention (multi-pass). Each key is `type:dimensions...`.

    Args:
        m: A raw mention dict (see SCHEMA at top of file).

    Returns:
        A list of string keys; mentions sharing ANY key become candidate pairs.
    """
    typ = m.get("type", "MISC")
    surface = m.get("surface", "")
    pk = phonetic_key(surface)
    pkj = phonetic_key_joined(surface)                     # tokenization-robust key (see helper)
    if not pk and not pkj:
        return []                                          # nothing comparable (e.g. pure punctuation)
    loc = _coarse_location(m.get("attrs", {}), m.get("provenance", {}))
    yr = _coarse_date(m.get("provenance", {}))
    keys = []
    if pk:
        keys.append(f"{typ}:ph={pk}")                      # pass 1: per-token phonetic (permissive)
        if loc:
            keys.append(f"{typ}:ph={pk}:loc={loc}")       # pass 2: + location (tightens persons/places)
        if yr:
            keys.append(f"{typ}:ph={pk}:yr={yr}")         # pass 3: + year
    # Pass 4: joined phonetic — the bridge across different tokenizations of the SAME name. A
    # multi-token name ("O Donnell") and the single-token form another scribe wrote ("ODonnell")
    # must meet in a shared block, so BOTH emit `phj`. We always add it (a single-token name's `phj`
    # may duplicate `ph`, which is harmless — a duplicate block key just merges into the same bucket).
    if pkj:
        keys.append(f"{typ}:phj={pkj}")                    # split-robust bridge
    return keys


def build_blocks(mentions: list) -> dict:
    """Invert mentions -> {blocking_key: [mention_index, ...]}. Singleton blocks are kept (a mention
    with no partner still becomes its own one-member entity downstream)."""
    blocks: dict = defaultdict(list)
    for idx, m in enumerate(mentions):
        for key in blocking_keys(m):
            blocks[key].append(idx)
    return blocks


# ==================================================================
# STEP 3 — Match: score a candidate pair with an ensemble of signals
# ==================================================================
# Within a block we score each pair on cheap, interpretable features and combine them into one
# similarity in [0,1]. Interpretable beats a black box here because David must be able to read the
# audit log and agree/disagree with each merge. The optional embedding signal (below) slots in as a
# fourth feature when enabled.

def _jaccard(a: set, b: set) -> float:
    """Token-set overlap: |A∩B| / |A∪B|. Catches reorderings/extra tokens ("grace panyard" vs
    "panyard grace") that edit distance would punish."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _levenshtein_ratio(a: str, b: str) -> float:
    """Normalized edit-distance similarity in [0,1] (1 == identical). Plain DP — fine at name
    lengths. Catches single-character OCR slips ("panyard" vs "panyord")."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        cur = [i] + [0] * lb
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
        prev = cur
    dist = prev[lb]
    return 1.0 - dist / max(la, lb)


def pair_score(m1: dict, m2: dict, emb_sims: dict | None = None) -> tuple:
    """Combine signals into one similarity + a human-readable reason.

    Signals (weights chosen to be conservative — favour precision, since a wrong *merge* is harder to
    notice than a wrong *split* and corrupts retrieval):
      - phonetic equality  (0.35) — same double-metaphone key,
      - levenshtein ratio  (0.30) — character-level closeness of normalized names,
      - token jaccard      (0.20) — set overlap of normalized tokens,
      - attribute agreement(0.15) — same city/role/etc. where both supply it,
      - embedding cosine   (bonus, only if `emb_sims` provided by the gated embedding signal).

    Args:
        m1, m2:   Two raw mentions.
        emb_sims: Optional {frozenset({mid1, mid2}): cosine} from `embedding_signal()`.

    Returns:
        (score in [0,1], reason dict with each component) — the reason goes straight into the audit.
    """
    n1, n2 = normalized_name(m1["surface"]), normalized_name(m2["surface"])
    t1, t2 = set(n1.split()), set(n2.split())
    # Phonetic equality: accept EITHER the per-token key OR the joined key. Taking both keeps the
    # matcher consistent with blocking — a name OCR split differently ("O Donnell" vs "ODonnell")
    # has equal *joined* phonetics even when the per-token keys differ, and should still count as a
    # phonetic hit. Without this, blocking would propose the pair but matching would reject it.
    ph = 1.0 if (phonetic_key(m1["surface"]) == phonetic_key(m2["surface"])
                 or phonetic_key_joined(m1["surface"]) == phonetic_key_joined(m2["surface"])) else 0.0
    # Levenshtein on the SPACELESS forms, so a stray space from OCR tokenization doesn't penalize an
    # otherwise-identical name ("odonnell" vs "o donnell" -> 1.0, not 0.889).
    lev = _levenshtein_ratio(n1.replace(" ", ""), n2.replace(" ", ""))
    jac = _jaccard(t1, t2)

    # Attribute agreement: reward shared, contradict-penalize differing known attrs (city/role).
    a1, a2 = m1.get("attrs", {}), m2.get("attrs", {})
    shared = [k for k in ("city", "location", "role", "order") if k in a1 and k in a2]
    if shared:
        agree = sum(normalized_name(str(a1[k])) == normalized_name(str(a2[k])) for k in shared) / len(shared)
    else:
        agree = 0.5                                        # neutral when we simply don't know

    score = 0.35 * ph + 0.30 * lev + 0.20 * jac + 0.15 * agree
    reason = {"phonetic_eq": ph, "lev": round(lev, 3), "jaccard": round(jac, 3), "attr_agree": round(agree, 3)}

    # Optional embedding bonus (only present when the gated signal ran). We blend rather than replace,
    # so the decision stays mostly interpretable.
    if emb_sims is not None:
        cos = emb_sims.get(frozenset({m1["mention_id"], m2["mention_id"]}))
        if cos is not None:
            reason["embed_cos"] = round(float(cos), 3)
            score = 0.85 * score + 0.15 * float(cos)
    return min(1.0, score), reason


# ------------------------------------------------------------------
# Embedding match signal — GATED (free local path offered; paid gated)
# ------------------------------------------------------------------
def embedding_signal(mentions: list, candidate_pairs: list,
                     gated: bool = True, model: str | None = None, dim: int | None = None) -> dict:
    """Cosine similarity over mention embeddings, computed ONLY for candidate pairs.

    WHY embeddings help: phonetics/edit-distance are blind to *meaning*. "the Capuchin friary at
    Harlem" and "St. Bonaventure Monastery" share no characters but embed near each other. We embed
    each mention (surface + a little attr context) once, then read off cosine for the pairs blocking
    already proposed — never the full n^2.

    Cost discipline: this routes through `lib.providers.embed`, which cost-logs every call. The
    **local** path (`bge-*` via fastembed) is $0 and downloads a small model on first use; the
    **paid** path (gemini/openai/voyage) bills per token. Both are GATED behind `gated` so nothing
    runs unless David flips it AND (for paid models) provides a key.

    Args:
        mentions:        Full mention list (for id->text lookup).
        candidate_pairs: List of (i, j) index pairs from blocking.
        gated:           If True (default) returns {} without embedding anything.
        model, dim:      Embedding space; default = a free local model so the smoke path costs $0.

    Returns:
        {frozenset({mid_i, mid_j}): cosine_float} for the requested pairs (empty when gated).
    """
    if gated:
        # TODO(david): set gated=False to enable. For $0 keep model="bge-small-en-v1.5", dim=384
        #              (local fastembed). For higher quality flip to a paid space + provide its key.
        return {}

    from lib.providers import embed                          # lazy import: only when actually used

    model = model or "bge-small-en-v1.5"                     # free local default
    dim = dim or 384
    texts, id_for_row = [], []
    for m in mentions:
        ctx = m["surface"]
        a = m.get("attrs", {})
        if a:
            ctx += " | " + ", ".join(f"{k}: {v}" for k, v in a.items())
        texts.append(ctx)
        id_for_row.append(m["mention_id"])
    vecs, _ = embed.embed_texts(texts, model, dim, task="document")   # <-- cost-logged inside
    row_of = {mid: r for r, mid in enumerate(id_for_row)}
    sims: dict = {}
    for i, j in candidate_pairs:
        vi, vj = vecs[row_of[mentions[i]["mention_id"]]], vecs[row_of[mentions[j]["mention_id"]]]
        sims[frozenset({mentions[i]["mention_id"], mentions[j]["mention_id"]})] = float(np.dot(vi, vj))
    return sims


# ------------------------------------------------------------------
# LLM adjudication of ambiguous clusters — GATED (wired, not run)
# ------------------------------------------------------------------
def llm_adjudicate(cluster_mentions: list, gated: bool = True, model: str | None = None) -> dict:
    """Ask an LLM whether a borderline cluster is really one entity (2025-SOTA tiebreaker).

    Rules + embeddings resolve the easy/medium cases; the residue — same surname, different city; a
    nickname with no overlap — is where an LLM's world knowledge shines (in-context clustering /
    LLM-as-judge report ~94% on name variants). We only call it on clusters flagged ambiguous, and
    the model returns a STRUCTURED verdict so the result is auditable, not prose.

    Cost discipline: routes through `lib.providers.llm.generate`, which cost-logs real token counts.
    GATED by default — this function NEVER calls the API unless `gated=False`.

    Args:
        cluster_mentions: The mentions tentatively grouped together.
        gated:            If True (default) returns a no-op verdict and bills nothing.
        model:            Generation model (default = config.DEFAULTS["llm"]).

    Returns:
        {"decision": "split"|"keep", "subclusters": [[mention_id,...], ...], "rationale": str,
         "confidence": float} — when gated, a {"decision":"keep", ...} no-op is returned.
    """
    if gated:
        # TODO(david): set gated=False to enable LLM adjudication. Recommended only for clusters the
        #              rule+embedding stage marks ambiguous (keeps token spend tiny). Consider the
        #              Batch API (-50%) + prompt caching on the instruction prefix for a full run.
        return {"decision": "keep", "subclusters": [[m["mention_id"] for m in cluster_mentions]],
                "rationale": "gated: no LLM call made", "confidence": 0.0}

    from lib.providers import llm                            # lazy import: only when actually used

    model = model or config.DEFAULTS["llm"]
    listing = "\n".join(
        f'- id={m["mention_id"]} surface="{m["surface"]}" attrs={json.dumps(m.get("attrs", {}))} '
        f'date={m.get("provenance", {}).get("date", "")}'
        for m in cluster_mentions
    )
    system = ("You are an archival entity-resolution adjudicator for the Solanus Casey papers. "
              "Decide whether the listed mentions all refer to the SAME real-world entity. "
              "Be conservative: only keep a merge you are confident about; otherwise split into "
              "subclusters. Reply with strict JSON.")
    prompt = (
        "Mentions:\n" + listing + "\n\n"
        'Return JSON: {"decision":"keep"|"split", '
        '"subclusters":[[ids that are the same entity], ...], '
        '"rationale":"short reason", "confidence":0..1}'
    )
    text, _usage = llm.generate(prompt, model=model, system=system, json_mode=True)  # cost-logged inside
    try:
        verdict = json.loads(text)
    except json.JSONDecodeError:
        verdict = {"decision": "keep",
                   "subclusters": [[m["mention_id"] for m in cluster_mentions]],
                   "rationale": "unparseable LLM reply; defaulted to keep", "confidence": 0.0}
    return verdict


# ==================================================================
# STEP 4 — Cluster: agreeing pairs -> entities (connected components)
# ==================================================================
# Pairwise matches form a graph: mentions are nodes, an edge joins a pair scoring above MATCH_TAU.
# An *entity* is a connected component of that graph (transitive closure: if A~B and B~C then A, B,
# C are one entity). We implement it with a tiny union-find — O(n α(n)), no third-party graph lib.
# AMBIG_TAU < MATCH_TAU marks pairs in the grey zone, so a component touched by any grey edge is
# flagged for (gated) LLM adjudication or David's review queue.
MATCH_TAU = 0.62        # >= this: confident same-entity edge (favours precision; tune on gold)
AMBIG_TAU = 0.50        # [AMBIG_TAU, MATCH_TAU): grey zone -> flag for review/LLM


# ==================================================================
# CAUTIOUS MERGE — precision-first decision layer
#   (design: docs/cautious_merge_spec.json; calibrated on eval/merge_calibration.json)
# ==================================================================
# A wrong MERGE silently corrupts the archive and is hard to detect later; a wrong SPLIT is easily
# fixed. So every decision favours precision: fuzzy merges REQUIRE a distinctive shared "anchor"
# token, hard vetoes pre-empt everything, and the grey zone is handed to an LLM gate that defaults to
# "different". The decision ladder is: VETO -> AUTO-MERGE -> LLM-GATE -> ABSTAIN.
LEV_EXACT_AUTO     = 0.92     # min lev_ratio for the OCR-garble auto-merge (needs phonetic_eq + anchor)
INIT_MIN_LEN       = 3        # initialisms shorter than this are too collision-prone to act on
BLOCK_MAX          = 1500     # refuse all-pairs on blocks larger than this (rely on tighter sub-keys)
LLM_CONFIDENCE_MIN = 0.80     # LLM must be >= this confident on "same" to upgrade a grey pair
ANCHOR_DF_TOP_FRAC = 0.01     # a token in the top 1% per-type doc-frequency is "common" (non-anchor)
LOW_RISK_AUTO = {"PLACE", "ORG", "CONDITION", "FAVOR", "MISC", "DATE"}  # exact-key auto-merge OK
MAX_LLM_PAIRS = int(os.environ.get("RESOLVE_MAX_LLM_PAIRS", "12000"))  # cap on LLM-gate calls/run (strongest first)
# Institution head-nouns: a friary is NOT a monastery is NOT a hospital, even at the same saint/site.
INSTITUTION_HEADS = {"friary", "monastery", "hospital", "church", "convent", "parish", "seminary",
                     "college", "university", "cathedral", "shrine", "chapel", "asylum", "sanitarium",
                     "sanatorium", "academy", "orphanage", "institute", "clinic", "rectory", "abbey"}

STOP_INIT = {"of", "the", "and", "for", "de", "du", "la", "le", "a", "an", "los", "el"}
# Bare generic / relationship / pronominal words — NOT specific named people. Never a merge anchor.
GENERIC_PERSON_WORDS = {
    "husband", "wife", "mother", "father", "son", "daughter", "parents", "parent", "children",
    "child", "baby", "babe", "boy", "girl", "sister", "brother", "grandma", "grandmother",
    "grandfather", "grandpa", "aunt", "uncle", "cousin", "niece", "nephew", "family", "friend",
    "neighbor", "neighbour", "patient", "man", "woman", "lady", "gentleman", "widow", "widower",
    "self", "myself", "him", "her", "them", "boyfriend", "girlfriend", "spouse", "twins",
}
TITLE_GENDER = {"mr": "m", "master": "m", "mrs": "f", "miss": "f", "ms": "f"}          # Mr vs Mrs => diff person
GEN_SUFFIX   = {"sr", "jr", "ii", "iii", "iv", "senior", "junior", "2nd", "3rd"}      # Sr vs Jr => diff person
NEG_WORDS    = {"non", "not", "no", "never", "without", "un"}
AVERT_WORDS  = {"avert", "averted", "avoid", "avoided", "prevent", "prevented", "protection",
                "fear", "feared", "fears", "threatened", "danger", "against", "lest"}
GOOD_OUTCOME = {"cured", "cure", "healed", "healing", "recovered", "recovery", "ok", "well",
                "better", "saved", "granted", "improved", "improving"}
BAD_OUTCOME  = {"died", "death", "dead", "worse", "worsened", "hopeless", "failed", "incurable",
                "terminal", "fatal"}


def initials_of(surface: str) -> str:
    """Initials of the EXPANDED content tokens (stopwords dropped): 'Seraphic Mass Association' ->
    'sma'; 'S.M.A.' (-> tokens s,m,a) -> 'sma' — so an initialism and its spelled-out form match."""
    return "".join(t[0] for t in normalize_tokens(surface) if t and t not in STOP_INIT)


def _gender_token(surface: str):
    """Courtesy-title gender ('m'/'f') if present, else None — Mr vs Mrs of one surname = two people."""
    for t in re.sub(r"[^a-z ]", " ", _strip_accents(surface).lower()).split():
        if t in TITLE_GENDER:
            return TITLE_GENDER[t]
    return None


def _gen_suffixes(surface: str) -> set:
    """Generational suffixes present (Sr/Jr/III) — a difference here means different individuals."""
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", _strip_accents(surface).lower()).split()} & GEN_SUFFIX


def _polarity(surface: str) -> tuple:
    """(negated, good_outcome, bad_outcome) flags for FAVOR/CONDITION/OUTCOME polarity vetoes."""
    toks = set(normalize_tokens(surface)) | set(re.sub(r"[^a-z ]", " ", surface.lower()).split())
    return (bool(toks & NEG_WORDS) or bool(toks & AVERT_WORDS),
            bool(toks & GOOD_OUTCOME), bool(toks & BAD_OUTCOME))


def is_generic_only(surface: str) -> bool:
    """True if the surface is ONLY generic relationship/role words (husband, the wife, my mother)."""
    toks = normalize_tokens(surface)
    return bool(toks) and all(t in GENERIC_PERSON_WORDS for t in toks)


# Devotional figures — real to the corpus but NOT the human correspondents the PERSON network is about.
_DEITY = {"jesus", "christ", "god", "lord", "our lord", "holy ghost", "holy spirit", "saviour",
          "savior", "our saviour", "our savior", "blessed mother", "our lady", "holy family"}
_DETERMINERS = {"his", "her", "my", "your", "their", "our", "the", "a", "an"}


def nonperson_reason(surface: str) -> str | None:
    """Why this PERSON surface is NOT a named human (so it can be dropped from the person network):
    a bare kinship/role word, a title/punctuation-only stub, initials-only, a quantified group, a
    possessive anaphor ('his wife'), or a deity. Returns the reason, or None if it looks like a name.
    """
    raw = (surface or "").strip()
    toks = normalize_tokens(raw)                            # titles stripped, lowercased
    if not toks:
        return "title_or_punct_only"                        # "Dr.", "Mrs. ?", "—"
    low = " ".join(toks)
    if low in _DEITY:
        return "deity"
    if all(t in GENERIC_PERSON_WORDS for t in toks):
        return "kinship"
    nondet = [t for t in toks if t not in _DETERMINERS]     # "his wife" -> ["wife"]
    if nondet and all(t in GENERIC_PERSON_WORDS for t in nondet):
        return "anaphora"
    if re.match(r"^([a-z]\.?\s*){1,4}$", low):              # "m", "t b", "n n" (initials only)
        return "initials_only"
    if re.match(r"^\d+\s+(small |little |grand[- ]|dear |young |holy )*"
                r"(child|children|kid|kids|priest|nun|novice|people|person|sister|brother|son|"
                r"daughter|baby|babies|men|women|boy|girl|patient)", raw.lower()):
        return "quantified_group"
    return None


def cleanup_nonpersons(write: bool = True) -> dict:
    """One-off: drop PERSON entities whose canonical name is a non-person (kinship/title/initials/group/
    anaphor/deity) from entities.json — applies the NER quality gate retroactively without a re-resolve.
    Backs up to entities_with_nonpersons.json. Returns a count by reason."""
    doc = json.loads(OUT_PATH.read_text())
    ents = doc.get("entities", [])
    kept, reasons = [], defaultdict(int)
    for e in ents:
        r = nonperson_reason(e.get("canonical_name", "")) if e.get("type") == "PERSON" else None
        if r:
            reasons[r] += 1
        else:
            kept.append(e)
    if write:
        (config.DATA / "entities_with_nonpersons.json").write_text(OUT_PATH.read_text())
        doc["entities"] = kept
        doc["_meta"] = {**doc.get("_meta", {}), "nonperson_cleanup": dict(reasons), "n_entities": len(kept)}
        OUT_PATH.write_text(json.dumps(doc, ensure_ascii=False, indent=1))
    print(f"cleanup_nonpersons: {len(ents)} -> {len(kept)} entities (dropped {sum(reasons.values())}: {dict(reasons)})")
    return {"before": len(ents), "after": len(kept), "dropped": dict(reasons)}


def build_common_tokens(mentions: list) -> dict:
    """Per-type set of 'common' tokens (top ANCHOR_DF_TOP_FRAC by document frequency) — these are NOT
    distinctive, so sharing only a common token (a surname like 'smith', a head noun like 'hospital')
    cannot anchor a fuzzy merge. Document frequency = number of DISTINCT surfaces a token appears in."""
    seen: dict = defaultdict(lambda: defaultdict(set))      # type -> token -> {surfaces}
    for m in mentions:
        typ, surf = m.get("type", "MISC"), m["surface"]
        for tok in set(normalize_tokens(surf)):
            seen[typ][tok].add(surf)
    common: dict = {}
    for typ, toks in seen.items():
        dfs = sorted((len(s) for s in toks.values()), reverse=True)
        if len(dfs) < 50:
            common[typ] = set()                              # too few to call anything "common"
            continue
        cut = dfs[max(0, int(len(dfs) * ANCHOR_DF_TOP_FRAC)) - 1]
        thresh = max(cut, 4)                                 # also require it to actually recur
        common[typ] = {tok for tok, s in toks.items() if len(s) >= thresh}
    return common


def shared_anchor(s1: str, s2: str, typ: str, common: dict) -> tuple:
    """(has_anchor, distinctive_tokens): do the two surfaces share a DISTINCTIVE token (a surname /
    rare given name / proper place-or-institution token) — NOT merely a common name or generic head
    noun? Required-true for any fuzzy (non-exact) merge, so 'Mary X' never merges with 'Mary Y'."""
    t1, t2 = set(normalize_tokens(s1)), set(normalize_tokens(s2))
    com = common.get(typ, set())
    distinctive = {t for t in (t1 & t2)
                   if len(t) > 2 and t not in com and t not in GENERIC_PERSON_WORDS and t not in STOP_INIT}
    return bool(distinctive), distinctive


def abbrev_expansion_match(s1: str, s2: str) -> bool:
    """True if the two surfaces are the same after alias/abbreviation expansion, OR one is a verified
    initialism of the other (single expanded token == the other's initials, other side multi-token)."""
    n1, n2 = normalized_name(s1), normalized_name(s2)
    if n1 and n1 == n2:
        return True
    t1, t2 = n1.split(), n2.split()
    i1, i2 = initials_of(s1), initials_of(s2)
    if len(t1) == 1 and len(t1[0]) >= INIT_MIN_LEN and t1[0] == i2 and len(t2) >= 2:
        return True
    if len(t2) == 1 and len(t2[0]) >= INIT_MIN_LEN and t2[0] == i1 and len(t1) >= 2:
        return True
    return False


def _initial_expansion(t1: set, t2: set) -> bool:
    """True if one side has a lone initial that begins a full token on the other (S. <-> Solanus,
    E'd <-> Edward) — the classic abbreviated-given-name case, used WITH a shared surname anchor."""
    s1 = {t for t in t1 if len(t) == 1}
    s2 = {t for t in t2 if len(t) == 1}
    f1 = {t for t in t1 if len(t) > 1}
    f2 = {t for t in t2 if len(t) > 1}
    return (any(f.startswith(s) for s in s1 for f in f2) or
            any(f.startswith(s) for s in s2 for f in f1))


def _hard_veto(m1: dict, m2: dict, typ: str) -> str | None:
    """STEP A — return a veto reason (never-merge) or None. The precision backbone."""
    s1, s2 = m1["surface"], m2["surface"]
    if typ == "PERSON" and (is_generic_only(s1) or is_generic_only(s2)):
        return "generic_word"
    g1, g2 = _gender_token(s1), _gender_token(s2)
    if g1 and g2 and g1 != g2:
        return "title_gender"
    su1, su2 = _gen_suffixes(s1), _gen_suffixes(s2)
    if (su1 or su2) and su1 != su2:
        return "generational_suffix"
    if typ in ("FAVOR", "CONDITION", "OUTCOME", "MISC"):
        neg1, good1, bad1 = _polarity(s1)
        neg2, good2, bad2 = _polarity(s2)
        if neg1 != neg2:
            return "polarity_negation"
        if (good1 and bad2) or (bad1 and good2):
            return "polarity_outcome"
    if typ in ("PLACE", "ORG"):
        h1 = set(normalize_tokens(s1)) & INSTITUTION_HEADS
        h2 = set(normalize_tokens(s2)) & INSTITUTION_HEADS
        if h1 and h2 and h1 != h2:                          # friary vs monastery vs hospital => distinct
            return "institution_type"
    return None


def decide_pair(m1: dict, m2: dict, common: dict, emb_sims: dict | None = None) -> tuple:
    """The cautious decision ladder for one candidate pair.

    Returns (decision, score, reason) where decision is one of:
      'veto'    — a hard guard fired; never merge (and don't even ask the LLM),
      'merge'   — deterministic high-precision auto-merge,
      'llm'     — grey/soft-risk; route to the LLM precision gate (default 'different'),
      'abstain' — leave separate (the safe default).
    """
    s1, s2 = m1["surface"], m2["surface"]
    typ = m1.get("type", "MISC")
    score, reason = pair_score(m1, m2, emb_sims)
    n1, n2 = normalized_name(s1), normalized_name(s2)
    anchored, distinctive = shared_anchor(s1, s2, typ, common)
    reason["anchor"] = sorted(distinctive)[:3]

    # STEP A — hard vetoes
    veto = _hard_veto(m1, m2, typ)
    if veto:
        reason["veto"] = veto
        return "veto", score, reason

    exact = bool(n1) and n1 == n2
    abbrev = abbrev_expansion_match(s1, s2)
    t1, t2 = set(n1.split()), set(n2.split())
    shared_nongeneric = {t for t in (t1 & t2)
                         if len(t) > 2 and t not in GENERIC_PERSON_WORDS and t not in STOP_INIT}
    set_equal = bool(t1) and t1 == t2                        # same tokens, maybe reordered
    subset = bool(t1) and bool(t2) and (t1 <= t2 or t2 <= t1)  # one is the other + extra tokens

    # STEP B — deterministic auto-merge (high precision)
    if exact and typ in LOW_RISK_AUTO:
        return "merge", 1.0, {**reason, "rule": "exact_lowrisk"}
    if exact and typ in ("PERSON", "ROLE") and (anchored or typ == "ROLE"):
        return "merge", 1.0, {**reason, "rule": "exact_anchored"}
    # token-set equality (reordering): same tokens => same entity, provided it's not a single bare
    # common token (a lone surname like "smith"), which stays risky -> routed to the LLM below.
    if set_equal and (typ in LOW_RISK_AUTO or anchored or len(t1) >= 2):
        return "merge", 1.0, {**reason, "rule": "tokenset_equal"}
    if reason.get("phonetic_eq") == 1.0 and reason.get("lev", 0) >= LEV_EXACT_AUTO and anchored:
        return "merge", score, {**reason, "rule": "ocr_garble_anchored"}
    if abbrev and (anchored or exact or typ in ("ORG", "MISC", "PLACE")):
        return "merge", max(score, MATCH_TAU), {**reason, "rule": "abbrev_expansion"}

    # DATES are exact-only (handled above): two close-but-unequal dates are DIFFERENT, never a fuzzy
    # merge. Routing them to the LLM both wastes the budget and risks collapsing distinct dates.
    if typ == "DATE":
        return "abstain", score, reason

    # STEP C — LLM precision gate. Routing is TIGHT, because a generous "any shared token" rule floods
    # the gate (1.5M pairs inside big phonetic blocks). For NAMED entities (person/place/org/role) the
    # distinctive-anchor / abbreviation / subset / set-equal filters bound it — a common-surname-only
    # pair never routes (it would just cost an LLM call to be told "different"). For CONCEPT types
    # (condition/favor/misc/outcome) there is no "same string, different entity" risk, so we ALSO route
    # near-string matches (phonetic-equal or high edit-similarity) to catch OCR/abbrev/plural variants.
    if typ in ("CONDITION", "FAVOR", "MISC", "OUTCOME"):
        # concept types: no "same string, different entity" risk -> also route near-string matches
        candidate = (anchored or abbrev or subset or set_equal
                     or reason.get("phonetic_eq") == 1.0 or reason.get("lev", 0) >= 0.80)
    else:
        # named entities: route any plausible candidate to the LLM (which defaults to 'different', so a
        # same-surname-different-person pair is cheaply rejected). Component-dedup keeps the call count
        # bounded; being generous here is what recovers recall (Solanus/Casey/Doll variants etc.).
        candidate = (anchored or abbrev or subset or set_equal or bool(shared_nongeneric)
                     or _initial_expansion(t1, t2))
    if candidate:
        return "llm", score, {**reason, "route": "llm", "abbrev": abbrev}

    # STEP D — abstain (safe default)
    return "abstain", score, reason


def llm_same_pair(m1: dict, m2: dict, model: str | None = None) -> dict:
    """LLM precision gate for ONE grey pair. Default 'different'; returns {same, confidence, reason}.

    A wrong merge silently corrupts the archive, so the prompt makes the model default to 'different'
    and only assert 'same' for a clear acronym<->expansion, OCR/spelling variant, or identical full
    name. Routed through lib.providers.llm.generate (cost-logged); errors fail safe to 'different'.
    """
    from lib.providers import llm                            # lazy import: only when actually used
    model = model or config.DEFAULTS["llm"]
    typ = (m1.get("type") or "entity").lower()
    prompt = (
        f'Two {typ} references from the Father Solanus Casey archive (1920s-1950s Capuchin '
        f'correspondence + prayer-favor notebooks; heavy OCR noise).\n'
        f'  A: "{m1["surface"]}"\n  B: "{m2["surface"]}"\n\n'
        f'Are A and B the SAME real-world {typ}? A wrong MERGE silently corrupts the archive and is '
        f'hard to detect, while a split is easily fixed — so DEFAULT to "different". Answer same=true '
        f'ONLY when it is clearly more likely than not, with no plausible competing referent: an '
        f'acronym and its spelled-out form, an OCR/abbreviation/spelling variant, or the same full '
        f'name. Two people who merely share a surname OR a first name are DIFFERENT. A courtesy-title '
        f'gender difference (Mr vs Mrs) or a generational suffix (Sr vs Jr) means DIFFERENT people. '
        f'Generic words (wife, husband, mother) are not a specific entity. '
        f'Reply STRICT JSON: {{"same": true|false, "confidence": 0.0-1.0, "reason": "<short>"}}.'
    )
    try:
        txt, _ = llm.generate(prompt, model=model, json_mode=True, temperature=0.0)
        v = json.loads(txt)
        return {"same": bool(v.get("same")), "confidence": float(v.get("confidence", 0.0)),
                "reason": str(v.get("reason", ""))[:200]}
    except Exception as e:                                   # fail safe: never merge on an error
        return {"same": False, "confidence": 0.0, "reason": f"adjudicator error: {str(e)[:80]}"}


def adjudicate_pairs(llm_pairs: list, mentions: list, model: str | None = None,
                     workers: int | None = None) -> list:
    """Resolve grey pairs through llm_same_pair IN PARALLEL (latency-bound calls must overlap — same
    lesson as NER/embeddings). Returns verdicts aligned 1:1 with llm_pairs."""
    from concurrent.futures import ThreadPoolExecutor
    workers = workers or int(os.environ.get("RESOLVE_LLM_WORKERS", "12"))
    print(f"  LLM precision gate: {len(llm_pairs)} grey pair(s) on {max(1, workers)} workers...")

    def _work(pair):
        i, j, _ = pair
        return llm_same_pair(mentions[i], mentions[j], model)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        return list(ex.map(_work, llm_pairs))


class _UnionFind:
    """Disjoint-set with path compression + union by rank. Plain, fast, and easy to audit."""

    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]    # halve the path as we go
            x = self.parent[x]
        return x

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def cluster_mentions(mentions: list, blocks: dict, common: dict, emb_sims: dict | None = None) -> tuple:
    """Run the cautious decision ladder over within-block pairs.

    Unlike the old single-threshold version, each candidate pair is routed by ``decide_pair`` into
    auto-merge / LLM-gate / abstain / veto. We do NOT call the LLM here (that would serialize a
    network call inside the O(pairs) loop); instead we collect the grey pairs and let ``run`` resolve
    them in parallel, then union the confirmed ones.

    Args:
        mentions: the full mention list.
        blocks:   {blocking_key: [indices]} from build_blocks.
        common:   per-type common-token sets from build_common_tokens (for the anchor gate).
        emb_sims: optional embedding cosines from the (gated) embedding signal.

    Returns:
        (auto_edges, llm_pairs, audit) where
          auto_edges = [(i, j)]                deterministic high-precision merges,
          llm_pairs  = [(i, j, reason)]        grey pairs for the LLM precision gate,
          audit      = [row, ...]              every non-abstain decision (incl. vetoes) for the log.
    """
    auto_edges: list = []
    llm_pairs: list = []
    audit: list = []
    seen_pairs: set = set()                                  # a pair can appear in several blocks
    skipped_blocks = 0

    for key, idxs in blocks.items():
        if len(idxs) < 2:
            continue                                         # singleton block: nothing to compare
        if len(idxs) > BLOCK_MAX:
            # A giant block is almost always a generic-token pile-up. Refuse the O(n^2) all-pairs here;
            # real pairs still meet in the tighter sub-key blocks (ph+loc / ph+yr / init / phjx).
            skipped_blocks += 1
            continue
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                i, j = idxs[a], idxs[b]
                pk = (i, j) if i < j else (j, i)
                if pk in seen_pairs:
                    continue
                seen_pairs.add(pk)
                decision, score, reason = decide_pair(mentions[i], mentions[j], common, emb_sims)
                if decision == "abstain":
                    continue                                 # leave separate; don't even log
                row = {"a": mentions[i]["mention_id"], "b": mentions[j]["mention_id"],
                       "a_surface": mentions[i]["surface"], "b_surface": mentions[j]["surface"],
                       "type": mentions[i].get("type", "MISC"), "block_key": key,
                       "score": round(score, 3), "decision": decision, "signals": reason}
                audit.append(row)
                if decision == "merge":
                    auto_edges.append((i, j))
                elif decision == "llm":
                    llm_pairs.append((i, j, reason))
                # "veto" -> recorded, never merged
    if skipped_blocks:
        print(f"  [block guard] skipped {skipped_blocks} block(s) > BLOCK_MAX={BLOCK_MAX} "
              f"(generic-token pile-ups; real pairs still meet in tighter sub-keys)")
    return auto_edges, llm_pairs, audit


# ==================================================================
# STEP 5 — Canonicalize: one authority record per entity
# ==================================================================
# A component of mentions becomes a single record. We pick the canonical name (the most "complete"
# variant — longest by token count, breaking ties by frequency), collect every distinct surface as a
# variant, merge attributes, gather mention provenance (so the record can cite exact regions), and
# attach a confidence derived from internal agreement + OCR confidence of the underlying regions.

def _canonical_name(surfaces: list) -> str:
    """Choose a clean, well-attested surface as the display name.

    We want the form people actually WROTE most often — not the longest string. Under OCR the longest
    surface is usually a garbled multi-line run (e.g. "Fr. Solanus O.F.M. Cap. Delegate Fr. Solanus,
    O.7.M. Cap."), so a length-first rule reliably picks the WORST variant as the label. Instead we
    (1) drop outlier-long surfaces (>6 tokens almost always means concatenated OCR noise for a
    name/place/condition) — unless every surface is long, then keep them all — and (2) rank the rest by
    frequency, preferring a fuller-but-concise form and then the shorter raw string on ties (less OCR
    cruft like trailing punctuation/spaces). "Grace Panyard" still beats a one-off "Grace."
    """
    freq: dict = defaultdict(int)
    for s in surfaces:
        freq[s.strip()] += 1
    uniq = list(freq)
    toks = {s: len(normalize_tokens(s)) for s in uniq}
    concise = [s for s in uniq if 1 <= toks[s] <= 6] or uniq
    return sorted(concise, key=lambda s: (freq[s], toks[s], -len(s)), reverse=True)[0]


def _entity_id(typ: str, canonical: str, seq: int) -> str:
    """Stable, human-legible id, e.g. 'person:grace_panyard:0007'. The seq guarantees uniqueness even
    when two different real people normalize to the same slug."""
    slug = re.sub(r"[^a-z0-9]+", "_", normalized_name(canonical)).strip("_") or "unnamed"
    return f"{typ.lower()}:{slug}:{seq:04d}"


def _confidence(members: list, mentions: list) -> float:
    """A 0..1 trust score for a cluster: blend of (a) how internally consistent the names are and
    (b) the average OCR confidence of the source regions. A two-mention cluster of identical strings
    pulled from high-confidence regions should score near 1.0; a sprawling, ragged cluster lower."""
    surfaces = [normalized_name(mentions[i]["surface"]) for i in members]
    # Internal name agreement: mean pairwise levenshtein (cap the pair count for big clusters).
    if len(surfaces) == 1:
        name_agree = 1.0
    else:
        pairs = [(a, b) for a in range(len(surfaces)) for b in range(a + 1, len(surfaces))][:200]
        name_agree = sum(_levenshtein_ratio(surfaces[a], surfaces[b]) for a, b in pairs) / max(1, len(pairs))
    confs = [mentions[i].get("provenance", {}).get("min_conf") for i in members]
    confs = [c for c in confs if isinstance(c, (int, float))]
    ocr = sum(confs) / len(confs) if confs else 0.85       # assume decent OCR if unknown
    return round(0.6 * name_agree + 0.4 * ocr, 3)


def canonicalize(components: list, mentions: list, ambiguous: set) -> list:
    """Turn mention-index components into canonical authority records.

    Args:
        components: List of lists of mention indices (from clustering).
        mentions:   The full mention list.
        ambiguous:  Set of component roots flagged grey (for the `needs_review` flag).

    Returns:
        A list of canonical-record dicts ready to serialize to entities.json.
    """
    records: list = []
    # Recover each component's union-find root so we can test membership in `ambiguous`.
    member_to_root = {}
    for comp in components:
        for idx in comp:
            member_to_root[idx] = comp[0]                   # any stable representative for the group
    root_is_ambiguous = {comp[0] for comp in components if any(i in ambiguous for i in comp)}

    for seq, comp in enumerate(sorted(components, key=lambda c: -len(c))):   # biggest entities first
        ms = [mentions[i] for i in comp]
        typ = ms[0].get("type", "MISC")
        surfaces = [m["surface"] for m in ms]
        canonical = _canonical_name(surfaces)
        # Merge attributes: keep the most common non-empty value per key (simple majority vote).
        attr_votes: dict = defaultdict(lambda: defaultdict(int))
        for m in ms:
            for k, v in (m.get("attrs") or {}).items():
                if v not in (None, "", []):
                    attr_votes[k][json.dumps(v, sort_keys=True)] += 1
        attrs = {k: json.loads(max(vs, key=vs.get)) for k, vs in attr_votes.items()}

        record = {
            "id": _entity_id(typ, canonical, seq),
            "type": typ,
            "canonical_name": canonical,
            "variants": sorted({s for s in surfaces}),      # every distinct surface form we saw
            "attrs": attrs,
            "mentions": [                                    # provenance so retrieval can cite regions
                {"mention_id": m["mention_id"], "surface": m["surface"],
                 "provenance": m.get("provenance", {})}
                for m in ms
            ],
            "confidence": _confidence(comp, mentions),
            "needs_review": comp[0] in root_is_ambiguous,   # grey-edge cluster -> David's queue
            "authority": {"wikidata": None, "viaf": None, "geonames": None, "getty_tgn": None},
        }
        records.append(record)
    return records


# ==================================================================
# STEP 6 — Authority reconciliation HOOKS — GATED (requests, not run)
# ==================================================================
# Authority control means linking each entity to a GLOBAL identifier so the archive interoperates
# with the scholarly world: persons -> Wikidata QIDs + VIAF; places -> GeoNames IDs + Getty TGN.
# This gives disambiguation, alternate-name enrichment, and Linked-Data export for free. These are
# real HTTP shapes via `requests`, but every one is GATED and cost-logged (as $0 — these public APIs
# don't bill, yet we still record the call so the run ledger is complete). Nothing fires unless
# `gated=False`.

def reconcile_person(name: str, attrs: dict | None = None, gated: bool = True) -> dict:
    """Look a person up in Wikidata + VIAF (GATED).

    Args:
        name:  The canonical person name to search.
        attrs: Optional disambiguators (birth/death year, role) to filter candidates.
        gated: If True (default), returns empty links and makes NO network call.

    Returns:
        {"wikidata": qid|None, "viaf": id|None, "candidates": [...]} (empty when gated).
    """
    if gated:
        # TODO(david): set gated=False to enable. Be polite: add a User-Agent, cache results, and
        #              rate-limit (these are free public endpoints — don't hammer them).
        return {"wikidata": None, "viaf": None, "candidates": []}

    import requests                                          # lazy: only when actually reconciling

    out = {"wikidata": None, "viaf": None, "candidates": []}
    headers = {"User-Agent": "SolanusArchivalTool/0.1 (research; contact: davidtobiasfalk@gmail.com)"}

    # --- Wikidata entity search (wbsearchentities) ------------------------------------------------
    try:
        r = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={"action": "wbsearchentities", "search": name, "language": "en",
                    "type": "item", "format": "json", "limit": 5},
            headers=headers, timeout=15,
        )
        hits = r.json().get("search", [])
        out["candidates"].extend({"source": "wikidata", "id": h["id"], "label": h.get("label", ""),
                                  "description": h.get("description", "")} for h in hits)
        if hits:
            out["wikidata"] = hits[0]["id"]
        costlog.log("wikidata", "wbsearchentities", "reconcile", items=len(hits), usd=0.0, meta=name[:60])
    except Exception as exc:                                 # network/JSON errors must not crash a run
        costlog.log("wikidata", "wbsearchentities", "reconcile", items=0, usd=0.0, meta=f"ERR:{exc}")

    # --- VIAF AutoSuggest -------------------------------------------------------------------------
    try:
        r = requests.get("https://viaf.org/viaf/AutoSuggest",
                         params={"query": name}, headers=headers, timeout=15)
        res = r.json().get("result") or []
        out["candidates"].extend({"source": "viaf", "id": h.get("viafid"), "label": h.get("term", "")}
                                 for h in res[:5])
        if res:
            out["viaf"] = res[0].get("viafid")
        costlog.log("viaf", "autosuggest", "reconcile", items=len(res), usd=0.0, meta=name[:60])
    except Exception as exc:
        costlog.log("viaf", "autosuggest", "reconcile", items=0, usd=0.0, meta=f"ERR:{exc}")
    return out


def reconcile_place(name: str, attrs: dict | None = None, gated: bool = True) -> dict:
    """Look a place up in GeoNames + Getty TGN (GATED).

    Args:
        name:  The canonical place name to search.
        attrs: Optional disambiguators (country/admin region).
        gated: If True (default), returns empty links and makes NO network call.

    Returns:
        {"geonames": id|None, "getty_tgn": id|None, "candidates": [...]} (empty when gated).
    """
    if gated:
        # TODO(david): set gated=False to enable. GeoNames needs a free 'username' (env GEONAMES_USER);
        #              Getty TGN is a public SPARQL endpoint. Cache + rate-limit as for persons.
        return {"geonames": None, "getty_tgn": None, "candidates": []}

    import os
    import requests                                          # lazy: only when actually reconciling

    out = {"geonames": None, "getty_tgn": None, "candidates": []}
    headers = {"User-Agent": "SolanusArchivalTool/0.1 (research; contact: davidtobiasfalk@gmail.com)"}

    # --- GeoNames search ---------------------------------------------------------------------------
    try:
        user = os.environ.get("GEONAMES_USER", "demo")
        r = requests.get("http://api.geonames.org/searchJSON",
                         params={"q": name, "maxRows": 5, "username": user},
                         headers=headers, timeout=15)
        hits = r.json().get("geonames", [])
        out["candidates"].extend({"source": "geonames", "id": h.get("geonameId"),
                                  "label": h.get("name", ""), "country": h.get("countryName", "")}
                                 for h in hits)
        if hits:
            out["geonames"] = hits[0].get("geonameId")
        costlog.log("geonames", "searchJSON", "reconcile", items=len(hits), usd=0.0, meta=name[:60])
    except Exception as exc:
        costlog.log("geonames", "searchJSON", "reconcile", items=0, usd=0.0, meta=f"ERR:{exc}")

    # --- Getty TGN (Vocabulary SPARQL) -------------------------------------------------------------
    try:
        sparql = (
            'SELECT ?subj ?label WHERE { '
            '?subj a gvp:Subject ; luc:term "%s" ; gvp:prefLabelGVP/xl:literalForm ?label . } '
            'LIMIT 5' % name.replace('"', '')
        )
        r = requests.get("http://vocab.getty.edu/sparql.json",
                         params={"query": sparql}, headers=headers, timeout=20)
        rows = r.json().get("results", {}).get("bindings", [])
        out["candidates"].extend({"source": "getty_tgn", "id": b["subj"]["value"],
                                  "label": b.get("label", {}).get("value", "")} for b in rows)
        if rows:
            out["getty_tgn"] = rows[0]["subj"]["value"]
        costlog.log("getty", "tgn_sparql", "reconcile", items=len(rows), usd=0.0, meta=name[:60])
    except Exception as exc:
        costlog.log("getty", "tgn_sparql", "reconcile", items=0, usd=0.0, meta=f"ERR:{exc}")
    return out


# ==================================================================
# Optional scale-up — Splink (probabilistic record linkage)
# ==================================================================
def splink_plan() -> dict:
    """How to swap THIS rule+embedding matcher for Splink when the corpus outgrows in-process ER.

    Splink (MoJ Analytical Services) implements the Fellegi–Sunter model with the EM algorithm to
    *learn* match weights from the data, and runs on a DuckDB/Spark backend — millions of records on
    a laptop. Our pieces map onto it almost one-to-one, so this is a drop-in scale path, not a
    rewrite:

      - blocking_keys()  ->  Splink `blocking_rules_to_generate_predictions`
                             (e.g. l.phonetic_key = r.phonetic_key AND l.year = r.year)
      - pair_score()'s   ->  Splink `comparisons` (a ComparisonLevel per signal):
        signals               * name: jaro_winkler / levenshtein thresholds,
                               * phonetic: exact-match on the metaphone column,
                               * city/role: exact + 'else' levels,
                               * (embedding cosine can be a custom SQL comparison if precomputed).
      - cluster_mentions()->  Splink `cluster_pairwise_predictions_at_threshold(match_probability)`
                             (its own connected-components, like our union-find).

    Returns a documentation dict (no Splink import, nothing executed) so callers can introspect the
    plan and so this stays runnable without the dependency installed.
    """
    return {
        "library": "splink",
        "backend": "duckdb (laptop) | spark (cluster)",
        "model": "Fellegi-Sunter probabilistic linkage; EM-trained match weights",
        "maps_from": {
            "blocking_keys()": "blocking_rules_to_generate_predictions",
            "pair_score() signals": "comparisons / ComparisonLevels (jaro_winkler, exact, custom-sql)",
            "cluster_mentions()": "cluster_pairwise_predictions_at_threshold",
        },
        "when": "tens of thousands+ mentions, or when learned (not hand-tuned) weights are wanted",
        "note": "precompute a `phonetic_key` and `norm_name` column with this module's functions; "
                "feed those plus attrs to Splink so blocking/comparisons reuse the same normalization.",
    }


# ==================================================================
# I/O — read raw mentions; a documented demo fallback when absent
# ==================================================================
# The NER stage (extract_entities) emits a richer mention shape than this resolver's native schema:
# the surface text lives under "text" (not "surface"), there is no "mention_id", and it uses a finer
# type vocabulary (ROLE_TITLE, RELIGIOUS_TERM, OUTCOME, RELATION, ...). This map folds those into the
# resolver's coarser TYPES so blocking/clustering happens within sensible buckets; the fine-grained
# original is preserved on each mention as "ner_type" for the graph/UI.
NER_TYPE_MAP = {
    "PERSON": "PERSON", "PLACE": "PLACE", "ORG": "ORG",
    "CONDITION": "CONDITION", "FAVOR": "FAVOR", "OUTCOME": "FAVOR",
    "ROLE_TITLE": "ROLE", "ROLE": "ROLE", "RELIGIOUS_TERM": "MISC", "DATE": "DATE",
}
# RELATION mentions describe an EDGE between people ("X — correspondent — Y"); they are consumed by
# build_graph from the raw file, not deduplicated into authority records here, so we skip them.
NER_SKIP_TYPES = {"RELATION"}


def _adapt_ner_mentions(raw: list) -> list:
    """Bridge extract_entities' mention shape to this resolver's native schema (non-destructive).

    Renames ``text`` -> ``surface``, mints a stable unique ``mention_id`` from provenance, maps the
    NER type into the resolver vocabulary (keeping the original under ``ner_type``), and drops RELATION
    rows + empty/"none" surfaces. Mentions already in native shape pass straight through.
    """
    out = []
    for i, m in enumerate(raw):
        if "surface" in m and "mention_id" in m:          # already native — leave untouched
            out.append(m)
            continue
        t = (m.get("type") or "MISC").upper()
        if t in NER_SKIP_TYPES:
            continue
        surface = (m.get("surface") or m.get("text") or "").strip()
        if not surface or surface.lower() == "none":      # NER emits "None" for absent slots — skip
            continue
        rtype = NER_TYPE_MAP.get(t, "MISC")
        # NER quality gate: a PERSON surface that's really a bare kinship word, a title/punct stub,
        # initials only, a quantified group, an anaphor ("his wife"), or a deity is NOT a named human
        # — drop it so it never becomes a person entity/node (the single largest defect class, ~12%).
        if rtype == "PERSON" and nonperson_reason(surface):
            continue
        prov = m.get("provenance") or {}
        doc = prov.get("doc_id", m.get("unit_id", ""))
        rid = prov.get("rid", "")
        out.append({
            "mention_id": f"{doc}::{rid}::m{i}",           # globally unique (i is the file line index)
            "type":       rtype,
            "ner_type":   t,                               # keep the fine-grained original for the graph
            "surface":    surface,
            "attrs":      m.get("attrs") or {},
            "provenance": prov,
            "source":     m.get("source", "llm"),
        })
    return out


def _load_raw_mentions() -> tuple:
    """Read data/entities_raw.jsonl into a list of mention dicts.

    Returns:
        (mentions, source) where source is "entities_raw.jsonl" or "demo-fallback".

    If the NER stage hasn't produced entities_raw.jsonl yet (it's an upstream Part-B stage), we fall
    back to a tiny set of mentions DERIVED FROM STRUCTURED FIELDS (letter recipients) so this module
    is smoke-testable today without inventing data or making any model call. The fallback is clearly
    labelled in the output's "_meta" and is NOT how a real run sources mentions.
    """
    if RAW_PATH.exists():
        raw = [json.loads(l) for l in RAW_PATH.read_text().splitlines() if l.strip()]
        return _adapt_ner_mentions(raw), "entities_raw.jsonl"

    # ---- demo fallback: harvest recipient names straight from the letters' structured fields ----
    # These fields are already separated per letter (no NER needed), so they are a legitimate, free
    # source of PERSON mentions to exercise normalize->block->match->cluster end-to-end. We cap the
    # count to keep a smoke run instant.
    mentions, seq = [], 0
    for d in chunks_lib.json.loads(config.DOCUMENTS.read_text()):
        recip = (d.get("recipient") or "").strip()
        if not recip:
            continue
        # find the region that carries the recipient text, for honest provenance/vertices
        prov = {"doc_id": d["id"], "rid": "", "page": d.get("page_number_in_type"),
                "pdf_page": d.get("pdf_page_number"), "date": d.get("date", "")}
        for r in d.get("regions", []):
            if r.get("category") == "src_recipient":
                prov.update({"rid": r.get("rid", ""), "vertices": r.get("vertices"),
                             "min_conf": r.get("min_conf")})
                break
        mentions.append({"mention_id": f'{d["id"]}::recipient::m{seq}', "type": "PERSON",
                         "surface": recip, "attrs": {}, "provenance": prov})
        seq += 1
        if seq >= 200:                                       # smoke cap
            break
    return mentions, "demo-fallback(letter recipients)"


# ==================================================================
# Orchestration — the stage entry point the pipeline DAG calls
# ==================================================================
def run(use_embeddings: bool = False, use_llm: bool = False, reconcile: bool = False,
        emb_model: str | None = None, emb_dim: int | None = None) -> dict:
    """Run the full A3 resolution pipeline and write entities.json + the merge audit log.

    By default every paid/heavy/network signal is OFF (gated): the rule-based core runs offline and
    costs $0. Flip the flags (and provide keys where needed) to enable each optional layer — each
    one still routes through lib.costlog.

    Args:
        use_embeddings: Enable the embedding match signal (free local path by default).
        use_llm:        Enable LLM adjudication of ambiguous clusters (GATED; bills tokens).
        reconcile:      Enable Wikidata/VIAF + GeoNames/Getty authority lookups (GATED; $0 APIs).
        emb_model/emb_dim: Embedding space to use when use_embeddings is True.

    Returns:
        A small summary dict {mentions, entities, blocks, merges, ambiguous, source, out, audit}.
    """
    config.DATA.mkdir(parents=True, exist_ok=True)

    # ----- load -----
    mentions, source = _load_raw_mentions()
    if not mentions:
        OUT_PATH.write_text(json.dumps({"_meta": {"source": source, "note": "no mentions found"},
                                        "entities": []}, indent=2))
        return {"mentions": 0, "entities": 0, "blocks": 0, "merges": 0, "ambiguous": 0,
                "source": source, "out": str(OUT_PATH), "audit": str(AUDIT_PATH)}

    # ----- block -----
    blocks = build_blocks(mentions)

    # ----- (gated) embedding signal: only score the pairs blocking proposed -----
    emb_sims = None
    if use_embeddings:
        candidate_pairs = []
        for idxs in blocks.values():
            for a in range(len(idxs)):
                for b in range(a + 1, len(idxs)):
                    candidate_pairs.append((idxs[a], idxs[b]))
        emb_sims = embedding_signal(mentions, candidate_pairs, gated=False,
                                    model=emb_model, dim=emb_dim)

    # ----- anchor gate: per-type common-token table -----
    common = build_common_tokens(mentions)

    # ----- match + cluster (cautious ladder: veto / auto-merge / llm / abstain) -----
    auto_edges, llm_pairs, audit = cluster_mentions(mentions, blocks, common, emb_sims)
    uf = _UnionFind(len(mentions))
    for i, j in auto_edges:
        uf.union(i, j)

    # ----- dedup grey pairs to unique COMPONENT pairs (we merge ENTITIES, not mention-pairs) -----
    # Inside a big block, millions of mention-pairs collapse to a handful of distinct cluster-vs-cluster
    # questions once the deterministic auto-merges are in. Keep, per component pair, the strongest
    # representative so a cap (if hit) sheds the weakest candidates first.
    comp_pairs: dict = {}
    for (i, j, reason) in llm_pairs:
        ra, rb = uf.find(i), uf.find(j)
        if ra == rb:
            continue                                         # already one entity via auto-merge
        key = (ra, rb) if ra < rb else (rb, ra)
        # rank for the cap: near-identical strings AND abbreviation/initialism candidates first, so a
        # real merge (S.M.A. <-> Seraphic Mass Assoc., or an OCR variant) outranks a weak surname-share.
        strength = (reason.get("lev", 0) + (1.0 if reason.get("anchor") else 0.0)
                    + (1.5 if reason.get("abbrev") else 0.0))
        prev = comp_pairs.get(key)
        if prev is None or strength > prev[3]:
            comp_pairs[key] = (i, j, reason, strength)
    unique_grey = sorted(comp_pairs.values(), key=lambda x: x[3], reverse=True)

    # ----- LLM precision gate on the unique grey pairs (parallel; defaults to 'different') -----
    grey_nodes: set = set()
    llm_same = 0
    deferred = max(0, len(unique_grey) - MAX_LLM_PAIRS)
    to_judge = unique_grey[:MAX_LLM_PAIRS]
    print(f"  grey pairs: {len(llm_pairs)} raw -> {len(unique_grey)} unique component-pairs"
          + (f"; judging top {MAX_LLM_PAIRS}, deferring {deferred} to review" if deferred else ""))
    if deferred:                                             # persist the long tail so no pair is silently lost
        with open(config.DATA / "merge_deferred.jsonl", "w") as f:
            for (i, j, r, s) in unique_grey[MAX_LLM_PAIRS:]:
                f.write(json.dumps({"a_surface": mentions[i]["surface"], "b_surface": mentions[j]["surface"],
                                    "type": mentions[i].get("type", "MISC"), "strength": round(s, 3)},
                                   ensure_ascii=False) + "\n")
    if use_llm and to_judge:
        verdicts = adjudicate_pairs([(i, j, r) for (i, j, r, _) in to_judge], mentions)
        for (i, j, _r, _s), v in zip(to_judge, verdicts):
            same = bool(v.get("same")) and float(v.get("confidence", 0.0)) >= LLM_CONFIDENCE_MIN
            audit.append({"a": mentions[i]["mention_id"], "b": mentions[j]["mention_id"],
                          "a_surface": mentions[i]["surface"], "b_surface": mentions[j]["surface"],
                          "type": mentions[i].get("type", "MISC"),
                          "decision": "llm_same" if same else "llm_different",
                          "score": float(v.get("confidence", 0.0)), "signals": v})
            if same:
                uf.union(i, j)
                llm_same += 1
            else:
                grey_nodes.update((i, j))
    else:
        for (i, j, _r, _s) in unique_grey:                   # no LLM: leave grey pairs separate, flag them
            grey_nodes.update((i, j))

    # ----- components from the union-find -----
    comp_of: dict = defaultdict(list)
    for idx in range(len(mentions)):
        comp_of[uf.find(idx)].append(idx)
    components = list(comp_of.values())
    ambiguous = {uf.find(n) for n in grey_nodes}

    # ----- canonicalize -----
    records = canonicalize(components, mentions, ambiguous)

    # ----- (gated) authority reconciliation -----
    if reconcile:
        for rec in records:
            if rec["type"] == "PERSON":
                links = reconcile_person(rec["canonical_name"], rec["attrs"], gated=False)
                rec["authority"].update({"wikidata": links["wikidata"], "viaf": links["viaf"]})
            elif rec["type"] == "PLACE":
                links = reconcile_place(rec["canonical_name"], rec["attrs"], gated=False)
                rec["authority"].update({"geonames": links["geonames"], "getty_tgn": links["getty_tgn"]})

    # ----- write outputs (non-destructive) -----
    auto_merges, llm_merges, vetoes = len(auto_edges), llm_same, sum(1 for a in audit if a.get("decision") == "veto")
    out_doc = {
        "_meta": {
            "stage": "resolve_entities (Tier-1 A3, cautious merge)",
            "source": source,
            "n_mentions": len(mentions),
            "n_entities": len(records),
            "n_blocks": len(blocks),
            "thresholds": {"match_tau": MATCH_TAU, "ambig_tau": AMBIG_TAU,
                           "lev_exact_auto": LEV_EXACT_AUTO, "llm_confidence_min": LLM_CONFIDENCE_MIN},
            "merges": {"auto": auto_merges, "llm_confirmed": llm_merges,
                       "llm_pairs_seen": len(llm_pairs), "vetoes": vetoes},
            "signals": {"rules": True, "anchor_gate": True, "acronym": True,
                        "embeddings": bool(use_embeddings), "llm_gate": bool(use_llm),
                        "authority_reconcile": bool(reconcile)},
            "config_fingerprint": config.fingerprint(),
        },
        "entities": records,
    }
    OUT_PATH.write_text(json.dumps(out_doc, indent=2, ensure_ascii=False))
    with open(AUDIT_PATH, "w") as f:
        for row in audit:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return {"mentions": len(mentions), "entities": len(records), "blocks": len(blocks),
            "auto_merges": auto_merges, "llm_confirmed": llm_merges, "llm_pairs": len(llm_pairs),
            "vetoes": vetoes, "ambiguous": len(ambiguous), "source": source,
            "out": str(OUT_PATH), "audit": str(AUDIT_PATH)}


# ==================================================================
# CLI — smoke-run the offline core; everything paid stays gated
# ==================================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Tier-1 A3 entity resolution (grouping).")
    ap.add_argument("--use-embeddings", action="store_true",
                    help="enable embedding match signal (free local path by default)")
    ap.add_argument("--use-llm", action="store_true",
                    help="enable LLM adjudication of ambiguous clusters (GATED; bills tokens)")
    ap.add_argument("--reconcile", action="store_true",
                    help="enable Wikidata/VIAF + GeoNames/Getty authority lookups (GATED)")
    a = ap.parse_args()

    summary = run(use_embeddings=a.use_embeddings, use_llm=a.use_llm, reconcile=a.reconcile)

    # House-style SUMMARY block at the end of a runnable script.
    print("=" * 60)
    print("resolve_entities SUMMARY")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k:<10} : {v}")
    print("-" * 60)
    print("  cost ledger (lib.costlog):")
    print(json.dumps(costlog.summary(), indent=2))
