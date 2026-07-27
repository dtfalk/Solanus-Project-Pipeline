"""prompts.py — SINGLE SOURCE OF TRUTH for every system/instruction prompt in the reverse-desanitizer track.

WHY THIS FILE EXISTS (the system-prompt policy for all LLMs in the flow):

A fine-tuned model's behaviour is conditioned on the EXACT system message + chat template it saw in
training. If the translator's system prompt at inference differs by even a word from the one used to build
its training examples, the adapter is being run off-distribution and quietly underperforms. So the rule is:

  • TRANSLATOR_SYS is defined ONCE here and imported by BOTH train_translator.py (training) and
    restyle_flow.py (inference). Never inline a second copy. Change it here, then RETRAIN — the two must
    always byte-match. (The separate UNIFIED-author track keeps its own PERSONA in train_qlora.py; this
    file governs only the translator/desanitizer track.)

  • The FAITHFULNESS JUDGE is the same prompt whether it gates TRAINING DATA (augment_pairs.py) or VERIFIES
    a restyle at INFERENCE (restyle_flow.py). Using one judge for both means the bar the data passes is the
    bar inference enforces — train and eval agree (per the research).

  • The NEUTRALIZER that builds the training INPUT is prompted to imitate the big inference model's own
    answer style, so train-input ≈ inference-input (distribution match — the single highest-value lever).

  • The base "answerer" (the archive's RAG model) keeps its system prompt in the app (server.py); it is a
    separate concern, but note its OUTPUT is what the translator receives — so the 'modern' neutralizer
    below is deliberately shaped to look like that kind of answer.

Roles at a glance:
  answerer (app)  -> plain reasoned answer  ── system prompt lives in app/server.py
  neutralizer     -> builds NEUTRAL training inputs that look like the answerer's output (here)
  TRANSLATOR      -> restyle to his voice    ── TRANSLATOR_SYS, identical train & inference (here)
  verifier        -> faithfulness judge      ── faithfulness_prompt(), shared gate+inference (here)
  repairer        -> fix flagged issues       ── repair_prompt() (here)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------------------------------------
# THE TRANSLATOR system message. IDENTICAL at train time and inference. Edit here only, then retrain.
# Kept deliberately compact + behaviour-defining (a single-purpose adapter wants one stable instruction).
# ---------------------------------------------------------------------------------------------------------
TRANSLATOR_SYS = (
    "You are a faithful style translator. Rewrite the user's passage of plain, modern English into the "
    "writing voice of Father Solanus Casey, the Capuchin friar (1870-1957) — his gentle, humble, grateful "
    "idiom ('Deo gratias', 'Thanks be to God', confidence in Providence). Preserve the meaning and every "
    "fact EXACTLY; change only the voice. Output ONLY the rewritten passage, nothing else.")

# INSTRUCTION-DROPOUT so the restyle is a PROMPT-INDEPENDENT skill, not a response to one fixed string.
# We train across these system messages (including EMPTY — the most important), so at serve time you can set
# ANY user-facing system prompt (or none) WITHOUT retraining. The voice transform lives in the weights.
TRANSLATOR_SYS_VARIANTS = (
    "",                                   # empty: the skill must fire with NO system prompt at all
    TRANSLATOR_SYS,
    ("Rewrite the user's text in the writing voice of Father Solanus Casey (1870-1957). Preserve the "
     "meaning and every fact exactly; change only the voice. Output only the rewrite."),
    ("Restyle the passage into Solanus Casey's gentle, humble, devotional idiom without altering any fact. "
     "Return only the restyled passage."),
)


def translator_system(rng):
    """Pick a system message for ONE training example (instruction-dropout). rng = seeded random.Random."""
    return rng.choice(TRANSLATOR_SYS_VARIANTS)


# ---------------------------------------------------------------------------------------------------------
# NEUTRALIZERS — build the NEUTRAL side of a training pair from one of his authentic passages.
#   'modern'    : imitate the big inference model's answer style (DISTRIBUTION MATCH — preferred). Run this
#                 with the SAME big model used at inference for best alignment.
#   'plain'     : a cheap paraphrase, for lexical variety.
#   'roundtrip' : style-stripping via a translation pivot (content preserved, authorial style reduced).
# These are user-turn task prompts (the neutralizer model needs no special persona).
# ---------------------------------------------------------------------------------------------------------
def neutralizer(text: str, mode: str = "modern", pivot: str = "German") -> str:
    if mode == "modern":
        return ("You are a careful, modern AI assistant. Restate ALL of the content, claims, advice and "
                "intent of the passage below as a plain, clear, well-reasoned answer in neutral modern "
                "English — the way a helpful assistant explains things: direct, organised, no archaic idiom, "
                "no period diction, no first-person religious voice, no salutations or letter formatting. "
                "Preserve every fact and the full meaning. Output only the restatement.\n\nPassage:\n" + text)
    if mode == "roundtrip":
        return (f"Translate the passage below into {pivot}, then translate your {pivot} back into natural "
                "modern English. Preserve the meaning and all facts; the result should read as plain modern "
                "prose. Output ONLY the final English.\n\nPassage:\n" + text)
    return ("Paraphrase the passage below into plain, neutral, modern English. Keep the meaning and all "
            "facts; strip any personal voice, era, or idiom. Return ONLY the paraphrase.\n\nPassage:\n" + text)


# ---------------------------------------------------------------------------------------------------------
# FAITHFULNESS JUDGE — used as BOTH a training-data gate and the inference verifier (so they agree).
# Structured rubric (content_equivalence + no_hallucination), returns the SPECIFIC missing/added items so
# the repair step can act on them. Run at temperature 0.
# ---------------------------------------------------------------------------------------------------------
def faithfulness_prompt(original: str, restyled: str) -> str:
    return (
        "Compare NEUTRAL and STYLED. STYLED should preserve NEUTRAL's exact meaning while changing only the "
        "writing voice. Devotional phrasing that asserts no NEW factual claim is acceptable.\n"
        "Score each 1-5:\n"
        "  content_equivalence — do they assert the same facts, advice and intent, with nothing omitted?\n"
        "  no_hallucination    — does STYLED add NOTHING (no fact/name/number/claim) not entailed by NEUTRAL?\n"
        'Return ONLY JSON: {"content_equivalence":int,"no_hallucination":int,"verdict":"pass|fail",'
        '"missing":["facts in NEUTRAL dropped by STYLED"],"added":["things STYLED added not in NEUTRAL"]}. '
        "Set verdict to \"pass\" ONLY if BOTH scores are >= 4.\n\n"
        f"NEUTRAL:\n{original}\n\nSTYLED:\n{restyled}")


# ---------------------------------------------------------------------------------------------------------
# REPAIRER — edit a draft to fix exactly the judge's flagged issues, keeping the voice. Feeds missing/added.
# ---------------------------------------------------------------------------------------------------------
def cite_transfer_prompt(plain_cited: str, styled: str) -> str:
    """Re-attach citation markers from the cited plain answer onto the (citation-free) styled answer. The
    big model aligns claims semantically, so it survives the translator merging/reordering sentences."""
    return (
        "PLAIN is a cited answer. STYLED is the SAME answer rewritten in Father Solanus Casey's voice but "
        "with the citation markers removed. Put PLAIN's citation markers (e.g. [3] or [3, 5]) back into "
        "STYLED, each placed immediately after the claim it supports. Use EXACTLY the markers that appear in "
        "PLAIN — add none, drop none, invent none — and change STYLED's wording in no other way. Output ONLY "
        f"the cited STYLED passage.\n\nPLAIN:\n{plain_cited}\n\nSTYLED:\n{styled}")


def repair_prompt(original: str, restyled: str, missing, added) -> str:
    miss = "; ".join(missing) if missing else "(none)"
    add = "; ".join(added) if added else "(none)"
    return (
        "Revise the STYLED passage so it is faithful to NEUTRAL while KEEPING Father Solanus Casey's gentle, "
        "humble writing voice. Specifically: RESTORE anything missing, REMOVE anything added, change the "
        "meaning nowhere, and introduce no new facts. Output ONLY the corrected passage.\n"
        f"RESTORE (was dropped): {miss}\nREMOVE (was added): {add}\n\n"
        f"NEUTRAL:\n{original}\n\nSTYLED:\n{restyled}")
