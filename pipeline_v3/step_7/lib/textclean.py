"""lib/textclean.py — shared, conservative OCR text cleanup (used by chunks, NER input, enrichment).

The scans break words across lines with a trailing hyphen ("Hus-\nband", "Diabe- tis", "En-\nrolled").
Azure preserves those as a hyphen + whitespace, which then pollutes retrieval, embeddings, and the
displayed answer. We collapse them — carefully, so we never damage a REAL hyphenated compound.

The safe signal: a hyphen immediately followed by WHITESPACE and a LOWERCASE letter is a line-break
artifact, because a genuine compound ("well-known", "twenty-one") is written with NO space after the
hyphen. So `X- y` / `X-\n y` -> `Xy`, while `well-known` is left untouched.

(A fuller version would use the per-word polygon geometry — line angle/skew + whether the next line's
first characters continue the broken word — to disambiguate every case; that's a step_5/6 enhancement.
This string-level pass captures the overwhelming majority with near-zero risk.)
"""
from __future__ import annotations
import re

# letter, hyphen(s), run of whitespace (incl. newlines), then a LOWERCASE continuation letter.
_LINEBREAK_HYPHEN = re.compile(r"([A-Za-z])-+\s+([a-z])")
# also the end-of-string-fragment case ("...Hus-" joined to a following fragment starting lowercase)
_TRAILING_HYPHEN = re.compile(r"([A-Za-z])-+\s*\n\s*([a-z])")

# --- PDF-extraction glyph artifacts -------------------------------------------------------------
# Some source PDFs (notably Crosby's *Thank God Ahead of Time*) extract two families of glyphs that
# render as empty boxes / corrupt the text, embeddings, and any displayed answer:
#   * Private-Use-Area "old-style figures" U+F730..U+F739: these ARE the digits 0..9, offset from
#     U+F730 (so chr(0xF730 + n) == digit n). They wreck dates and page numbers ("19" -> two boxes).
#   * Typographic f-ligatures U+FB00..U+FB04 (ff fi fl ffi ffl): expand to plain letter runs so
#     retrieval matches "confidence" rather than "con<ligature>dence".
# str.translate maps each codepoint to a (possibly multi-char) ASCII string in one pass.
_GLYPH_MAP = {0xF730 + n: str(n) for n in range(10)}
_GLYPH_MAP.update({0xFB00: "ff", 0xFB01: "fi", 0xFB02: "fl", 0xFB03: "ffi", 0xFB04: "ffl"})


def normalize_glyphs(text: str) -> str:
    """Replace PDF-extraction glyph artifacts (PUA old-style digits + f-ligatures) with plain ASCII."""
    if not text:
        return text
    return text.translate(_GLYPH_MAP)


def dehyphenate(text: str) -> str:
    """Collapse OCR line-break hyphenation; leave real compounds (no space after the hyphen) intact."""
    if not text or "-" not in text:
        return text
    prev = None
    out = text
    # iterate to catch chains ("Dia- be- tis"); converges in a couple of passes
    while out != prev:
        prev = out
        out = _TRAILING_HYPHEN.sub(r"\1\2", out)
        out = _LINEBREAK_HYPHEN.sub(r"\1\2", out)
    return out


def clean(text: str) -> str:
    """The standard cleanup applied to corpus text before it is chunked/embedded/shown: normalize PDF
    glyph artifacts (PUA digits + ligatures), de-hyphenate line breaks, and normalize whitespace runs
    (keep it minimal + non-destructive otherwise)."""
    if not text:
        return text
    t = normalize_glyphs(text)                # PUA old-style digits + f-ligatures -> ASCII
    t = dehyphenate(t)
    t = re.sub(r"[ \t]+\n", "\n", t)          # trailing spaces before newlines
    return t


if __name__ == "__main__":
    # Build the artifact test inputs via chr() so this file stays plain ASCII on disk.
    pua_date = "".join(chr(0xF730 + int(d)) for d in "1924")             # PUA digits -> "1924"
    lig_conf = "con" + chr(0xFB01) + "dence"                            # fi  -> confidence
    lig_offer = "o" + chr(0xFB00) + "er su" + chr(0xFB03) + "ciently"   # ff/ffi -> offer sufficiently
    for s in ["Hus- band", "Diabe-\ntis", "En- rolled", "well-known", "twenty-one",
              "Mrs. Clair- mont reported", "Dia- be- tis",
              f"born in {pua_date}", lig_conf, lig_offer]:
        print(f"{s!r:40} -> {clean(s)!r}")
