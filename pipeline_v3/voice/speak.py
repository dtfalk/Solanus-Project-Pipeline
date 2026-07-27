"""speak.py — Stages 3-4: clone the voice (zero-shot) and speak text in it.

Zero-shot clone with CosyVoice 3 (primary: Apache-2.0, ~3s reference, breath/soft-tone control), against ONE
frozen reference clip for cross-utterance consistency. Input text comes from the trained TEXT mimicker
(../mimicker) — his documented words — NOT free-form generation. Pipeline: normalize -> sentence-chunk ->
synth each chunk against the frozen reference with identical settings + soft/breathy style -> stitch.

SCAFFOLD: the CosyVoice call is sketched (install from FunAudioLLM/CosyVoice). Needs a curated reference clip
at config.FROZEN_REFERENCE (from restore_audio.py -> make_references). Comparators (Llasa-8B, F5-TTS) swap in
at the marked point.

    python speak.py "Deo gratias. I received your welcome letter ..."
"""
from __future__ import annotations
import argparse
import re

import config


def normalize(text: str) -> str:
    """Expand abbreviations/numbers for TTS; guard false sentence breaks (Mr./Mrs./Ave./St.)."""
    text = re.sub(r"\bDr\.", "Doctor", text)
    text = re.sub(r"\b(Mr|Mrs|Ms|St|Ave|Rev)\.", r"\1<dot>", text)   # protect, restore after chunking
    return text


def chunks(text: str, max_words: int = 40):
    sents = re.split(r"(?<=[.!?])\s+", text)
    cur, n = [], 0
    for s in sents:
        w = len(s.split())
        if n + w > max_words and cur:
            yield " ".join(cur).replace("<dot>", "."); cur, n = [], 0
        cur.append(s); n += w
    if cur:
        yield " ".join(cur).replace("<dot>", ".")


def synth_chunk(text: str, ref_wav: str, idx: int):
    """One chunk -> wav via CosyVoice 3 zero-shot, soft/breathy styling. TODO: wire the real model.

    Sketch (CosyVoice):
        from cosyvoice.cli.cosyvoice import CosyVoice2
        m = CosyVoice2("pretrained_models/CosyVoice2-0.5B")
        # natural-language instruction layers the documented soft/gentle/wispy character
        for out in m.inference_instruct2(text, "speak gently and softly, unhurried, slightly breathy",
                                         load_wav(ref_wav, 16000), stream=False):
            save(out['tts_speech'], config.RENDERS / f"chunk_{idx:03d}.wav")
    """
    out = config.RENDERS / f"chunk_{idx:03d}.wav"
    print(f"  [synth {idx}] '{text[:48]}...' -> {out.name}  (TODO: wire CosyVoice 3)")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("text")
    ap.add_argument("--ref", default=str(config.FROZEN_REFERENCE))
    a = ap.parse_args()
    parts = list(chunks(normalize(a.text)))
    print(f"reference: {a.ref}\nstyle: {config.VOICE_NOTES}\nchunks: {len(parts)}")
    rendered = [synth_chunk(c, a.ref, i) for i, c in enumerate(parts)]
    print(f"\nstitch {len(rendered)} chunks -> a single render in {config.RENDERS} "
          f"(embed provenance: '{config.DISCLOSURE[:60]}...').")
    print("Words are Solanus's own (from ../mimicker); only the audio is synthesized.")


if __name__ == "__main__":
    main()
