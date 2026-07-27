"""evaluate_voice.py — Stage 5: does the clone sound like him, and is it intelligible?

Three axes (pair automatic metrics with HUMAN review by people who know the 1945 recording):
  1. Speaker similarity — ECAPA/WavLM cosine between a render and the restored reference. NOTE: this only
     certifies similarity to a DEGRADED target, and zero-shot cloners tend to "average away" the soft/wispy/
     diphtheria-damaged character toward a generic healthy voice — the central failure mode to watch.
  2. Intelligibility — Whisper ASR WER of the render vs the intended text.
  3. Human — A/B against the real clip; flag any output that erases his recognizable timbre.

SCAFFOLD: wires speechbrain ECAPA + whisper; install requirements.txt.

    python evaluate_voice.py data/renders/chunk_000.wav --ref data/restored/solanus_1945.master.wav --text "Deo gratias..."
"""
from __future__ import annotations
import argparse
from pathlib import Path

import config  # noqa: F401  (paths/config available to extend)


def speaker_similarity(render: Path, ref: Path):
    try:
        from speechbrain.inference.speaker import EncoderClassifier
        import torch, torchaudio
        enc = EncoderClassifier.from_hparams(source="speechbrain/spkrec-ecapa-voxceleb")
        def emb(p):
            wav, sr = torchaudio.load(str(p)); return enc.encode_batch(wav).squeeze()
        a, b = emb(render), emb(ref)
        return float(torch.nn.functional.cosine_similarity(a, b, dim=0))
    except Exception as e:
        print(f"  [speaker-sim] unavailable ({str(e)[:60]})"); return None


def wer(render: Path, text: str):
    try:
        import whisper
        hyp = whisper.load_model("base").transcribe(str(render))["text"].lower().split()
        ref = text.lower().split()
        # quick Levenshtein-free token WER approximation
        import difflib
        sm = difflib.SequenceMatcher(None, ref, hyp)
        return round(1 - sm.ratio(), 3)
    except Exception as e:
        print(f"  [wer] unavailable ({str(e)[:60]})"); return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("render", type=Path)
    ap.add_argument("--ref", type=Path, required=True)
    ap.add_argument("--text", default=None)
    a = ap.parse_args()
    sim = speaker_similarity(a.render, a.ref)
    print(f"speaker similarity to restored reference: {sim}")
    if a.text:
        print(f"intelligibility (approx WER): {wer(a.render, a.text)}")
    print("\nReminder: confirm with HUMAN review — automatic similarity to a degraded 1945 target is a weak "
          "proxy, and the key risk is erasing his soft/wispy character toward a generic healthy voice.")


if __name__ == "__main__":
    main()
