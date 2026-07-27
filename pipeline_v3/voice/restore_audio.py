"""restore_audio.py — Stage 1: conservative restoration of the 1945 disc (run before any cloning).

Documented archival order (deepest damage first; two LIGHT denoise passes — a single aggressive pass
above ~18 dB reduction produces hollow/phasey artifacts). Produces the CANONICAL clone reference from a
conservative chain. Bandwidth-extension / super-resolution and any generative "re-speak" enhancer are
deliberately kept OUT of the canonical master (they invent timbre the disc never captured) — run them, if
at all, into data/superres/ as a clearly-labeled, non-canonical side output.

This is a SCAFFOLD: the open-source denoisers are wired; the disc-specific declick/decrackle steps are best
done in iZotope RX / CEDAR (archival standard) — hooks are marked TODO. Needs real audio in data/raw/ first
(Stage 0 — obtain from the Capuchin archive).

    python restore_audio.py data/raw/solanus_1945.wav
"""
from __future__ import annotations
import argparse
from pathlib import Path

import config


def declick_decrackle(in_path: Path, out_path: Path):
    """Disc click/crackle removal. BEST: iZotope RX (De-click, De-crackle) or CEDAR, run interactively or via
    their CLI/headless mode on the raw transfer. TODO: wire your licensed tool here; for now, copy-through."""
    # TODO: subprocess to RX/CEDAR headless, or a python declicker; placeholder passes audio through.
    import soundfile as sf
    audio, sr = sf.read(in_path)
    sf.write(out_path, audio, sr)
    print(f"  [declick/decrackle] TODO wire RX/CEDAR — passed through -> {out_path.name}")


def denoise(in_path: Path, out_path: Path, passes: int = 2):
    """Two LIGHT broadband denoise passes with DeepFilterNet (open-source, 48kHz). Keep each pass gentle."""
    try:
        from df.enhance import enhance, init_df, load_audio, save_audio
        model, df_state, _ = init_df()
        audio, _ = load_audio(str(in_path), sr=df_state.sr())
        for _ in range(passes):
            audio = enhance(model, df_state, audio)
        save_audio(str(out_path), audio, df_state.sr())
        print(f"  [denoise x{passes}] DeepFilterNet -> {out_path.name}")
    except Exception as e:
        print(f"  [denoise] DeepFilterNet unavailable ({str(e)[:60]}); install requirements.txt")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw", type=Path, help="raw transfer in data/raw/")
    a = ap.parse_args()
    if not a.raw.exists():
        raise SystemExit(f"{a.raw} not found — obtain the 1945 recording from the Capuchin archive first (Stage 0)")
    stem = a.raw.stem
    declicked = config.RESTORED / f"{stem}.declicked.wav"
    master = config.RESTORED / f"{stem}.master.wav"      # CANONICAL clone reference source
    declick_decrackle(a.raw, declicked)
    denoise(declicked, master, passes=2)
    print(f"\nConservative restoration master -> {master}")
    print("Keep this as the canonical reference. Run super-res (AudioSR/VoiceFixer) only into "
          "data/superres/ as a NON-canonical, labeled side output. Next: make_references.py")


if __name__ == "__main__":
    main()
