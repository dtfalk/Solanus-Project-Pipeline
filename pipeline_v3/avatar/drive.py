"""drive.py — Stage 5: drive the avatar with motion + speech (his documented words, his cloned voice).

SMPL-X is the single driving currency. Locomotion/blessing gestures retarget from AMASS or a mocap stand-in
(there is NO footage of him, so motion is borrowed). Co-speech gesture is generated from the audio with EMAGE
(emits SMPL-X + FLAME). The audio itself is the ../voice render of text from ../mimicker — his own words.
Facial/lip motion drives the FLAME-rigged head, or a 2D talking-head model produces video.

SCAFFOLD: documents the wiring; steps are TODO hooks to the external repos.

    python drive.py --audio ../voice/data/renders/answer.wav --mode realtime
"""
from __future__ import annotations
import argparse

import config


def cospeech_gesture(audio_path: str):
    """EMAGE: audio -> SMPL-X+FLAME co-speech gesture (face+body+hands). TODO: wire EMAGE -> config.MOTION."""
    print(f"  [gesture] {config.MOTION_SOURCE['cospeech_gesture']} on {audio_path} -> SMPL-X motion (TODO)")


def locomotion():
    """Retarget AMASS clips / a mocap stand-in into SMPL-X for gait + blessing gestures. TODO."""
    print(f"  [locomotion] retarget {config.MOTION_SOURCE['locomotion']} / stand-in -> SMPL-X (TODO)")


def talking_head(audio_path: str, mode: str):
    """Audio-driven face. Pick by quality/latency (config.TALKING_HEAD)."""
    pick = {"max": config.TALKING_HEAD["max_quality_closed"],
            "photoreal": config.TALKING_HEAD["open_photoreal"],
            "realtime": config.TALKING_HEAD["open_realtime"]}.get(mode, config.TALKING_HEAD["open_realtime"])
    print(f"  [talking-head] {pick} on {audio_path} (TODO)")
    if pick == config.TALKING_HEAD["open_photoreal"]:
        print("    NOTE: HunyuanVideo-Avatar ~60 min/10s on a 96GB GPU — run offline/batched, exceeds one L40S.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audio", required=True, help="../voice render of his documented words")
    ap.add_argument("--mode", choices=["max", "photoreal", "realtime"], default="realtime")
    a = ap.parse_args()
    locomotion()
    cospeech_gesture(a.audio)
    talking_head(a.audio, a.mode)
    print(f"\nComposite + (approx) relight + UE5 runtime via composite.py. Embed disclosure:\n  {config.DISCLOSURE}")
    print("Words: Solanus's own (../mimicker); voice: cloned (../voice); motion: synthesized.")


if __name__ == "__main__":
    main()
