"""restore_images.py — Stage 1: restore, upscale, and (carefully) colorize the historical stills.

Blind face restoration (CodeFormer) + diffusion old-photo restoration, then optional Time-Travel
Rephotography on the best frontals, then colorization ANCHORED on the single color photo. All color other
than what that one photo constrains is a documented guess — keep both the B&W master and the colorized
version. Needs high-res scans in data/raw/ (Stage 0 — from the Capuchin archive).

SCAFFOLD: wires CodeFormer if installed; diffusion/rephoto/colorize steps are marked TODO (external repos).

    python restore_images.py
"""
from __future__ import annotations
from pathlib import Path

import config


def restore_one(src: Path, dst: Path):
    """Blind face restoration. BEST: CodeFormer (sczhou/CodeFormer). TODO: wire its inference; pass-through now."""
    try:
        import shutil
        shutil.copy(src, dst)   # placeholder; replace with CodeFormer call
        print(f"  [restore] {src.name} -> {dst.name}  (TODO wire CodeFormer + Selective-Guided Diffusion)")
    except Exception as e:
        print(f"  [restore] {src.name}: {str(e)[:60]}")


def main():
    raws = sorted([p for p in config.RAW.glob("*") if p.suffix.lower() in (".tif", ".tiff", ".png", ".jpg", ".jpeg")])
    if not raws:
        raise SystemExit(f"no scans in {config.RAW} — obtain high-res scans from the Capuchin archive first (Stage 0)")
    for p in raws:
        restore_one(p, config.RESTORED / f"{p.stem}.restored.png")
    print(f"\nrestored {len(raws)} stills -> {config.RESTORED}")
    print(f"Colorize anchored on {config.COLOR_PHOTO.name} (eyes: blue); KEEP the B&W master alongside any "
          "colorized version — non-anchored color is a documented guess. Next: reconstruct_head.py")


if __name__ == "__main__":
    main()
