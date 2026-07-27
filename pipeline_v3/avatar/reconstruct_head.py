"""reconstruct_head.py — Stages 2-3: FLAME identity from photos, then a photoreal 3DGS head.

Stage 2: run Pixel3DMM on each restored frontal/profile to recover a metric FLAME mesh, then FUSE the
per-photo identity shape (beta) into one FLAME identity (the geometry anchor).
Stage 3: feed the ~4 best restored views into Avat3r (or FaceLift from the single best frontal) to get a
photoreal animatable 3D-Gaussian head, bound to the FLAME rig for expression control.

Honest: profile, back-of-head, ears, hair volume and all color off the captured angles are GENERATIVE.
Identity metrics (ArcFace cosine) certify only the seen frontal view.

SCAFFOLD: documents the wiring to the external repos (Pixel3DMM, Avat3r); steps are TODO hooks.

    python reconstruct_head.py
"""
from __future__ import annotations

import config


def fit_flame_per_photo():
    """Pixel3DMM (simongiebenhain.github.io/pixel3dmm): per-image normals/UV -> FLAME fit.
    TODO: run Pixel3DMM over config.RESTORED/*.png -> per-photo FLAME params in config.HEAD."""
    restored = sorted(config.RESTORED.glob("*.png"))
    print(f"  [Stage 2] would fit FLAME on {len(restored)} restored stills via {config.HEAD_GEOMETRY} (TODO)")
    return restored


def fuse_identity(fits):
    """Average/robust-fuse the per-photo FLAME identity (beta) into one identity mesh -> config.HEAD/identity.flame.
    TODO: load per-photo params, fuse beta (median over shots), keep expression params free."""
    print(f"  [Stage 2] would fuse {len(fits)} FLAME fits -> one identity (TODO) -> {config.HEAD}/identity.flame")


def build_gaussian_head():
    """Stage 3: Avat3r (~4 best views) or FaceLift (single frontal) -> animatable 3DGS head bound to FLAME.
    TODO: select best views, run config.HEAD_APPEARANCE, bind Gaussians to the fused FLAME rig."""
    print(f"  [Stage 3] would build photoreal 3DGS head via {config.HEAD_APPEARANCE}, bound to {config.HEAD_RIG} "
          f"(TODO) -> {config.HEAD}/head.3dgs")
    print("  NOTE: ±90°/back/ears/hair/color are diffusion-invented; validate frontal identity (ArcFace).")


def main():
    fits = fit_flame_per_photo()
    fuse_identity(fits)
    build_gaussian_head()
    print("\nNext: build_body.py (LHM/SMPL-X + habit), then drive.py (motion + talking head).")


if __name__ == "__main__":
    main()
