"""config.py — Solanus digital-avatar configuration (separate track; see PLAN.md).

Best-version-first tool choices per stage, from the 2026 deep research. Edit here; scripts consume it.
Compute target: RCC Midway3 (4x L40S = 192 GB; see ../mimicker/SYSTEM.md). Heavy talking-head inference
(HunyuanVideo-Avatar) runs offline/batched.
"""
from __future__ import annotations
from pathlib import Path

AVATAR = Path(__file__).resolve().parent
PIPELINE_V3 = AVATAR.parent
REPO = PIPELINE_V3.parent

DATA = AVATAR / "data"
RAW = DATA / "raw"                  # high-res scans from the Capuchin archive (originals/negatives)
RESTORED = DATA / "restored"       # restored / upscaled / colorized stills
HEAD = DATA / "head"               # FLAME fits + 3DGS head
BODY = DATA / "body"               # SMPL-X body + habit sim
MOTION = DATA / "motion"           # retargeted AMASS / EMAGE co-speech gesture
RENDERS = DATA / "renders"
for d in (DATA, RAW, RESTORED, HEAD, BODY, MOTION, RENDERS):
    d.mkdir(parents=True, exist_ok=True)

COLOR_PHOTO = RAW / "solanus_color.tif"     # the ONE color photo — palette anchor (eyes: blue)

# --- per-stage model choices (best-version-first) ---
RESTORE = {"blind": "codeformer", "diffusion": "selective-guided-diffusion",
           "rephoto": "time-travel-rephotography", "colorize_anchor": str(COLOR_PHOTO)}
HEAD_GEOMETRY = "pixel3dmm"                  # single-image -> FLAME (SOTA geometry)
HEAD_APPEARANCE = "avat3r"                   # sparse (~4 img) animatable 3DGS head; alt: "facelift"
HEAD_RIG = "flame"
BODY_RECON = "lhm"                           # 1 image -> animatable full-body 3DGS; alt: "sith"
BODY_RIG = "smplx"
HABIT = {"pattern": "dress-1-to-3", "sim": "contourcraft"}   # Capuchin habit: hood/cowl/rope cincture
MOTION_SOURCE = {"locomotion": "amass", "cospeech_gesture": "emage"}   # no footage of him -> borrowed motion
TALKING_HEAD = {"max_quality_closed": "omnihuman-1.5", "open_photoreal": "hunyuanvideo-avatar",
                "open_realtime": "ditto+liveportrait", "open_4k_long": "hallo2"}
RELIGHT = "approximate"                      # true codec-avatar relight needs a light stage (unavailable)
RUNTIME = {"engine": "unreal-5.7", "gs_plugin": "xscene-ueplugin", "audio2face": "nvidia-ace"}

# --- drivers (his documented words / his cloned voice — NOT free-form generation) ---
TEXT_MIMICKER = PIPELINE_V3 / "mimicker"
VOICE = PIPELINE_V3 / "voice"

DISCLOSURE = ("Historically reconstructed digital likeness of Blessed Solanus Casey; geometry/color partly "
              "AI-generated and motion synthesized. Produced with permission of the Capuchin Franciscan "
              "Province of St. Joseph.")
