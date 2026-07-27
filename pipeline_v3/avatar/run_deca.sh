#!/usr/bin/env bash
# run_deca.sh — photo -> riggable FLAME head mesh (Faceit-ready), LOCAL, on the RTX 5070 Ti (Blackwell sm_120).
# This is the WORKING Stage-2 path (DECA), verified 2026-06-24. Output is a 5118-vert FLAME-topology head with
# real open lip-loops + jaw (NOT a sealed scan) -> import to Blender -> Faceit bakes the ARKit visemes.
#
# Env was built once as conda env 'flame' (python 3.10): torch 2.7.1+cu128 (sm_120), the PREBUILT pytorch3d
# cu128 wheel (source-build fails on Blackwell), face-alignment, kornia, chumpy (--no-build-isolation), numpy<1.24.
# FLAME generic_model.pkl + DECA weights pulled from the HF mirror camenduru/show into ~/DECA/data.
# DECA patch applied: decalib/datasets/detectors.py  LandmarksType._2D -> .TWO_D (newer face_alignment).
set -e
PHOTO="${1:-/Workspace/Projects/Solanus-Project-Pipeline/app/Solanus-Screenshot.png}"
OUT="${2:-$(dirname "$0")/output}"
source /home/david/miniconda3/etc/profile.d/conda.sh; conda activate flame
mkdir -p /home/david/friar_in "$OUT"
cp "$PHOTO" /home/david/friar_in/solanus.png
cd /home/david/DECA
python demos/demo_reconstruct.py -i /home/david/friar_in -s /home/david/friar_out \
  --saveObj True --rasterizer_type pytorch3d --device cuda --useTex False
# stage results
cp /home/david/friar_out/solanus/solanus.obj  "$OUT/"
cp /home/david/friar_out/solanus/solanus.mtl  "$OUT/"
cp /home/david/friar_out/solanus/solanus.png  "$OUT/"          # frontal face texture (FLAME UV)
cp /home/david/friar_out/solanus/solanus_detail.obj "$OUT/"    # 59k-vert displacement-detail mesh
cp /home/david/friar_out/solanus_vis.jpg "$OUT/solanus_fit_overlay.jpg"
python -c "import trimesh; m=trimesh.load('$OUT/solanus.obj',process=False); m.export('$OUT/solanus_head.glb'); print('glb', len(m.vertices),'verts')"
echo "DONE -> $OUT/solanus.obj (+ .glb, _detail.obj, texture). Next: Blender import -> Faceit -> bake visemes."
