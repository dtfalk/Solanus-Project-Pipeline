"""Bake a natural 'jawOpen' shape key onto Solanus driven by the user's hand-painted
lip/jaw vertex groups. Lower lip is the PRIMARY mover (drops to part from the still
upper lip, revealing the static teeth + dark cavity behind); the jaw/beard follows
with a smoothstep taper so the chin moves ~half the lip and the beard tip barely sways.
Reads app/Solanus_David_Edit.blend (has groups lower_lip/upper_lip/jaw + teeth/cavity).
"""
import bpy, sys
from mathutils import Vector

SRC = '/Workspace/Projects/Solanus-Project-Pipeline/app/Solanus_David_Edit.blend'
OUT = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_talking.glb'

# --- tunables (overridable via CLI after `--`) ---
LIP_DROP = 0.026     # lower lip travels straight down at full open
LIP_TUCK = 0.005     # + a touch backward/inward (+Y) so it rolls under, not just down
UP_LIP   = 0.003     # upper lip lifts a hair to make the parting read (kept tiny = natural)
JAW_FULL_Z = 0.745   # at/above this z the jaw follows the lip fully
JAW_ZERO_Z = 0.40    # at/below this z the jaw doesn't move (long beard tip stays put)
argv = sys.argv[sys.argv.index('--')+1:] if '--' in sys.argv else []
if len(argv) >= 5:
    LIP_DROP, LIP_TUCK, UP_LIP, JAW_FULL_Z, JAW_ZERO_Z = map(float, argv[:5])

bpy.ops.wm.open_mainfile(filepath=SRC)
head = bpy.data.objects['model']
me = head.data

def members(group_name):
    gi = head.vertex_groups[group_name].index
    out = set()
    for v in me.vertices:
        for g in v.groups:
            if g.group == gi:
                out.add(v.index); break
    return out

lower = members('lower_lip')
upper = members('upper_lip')
jaw   = members('jaw') - lower - upper   # priority: lip groups win at any seam overlap

def smoothstep(a, b, x):
    if b == a: return 0.0
    t = max(0.0, min(1.0, (x - a) / (b - a)))
    return t * t * (3 - 2 * t)

# fresh basis + jawOpen (remove any stale keys first so re-bakes are clean)
if me.shape_keys:
    head.shape_key_clear()
head.shape_key_add(name='Basis')
sk = head.shape_key_add(name='jawOpen'); sk.value = 0.0

moved = 0
for i, v in enumerate(me.vertices):
    base = v.co
    d = Vector((0.0, 0.0, 0.0))
    if i in lower:
        d = Vector((0.0, LIP_TUCK, -LIP_DROP))
    elif i in upper:
        d = Vector((0.0, 0.0, UP_LIP))
    elif i in jaw:
        f = smoothstep(JAW_ZERO_Z, JAW_FULL_Z, base.z)
        if f > 0:
            d = Vector((0.0, 0.0, -LIP_DROP * f))
    if d.length > 0:
        sk.data[i].co = base + d; moved += 1

print(f"BAKE lip_drop={LIP_DROP} tuck={LIP_TUCK} up={UP_LIP} jaw[{JAW_ZERO_Z},{JAW_FULL_Z}]")
print(f"  lower={len(lower)} upper={len(upper)} jaw={len(jaw)} -> moved {moved} verts")
# read back: does the shape key actually hold the delta vs basis?
basis = me.shape_keys.key_blocks['Basis']
deltas = [(sk.data[i].co - basis.data[i].co).length for i in range(len(me.vertices))]
print(f"  stored shape-key delta: max={max(deltas):.5f} nonzero={sum(1 for x in deltas if x>1e-6)}")

bpy.ops.export_scene.gltf(filepath=OUT, export_format='GLB',
                          export_morph=True, export_morph_normal=False,
                          export_morph_tangent=False, export_apply=False)
# verify the written glb actually carries nonzero morph deltas
import json as _j, struct as _s
_d = open(OUT, 'rb').read(); _ln = _s.unpack('<I', _d[12:16])[0]; _g = _j.loads(_d[20:20+_ln])
for _m in _g['meshes']:
    for _p in _m['primitives']:
        for _t in _p.get('targets', []):
            _a = _g['accessors'][_t['POSITION']]
            print(f"  EXPORT CHECK mesh '{_m.get('name')}': morph min={_a.get('min')} max={_a.get('max')}")
print("EXPORTED", OUT)
