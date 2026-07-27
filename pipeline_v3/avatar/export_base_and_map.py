"""Blender background can't export shape keys (depsgraph ignores them headless), so:
export a CLEAN base glb (textures+normals intact) and dump the per-vertex morph delta
map keyed by glTF-space position. A pure-python pass then injects the morph target.
Delta logic = the tuned lip/jaw open (lower lip primary, jaw smoothstep follow)."""
import bpy, json, sys
from mathutils import Vector

SRC = '/Workspace/Projects/Solanus-Project-Pipeline/app/Solanus_David_Edit.blend'
OUT = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_talking.glb'
MAP = sys.argv[sys.argv.index('--')+1] if '--' in sys.argv else '/tmp/delta_map.json'

LIP_DROP, LIP_TUCK, UP_LIP, JAW_FULL_Z, JAW_ZERO_Z = 0.026, 0.005, 0.003, 0.745, 0.40
extra = sys.argv[sys.argv.index('--')+2:] if '--' in sys.argv else []
if len(extra) >= 5:
    LIP_DROP, LIP_TUCK, UP_LIP, JAW_FULL_Z, JAW_ZERO_Z = map(float, extra[:5])

bpy.ops.wm.open_mainfile(filepath=SRC)
head = bpy.data.objects['model']; me = head.data

# Make the open mouth read as a real dark cavity (not a grey faceted box), and tone the
# stark-white teeth slab to bone. Non-destructive: only material colors on the interior props.
def set_principled(obj, base, rough, spec=0.1):
    if not obj: return
    if not obj.data.materials:
        m = bpy.data.materials.new(obj.name + '_mat'); obj.data.materials.append(m)
    for m in obj.data.materials:
        m.use_nodes = True
        bsdf = next((n for n in m.node_tree.nodes if n.type == 'BSDF_PRINCIPLED'), None)
        if not bsdf: continue
        bsdf.inputs['Base Color'].default_value = (*base, 1.0)
        if 'Roughness' in bsdf.inputs: bsdf.inputs['Roughness'].default_value = rough
        for sk in ('Specular IOR Level', 'Specular'):
            if sk in bsdf.inputs: bsdf.inputs[sk].default_value = spec
set_principled(bpy.data.objects.get('MouthCavity'), (0.012, 0.008, 0.008), 0.95, 0.0)
set_principled(bpy.data.objects.get('TeethUpper'), (0.52, 0.49, 0.44), 0.6)
set_principled(bpy.data.objects.get('TeethLower'), (0.52, 0.49, 0.44), 0.6)
# recede the boxy teeth INTO the mouth (front is -Y) so the dark cavity reads first and
# the teeth sit in shadow rather than as a white slab at the lip surface
from mathutils import Vector as _V
_tu = bpy.data.objects.get('TeethUpper'); _tl = bpy.data.objects.get('TeethLower')
if _tu: _tu.location += _V((0.0, 0.022, 0.004))
if _tl: _tl.location += _V((0.0, 0.026, -0.004))

def members(name):
    gi = head.vertex_groups[name].index
    return set(v.index for v in me.vertices for g in v.groups if g.group == gi)
lower = members('lower_lip'); upper = members('upper_lip')
jaw = members('jaw') - lower - upper

def smoothstep(a, b, x):
    if b == a: return 0.0
    t = max(0.0, min(1.0, (x - a) / (b - a)))
    return t * t * (3 - 2 * t)

# delta per original vertex, mapped to glTF axes: (bx,by,bz) -> (bx, bz, -by)
dmap = {}; moved = 0
for i, v in enumerate(me.vertices):
    co = v.co; d = Vector((0.0, 0.0, 0.0))
    if i in lower:   d = Vector((0.0, LIP_TUCK, -LIP_DROP))
    elif i in upper: d = Vector((0.0, 0.0, UP_LIP))
    elif i in jaw:
        f = smoothstep(JAW_ZERO_Z, JAW_FULL_Z, co.z)
        if f > 0: d = Vector((0.0, 0.0, -LIP_DROP * f))
    if d.length > 0:
        key = f"{round(co.x,5)},{round(co.z,5)},{round(-co.y,5)}"   # glTF-space position key
        dmap[key] = [round(d.x,6), round(d.z,6), round(-d.y,6)]     # glTF-space delta
        moved += 1
json.dump(dmap, open(MAP, 'w'))
print(f"MAP wrote {len(dmap)} deltas (moved {moved}; lower={len(lower)} upper={len(upper)} jaw={len(jaw)})")

bpy.ops.export_scene.gltf(filepath=OUT, export_format='GLB', export_morph=False)
print("EXPORTED BASE", OUT)
