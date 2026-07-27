"""Bake a small VISEME set onto Solanus for real phoneme lip-sync (vs the single jawOpen).
Computes per-vertex displacements geometrically from the user's lower_lip/upper_lip/jaw groups:
  vOpen  - jaw drop (AA)            vWide  - lips spread wide (EE/IH)
  vRound - lips pucker forward (OO) vClose - lips pressed (M/B/P)   vFV - lower lip to teeth (F/V)
Writes a base glb (darkened cavity, receded teeth) + a JSON of glTF-space delta maps per viseme;
inject_visemes.py then injects them all as morph targets. avatar.mjs maps Azure viseme IDs onto these.
"""
import bpy, json, sys
from mathutils import Vector

SRC = '/Workspace/Projects/Solanus-Project-Pipeline/app/Solanus_David_Edit.blend'
OUT = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_visemes.glb'
MAP = sys.argv[sys.argv.index('--')+1] if '--' in sys.argv else '/tmp/viseme_maps.json'

bpy.ops.wm.open_mainfile(filepath=SRC)
head = bpy.data.objects['model']; me = head.data

# darken cavity + recede/tone the boxy teeth (same as the working talking bake)
def set_principled(obj, base, rough, spec=0.1):
    if not obj: return
    if not obj.data.materials:
        m = bpy.data.materials.new(obj.name+'_mat'); obj.data.materials.append(m)
    for m in obj.data.materials:
        m.use_nodes = True
        b = next((n for n in m.node_tree.nodes if n.type == 'BSDF_PRINCIPLED'), None)
        if not b: continue
        b.inputs['Base Color'].default_value = (*base, 1.0)
        if 'Roughness' in b.inputs: b.inputs['Roughness'].default_value = rough
        for sk in ('Specular IOR Level', 'Specular'):
            if sk in b.inputs: b.inputs[sk].default_value = spec
set_principled(bpy.data.objects.get('MouthCavity'), (0.012, 0.008, 0.008), 0.95, 0.0)
set_principled(bpy.data.objects.get('TeethUpper'), (0.52, 0.49, 0.44), 0.6)
set_principled(bpy.data.objects.get('TeethLower'), (0.52, 0.49, 0.44), 0.6)
for nm, off in (('TeethUpper', (0, 0.022, 0.004)), ('TeethLower', (0, 0.026, -0.004))):
    o = bpy.data.objects.get(nm)
    if o: o.location += Vector(off)

def members(name):
    gi = head.vertex_groups[name].index
    return set(v.index for v in me.vertices for g in v.groups if g.group == gi)
lower = members('lower_lip'); upper = members('upper_lip'); jaw = members('jaw') - lower - upper
lips = lower | upper

# mouth frame from the lip groups
co = {i: me.vertices[i].co.copy() for i in (lips | jaw)}
lx = [co[i].x for i in lips]; lz = [co[i].z for i in lips]
mcx = sum(lx)/len(lx); mcz = sum(lz)/len(lz); halfw = (max(lx)-min(lx))/2 or 0.14

def smoothstep(a, b, x):
    if b == a: return 0.0
    t = max(0.0, min(1.0, (x-a)/(b-a))); return t*t*(3-2*t)

def jaw_follow(z):                       # jaw tapers from the lip line down to the chin
    return smoothstep(0.45, 0.745, z)

# each viseme returns a Blender-space delta for vertex index i
def delta(name, i):
    c = co[i]; d = Vector((0, 0, 0))
    inlip = i in lips; inlow = i in lower; inup = i in upper
    if name == 'vOpen':                  # AA — jaw/lower-lip drop (the proven open)
        if inlow: d = Vector((0, 0.005, -0.030))
        elif inup: d = Vector((0, 0, 0.004))
        elif i in jaw:
            f = jaw_follow(c.z)
            if f: d = Vector((0, 0, -0.030*f))
    elif name == 'vWide':                # EE/IH — corners pull outward, slight part
        if inlip:
            side = 1.0 if c.x >= mcx else -1.0
            spread = smoothstep(0.0, halfw, abs(c.x-mcx))     # corners move most
            d = Vector((side*0.018*spread, 0.002, (-0.006 if inlow else 0.003)))
    elif name == 'vRound':               # OO/UW — gentle pucker inward + forward (front is -Y); barely open
        if inlip:
            d = Vector((-(c.x-mcx)*0.22, -0.014, (-0.004 if inlow else 0.003)))
    elif name == 'vClose':               # M/B/P — press lips together
        if inlow: d = Vector((0, 0, 0.006))
        elif inup: d = Vector((0, 0, -0.004))
    elif name == 'vFV':                  # F/V — lower lip up & back toward upper teeth
        if inlow: d = Vector((0, 0.008, 0.012))
        elif inup: d = Vector((0, 0, -0.002))
    return d

VISEMES = ['vOpen', 'vWide', 'vRound', 'vClose', 'vFV']
maps = {}
for name in VISEMES:
    m = {}; n = 0
    for i in (lips | jaw):
        d = delta(name, i)
        if d.length > 1e-5:
            c = co[i]
            key = f"{round(c.x,5)},{round(c.z,5)},{round(-c.y,5)}"      # glTF-space pos key
            m[key] = [round(d.x,6), round(d.z,6), round(-d.y,6)]        # glTF-space delta
            n += 1
    maps[name] = m
    print(f"  {name}: {n} verts moved")
json.dump({'order': VISEMES, 'maps': maps}, open(MAP, 'w'))
print("MAPS ->", MAP)

bpy.ops.export_scene.gltf(filepath=OUT, export_format='GLB', export_morph=False)
print("EXPORTED BASE", OUT)
