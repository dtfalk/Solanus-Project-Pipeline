"""Verify the EXPORTED glb's jawOpen morph visually. Headless Blender won't evaluate
shape-key VALUES, so we read the imported morph data and bake it onto real vertices
(== what three.js does with morphTargetInfluence=1), then render closed vs open."""
import bpy, sys
from mathutils import Vector

GLB = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_talking.glb'
OUTDIR = sys.argv[sys.argv.index('--')+1] if '--' in sys.argv else '/tmp'

bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath=GLB)
sc = bpy.context.scene
for eng in ('BLENDER_EEVEE_NEXT', 'BLENDER_EEVEE'):
    try: sc.render.engine = eng; break
    except Exception: pass
sc.render.resolution_x = 760; sc.render.resolution_y = 900

head = max((o for o in sc.objects if o.type == 'MESH'), key=lambda o: len(o.data.vertices))
me = head.data
basis = jk = None
if me.shape_keys:
    for kb in me.shape_keys.key_blocks:
        if kb.name == 'Basis': basis = kb
        if kb.name == 'jawOpen': jk = kb
print("shape keys:", [k.name for k in me.shape_keys.key_blocks] if me.shape_keys else None)
base_co = [v.co.copy() for v in me.vertices]            # untouched basis
open_co = [jk.data[i].co.copy() for i in range(len(me.vertices))] if jk else base_co
maxd = max((open_co[i]-base_co[i]).length for i in range(len(me.vertices))) if jk else 0
print(f"morph max vertex travel = {maxd:.4f}")
# CRITICAL: with shape keys present, render uses the key mix and ignores me.vertices.co edits.
# Clear them so our direct coord writes actually deform the rendered mesh.
head.shape_key_clear()

world = bpy.data.worlds.new('w'); sc.world = world; world.use_nodes = True
world.node_tree.nodes['Background'].inputs[0].default_value = (0.05, 0.045, 0.04, 1)
def light(n, loc, e):
    d = bpy.data.lights.new(n, 'AREA'); d.energy = e; d.size = 2.0
    o = bpy.data.objects.new(n, d); o.location = loc; sc.collection.objects.link(o)
light('key', (-1.0, -2.2, 1.4), 220); light('fill', (1.4, -1.6, 1.0), 90); light('rim', (0.0, 1.6, 1.6), 120)

cam_d = bpy.data.cameras.new('cam'); cam_d.lens = 50
cam = bpy.data.objects.new('cam', cam_d); sc.collection.objects.link(cam); sc.camera = cam
target = Vector((-0.05, -0.55, 0.86)); cam.location = Vector((-0.03, -2.7, 0.95))   # head-and-shoulders, like the app
cam.rotation_euler = (target - cam.location).to_track_quat('-Z', 'Y').to_euler()

def render(tag, co_list):
    for i, v in enumerate(me.vertices): v.co = co_list[i]
    me.update()
    sc.render.filepath = f'{OUTDIR}/mouth_{tag}.png'
    bpy.ops.render.render(write_still=True)
    print("rendered", tag)

render('closed', base_co)
render('open', open_co)
print("RENDER DONE")
