"""Render neutral + each viseme as a contact sheet to verify the mouth shapes."""
import bpy, sys
from mathutils import Vector
GLB = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_visemes.glb'
OUT = sys.argv[sys.argv.index('--')+1] if '--' in sys.argv else '/tmp'
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath=GLB)
sc = bpy.context.scene
for eng in ('BLENDER_EEVEE_NEXT', 'BLENDER_EEVEE'):
    try: sc.render.engine = eng; break
    except Exception: pass
sc.render.resolution_x = 420; sc.render.resolution_y = 520
head = max((o for o in sc.objects if o.type == 'MESH'), key=lambda o: len(o.data.vertices))
me = head.data
keys = [k.name for k in me.shape_keys.key_blocks] if me.shape_keys else []
print("keys:", keys)
basis = [v.co.copy() for v in me.vertices]
morphs = {}
if me.shape_keys:
    for kb in me.shape_keys.key_blocks:
        if kb.name != 'Basis':
            morphs[kb.name] = [kb.data[i].co.copy() for i in range(len(me.vertices))]
head.shape_key_clear()
world = bpy.data.worlds.new('w'); sc.world = world; world.use_nodes = True
world.node_tree.nodes['Background'].inputs[0].default_value = (0.05, 0.045, 0.04, 1)
def light(n, loc, e):
    d = bpy.data.lights.new(n, 'AREA'); d.energy = e; d.size = 2.0
    o = bpy.data.objects.new(n, d); o.location = loc; sc.collection.objects.link(o)
light('k', (-1.0, -2.2, 1.4), 220); light('f', (1.4, -1.6, 1.0), 90); light('r', (0.0, 1.6, 1.6), 120)
cam_d = bpy.data.cameras.new('c'); cam_d.lens = 85
cam = bpy.data.objects.new('c', cam_d); sc.collection.objects.link(cam); sc.camera = cam
target = Vector((-0.06, -0.58, 0.80)); cam.location = Vector((-0.05, -1.75, 0.86))
cam.rotation_euler = (target - cam.location).to_track_quat('-Z', 'Y').to_euler()
def render(tag, co):
    for i, v in enumerate(me.vertices): v.co = co[i]
    me.update(); sc.render.filepath = f'{OUT}/vis_{tag}.png'; bpy.ops.render.render(write_still=True)
    print('rendered', tag)
render('neutral', basis)
for name, co in morphs.items(): render(name, co)
print('DONE', list(morphs.keys()))
