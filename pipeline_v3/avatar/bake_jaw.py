import bpy, math
from mathutils import Vector
import numpy as np
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath='/home/david/solanus_faceit.glb')
obj=[o for o in bpy.context.scene.objects if o.type=='MESH'][0]
bpy.context.view_layer.objects.active=obj
me=obj.data
co=np.array([(obj.matrix_world @ v.co)[:] for v in me.vertices])
xr=(co[:,0].min(),co[:,0].max()); yr=(co[:,1].min(),co[:,1].max()); zr=(co[:,2].min(),co[:,2].max())
print(f"BBOX x{xr} y{yr} z{zr}")
zmin,zmax=zr; H=zmax-zmin            # Z is up in Blender
# nose = most forward (min Y) in upper head
upper=co[co[:,2]>zmin+0.55*H]
nose=upper[np.argmin(upper[:,1])]
nose_y,nose_z=nose[1],nose[2]
mouth_z=nose_z-0.05*H
falloff=0.17*H
front_y=nose_y+0.35*(yr[1]-yr[0])
print(f"H={H:.3f} nose_y={nose_y:.3f} nose_z={nose_z:.3f} mouth_z={mouth_z:.3f} front_y={front_y:.3f}")
obj.shape_key_add(name='Basis')
sk=obj.shape_key_add(name='jawOpen'); sk.value=0
n=0
for i,v in enumerate(me.vertices):
    w=obj.matrix_world @ v.co
    if w[2]<mouth_z and w[1]<front_y:
        t=min(1.0,(mouth_z-w[2])/falloff); t=t*t*(3-2*t)
        disp=Vector((0.0, 0.018*H*t, -0.055*H*t))   # back(+Y) + down(-Z)
        sk.data[i].co = v.co + obj.matrix_world.inverted().to_3x3() @ disp
        n+=1
print(f"jawOpen affects {n} verts")
bpy.ops.export_scene.gltf(filepath='/home/david/solanus_talk.glb', export_format='GLB', export_morph=True)
print("EXPORTED")
