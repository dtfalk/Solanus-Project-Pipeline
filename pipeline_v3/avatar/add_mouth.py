import bpy, math
from mathutils import Vector, Matrix
import numpy as np
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath='/home/david/solanus_faceit.glb')
face=[o for o in bpy.context.scene.objects if o.type=='MESH'][0]; face.name='Head'
# APPLY transforms so the head sits at identity (added geometry then shares one frame)
bpy.ops.object.select_all(action='DESELECT'); face.select_set(True)
bpy.context.view_layer.objects.active=face
bpy.ops.object.transform_apply(location=True,rotation=True,scale=True)
me=face.data
co=np.array([v.co[:] for v in me.vertices])
xr=(co[:,0].min(),co[:,0].max()); yr=(co[:,1].min(),co[:,1].max()); zr=(co[:,2].min(),co[:,2].max())
H=zr[1]-zr[0]
upper=co[co[:,2]>zr[0]+0.55*H]; nose=upper[np.argmin(upper[:,1])]
nose_y,nose_z=nose[1],nose[2]; mouth_z=nose_z-0.14*H
near=co[(np.abs(co[:,2]-mouth_z)<0.03*H)&(co[:,1]<nose_y+0.12*H)]
lip_y=near[:,1].min(); lip_w=0.16*(xr[1]-xr[0])
print(f"H={H:.3f} nose=({nose_y:.3f},{nose_z:.3f}) mouth_z={mouth_z:.3f} lip_y={lip_y:.3f} lip_w={lip_w:.3f}")
def add_box(name,loc,scale,color):
    bpy.ops.mesh.primitive_cube_add(size=1.0,location=loc); o=bpy.context.active_object; o.name=name; o.scale=scale
    m=bpy.data.materials.new(name+'_m'); m.use_nodes=True
    b=m.node_tree.nodes.get('Principled BSDF'); b.inputs['Base Color'].default_value=color; b.inputs['Roughness'].default_value=0.7
    o.data.materials.append(m); o.parent=face; return o
# dark cavity BEHIND the lips (forward is -Y, so behind = +Y); slightly below the seam
add_box('MouthCavity',(0.0, lip_y+0.085*H, mouth_z-0.012*H),(lip_w*1.05, 0.05*H, 0.06*H),(0.015,0.008,0.008,1))
bpy.context.view_layer.objects.active=face
face.shape_key_add(name='Basis'); sk=face.shape_key_add(name='jawOpen'); sk.value=0
hinge=Vector((0.0, nose_y+0.55*(yr[1]-nose_y), mouth_z+0.015*H))
seam_z=mouth_z+0.004*H; falloff=0.16*H; ang=math.radians(26); n=0
for i,v in enumerate(me.vertices):
    w=v.co
    if w[2]<seam_z and w[1]<(yr[0]+0.62*(yr[1]-yr[0])):
        t=min(1.0,(seam_z-w[2])/falloff); t=t*t*(3-2*t)
        rel=Vector(w)-hinge; sk.data[i].co=hinge+(Matrix.Rotation(ang*t,4,'X') @ rel); n+=1
print(f"jawOpen affects {n} verts")
bpy.ops.export_scene.gltf(filepath='/home/david/solanus_mouth.glb', export_format='GLB', export_morph=True)
print("EXPORTED")
