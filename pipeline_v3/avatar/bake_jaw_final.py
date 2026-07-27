import bpy, math
from mathutils import Vector, Matrix
import numpy as np
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath='/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_ready.glb')
sc=bpy.context.scene
head=[o for o in sc.objects if o.name=='model'][0]
def cz(name):
    o=[x for x in sc.objects if x.name==name]
    if not o: return None
    return np.array([(o[0].matrix_world@v.co)[2] for v in o[0].data.vertices]).mean()
uz,lz=cz('TeethUpper'),cz('TeethLower')
seam_z=(uz+lz)/2 if (uz and lz) else 0.785
print(f"teeth upper={uz:.3f} lower={lz:.3f} -> seam_z={seam_z:.3f}")
bpy.ops.object.select_all(action='DESELECT'); head.select_set(True); bpy.context.view_layer.objects.active=head
bpy.ops.object.transform_apply(location=True,rotation=True,scale=True)
me=head.data
co=np.array([v.co[:] for v in me.vertices]); xr=(co[:,0].min(),co[:,0].max()); Xw=xr[1]-xr[0]
jaw_halfw=0.45*Xw; chin_bot=seam_z-0.28
hinge=Vector((0.0, 0.10, seam_z+0.02)); ang=math.radians(22); n=0
head.shape_key_add(name='Basis'); sk=head.shape_key_add(name='jawOpen'); sk.value=0
c01=lambda x: max(0.0,min(1.0,x)); ss=lambda x:(lambda y:y*y*(3-2*y))(c01(x))
for i,v in enumerate(me.vertices):
    w=v.co
    if w[2]<seam_z and w[1]<-0.12 and abs(w[0])<jaw_halfw and w[2]>chin_bot-0.10:
        rise=c01((seam_z-w[2])/0.012); bottom=ss((w[2]-(chin_bot-0.10))/0.12); side=ss((jaw_halfw-abs(w[0]))/(0.12*Xw))
        wt=rise*bottom*side
        if wt>0:
            rel=Vector(w)-hinge; sk.data[i].co=hinge+(Matrix.Rotation(ang*wt,4,'X')@rel); n+=1
print(f"jawOpen affects {n} verts (seam {seam_z:.3f})")
bpy.ops.export_scene.gltf(filepath='/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_talking.glb', export_format='GLB', export_morph=True)
print("EXPORTED")
