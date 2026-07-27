"""Inject a 'jawOpen' morph target into a GLB by matching exported vertices to the
delta map (keyed by glTF-space position). Pure stdlib GLB surgery — no deps."""
import json, struct, sys

GLB = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_talking.glb'
MAP = sys.argv[1] if len(sys.argv) > 1 else '/tmp/delta_map.json'
TARGET_MESH = 'model.lips_and_jaw'

dmap = json.load(open(MAP))
raw = open(GLB, 'rb').read()
assert raw[:4] == b'glTF'
total = struct.unpack('<I', raw[8:12])[0]

# parse chunks
off = 12; json_bytes = bin_bytes = None
while off < total:
    clen, ctype = struct.unpack('<I4s', raw[off:off+8]); off += 8
    data = raw[off:off+clen]; off += clen
    if ctype == b'JSON': json_bytes = data
    elif ctype == b'BIN\x00': bin_bytes = data
g = json.loads(json_bytes)
bin_data = bytearray(bin_bytes)

# locate target primitive + its POSITION accessor
mesh = next(m for m in g['meshes'] if m.get('name') == TARGET_MESH)
prim = mesh['primitives'][0]
pos_acc = g['accessors'][prim['attributes']['POSITION']]
count = pos_acc['count']
bv = g['bufferViews'][pos_acc['bufferView']]
base_off = bv.get('byteOffset', 0) + pos_acc.get('byteOffset', 0)
stride = bv.get('byteStride') or 12

# read exported positions, build delta buffer
deltas = bytearray(); found = 0
mn = [1e9, 1e9, 1e9]; mx = [-1e9, -1e9, -1e9]
for i in range(count):
    x, y, z = struct.unpack_from('<3f', bin_data, base_off + i * stride)
    key = f"{round(x,5)},{round(y,5)},{round(z,5)}"
    d = dmap.get(key)
    if d is None:
        # tolerance retry at 4 decimals (float32 rounding near .xxxx5 boundaries)
        d = dmap.get(f"{round(x,4)},{round(y,4)},{round(z,4)}")
    if d is None: d = [0.0, 0.0, 0.0]
    else: found += 1
    deltas += struct.pack('<3f', *d)
    for c in range(3):
        mn[c] = min(mn[c], d[c]); mx[c] = max(mx[c], d[c])

print(f"matched {found}/{count} exported verts to deltas; delta min={mn} max={mx}")
if found == 0:
    sys.exit("FATAL: no vertex matched — position keying is off")

# align buffer to 4 bytes, append delta bufferView
while len(bin_data) % 4: bin_data.append(0)
new_bv_offset = len(bin_data)
bin_data += deltas
while len(bin_data) % 4: bin_data.append(0)

g['bufferViews'].append({'buffer': 0, 'byteOffset': new_bv_offset, 'byteLength': len(deltas)})
new_bv = len(g['bufferViews']) - 1
g['accessors'].append({'bufferView': new_bv, 'componentType': 5126, 'count': count,
                       'type': 'VEC3', 'min': mn, 'max': mx})
new_acc = len(g['accessors']) - 1
prim.setdefault('targets', []).append({'POSITION': new_acc})
mesh.setdefault('extras', {})['targetNames'] = ['jawOpen']
mesh['weights'] = [0.0]
g['buffers'][0]['byteLength'] = len(bin_data)

# re-serialize GLB
new_json = json.dumps(g, separators=(',', ':')).encode('utf-8')
while len(new_json) % 4: new_json += b' '
while len(bin_data) % 4: bin_data.append(0)
out = bytearray()
out += b'glTF' + struct.pack('<II', 2, 0)  # length patched below
out += struct.pack('<I4s', len(new_json), b'JSON') + new_json
out += struct.pack('<I4s', len(bin_data), b'BIN\x00') + bytes(bin_data)
struct.pack_into('<I', out, 8, len(out))
open(GLB, 'wb').write(out)
print(f"INJECTED jawOpen morph into {GLB} ({len(out)} bytes)")
