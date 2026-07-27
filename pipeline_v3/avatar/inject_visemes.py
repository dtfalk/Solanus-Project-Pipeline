"""Inject N viseme morph targets into the base glb (pure stdlib GLB surgery)."""
import json, struct, sys

GLB = '/Workspace/Projects/Solanus-Project-Pipeline/pipeline_v3/avatar/solanus_visemes.glb'
MAP = sys.argv[1] if len(sys.argv) > 1 else '/tmp/viseme_maps.json'
TARGET_MESH = 'model.lips_and_jaw'

doc = json.load(open(MAP)); order = doc['order']; maps = doc['maps']
raw = open(GLB, 'rb').read(); assert raw[:4] == b'glTF'
total = struct.unpack('<I', raw[8:12])[0]
off = 12; json_bytes = bin_bytes = None
while off < total:
    clen, ctype = struct.unpack('<I4s', raw[off:off+8]); off += 8
    data = raw[off:off+clen]; off += clen
    if ctype == b'JSON': json_bytes = data
    elif ctype == b'BIN\x00': bin_bytes = data
g = json.loads(json_bytes); bin_data = bytearray(bin_bytes)

mesh = next(m for m in g['meshes'] if m.get('name') == TARGET_MESH)
prim = mesh['primitives'][0]
pos_acc = g['accessors'][prim['attributes']['POSITION']]
count = pos_acc['count']; bv = g['bufferViews'][pos_acc['bufferView']]
base_off = bv.get('byteOffset', 0) + pos_acc.get('byteOffset', 0); stride = bv.get('byteStride') or 12
positions = [struct.unpack_from('<3f', bin_data, base_off + i*stride) for i in range(count)]

prim.setdefault('targets', [])
for name in order:
    dmap = maps[name]; deltas = bytearray()
    mn = [1e9]*3; mx = [-1e9]*3; found = 0
    for (x, y, z) in positions:
        d = dmap.get(f"{round(x,5)},{round(y,5)},{round(z,5)}") or dmap.get(f"{round(x,4)},{round(y,4)},{round(z,4)}")
        if d is None: d = [0.0, 0.0, 0.0]
        else: found += 1
        deltas += struct.pack('<3f', *d)
        for c in range(3): mn[c] = min(mn[c], d[c]); mx[c] = max(mx[c], d[c])
    while len(bin_data) % 4: bin_data.append(0)
    bv_off = len(bin_data); bin_data += deltas
    g['bufferViews'].append({'buffer': 0, 'byteOffset': bv_off, 'byteLength': len(deltas)})
    g['accessors'].append({'bufferView': len(g['bufferViews'])-1, 'componentType': 5126,
                           'count': count, 'type': 'VEC3', 'min': mn, 'max': mx})
    prim['targets'].append({'POSITION': len(g['accessors'])-1})
    print(f"  {name}: matched {found}/{count}  min={mn} max={mx}")

mesh.setdefault('extras', {})['targetNames'] = order
mesh['weights'] = [0.0]*len(order)
while len(bin_data) % 4: bin_data.append(0)
g['buffers'][0]['byteLength'] = len(bin_data)
new_json = json.dumps(g, separators=(',', ':')).encode('utf-8')
while len(new_json) % 4: new_json += b' '
out = bytearray(b'glTF') + struct.pack('<II', 2, 0)
out += struct.pack('<I4s', len(new_json), b'JSON') + new_json
out += struct.pack('<I4s', len(bin_data), b'BIN\x00') + bytes(bin_data)
struct.pack_into('<I', out, 8, len(out))
open(GLB, 'wb').write(out)
print(f"INJECTED {len(order)} visemes -> {GLB} ({len(out)} bytes)")
