#!/usr/bin/env python3
"""STEP_6 viewer #1 — page <-> segmentation (local web app, stdlib only).

LEFT  : the masked source page, gold polygons outlined and colour-coded by doc_N (notebook
        src_content entries each outlined too).
RIGHT : the JSON for what that page produced. Default shows the page summary (every letter /
        notebook entry touching this page). HOVER a region to highlight it; CLICK it to pin that
        single document's / notebook entry's JSON on the right.

Reads documents.json + notebooks.json (built by segment_documents.py / segment_notebooks.py) and
3_enriched. Read-only. Prev/Next or jump by index.

Usage:  venv/bin/python view_segments.py        then open http://localhost:8001
"""
from __future__ import annotations
import json, os, re, glob, argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

HERE = Path(__file__).resolve().parent
GOLD_RE = re.compile(r"page_\d+\.json")
ROOT = HERE / "3_enriched"
PAGES = []          # [{section, page, pdf, pdir}]
DOCS = []           # documents.json
NB = []             # notebooks.json


def gold_json(pdir):
    for f in os.listdir(pdir):
        if GOLD_RE.fullmatch(f):
            return os.path.join(pdir, f)
    return None


def load():
    global PAGES, DOCS, NB
    DOCS = json.loads((HERE / "documents.json").read_text()) if (HERE / "documents.json").exists() else []
    NB = json.loads((HERE / "notebooks.json").read_text()) if (HERE / "notebooks.json").exists() else []
    pages = []
    for jp in glob.glob(str(ROOT / "*" / "1_source_pages" / "page_*" / "page_*.json")):
        if not GOLD_RE.fullmatch(os.path.basename(jp)):
            continue
        m = json.loads(Path(jp).read_text())
        pages.append({"section": m.get("source_file", Path(jp).parts[-4]),
                      "page": m["page_number_in_type"], "pdf": m["pdf_page_number"],
                      "pdir": os.path.dirname(jp)})
    PAGES = sorted(pages, key=lambda p: (p["section"], p["page"]))


def payload(i):
    pg = PAGES[i]
    meta = json.loads(Path(gold_json(pg["pdir"])).read_text())
    polys = []
    for dk, doc in meta.get("documents", {}).items():
        for cat, ps in (doc or {}).items():
            for j, p in enumerate(ps or []):
                v = p.get("vertices", [])
                if len(v) >= 3:
                    polys.append({"rid": f"{dk}.{cat}.{j}", "dk": dk, "cat": cat,
                                  "quad": [[float(x["x"]), float(x["y"])] for x in v]})
    sec, page = pg["section"], pg["page"]
    records = [d for d in DOCS if d["section"] == sec and page in d.get("pages", [])]
    nb_page = next((e for e in NB if e["section"] == sec and e["page_number_in_type"] == page), None)
    entries = nb_page["entries"] if nb_page else []
    dk2rec = {}
    for d in records:
        for fr in d.get("fragments", []):
            if fr[0] == page:
                dk2rec[fr[1]] = d["id"]
    rid2ent = {e["rid"]: k for k, e in enumerate(entries) if e.get("rid")}
    return {"i": i, "total": len(PAGES), "section": sec, "page": page, "pdf": pg["pdf"],
            "width": int(meta["page_width"]), "height": int(meta["page_height"]),
            "is_notebook": bool(nb_page) and not records,
            "polys": polys, "records": records, "entries": entries, "notebook_page": nb_page,
            "dk2rec": dk2rec, "rid2ent": rid2ent}


HTML = r"""<!doctype html><html><head><meta charset=utf-8><title>segments</title><style>
 body{margin:0;font:13px system-ui,sans-serif;background:#1e1e1e;color:#ddd;display:flex;flex-direction:column;height:100vh}
 #bar{padding:6px 10px;background:#111;display:flex;gap:10px;align-items:center}
 #bar button,#bar input{font:13px system-ui;padding:3px 8px}
 #main{display:flex;flex:1;min-height:0}
 #left{flex:0 0 auto;overflow:auto;padding:10px;background:#1e1e1e}
 .pane{position:relative;background:#fff}
 .pane img{display:block}
 svg.ov{position:absolute;left:0;top:0}
 svg.ov polygon{fill:rgba(0,0,0,0);stroke-width:2;cursor:pointer}
 svg.ov polygon:hover{fill:rgba(80,160,255,.18)}
 svg.ov polygon.sel{fill:rgba(80,160,255,.30);stroke-width:4}
 #right{flex:1;overflow:auto;padding:10px;border-left:1px solid #333}
 #right h3{margin:4px 0;color:#9bd;font-size:13px}
 pre{white-space:pre-wrap;word-break:break-word;background:#161616;padding:8px;border-radius:4px;font:12px ui-monospace,monospace}
 .chip{display:inline-block;padding:1px 6px;margin:2px;border-radius:3px;background:#264;cursor:pointer}
 .tag{color:#888}
</style></head><body>
<div id=bar>
 <button onclick="go(-1)">&#9664; Prev</button>
 <span id=hdr></span>
 <button onclick="go(1)">Next &#9654;</button>
 <input id=jump type=number min=1 style=width:80px onkeydown="if(event.key=='Enter')jumpTo()">
 <span id=meta class=tag></span>
</div>
<div id=main><div id=left></div><div id=right></div></div>
<script>
const NS='http://www.w3.org/2000/svg', DISPLAY_W=720;
const COLORS=['#0bd','#f93','#9c6','#e6c','#6cf','#fa6'];
let idx=0,data=null;
async function boot(){data=await (await fetch('/api/page?i=0')).json();render();}
async function load(i){data=await (await fetch('/api/page?i='+i)).json();idx=data.i;render();}
function go(d){const n=idx+d;if(n>=0&&n<data.total)load(n);}
function jumpTo(){const n=parseInt(document.getElementById('jump').value)-1;if(n>=0&&n<data.total)load(n);}
function dkColor(dk){const m=(dk||'').match(/\d+/);return COLORS[(m?(+m[0]-1):0)%COLORS.length];}
function render(){
 const sc=DISPLAY_W/data.width,H=data.height*sc;
 document.getElementById('hdr').textContent=`[${idx+1}/${data.total}] ${data.section} pg#${data.page} (pdf ${data.pdf})`;
 document.getElementById('meta').textContent=(data.is_notebook?'notebook ':'')+`${data.records.length} doc(s), ${data.entries.length} entry(ies)`;
 const L=document.getElementById('left');L.innerHTML='';
 const pane=document.createElement('div');pane.className='pane';pane.style.width=DISPLAY_W+'px';pane.style.height=H+'px';
 const img=new Image();img.src='/img?i='+idx;img.width=DISPLAY_W;pane.appendChild(img);
 const svg=document.createElementNS(NS,'svg');svg.setAttribute('class','ov');svg.setAttribute('width',DISPLAY_W);svg.setAttribute('height',H);
 data.polys.forEach(p=>{const pl=document.createElementNS(NS,'polygon');
   pl.setAttribute('points',p.quad.map(q=>`${q[0]*sc},${q[1]*sc}`).join(' '));
   pl.setAttribute('stroke',dkColor(p.dk));pl.dataset.rid=p.rid;pl.dataset.dk=p.dk;
   pl.addEventListener('click',()=>pick(p));svg.appendChild(pl);});
 pane.appendChild(svg);L.appendChild(pane);
 summary();
}
function summary(){
 document.querySelectorAll('.sel').forEach(e=>e.classList.remove('sel'));
 const R=document.getElementById('right');R.innerHTML='<h3>PAGE SUMMARY — click a region to pin one</h3>';
 data.records.forEach(d=>{const c=document.createElement('span');c.className='chip';c.textContent=`${d.id} [${d.type}]`;c.onclick=()=>showDoc(d.id);R.appendChild(c);});
 data.entries.forEach((e,k)=>{const c=document.createElement('span');c.className='chip';c.style.background='#363';c.textContent=`entry ${k}: ${(e.text||'').slice(0,22)}`;c.onclick=()=>showEnt(k);R.appendChild(c);});
 const pre=document.createElement('pre');
 const obj={records:data.records.map(d=>({id:d.id,type:d.type,parent_doc:d.parent_doc,recipient:d.recipient,date:d.date,pages:d.pages})),
   entries:data.entries.map(e=>({rid:e.rid,date:e.date,page_label:e.page_label,text:(e.text||'').slice(0,60)}))};
 if(data.notebook_page) obj.notebook_page_text_by_label=data.notebook_page.text_by_label;
 pre.textContent=JSON.stringify(obj,null,2);
 R.appendChild(pre);
}
function pick(p){
 if(data.rid2ent[p.rid]!==undefined){showEnt(data.rid2ent[p.rid]);}
 else if(data.dk2rec[p.dk]){showDoc(data.dk2rec[p.dk]);}
 else summary();
 document.querySelectorAll('.sel').forEach(e=>e.classList.remove('sel'));
 document.querySelectorAll(`[data-rid="${CSS.escape(p.rid)}"]`).forEach(e=>e.classList.add('sel'));
}
function showDoc(id){const d=data.records.find(x=>x.id===id);const R=document.getElementById('right');
 R.innerHTML='<h3>DOCUMENT '+id+'</h3>';const pre=document.createElement('pre');pre.textContent=JSON.stringify(d,null,2);R.appendChild(pre);}
function showEnt(k){const e=data.entries[k];const R=document.getElementById('right');
 R.innerHTML='<h3>NOTEBOOK ENTRY '+k+'</h3>';const pre=document.createElement('pre');pre.textContent=JSON.stringify(e,null,2);R.appendChild(pre);}
boot();
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def _s(self, code, ctype, body):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query); i = int(q.get("i", ["0"])[0])
        if u.path == "/":
            self._s(200, "text/html; charset=utf-8", HTML.encode())
        elif u.path == "/api/page":
            self._s(200, "application/json", json.dumps(payload(i)).encode())
        elif u.path == "/img":
            png = next(iter(glob.glob(os.path.join(PAGES[i]["pdir"], "page_*.masked.png"))), None)
            self._s(200, "image/png", Path(png).read_bytes() if png else b"")
        else:
            self._s(404, "text/plain", b"not found")

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--port", type=int, default=8001); a = ap.parse_args()
    load()
    print(f"{len(PAGES)} pages, {len(DOCS)} docs, {len(NB)} notebook entries — http://localhost:{a.port}")
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    main()
