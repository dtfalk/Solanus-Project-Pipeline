#!/usr/bin/env python3
"""STEP_6 viewer #2 — review multi-page document spans (local web app, stdlib only).

For each document it lays out ALL the pages of its span in order (LEFT), with the doc_N
fragments that belong to THIS document outlined in colour and the other docs on a shared page
dimmed — so you can see at a glance whether the multi-page grouping is right. RIGHT shows the
document JSON plus a quick OK / FLAG + note you can save (-> span_review.json, NON-DESTRUCTIVE).

Defaults to multi-page documents (the ones worth checking); ?all=1 or the toggle shows every doc.

Usage:  venv/bin/python review_spans.py        then open http://localhost:8002
"""
from __future__ import annotations
import json, os, re, glob, argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE / "3_enriched"
GOLD_RE = re.compile(r"page_\d+\.json")
REVIEW = HERE / "span_review.json"
DOCS, IDX = [], {}          # IDX[(section, page_number_in_type)] = pdir


def gold_json(pdir):
    for f in os.listdir(pdir):
        if GOLD_RE.fullmatch(f):
            return os.path.join(pdir, f)
    return None


def load():
    global DOCS, IDX
    DOCS = json.loads((HERE / "documents.json").read_text())
    for jp in glob.glob(str(ROOT / "*" / "1_source_pages" / "page_*" / "page_*.json")):
        if not GOLD_RE.fullmatch(os.path.basename(jp)):
            continue
        m = json.loads(Path(jp).read_text())
        IDX[(m.get("source_file", Path(jp).parts[-4]), m["page_number_in_type"])] = os.path.dirname(jp)


def doc_list(multipage_only):
    return [d for d in DOCS if (len(d.get("pages", [])) > 1 or not multipage_only)]


def review_load():
    return json.loads(REVIEW.read_text()) if REVIEW.exists() else {}


def page_payload(section, page, mine_dks):
    pdir = IDX.get((section, page))
    if not pdir:
        return None
    meta = json.loads(Path(gold_json(pdir)).read_text())
    mine, other = [], []
    for dk, doc in meta.get("documents", {}).items():
        for cat, ps in (doc or {}).items():
            for j, p in enumerate(ps or []):
                v = p.get("vertices", [])
                if len(v) >= 3:
                    (mine if dk in mine_dks else other).append(
                        [[float(x["x"]), float(x["y"])] for x in v])
    return {"page": page, "width": int(meta["page_width"]), "height": int(meta["page_height"]),
            "mine": mine, "other": other}


def doc_payload(i, multipage_only):
    docs = doc_list(multipage_only)
    d = docs[i]
    frag = {}
    for pn, dk in d.get("fragments", []):
        frag.setdefault(pn, set()).add(dk)
    pages = [page_payload(d["section"], pn, frag.get(pn, set())) for pn in d.get("pages", [])]
    rv = review_load().get(d["id"], {})
    return {"i": i, "total": len(docs), "doc": d, "pages": [p for p in pages if p], "review": rv}


HTML = r"""<!doctype html><html><head><meta charset=utf-8><title>span review</title><style>
 body{margin:0;font:13px system-ui,sans-serif;background:#1e1e1e;color:#ddd;display:flex;flex-direction:column;height:100vh}
 #bar{padding:6px 10px;background:#111;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
 #bar button,#bar input{font:13px system-ui;padding:3px 8px}
 #main{display:flex;flex:1;min-height:0}
 #left{flex:1;overflow:auto;padding:10px;display:flex;flex-wrap:wrap;gap:10px;align-content:flex-start}
 .page{position:relative;background:#fff}
 .page img{display:block}
 .cap{position:absolute;top:0;left:0;background:#000a;color:#fff;padding:1px 5px;font-size:11px}
 svg.ov{position:absolute;left:0;top:0}
 svg.ov polygon.mine{fill:rgba(0,200,140,.16);stroke:#0c8;stroke-width:2}
 svg.ov polygon.other{fill:rgba(0,0,0,0);stroke:#a44;stroke-width:1;stroke-dasharray:4}
 #right{flex:0 0 420px;overflow:auto;padding:10px;border-left:1px solid #333}
 #right h3{margin:4px 0;color:#9bd}
 pre{white-space:pre-wrap;word-break:break-word;background:#161616;padding:8px;border-radius:4px;font:12px ui-monospace,monospace}
 .tag{color:#888}.ok{background:#264}.flag{background:#622}
</style></head><body>
<div id=bar>
 <button onclick="go(-1)">&#9664; Prev</button><span id=hdr></span><button onclick="go(1)">Next &#9654;</button>
 <input id=jump type=number min=1 style=width:70px onkeydown="if(event.key=='Enter')jumpTo()">
 <label><input type=checkbox id=mp checked onchange="boot()"> multi-page only</label>
 <span style=margin-left:auto></span>
 <button class=ok onclick="save('ok')">&#10003; OK</button>
 <button class=flag onclick="save('flag')">&#9873; Flag</button>
 <input id=note placeholder="note..." style=width:220px>
 <span id=status class=tag></span>
</div>
<div id=main><div id=left></div><div id=right></div></div>
<script>
const NS='http://www.w3.org/2000/svg', THUMB=300;
let idx=0,data=null;
function mpOnly(){return document.getElementById('mp').checked;}
async function boot(){idx=0;await load(0);}
async function load(i){data=await (await fetch(`/api/doc?i=${i}&all=${mpOnly()?0:1}`)).json();idx=data.i;render();}
function go(d){const n=idx+d;if(n>=0&&n<data.total)load(n);}
function jumpTo(){const n=parseInt(document.getElementById('jump').value)-1;if(n>=0&&n<data.total)load(n);}
function render(){
 const d=data.doc;
 document.getElementById('hdr').textContent=`[${idx+1}/${data.total}] ${d.id} [${d.type}] — ${d.pages.length} page(s)`;
 document.getElementById('note').value=(data.review&&data.review.note)||'';
 document.getElementById('status').textContent=data.review&&data.review.status?('marked '+data.review.status):'';
 const L=document.getElementById('left');L.innerHTML='';
 data.pages.forEach(pp=>{
   const sc=THUMB/pp.width,H=pp.height*sc;
   const box=document.createElement('div');box.className='page';box.style.width=THUMB+'px';box.style.height=H+'px';
   const img=new Image();img.src=`/img?section=${encodeURIComponent(d.section)}&page=${pp.page}`;img.width=THUMB;box.appendChild(img);
   const svg=document.createElementNS(NS,'svg');svg.setAttribute('class','ov');svg.setAttribute('width',THUMB);svg.setAttribute('height',H);
   pp.other.forEach(q=>addpoly(svg,q,sc,'other'));pp.mine.forEach(q=>addpoly(svg,q,sc,'mine'));
   box.appendChild(svg);
   const cap=document.createElement('div');cap.className='cap';cap.textContent='pg#'+pp.page;box.appendChild(cap);
   L.appendChild(box);
 });
 const R=document.getElementById('right');R.innerHTML='<h3>'+d.id+'</h3>';
 const pre=document.createElement('pre');pre.textContent=JSON.stringify(d,null,2);R.appendChild(pre);
}
function addpoly(svg,quad,sc,cls){const pl=document.createElementNS(NS,'polygon');
 pl.setAttribute('points',quad.map(q=>`${q[0]*sc},${q[1]*sc}`).join(' '));pl.setAttribute('class',cls);svg.appendChild(pl);}
async function save(status){
 const note=document.getElementById('note').value;
 const j=await (await fetch('/api/save',{method:'POST',body:JSON.stringify({id:data.doc.id,status,note})})).json();
 document.getElementById('status').textContent='saved '+status+' @ '+new Date().toLocaleTimeString();data.review={status,note};
}
document.addEventListener('keydown',e=>{if(e.target.id==='note')return;if(e.key==='ArrowRight')go(1);if(e.key==='ArrowLeft')go(-1);});
boot();
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def _s(self, code, ctype, body):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        if u.path == "/":
            self._s(200, "text/html; charset=utf-8", HTML.encode())
        elif u.path == "/api/doc":
            i = int(q.get("i", ["0"])[0]); allf = q.get("all", ["0"])[0] == "1"
            self._s(200, "application/json", json.dumps(doc_payload(i, not allf)).encode())
        elif u.path == "/img":
            pdir = IDX.get((q.get("section", [""])[0], int(q.get("page", ["0"])[0])))
            png = next(iter(glob.glob(os.path.join(pdir, "page_*.masked.png"))), None) if pdir else None
            self._s(200, "image/png", Path(png).read_bytes() if png else b"")
        else:
            self._s(404, "text/plain", b"not found")

    def do_POST(self):
        if urlparse(self.path).path == "/api/save":
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}")
            data = review_load(); data[body["id"]] = {"status": body.get("status"), "note": body.get("note", "")}
            REVIEW.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            self._s(200, "application/json", json.dumps({"ok": True}).encode())
        else:
            self._s(404, "text/plain", b"not found")

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--port", type=int, default=8002); a = ap.parse_args()
    load()
    print(f"{len(DOCS)} docs ({len(doc_list(True))} multi-page) — http://localhost:{a.port}")
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    main()
