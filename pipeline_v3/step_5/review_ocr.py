#!/usr/bin/env python3
"""STEP_5 — OCR review tool (local web app, stdlib only).

LEFT pane : the masked source image with the gold polygons outlined.
RIGHT pane: the same polygons, with every OCR'd WORD placed at its ACTUAL page position
            (from the Azure word boxes) — a true 1:1 spatial comparison. Words are editable
            in place; low-confidence words are highlighted (amber <0.90, red <0.70); rendered
            in a typewriter monospace (Courier Prime), each word sized to its own box.

Save writes a COPY — extract_azure.reviewed.json — preserving edits WITH positions, so a
re-open re-renders your corrections in place. extract_azure.json is never touched.

Reads/writes under --root (default 3_enriched).

Usage:
  venv/bin/python review_ocr.py Volume_2/1_source_pages/page_252
  venv/bin/python review_ocr.py Volume_2/1_source_pages            # whole volume
then open the printed http://localhost:PORT in a browser.
"""
from __future__ import annotations
import json, argparse
from pathlib import Path
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import extract_azure as E                          # reuse collect_regions -> geometry in lockstep

HERE = Path(__file__).resolve().parent
ROOT = HERE / "3_enriched"                         # dataset root (override with --root)
PAGES: list[Path] = []


def resolve(args):
    out = []
    for a in args:
        p = Path(a) if Path(a).is_absolute() else ROOT / a
        if list(p.glob("page_*.masked.png")):
            out.append(p)
        else:                                       # a section/sub dir -> expand to its pages
            out += [c for c in sorted(p.glob("page_*")) if list(c.glob("page_*.masked.png"))]
    return out


def build_payload(i):
    pd = PAGES[i]
    meta = json.load(open(next(pd.glob("page_*.json"))))
    geom = E.collect_regions(meta)                  # (rid, dk, cat, pts, bbox)
    exf = pd / "extract_azure.json"
    ex = json.load(open(exf)) if exf.exists() else {"regions": []}
    tmap = {r["rid"]: r for r in ex.get("regions", [])}
    rvf = pd / "extract_azure.reviewed.json"
    rmap = {}
    if rvf.exists():
        rmap = {r["rid"]: r for r in json.load(open(rvf)).get("regions", [])}
    regions = []
    for (rid, dk, cat, pts, bb) in geom:
        t = tmap.get(rid, {})
        rev = rmap.get(rid)
        disp_words = rev["words"] if (rev and rev.get("words")) else t.get("words", [])
        regions.append({
            "rid": rid, "doc": dk, "category": cat,
            "quad": [[float(x), float(y)] for (x, y) in pts],
            "bbox": [float(v) for v in bb],
            "full_page": t.get("full_page", ""), "per_polygon": t.get("per_polygon", ""),
            "min_conf": t.get("min_conf"), "agree": t.get("agree"),
            "words": disp_words,                    # [{t, c, box:[x0,y0,x1,y1]}] in page-pixel coords
            "reviewed": (rev.get("text") if rev else t.get("per_polygon", "")),
        })
    return {"index": i, "total": len(PAGES), "page": str(pd.relative_to(ROOT)),
            "width": int(meta["page_width"]), "height": int(meta["page_height"]),
            "has_ocr": exf.exists(), "reviewed": rvf.exists(), "regions": regions}


def save_reviewed(i, payload):
    pd = PAGES[i]
    rvf = pd / "extract_azure.reviewed.json"
    rvf.write_text(json.dumps({
        "page": str(pd.relative_to(ROOT)), "source": "extract_azure.json",
        "reviewed_at": datetime.now().isoformat(timespec="seconds"),
        "regions": [{"rid": rid, "text": d.get("text", ""), "words": d.get("words", [])}
                    for rid, d in payload.items()],
    }, indent=2, ensure_ascii=False))
    return rvf


HTML = r"""<!doctype html><html><head><meta charset=utf-8><title>OCR review</title>
<style>
 body{margin:0;font:13px system-ui,sans-serif;background:#1e1e1e;color:#ddd}
 #bar{padding:6px 10px;background:#111;display:flex;gap:10px;align-items:center;position:sticky;top:0;z-index:5}
 #bar button{font:13px system-ui;padding:3px 10px}
 #panes{display:flex;gap:14px;padding:12px;align-items:flex-start}
 .pane{position:relative;background:#fff;flex:0 0 auto}
 .pane img{display:block}
 svg.ov{position:absolute;left:0;top:0}
 svg.ov polygon{fill:rgba(0,120,255,.04);stroke:#0a7;stroke-width:1.2;pointer-events:all;cursor:pointer}
 svg.ov polygon.low{stroke:#e33}  svg.ov polygon.dis{stroke:#e90}
 svg.ov polygon.sel{stroke:#06f;stroke-width:3;fill:rgba(0,100,255,.10)}
 #right .word{position:absolute;white-space:nowrap;color:#111;line-height:1;background:#fff;cursor:text;
   font-family:'Courier Prime','Courier New','Nimbus Mono','DejaVu Sans Mono',monospace}
 #right .word.lc1{background:#ffe08a}  #right .word.lc2{background:#ff9a9a}
 #right .word.sel{outline:1px solid #06f}
 #info{padding:8px 10px;background:#111;font-size:12px;min-height:48px}
 #info .ref{color:#9bd;margin-top:3px;white-space:pre-wrap}
 .tag{color:#888}
</style></head><body>
<div id=bar>
 <button onclick="go(-1)">&#9664; Prev</button>
 <span id=hdr></span>
 <button onclick="go(1)">Next &#9654;</button>
 <button onclick="save()" style="margin-left:auto">&#128190; Save reviewed</button>
 <span id=status class=tag></span>
</div>
<div id=panes>
 <div class=pane id=left></div>
 <div class=pane id=right></div>
</div>
<div id=info>click a box to inspect &middot; words sit where they do on the page &middot; highlight <span style="background:#ffe08a">conf&lt;0.90</span> <span style="background:#ff9a9a">conf&lt;0.70</span></div>
<script>
const NS='http://www.w3.org/2000/svg', DISPLAY_W=760, HI1=0.90, HI2=0.70;
let idx=0, data=null, TOTAL=1;
const esc=s=>(s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
function badge(r){return (r.min_conf!=null&&r.min_conf<HI1?' low':'')+(r.agree===false?' dis':'');}
async function boot(){TOTAL=(await (await fetch('/api/pages')).json()).total; load(0);}
async function load(i){idx=i; data=await (await fetch('/api/page?i='+i)).json(); render();}
function go(d){const n=idx+d; if(n>=0&&n<TOTAL) load(n);}
function regionSVG(scale,H){
  const svg=document.createElementNS(NS,'svg'); svg.setAttribute('class','ov');
  svg.setAttribute('width',DISPLAY_W); svg.setAttribute('height',H);
  data.regions.forEach(r=>{const pl=document.createElementNS(NS,'polygon');
    pl.setAttribute('points',r.quad.map(p=>`${p[0]*scale},${p[1]*scale}`).join(' '));
    pl.setAttribute('class','poly'+badge(r)); pl.dataset.rid=r.rid;
    pl.addEventListener('click',()=>select(r.rid)); svg.appendChild(pl);});
  return svg;
}
function render(){
  const scale=DISPLAY_W/data.width, H=data.height*scale;
  document.getElementById('hdr').textContent=`Page ${idx+1}/${TOTAL} — ${data.page}`+
    (data.has_ocr?'':'  ⚠ NO OCR YET (run extract_azure.py)')+(data.reviewed?'  ✓reviewed':'');
  const L=document.getElementById('left'); L.innerHTML=''; L.style.width=DISPLAY_W+'px'; L.style.height=H+'px';
  const img=new Image(); img.src='/img?i='+idx; img.width=DISPLAY_W; L.appendChild(img);
  L.appendChild(regionSVG(scale,H));
  const R=document.getElementById('right'); R.innerHTML=''; R.style.width=DISPLAY_W+'px'; R.style.height=H+'px';
  R.appendChild(regionSVG(scale,H));
  data.regions.forEach(r=>(r.words||[]).forEach((w,wi)=>{
    if(!w.box) return;
    const e=document.createElement('span'); e.className='word'+(w.c<HI2?' lc2':(w.c<HI1?' lc1':''));
    e.style.left=(w.box[0]*scale)+'px'; e.style.top=(w.box[1]*scale)+'px';
    e.style.fontSize=Math.max(5,(w.box[3]-w.box[1])*scale*0.9)+'px';
    e.contentEditable='true'; e.textContent=w.t; e.title='conf '+w.c;
    e.dataset.rid=r.rid; e.dataset.wi=wi; e.addEventListener('focus',()=>select(r.rid));
    R.appendChild(e);
  }));
}
function select(rid){
  document.querySelectorAll('.sel').forEach(e=>e.classList.remove('sel'));
  document.querySelectorAll('[data-rid="'+CSS.escape(rid)+'"]').forEach(e=>e.classList.add('sel'));
  const r=data.regions.find(x=>x.rid===rid); if(!r) return;
  document.getElementById('info').innerHTML=
    `<b>${r.rid}</b> <span class=tag>&middot; ${r.category} &middot; conf ${r.min_conf??'—'} &middot; agree ${r.agree}</span>`+
    `<div class=ref><b>full-page:</b> ${esc(r.full_page)}</div>`+
    `<div class=ref><b>per-polygon:</b> ${esc(r.per_polygon)}</div>`;
}
async function save(){
  const edit={};
  document.querySelectorAll('#right .word').forEach(e=>{edit[e.dataset.rid+'|'+e.dataset.wi]=e.textContent;});
  const payload={};
  data.regions.forEach(r=>{const ws=(r.words||[]).map((w,wi)=>({t:(edit[r.rid+'|'+wi]??w.t),c:w.c,box:w.box}));
    payload[r.rid]={text:ws.map(x=>x.t).join(' '),words:ws};});
  const j=await (await fetch('/api/save?i='+idx,{method:'POST',body:JSON.stringify(payload)})).json();
  document.getElementById('status').textContent='saved '+j.saved+' @ '+new Date().toLocaleTimeString();
}
document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key==='s'){e.preventDefault();save();}});
boot();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        i = int(q.get("i", ["0"])[0])
        if u.path == "/":
            self._send(200, "text/html; charset=utf-8", HTML.encode())
        elif u.path == "/api/pages":
            self._send(200, "application/json", json.dumps({"total": len(PAGES)}).encode())
        elif u.path == "/api/page":
            self._send(200, "application/json", json.dumps(build_payload(i)).encode())
        elif u.path == "/img":
            self._send(200, "image/png", next(PAGES[i].glob("page_*.masked.png")).read_bytes())
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self):
        u = urlparse(self.path); q = parse_qs(u.query)
        if u.path == "/api/save":
            i = int(q.get("i", ["0"])[0])
            n = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(n) or b"{}")
            rvf = save_reviewed(i, payload)
            self._send(200, "application/json", json.dumps({"saved": rvf.name}).encode())
        else:
            self._send(404, "text/plain", b"not found")

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pages", nargs="*", help="page/section dirs (default: ALL pages under --root)")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--root", default="3_enriched", help="dataset root to read from / write into (default: 3_enriched)")
    a = ap.parse_args()
    global ROOT
    ROOT = (HERE / a.root).resolve()
    if a.pages:
        PAGES.extend(resolve(a.pages))
    else:                                            # no args -> every page under --root
        PAGES.extend(p for p in sorted(ROOT.glob("*/*/page_*"))
                     if p.is_dir() and ".backups" not in p.parts and list(p.glob("page_*.masked.png")))
    if not PAGES:
        raise SystemExit("no pages found (need folders with a page_*.masked.png)")
    print(f"{len(PAGES)} page(s) — open  http://localhost:{a.port}  (Ctrl-C to stop)")
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
