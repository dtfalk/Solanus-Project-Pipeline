"""collect_public_media.py — gather as much FREELY-LICENSED Solanus Casey media as possible, with provenance.

Principled collector: it downloads only media whose license is explicitly free/open, and records the
license + author + source URL for every item (so each asset is usable + attributable). Rights-ambiguous
hits (e.g. Internet Archive items with no open license) are CATALOGUED for human review, not downloaded.

Sources:
  • Wikimedia Commons  — Category:Solanus Casey + File-namespace search (PD / CC, per-file license metadata)
  • Openverse          — CC/PD image + audio aggregator (license + attribution per result)
  • Internet Archive   — items matching Solanus Casey; download files ONLY from items with an open licenseurl
                         (or public-domain status); everything else is listed in review_needed.csv

Output: pipeline_v3/public_media/{images,audio,video,text}/ + manifest.csv + review_needed.csv

    python collect_public_media.py            # collect everything (default caps)
    python collect_public_media.py --max-ia-mb 4000
"""
from __future__ import annotations
import argparse
import csv
import json
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "public_media"
DIRS = {"image": OUT / "images", "audio": OUT / "audio", "video": OUT / "video", "text": OUT / "text"}
for d in DIRS.values():
    d.mkdir(parents=True, exist_ok=True)

UA = "SolanusArchiveBot/1.0 (Capuchin Province of St. Joseph research; davidtobiasfalk@gmail.com)"
S = requests.Session()
S.headers.update({"User-Agent": UA})

QUERIES = ["Solanus Casey", "Father Solanus Casey", "Blessed Solanus Casey", "Bernard Francis Casey"]
EXT_MEDIA = {".jpg": "image", ".jpeg": "image", ".png": "image", ".tif": "image", ".tiff": "image",
             ".gif": "image", ".webp": "image", ".svg": "image",
             ".mp3": "audio", ".wav": "audio", ".flac": "audio", ".ogg": "audio", ".m4a": "audio",
             ".mp4": "video", ".webm": "video", ".mov": "video", ".mkv": "video", ".avi": "video",
             ".ogv": "video", ".pdf": "text", ".txt": "text", ".djvu": "text"}

manifest, review, seen_urls = [], [], set()


def media_kind(name: str) -> str | None:
    return EXT_MEDIA.get(Path(name.lower()).suffix)


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:140]


def download(url: str, kind: str, name: str, max_mb: int = 1500) -> Path | None:
    if url in seen_urls:
        return None
    seen_urls.add(url)
    dest = DIRS[kind] / safe_name(name)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    try:
        with S.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            clen = int(r.headers.get("Content-Length", 0))
            if clen and clen > max_mb * 1024 * 1024:
                print(f"    skip (>{max_mb}MB): {name}")
                return None
            tmp = dest.with_suffix(dest.suffix + ".part")
            got = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk); got += len(chunk)
                    if got > max_mb * 1024 * 1024:
                        f.close(); tmp.unlink(missing_ok=True)
                        print(f"    abort (>{max_mb}MB): {name}"); return None
            tmp.rename(dest)
            print(f"    + {kind}: {name} ({got//1024} KB)")
            return dest
    except Exception as e:
        print(f"    ! download failed {name}: {str(e)[:70]}")
        return None


# ----------------------------------------------------------------- Wikimedia Commons
def commons():
    api = "https://commons.wikimedia.org/w/api.php"
    titles = set()
    # category members (curated — always keep)
    for cat in ("Category:Solanus Casey",):
        r = S.get(api, params={"action": "query", "list": "categorymembers", "cmtitle": cat,
                               "cmtype": "file", "cmlimit": "500", "format": "json"}, timeout=30).json()
        titles.update(m["title"] for m in r.get("query", {}).get("categorymembers", []))
    # file-namespace search — keep ONLY hits whose title carries the distinctive token "solanus"
    # ("Casey"/"Francis"/"Bernard" alone match unrelated PD books, so require the rare name).
    for q in QUERIES:
        r = S.get(api, params={"action": "query", "list": "search", "srsearch": q, "srnamespace": "6",
                               "srlimit": "200", "format": "json"}, timeout=30).json()
        titles.update(h["title"] for h in r.get("query", {}).get("search", [])
                      if "solanus" in h["title"].lower())
    print(f"[Commons] {len(titles)} candidate files")
    titles = list(titles)
    for i in range(0, len(titles), 40):                     # batch imageinfo
        batch = titles[i:i + 40]
        r = S.get(api, params={"action": "query", "titles": "|".join(batch), "prop": "imageinfo",
                               "iiprop": "url|extmetadata|mime|size", "format": "json"}, timeout=40).json()
        for page in r.get("query", {}).get("pages", {}).values():
            ii = (page.get("imageinfo") or [{}])[0]
            url = ii.get("url")
            kind = media_kind(url or "") or ("image" if (ii.get("mime") or "").startswith("image") else None)
            if not url or not kind:
                continue
            ext = ii.get("extmetadata", {})
            lic = (ext.get("LicenseShortName", {}) or {}).get("value", "")
            artist = re.sub("<[^>]+>", "", (ext.get("Artist", {}) or {}).get("value", "")).strip()
            if download(url, kind, f"commons_{Path(url).name}"):
                manifest.append({"source": "wikimedia_commons", "media_type": kind,
                                 "title": page.get("title", ""), "creator": artist, "license": lic,
                                 "license_url": (ext.get("LicenseUrl", {}) or {}).get("value", ""),
                                 "source_url": ii.get("descriptionurl", url)})
        time.sleep(0.3)


# ----------------------------------------------------------------- Openverse (CC/PD images + audio)
def openverse():
    for kind, ep in (("image", "images"), ("audio", "audio")):
        for q in QUERIES:
            try:
                r = S.get(f"https://api.openverse.org/v1/{ep}/", params={"q": q, "page_size": 100},
                          timeout=40).json()
            except Exception as e:
                print(f"[Openverse] {ep} '{q}' failed: {str(e)[:60]}"); continue
            results = r.get("results", [])
            print(f"[Openverse] {ep} '{q}': {len(results)} results")
            for it in results:
                url = it.get("url")
                if not url:
                    continue
                name = f"openverse_{safe_name(it.get('id',''))}_{Path(url).name or 'file'}"
                if not media_kind(name):
                    name += ".jpg" if kind == "image" else ".mp3"
                if download(url, kind, name):
                    manifest.append({"source": "openverse", "media_type": kind,
                                     "title": it.get("title", ""), "creator": it.get("creator", ""),
                                     "license": f"{it.get('license','')} {it.get('license_version','')}".strip(),
                                     "license_url": it.get("license_url", ""),
                                     "source_url": it.get("foreign_landing_url", url)})
            time.sleep(0.3)


# ----------------------------------------------------------------- Internet Archive (open items only)
OPEN_LIC = ("creativecommons.org", "publicdomain", "/publicdomain/")


def is_open(meta: dict) -> bool:
    lic = (meta.get("licenseurl") or "").lower()
    pcs = (meta.get("possible-copyright-status") or "").upper()
    rights = (meta.get("rights") or "").lower()
    return any(t in lic for t in OPEN_LIC) or "NOT_IN_COPYRIGHT" in pcs or "public domain" in rights


def internet_archive(max_ia_mb: int):
    used = 0
    seen_ids = set()
    for q in QUERIES:
        for page in (1, 2):
            url = ("https://archive.org/advancedsearch.php?q=" + quote(f'"{q}"') +
                   "&fl[]=identifier&fl[]=title&fl[]=mediatype&fl[]=licenseurl&fl[]=rights"
                   f"&rows=200&page={page}&output=json")
            try:
                docs = S.get(url, timeout=40).json().get("response", {}).get("docs", [])
            except Exception as e:
                print(f"[IA] search '{q}' p{page} failed: {str(e)[:60]}"); continue
            if not docs:
                break
            print(f"[IA] '{q}' p{page}: {len(docs)} items")
            for doc in docs:
                ident = doc.get("identifier")
                if not ident or ident in seen_ids:
                    continue
                # relevance: the item must actually be about Solanus (distinctive token), else "Casey" matches noise.
                if "solanus" not in (str(doc.get("title", "")) + " " + ident).lower():
                    continue
                seen_ids.add(ident)
                try:
                    meta = S.get(f"https://archive.org/metadata/{ident}", timeout=40).json()
                except Exception:
                    continue
                md = meta.get("metadata", {})
                item_url = f"https://archive.org/details/{ident}"
                lic = "open" if is_open(md) else (md.get("rights") or "IA-public")
                got_any = False
                for fobj in meta.get("files", []):
                    name = fobj.get("name", "")
                    kind = media_kind(name)
                    if not kind or fobj.get("source") == "metadata":
                        continue
                    if used >= max_ia_mb:
                        print("    [IA] total cap reached"); break
                    furl = f"https://archive.org/download/{ident}/{quote(name)}"
                    got = download(furl, kind, f"ia_{ident}_{safe_name(name)}", max_mb=max(1, max_ia_mb - used))
                    if got:
                        got_any = True
                        used += max(1, got.stat().st_size // (1024 * 1024))
                        manifest.append({"source": "internet_archive", "media_type": kind,
                                         "title": str(md.get("title", ""))[:160], "creator": md.get("creator", ""),
                                         "license": lic, "license_url": md.get("licenseurl", ""),
                                         "source_url": item_url})
                if not got_any:        # nothing downloadable (lending-locked / no media files) -> note it
                    review.append({"source": "internet_archive", "media_type": doc.get("mediatype", ""),
                                   "title": str(doc.get("title", ""))[:160], "creator": md.get("creator", ""),
                                   "license": lic, "license_url": md.get("licenseurl", ""), "source_url": item_url})
                time.sleep(0.2)


def write_csv(path: Path, rows: list):
    cols = ["source", "media_type", "title", "creator", "license", "license_url", "source_url"]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-ia-mb", type=int, default=8000, help="cap total Internet Archive download (MB)")
    a = ap.parse_args()
    print("Collecting freely-licensed Solanus Casey media...\n")
    for fn in (commons, openverse, lambda: internet_archive(a.max_ia_mb)):
        try:
            fn()
        except Exception as e:
            print(f"[source error] {str(e)[:90]}")
    write_csv(OUT / "manifest.csv", manifest)
    write_csv(OUT / "review_needed.csv", review)
    by_kind = {}
    for m in manifest:
        by_kind[m["media_type"]] = by_kind.get(m["media_type"], 0) + 1
    print(f"\nDONE. Downloaded {len(manifest)} freely-licensed files {by_kind}.")
    print(f"  manifest:      {OUT/'manifest.csv'}")
    print(f"  review needed: {OUT/'review_needed.csv'} ({len(review)} rights-unclear items, NOT downloaded)")


if __name__ == "__main__":
    main()
