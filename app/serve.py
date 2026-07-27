#!/usr/bin/env python3
"""app/serve.py — launch the Solanus archival dev-tool web server from ANY working directory.

`uvicorn app.server:app` only resolves if the repo root (the directory that CONTAINS ``app/``) happens
to be on sys.path — which depends on where you launched it, so running from ``pipeline_v3/`` fails with
``ModuleNotFoundError: No module named 'app'``. This entry point fixes that by resolving everything
RELATIVE TO ITSELF: it adds the repo root to sys.path and hands uvicorn the same root as ``app_dir``
(which uvicorn injects into the worker's sys.path too, so ``--reload`` keeps working). Net effect:

    pipeline_v3/step_7/venv/bin/python app/serve.py --port 8000      # works from anywhere
    pipeline_v3/step_7/venv/bin/python /abs/path/to/app/serve.py     # also works

All server-side asset/data paths are already anchored to ``__file__`` (see server.py ``_APP`` and
config.py), so once the import resolves, nothing else depends on the working directory.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent          # .../app/serve.py  ->  repo root
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def main() -> None:
    ap = argparse.ArgumentParser(description="Serve the Solanus archival dev tool (CWD-independent).")
    ap.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    ap.add_argument("--port", type=int, default=8000, help="port (default 8000)")
    ap.add_argument("--reload", action="store_true", help="auto-reload on code changes (dev)")
    a = ap.parse_args()
    import uvicorn
    print(f"Serving Solanus dev tool at http://{a.host}:{a.port}  (repo root: {REPO})")
    # app_dir=REPO makes uvicorn put the repo root on sys.path in the (re)worker process, so the
    # "app.server:app" import string resolves regardless of where this script was invoked from.
    uvicorn.run("app.server:app", host=a.host, port=a.port, reload=a.reload, app_dir=str(REPO))


if __name__ == "__main__":
    main()
