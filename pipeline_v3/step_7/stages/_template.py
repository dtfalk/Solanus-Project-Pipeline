"""stages/_template.py — copy this to make a new pipeline stage.

A stage:
  - READS only its declared inputs, WRITES only its declared outputs (so the DAG can hash it),
  - is NON-DESTRUCTIVE (write new files; never delete/overwrite source artifacts in place),
  - logs every model/API call via lib.costlog.log(...),
  - is idempotent (re-running with unchanged inputs yields the same outputs).

Then register it in step_7/run.py:
    p.add(Stage("my_stage", my_module.run, deps=[...], inputs=[...], outputs=[...]))
"""
from __future__ import annotations
import sys
from pathlib import Path

STEP7 = Path(__file__).resolve().parent.parent
if str(STEP7) not in sys.path:
    sys.path.insert(0, str(STEP7))

import config              # noqa: E402  (paths + model registry)
from lib import costlog    # noqa: E402  (always log prices)


def run() -> None:
    config.DATA.mkdir(parents=True, exist_ok=True)
    # ... read inputs, do work, write outputs ...
    # cost example:
    #   costlog.log(provider="gemini", model=config.DEFAULTS["llm"], op="extract",
    #               input_tokens=..., output_tokens=..., meta="extract_entities/page_x")
    raise NotImplementedError("template stage")


if __name__ == "__main__":
    run()
