"""step_7/lib/pipeline.py — content-hashed stage DAG for diff-and-rerun reproducibility.

Each `Stage` declares: name, fn() (does the work), deps (upstream stage names), inputs (path
globs it reads), outputs (paths it writes). A stage's FINGERPRINT = sha256 over:
  - the source of fn()              -> code edits make it stale
  - the config fingerprint          -> config edits make it stale
  - the contents of all input files -> upstream/data edits make it stale
  - the fingerprints of upstream stages
A stage is STALE when its fingerprint differs from the stored one, OR an output is missing, OR an
upstream stage re-ran this pass. `status()` prints the diff; `run()` executes stale stages in
topological order and forces everything downstream — so refining an early artifact propagates all
the way up. State persists in step_7/_pipeline_state.json. Stages must be NON-DESTRUCTIVE
(write new artifacts); the runner never deletes anything. Stdlib only.
"""
from __future__ import annotations
import glob as _glob
import hashlib
import inspect
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

STEP7 = Path(__file__).resolve().parent.parent
STATE_PATH = STEP7 / "_pipeline_state.json"


def _file_hash(path: str) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except (FileNotFoundError, IsADirectoryError, PermissionError):
        return "MISSING"


@dataclass
class Stage:
    name: str
    fn: Callable[[], None]
    deps: list = field(default_factory=list)      # upstream stage names
    inputs: list = field(default_factory=list)    # path globs this stage READS
    outputs: list = field(default_factory=list)   # path globs this stage WRITES (for reporting + missing-check)
    note: str = ""


class Pipeline:
    def __init__(self, config_fingerprint: str = ""):
        self.stages: dict[str, Stage] = {}
        self.config_fingerprint = config_fingerprint

    def add(self, stage: Stage) -> Stage:
        if stage.name in self.stages:
            raise ValueError(f"duplicate stage: {stage.name}")
        self.stages[stage.name] = stage
        return stage

    # ---- ordering -------------------------------------------------------
    def topo(self) -> list:
        order, done, temp = [], set(), set()

        def visit(n):
            if n in done:
                return
            if n in temp:
                raise ValueError(f"dependency cycle at {n}")
            temp.add(n)
            for d in self.stages[n].deps:
                if d not in self.stages:
                    raise ValueError(f"stage '{n}' depends on unknown stage '{d}'")
                visit(d)
            temp.discard(n)
            done.add(n)
            order.append(n)

        for n in self.stages:
            visit(n)
        return order

    # ---- fingerprints ---------------------------------------------------
    def _inputs_fp(self, st: Stage) -> str:
        files: list[str] = []
        for pat in st.inputs:
            files.extend(_glob.glob(pat, recursive=True))
        h = hashlib.sha256()
        for f in sorted(set(files)):
            h.update(f.encode())
            h.update(_file_hash(f).encode())
        return h.hexdigest()

    def fingerprints(self) -> dict:
        fps: dict[str, str] = {}
        for name in self.topo():
            st = self.stages[name]
            h = hashlib.sha256()
            try:
                src = inspect.getsource(st.fn)            # code edits -> stale
            except (OSError, TypeError):
                src = getattr(st.fn, "__qualname__", repr(st.fn))   # fn w/o source file (exec/REPL)
            h.update(src.encode())
            h.update(self.config_fingerprint.encode())
            h.update(self._inputs_fp(st).encode())
            for d in st.deps:
                h.update(fps[d].encode())
            fps[name] = h.hexdigest()
        return fps

    # ---- state ----------------------------------------------------------
    def _load_state(self) -> dict:
        return json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}

    def _save_state(self, state: dict):
        STATE_PATH.write_text(json.dumps(state, indent=2))

    def _outputs_present(self, st: Stage) -> bool:
        if not st.outputs:
            return True
        return all(_glob.glob(o, recursive=True) or Path(o).exists() for o in st.outputs)

    # ---- inspect / run --------------------------------------------------
    def status(self) -> dict:
        fps = self.fingerprints()
        state = self._load_state()
        rep = {}
        for name in self.topo():
            st = self.stages[name]
            changed = state.get(name, {}).get("fingerprint") != fps[name]
            missing = not self._outputs_present(st)
            reasons = []
            if changed:
                reasons.append("inputs/code/config changed" if name in state else "never run")
            if missing:
                reasons.append("output missing")
            rep[name] = {"stale": bool(changed or missing), "reasons": reasons,
                         "deps": st.deps, "outputs": st.outputs}
        return rep

    def seed(self) -> list:
        """Record the CURRENT input/code/config fingerprints as the baseline WITHOUT running anything, for
        every stage whose declared outputs already exist. Use after a fresh checkout (or when the state
        file was lost) so a subsequent `run` doesn't re-execute the whole DAG — and, critically, doesn't
        rebuild graph.json and wipe the in-place post-passes — just because there was no state to compare
        against. Safe: run()/status() check _outputs_present independently, so a stage with a genuinely
        missing output still shows stale and will run; seeding only silences the 'never run' false alarm."""
        fps = self.fingerprints()
        state = self._load_state()
        seeded = []
        for name in self.topo():
            if self._outputs_present(self.stages[name]):
                state[name] = {"fingerprint": fps[name], "ran_at": "seeded", "secs": 0}
                seeded.append(name)
        self._save_state(state)
        return seeded

    def run(self, only: Optional[list] = None, dry: bool = False, force: bool = False) -> list:
        """Run stale stages (and everything downstream of anything that runs) in topo order.
        `only` restricts to a subset (still respects ordering). `dry` reports without executing."""
        state = self._load_state()
        ran, dirty = [], set()
        for name in self.topo():
            st = self.stages[name]
            fp_now = self.fingerprints()[name]   # recompute (picks up outputs written by upstream this pass)
            changed = force or state.get(name, {}).get("fingerprint") != fp_now
            missing = not self._outputs_present(st)
            upstream_ran = any(d in dirty for d in st.deps)
            wanted = only is None or name in only
            if (changed or missing or upstream_ran) and wanted:
                dirty.add(name)
                ran.append(name)
                if dry:
                    continue
                t0 = time.time()
                st.fn()
                state[name] = {"fingerprint": self.fingerprints()[name],
                               "ran_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                               "secs": round(time.time() - t0, 2)}
                self._save_state(state)
        return ran
