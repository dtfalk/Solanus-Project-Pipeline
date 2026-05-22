"""Launch helper for using normalized_editor with pipeline_v2 workspace paths.

This script does not modify legacy data. It only prepares environment variables
so the existing editor reads and writes within pipeline_v2/workspace.
"""

from __future__ import annotations

import os
import subprocess
import sys

from pipeline_v2.config import ensure_pipeline_directories, get_paths


def build_editor_environment() -> dict[str, str]:
    """Build environment overrides required for pipeline_v2 editor launch.

    Args:
        None.

    Returns:
        Environment dictionary ready to pass into subprocess.run.
    """
    paths = get_paths()
    ensure_pipeline_directories(paths)

    env = os.environ.copy()
    env["NORMALIZED_EDITOR_NORMALIZED_DIR"] = str(paths.annotation_seed_dir)
    env["NORMALIZED_EDITOR_FINAL_DIR"] = str(paths.final_dir)
    env["NORMALIZED_EDITOR_PDF_SOURCE_DIR"] = str(paths.sample_pages_dir)
    return env


def main() -> None:
    """Launch step_3 normalized editor configured for pipeline_v2 workspace.

    Args:
        None.

    Returns:
        None. Propagates child process exit code.
    """
    paths = get_paths()
    env = build_editor_environment()

    command = [
        sys.executable,
        str(paths.root_dir / "step_3" / "normalized_editor.py"),
    ]

    completed_process = subprocess.run(command, env = env, check = False)
    raise SystemExit(completed_process.returncode)


if __name__ == "__main__":
    main()
