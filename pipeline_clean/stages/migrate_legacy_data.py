"""Explicit migration script for importing legacy data into pipeline_clean.

Run with:
    python pipeline_clean/stages/migrate_legacy_data.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from time import time

from pipeline_clean.config import (
    ANNOTATION_SEED_DIR,
    FINAL_DIR,
    MIGRATION_ENABLE,
    MIGRATION_SOURCE_ANNOTATION_DIRS,
    MIGRATION_SOURCE_POLYGONS_DIR,
    POLYGONS_DIR,
    ensure_workspace_dirs,
)


def copy_polygon_tree(report):
    """Copy legacy polygon data into pipeline_clean workspace.

    Args:
        report: Mutable report dictionary.

    Returns:
        None.
    """
    source_root = Path(MIGRATION_SOURCE_POLYGONS_DIR)
    if not source_root.exists():
        report["issues"].append(f"Missing polygon source: {source_root}")
        return

    copied_files = 0
    for path in source_root.rglob("*.json"):
        relative = path.relative_to(source_root)
        target = POLYGONS_DIR / relative
        target.parent.mkdir(parents = True, exist_ok = True)
        shutil.copy2(path, target)
        copied_files += 1

    report["polygon_json_copied"] = copied_files


def copy_annotation_sources(report):
    """Copy annotation data with precedence order into seed and final trees.

    Args:
        report: Mutable report dictionary.

    Returns:
        None.
    """
    copied_seed = 0
    copied_final = 0

    for source_root in MIGRATION_SOURCE_ANNOTATION_DIRS:
        source_root = Path(source_root)
        if not source_root.exists():
            report["issues"].append(f"Missing annotation source: {source_root}")
            continue

        for path in source_root.rglob("*.json"):
            relative = path.relative_to(source_root)
            target_seed = ANNOTATION_SEED_DIR / relative
            target_final = FINAL_DIR / relative

            if not target_seed.exists():
                target_seed.parent.mkdir(parents = True, exist_ok = True)
                shutil.copy2(path, target_seed)
                copied_seed += 1

            if not target_final.exists():
                target_final.parent.mkdir(parents = True, exist_ok = True)
                shutil.copy2(path, target_final)
                copied_final += 1

    report["annotation_seed_json_copied"] = copied_seed
    report["annotation_final_json_copied"] = copied_final


def write_report(report):
    """Write migration report JSON to workspace root.

    Args:
        report: Migration report dictionary.

    Returns:
        None.
    """
    report_path = POLYGONS_DIR.parent / f"migration_report_{int(time())}.json"
    with open(report_path, mode = "w", encoding = "utf-8") as file_handle:
        json.dump(report, file_handle, indent = 2)
        file_handle.write("\n")

    print(f"Migration report: {report_path}", flush = True)


def main():
    """Run explicit legacy migration when enabled in config.

    Args:
        None.

    Returns:
        None.
    """
    ensure_workspace_dirs()

    if not MIGRATION_ENABLE:
        print("Migration is disabled in pipeline_clean/config.py", flush = True)
        return

    report = {
        "migration_enabled": True,
        "polygon_json_copied": 0,
        "annotation_seed_json_copied": 0,
        "annotation_final_json_copied": 0,
        "issues": [],
    }

    copy_polygon_tree(report)
    copy_annotation_sources(report)
    write_report(report)


if __name__ == "__main__":
    main()
