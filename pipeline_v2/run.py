"""Command-line entrypoint for pipeline_v2.

The default workflow is sample-first for few-shot curation:
1) Build or update structured manifest.
2) Package selected pages and annotation seeds.
3) Export cleaned final outputs after manual editing.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline_v2.config import (
    DEFAULT_MANIFEST_NAME,
    ensure_pipeline_directories,
    get_paths,
)
from pipeline_v2.stages.export_cleaned_final import export_cleaned_final
from pipeline_v2.stages.few_shot_manifest import (
    build_manifest_from_mapping,
    get_selected_pages,
    load_manifest,
    parse_legacy_pages_txt,
    validate_manifest,
    write_manifest,
)
from pipeline_v2.stages.package_annotation_inputs import package_annotation_inputs


def resolve_manifest_path(manifest_path_arg: str | None) -> Path:
    """Resolve manifest path from CLI argument or default location.

    Args:
        manifest_path_arg: Optional path provided by CLI.

    Returns:
        Absolute manifest path.
    """
    paths = get_paths()
    if manifest_path_arg:
        return Path(manifest_path_arg).resolve()

    return (paths.manifests_dir / DEFAULT_MANIFEST_NAME).resolve()


def command_init_manifest(args: argparse.Namespace) -> None:
    """Create structured few-shot manifest from legacy pages.txt selection.

    Args:
        args: Parsed CLI arguments.

    Returns:
        None. Manifest is written to disk.
    """
    paths = get_paths()
    ensure_pipeline_directories(paths)

    pages_file = Path(args.pages_file).resolve()
    if not pages_file.exists():
        raise FileNotFoundError(f"Pages file not found: {pages_file}")

    page_mapping = parse_legacy_pages_txt(pages_file)
    manifest = build_manifest_from_mapping(
        page_mapping = page_mapping,
        source_label = str(pages_file),
    )

    manifest_path = resolve_manifest_path(args.manifest)
    write_manifest(manifest_path, manifest)

    print(f"Manifest written: {manifest_path}", flush = True)
    print(f"Documents: {len(page_mapping)}", flush = True)
    print(
        f"Pages: {sum(len(page_numbers) for page_numbers in page_mapping.values())}",
        flush = True,
    )


def command_validate_manifest(args: argparse.Namespace) -> None:
    """Validate structured few-shot manifest and print any issues.

    Args:
        args: Parsed CLI arguments.

    Returns:
        None. Raises RuntimeError on invalid manifest.
    """
    manifest_path = resolve_manifest_path(args.manifest)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = load_manifest(manifest_path)
    issues = validate_manifest(manifest)
    if issues:
        for issue in issues:
            print(f"MANIFEST ISSUE: {issue}", flush = True)
        raise RuntimeError("Manifest validation failed.")

    print(f"Manifest valid: {manifest_path}", flush = True)


def command_package_annotation(args: argparse.Namespace) -> None:
    """Package selected pages into annotation-ready workspace for editor use.

    Args:
        args: Parsed CLI arguments.

    Returns:
        None. Files are copied into pipeline_v2/workspace.
    """
    paths = get_paths()
    ensure_pipeline_directories(paths)

    manifest_path = resolve_manifest_path(args.manifest)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = load_manifest(manifest_path)
    issues = validate_manifest(manifest)
    if issues:
        for issue in issues:
            print(f"MANIFEST ISSUE: {issue}", flush = True)
        raise RuntimeError("Manifest validation failed.")

    selected_pages = get_selected_pages(
        manifest            = manifest,
        include_candidates  = args.include_candidates,
    )

    stats = package_annotation_inputs(
        paths          = paths,
        selected_pages = selected_pages,
    )

    print("Packaging summary", flush = True)
    print(f"  Selected pages : {stats.selected_pages}", flush = True)
    print(f"  Copied PDFs    : {stats.copied_pdfs}", flush = True)
    print(f"  Copied JSON    : {stats.copied_json}", flush = True)
    print(f"  Missing PDFs   : {stats.missing_pdfs}", flush = True)
    print(f"  Missing JSON   : {stats.missing_json}", flush = True)


def command_export_cleaned(_: argparse.Namespace) -> None:
    """Export CLEANED_FINAL from pipeline_v2 editable final workspace.

    Args:
        _: Parsed CLI arguments (unused).

    Returns:
        None. Exported files are written to CLEANED_FINAL.
    """
    paths = get_paths()
    ensure_pipeline_directories(paths)

    stats = export_cleaned_final(paths)
    print("Export summary", flush = True)
    print(f"  Page folders  : {stats.pages_seen}", flush = True)
    print(f"  JSON exported : {stats.json_exported}", flush = True)
    print(f"  PDF exported  : {stats.pdf_exported}", flush = True)
    print(f"  Schema issues : {stats.schema_issues}", flush = True)


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for pipeline_v2 command-line interface.

    Args:
        None.

    Returns:
        Configured ArgumentParser.
    """
    paths = get_paths()

    parser = argparse.ArgumentParser(
        description = "Run structured pipeline_v2 few-shot preparation stages.",
    )
    subparsers = parser.add_subparsers(dest = "command", required = True)

    init_manifest_parser = subparsers.add_parser(
        "init-manifest",
        help = "Generate structured manifest from legacy pages.txt",
    )
    init_manifest_parser.add_argument(
        "--pages-file",
        default = str(paths.root_dir / "pages.txt"),
        help = "Path to legacy pages.txt-like file.",
    )
    init_manifest_parser.add_argument(
        "--manifest",
        default = str(paths.manifests_dir / DEFAULT_MANIFEST_NAME),
        help = "Output manifest JSON path.",
    )
    init_manifest_parser.set_defaults(func = command_init_manifest)

    validate_manifest_parser = subparsers.add_parser(
        "validate-manifest",
        help = "Validate structured manifest before packaging.",
    )
    validate_manifest_parser.add_argument(
        "--manifest",
        default = str(paths.manifests_dir / DEFAULT_MANIFEST_NAME),
        help = "Manifest JSON path.",
    )
    validate_manifest_parser.set_defaults(func = command_validate_manifest)

    package_annotation_parser = subparsers.add_parser(
        "package-annotation",
        help = "Package selected pages and JSON into annotation-ready workspace.",
    )
    package_annotation_parser.add_argument(
        "--manifest",
        default = str(paths.manifests_dir / DEFAULT_MANIFEST_NAME),
        help = "Manifest JSON path.",
    )
    package_annotation_parser.add_argument(
        "--include-candidates",
        action = "store_true",
        help = "Include candidate status pages in addition to approved pages.",
    )
    package_annotation_parser.set_defaults(func = command_package_annotation)

    export_cleaned_parser = subparsers.add_parser(
        "export-cleaned",
        help = "Export cleaned final outputs from pipeline_v2 workspace/final.",
    )
    export_cleaned_parser.set_defaults(func = command_export_cleaned)

    return parser


def main() -> None:
    """Run pipeline_v2 command dispatcher.

    Args:
        None.

    Returns:
        None. Dispatches selected command handler.
    """
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
