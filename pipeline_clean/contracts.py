"""Shared contracts for standalone pipeline_clean workflow."""

from __future__ import annotations

from typing import Final


PAGE_PREFIX: Final[str] = "page_"
PAGE_DIGITS: Final[int] = 3

ANNOTATION_SCHEMA_KEYS: Final[tuple[str, ...]] = (
    "src_content",
    "src_origin",
    "src_recipient",
    "src_location_recipient",
    "src_location_sender",
    "src_date",
    "src_greeting",
    "src_farewell",
    "src_signature",
    "src_margin_note",
    "src_insertion",
    "src_other",
    "archv_commentary",
    "archv_format_note",
    "archv_date",
    "archv_possessor",
    "archv_other",
    "struct_id",
    "struct_doc",
    "struct_commentary",
    "struct_other",
    "other",
)

POLYGON_KEYS: Final[tuple[str, ...]] = (
    "vertices",
    "connections",
    "id",
)

EXPORT_EXCLUDED_KEYS: Final[tuple[str, ...]] = (
    "src_margin_note",
    "src_insertion",
)


def format_page_name(page_number: int) -> str:
    """Return canonical page token.

    Args:
        page_number: Positive page number.

    Returns:
        Canonical token formatted as page_NNN.
    """
    if page_number < 1:
        raise ValueError(f"Page number must be >= 1, got {page_number}.")

    return f"{PAGE_PREFIX}{page_number:0{PAGE_DIGITS}d}"


def parse_page_name(page_name: str) -> int:
    """Parse canonical page token into integer page number.

    Args:
        page_name: Canonical token formatted as page_NNN.

    Returns:
        Parsed page number.
    """
    if not page_name.startswith(PAGE_PREFIX):
        raise ValueError(f"Invalid page token: {page_name}")

    suffix = page_name[len(PAGE_PREFIX):]
    if not suffix.isdigit():
        raise ValueError(f"Invalid page token: {page_name}")

    return int(suffix)
