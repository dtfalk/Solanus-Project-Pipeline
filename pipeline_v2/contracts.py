"""Shared schema and naming contracts used across pipeline_v2.

This module centralizes assumptions that were previously spread across
multiple scripts. Keeping these definitions in one location reduces the risk
of silent drift when scripts evolve independently.
"""

from __future__ import annotations

from typing import Final


PAGE_NAME_PREFIX: Final[str] = "page_"
PAGE_NAME_WIDTH: Final[int] = 3

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

POLYGON_ENTRY_KEYS: Final[tuple[str, ...]] = (
    "vertices",
    "connections",
    "id",
)

REMOVED_AT_EXPORT_KEYS: Final[tuple[str, ...]] = (
    "src_insertion",
    "src_margin_note",
)


def format_page_name(page_number: int) -> str:
    """Format an integer page number into the canonical folder/file name.

    Args:
        page_number: Positive page number.

    Returns:
        Canonical page token like "page_001".
    """
    if page_number < 0:
        raise ValueError(f"Page number must be non-negative, got {page_number}.")

    return f"{PAGE_NAME_PREFIX}{page_number:0{PAGE_NAME_WIDTH}d}"


def parse_page_name(page_token: str) -> int:
    """Parse canonical page token into an integer page number.

    Args:
        page_token: Token like "page_001".

    Returns:
        Parsed integer page number.
    """
    if not page_token.startswith(PAGE_NAME_PREFIX):
        raise ValueError(f"Invalid page token: {page_token}")

    suffix = page_token[len(PAGE_NAME_PREFIX):]
    if not suffix.isdigit():
        raise ValueError(f"Invalid page token: {page_token}")

    return int(suffix)
