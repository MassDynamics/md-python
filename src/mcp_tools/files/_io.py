"""Shared low-level file I/O helpers used by both metadata and md_format tools."""

import csv
import os
from typing import List, Tuple


def _sniff_delimiter(file_path: str, sample_bytes: int = 8192) -> str:
    """Detect delimiter using csv.Sniffer on the first sample_bytes of the file.

    Accepts tab, comma, semicolon, or pipe. Falls back to extension heuristic
    (.tsv/.txt → tab, everything else → comma) if Sniffer fails or returns an
    unexpected character.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            sample = f.read(sample_bytes)
        dialect = csv.Sniffer().sniff(sample, delimiters="\t,;|")
        if dialect.delimiter in ("\t", ",", ";", "|"):
            return dialect.delimiter
    except csv.Error:
        pass
    # Extension fallback
    return "\t" if os.path.splitext(file_path)[1].lower() in (".tsv", ".txt") else ","


def _read_header_only(file_path: str, delimiter: str) -> List[str]:
    """Read just the header row — minimal I/O, used for entity-data detection."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        return next(reader, [])


def _read_preview(
    file_path: str, delimiter: str, max_rows: int
) -> Tuple[List[str], List[List[str]]]:
    """Read header + up to max_rows data rows. Stops early — never reads the full file."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, [])
        rows: List[List[str]] = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)
    return header, rows


def _read_full(file_path: str, delimiter: str) -> Tuple[List[str], List[List[str]]]:
    """Read header + all rows. Only call after entity-data check has passed
    and the file is confirmed to be a small metadata/design CSV."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, [])
        rows = list(reader)
    return header, rows
