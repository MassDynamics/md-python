"""Shared fixtures and helpers for mcp_tools.files tests."""

import csv
import os
import tempfile

import pytest


def write_csv(rows, suffix=".csv"):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False, newline="", encoding="utf-8"
    )
    csv.writer(f).writerows(rows)
    f.close()
    return f.name


def write_tsv(rows):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".tsv", delete=False, newline="", encoding="utf-8"
    )
    csv.writer(f, delimiter="\t").writerows(rows)
    f.close()
    return f.name


@pytest.fixture(autouse=True)
def cleanup():
    """Collect temp file paths during a test and delete them afterwards."""
    created = []
    yield created
    for p in created:
        try:
            os.unlink(p)
        except OSError:
            pass
