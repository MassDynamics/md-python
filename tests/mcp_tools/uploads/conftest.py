"""Shared fixtures for mcp_tools.uploads tests."""

import pytest

DESIGN = [
    ["filename", "sample_name", "condition"],
    ["file1.tsv", "s1", "ctrl"],
    ["file2.tsv", "s2", "treated"],
]

METADATA = [
    ["sample_name", "dose"],
    ["s1", "0"],
    ["s2", "10"],
]


@pytest.fixture
def design():
    return [row[:] for row in DESIGN]


@pytest.fixture
def metadata():
    return [row[:] for row in METADATA]
