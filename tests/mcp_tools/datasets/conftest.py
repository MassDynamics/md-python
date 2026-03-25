"""Shared helpers for dataset tests."""

from unittest.mock import MagicMock


def mock_dataset(
    id: str = "ds-1",
    name: str = "My Dataset",
    type: str = "INTENSITY",
    state: str = "COMPLETED",
) -> MagicMock:
    ds = MagicMock()
    ds.id = id
    ds.name = name
    ds.type = type
    ds.state = state
    ds.__str__ = lambda self: f"Dataset: {name}"
    return ds
