"""Shared fixtures and helpers for pipeline tests."""

from unittest.mock import MagicMock

INTENSITY_ID = "435d321c-281e-4722-b08d-08f5b15de17f"
OUTPUT_ID = "6842e0e3-f855-4d37-8e92-6ca415f61706"

SAMPLE_METADATA = [
    ["sample_name", "condition", "dose"],
    ["s1", "ctrl", "0"],
    ["s2", "ctrl", "0"],
    ["s3", "treated", "10"],
    ["s4", "treated", "10"],
]


def mock_initial_ds(dataset_id: str = INTENSITY_ID) -> MagicMock:
    ds = MagicMock()
    ds.id = dataset_id
    return ds


def mock_dr_ds(
    dataset_id: str = OUTPUT_ID, name: str = "My DR", state: str = "COMPLETED"
) -> MagicMock:
    ds = MagicMock()
    ds.id = dataset_id
    ds.name = name
    ds.type = "DOSE_RESPONSE"
    ds.state = state
    return ds
