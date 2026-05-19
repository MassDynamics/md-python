"""Shared helpers for dataset tests."""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock


def mock_dataset(
    id: str = "ds-1",
    name: str = "My Dataset",
    type: str = "INTENSITY",
    state: str = "COMPLETED",
    job_slug: str = "",
    job_run_params: Optional[Dict[str, Any]] = None,
    input_dataset_ids: Optional[List[str]] = None,
    sample_names: Optional[List[str]] = None,
    job_run_start_time: Any = None,
    error_message: Optional[str] = None,
) -> MagicMock:
    ds = MagicMock()
    ds.id = id
    ds.name = name
    ds.type = type
    ds.state = state
    ds.job_slug = job_slug
    ds.job_run_params = job_run_params if job_run_params is not None else {}
    ds.input_dataset_ids = input_dataset_ids if input_dataset_ids is not None else []
    ds.sample_names = sample_names
    ds.job_run_start_time = job_run_start_time
    ds.error_message = error_message
    ds.__str__ = lambda self: f"Dataset: {name}"
    return ds
