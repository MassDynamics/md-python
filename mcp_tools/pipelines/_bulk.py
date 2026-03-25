"""Shared helpers for all *_bulk pipeline submission tools."""

import concurrent.futures
import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .._client import get_client

_MAX_BULK_JOBS = 500
_BULK_WORKERS = 20
_TERMINAL_STATES: Set[str] = {"COMPLETED", "FAILED", "ERROR", "CANCELLED"}


def _bulk_prefetch_upload_data(
    upload_id: str, existing_type: str
) -> Tuple[Optional[str], Dict[str, str]]:
    """Fetch all datasets for an upload in one API call.

    Returns:
        (intensity_dataset_id_or_none, {dataset_name: dataset_id})
        where the dict contains existing datasets of existing_type only.
    """
    try:
        datasets = get_client().datasets.list_by_upload(upload_id)
        intensity_id = next(
            (str(d.id) for d in datasets if getattr(d, "type", None) == "INTENSITY"),
            None,
        )
        existing = {
            d.name: str(d.id)
            for d in datasets
            if getattr(d, "type", None) == existing_type
        }
        return intensity_id, existing
    except Exception:
        return None, {}


def _run_jobs_parallel(
    jobs: List[Dict[str, Any]],
    process_fn: Callable[[int, Dict[str, Any]], Dict[str, Any]],
) -> str:
    """Validate job count, submit all jobs in parallel, return ordered JSON array.

    Returns an error JSON object (not array) if the job count exceeds _MAX_BULK_JOBS.
    process_fn(index, job) must return a result dict with at least {"index": index}.
    """
    if len(jobs) > _MAX_BULK_JOBS:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. Maximum per call is {_MAX_BULK_JOBS}. "
                    "Split the call into smaller batches."
                )
            }
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=_BULK_WORKERS) as executor:
        futures = [executor.submit(process_fn, i, job) for i, job in enumerate(jobs)]
        results = [f.result() for f in futures]

    return json.dumps(results, indent=2)
