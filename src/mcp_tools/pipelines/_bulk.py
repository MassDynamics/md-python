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
    """Validate job count, submit all jobs in parallel, return summary + results.

    Returns a JSON envelope:

        {
            "summary": {
                "total": N,
                "submitted": int,    # got a dataset_id back
                "skipped": int,      # if_exists='skip' matched an existing dataset
                "failed": int,       # at least one of {error, error_code} present
                "failed_indices": [int, ...],  # ordered by job index
            },
            "results": [ {...per-job entry...}, ... ],
        }

    The summary lives at the TOP of the response so a partial failure can never
    be silently dropped — the LLM sees `failed: K/N` before reading any job.

    Returns an error JSON object (no `results` key) if the job count exceeds
    _MAX_BULK_JOBS.

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

    submitted = sum(
        1 for r in results if "dataset_id" in r and not r.get("skipped")
    )
    skipped = sum(1 for r in results if r.get("skipped"))
    failed_indices = sorted(
        r["index"] for r in results if "error" in r or "error_code" in r
    )
    summary: Dict[str, Any] = {
        "total": len(results),
        "submitted": submitted,
        "skipped": skipped,
        "failed": len(failed_indices),
        "failed_indices": failed_indices,
    }

    return json.dumps({"summary": summary, "results": results}, indent=2)
