import concurrent.futures
import json
import time
from collections import Counter
from typing import Dict, List, Optional

from . import mcp
from ._client import get_client

_DATASETS_BULK_MAX = 500
_DATASETS_BULK_WORKERS = 20
_TERMINAL_STATES = {"COMPLETED", "FAILED", "ERROR", "CANCELLED"}


def _fetch_dataset_state(job: Dict[str, str]) -> Dict[str, str]:
    """Fetch the current state of one dataset. Returns job dict augmented with 'state'.

    State is one of the API states (COMPLETED, RUNNING, FAILED, etc.) on success.
    Returns state="FETCH_ERROR" with an "error" key when the API call fails.

    upload_id is optional. When omitted, the dataset is fetched directly by ID
    (GET /datasets/:id). When provided, list_by_upload is used instead — useful
    when the direct endpoint is unavailable or for batching.
    """
    dataset_id = job["dataset_id"]
    upload_id = job.get("upload_id")
    try:
        if upload_id:
            datasets = get_client().datasets.list_by_upload(upload_id)
            ds = next((d for d in datasets if str(d.id) == dataset_id), None)
            if ds is None:
                return {
                    "dataset_id": dataset_id,
                    "upload_id": upload_id,
                    "state": "NOT_FOUND",
                    "error": "Dataset ID not found in upload — it may still be queued or the ID may be wrong.",
                }
        else:
            ds = get_client().datasets.get_by_id(dataset_id)
        return {"dataset_id": dataset_id, "state": ds.state}
    except Exception as e:
        result: Dict[str, str] = {
            "dataset_id": dataset_id,
            "state": "FETCH_ERROR",
            "error": f"API call failed: {e}",
        }
        if upload_id:
            result["upload_id"] = upload_id
        return result


@mcp.tool()
def list_jobs(upload_id: Optional[str] = None) -> str:
    """List pipeline jobs — either the global catalog or executed runs for a specific upload.

    Args:
        upload_id: when provided, returns executed pipeline runs for that upload
            (same as list_datasets, useful for checking which jobs have been submitted).
            When omitted, returns the global catalog of available pipeline types
            (slugs, names, descriptions).

    Without upload_id: returns job slugs you can pass to describe_pipeline() and run_*.
    Typical slugs: normalisation_imputation, pairwise_comparison, dose_response.

    With upload_id: returns all datasets for that upload (INTENSITY, DOSE_RESPONSE, etc.).
    To filter by type, use list_datasets(upload_id, type_filter="DOSE_RESPONSE") instead.
    """
    if upload_id is not None:
        datasets = get_client().datasets.list_by_upload(upload_id)
        if not datasets:
            return "No pipeline jobs found for this upload"
        lines = [f"Found {len(datasets)} job(s) for upload {upload_id}:"]
        for ds in datasets:
            lines.append(
                f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
            )
        return "\n".join(lines)

    jobs = get_client().jobs.list()
    if not jobs:
        return "No jobs available"
    return json.dumps(jobs, indent=2)


@mcp.tool()
def list_datasets(upload_id: str, type_filter: Optional[str] = None) -> str:
    """List all datasets associated with an upload, with their IDs, names, types, and states.

    Args:
        upload_id: the upload UUID to list datasets for.
        type_filter: restrict output to one dataset type, e.g. "DOSE_RESPONSE",
            "PAIRWISE_COMPARISON", "INTENSITY". Case-insensitive.
            Use this to check which pipeline jobs have already been submitted.

    Dataset types: INTENSITY (input for pipelines), PAIRWISE_COMPARISON, DOSE_RESPONSE.

    For the common case of finding the INTENSITY dataset ID, prefer find_initial_dataset.
    """
    datasets = get_client().datasets.list_by_upload(upload_id)
    if type_filter:
        datasets = [d for d in datasets if d.type == type_filter.upper()]
    if not datasets:
        return "No datasets found for this upload"
    lines = [f"Found {len(datasets)} dataset(s):"]
    for ds in datasets:
        lines.append(
            f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
        )
    return "\n".join(lines)


@mcp.tool()
def find_initial_datasets(upload_ids: List[str]) -> str:
    """Find the initial INTENSITY dataset for multiple uploads in one call.

    Equivalent to calling find_initial_dataset for each upload_id individually,
    but collapses all lookups into a single round-trip. Use this when you have
    many uploads and need their dataset IDs before running pipelines.

    Returns JSON: {upload_id: {"dataset_id": "..."} | {"error": "..."}}
    Errors are recorded inline — all uploads are checked regardless of failures.
    """
    c = get_client()
    results: dict = {}
    for uid in upload_ids:
        try:
            ds = c.datasets.find_initial_dataset(uid)
            if ds:
                results[uid] = {"dataset_id": str(ds.id)}
            else:
                results[uid] = {"error": "No initial dataset found"}
        except Exception as e:
            results[uid] = {"error": str(e)}
    return json.dumps(results, indent=2)


@mcp.tool()
def find_initial_dataset(upload_id: str) -> str:
    """Find the initial INTENSITY dataset for an upload.

    PREFER find_initial_datasets when looking up multiple uploads at once (one call).

    Call this after wait_for_upload returns COMPLETED. The dataset ID returned here
    is what you pass as input_dataset_ids to every run_* pipeline tool.

    Returns the dataset ID and details on success, or an error if the upload
    has not finished processing yet.
    """
    ds = get_client().datasets.find_initial_dataset(upload_id)
    if not ds:
        return "No initial INTENSITY dataset found for this upload"
    return f"Initial dataset found.\nID: {ds.id}\n{ds}"


@mcp.tool()
def wait_for_dataset(
    upload_id: str,
    dataset_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check pipeline dataset status, polling until a terminal state or timeout.

    Args:
        upload_id: the upload UUID the dataset belongs to.
        dataset_id: the dataset UUID returned by a run_* tool.
        poll_seconds: seconds between status checks (default 5).
        timeout_seconds: max seconds to wait before returning current status (default 45).
            Keep below 60 — the MCP client enforces a hard 60-second per-call limit.
            If timeout is reached, call again to continue monitoring.

    Terminal states (stops polling): COMPLETED, FAILED, ERROR, CANCELLED.
    Non-terminal (call again): RUNNING, PENDING.

    On COMPLETED: use dataset_id as input_dataset_ids for the next pipeline.
    On FAILED/ERROR: call retry_dataset(dataset_id) to re-run.
    """
    try:
        ds = get_client().datasets.wait_until_complete(
            upload_id, dataset_id, poll_s=poll_seconds, timeout_s=timeout_seconds
        )
        return str(ds)
    except TimeoutError:
        # Return current state — caller should call again to continue monitoring
        try:
            datasets = get_client().datasets.list_by_upload(upload_id)
            ds = next((d for d in datasets if str(d.id) == dataset_id), None)
            if ds:
                return (
                    f"State: {ds.state}. Pipeline not yet complete — "
                    f"call wait_for_dataset again to continue monitoring.\n{ds}"
                )
            return "Dataset not yet visible — call wait_for_dataset again to continue monitoring."
        except Exception as e:
            return f"State unknown (could not fetch dataset): {e}. Call wait_for_dataset again."
    except Exception as e:
        return f"Dataset {dataset_id} failed: {e}"


@mcp.tool()
def retry_dataset(dataset_id: str) -> str:
    """Retry a failed or errored pipeline job.

    Call this when wait_for_dataset returns a FAILED or ERROR state.
    After retrying, call wait_for_dataset again to monitor the new run.
    """
    ok = get_client().datasets.retry(dataset_id)
    return "Dataset retry triggered successfully" if ok else "Failed to retry dataset"


@mcp.tool()
def delete_dataset(dataset_id: str) -> str:
    """Permanently delete a pipeline result dataset.

    Use this to remove unwanted or failed analysis results. This action
    cannot be undone. Do not delete the initial INTENSITY dataset unless
    you intend to re-process the upload from scratch.
    """
    ok = get_client().datasets.delete(dataset_id)
    return "Dataset deleted successfully" if ok else "Failed to delete dataset"


@mcp.tool()
def wait_for_datasets_bulk(
    jobs: List[Dict[str, str]],
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check the status of multiple pipeline datasets, polling until all are terminal.

    Args:
        jobs: list of job dicts. Max 500. Each dict must have "dataset_id".
            "upload_id" is optional — omit it to look up datasets directly by ID
            (simpler, avoids upload_id mapping errors). Include it only if needed.
            Examples:
              [{"dataset_id": "abc"}]                          # preferred
              [{"upload_id": "up-1", "dataset_id": "abc"}]    # also valid
        poll_seconds: seconds between status sweeps (default 5).
        timeout_seconds: max seconds before returning current summary (default 45).
            Keep below 60 — the MCP client enforces a hard 60-second per-call limit.
            If timeout is reached, call again to continue monitoring.

    Fetches all dataset states concurrently (up to 20 parallel connections).
    Polls until all datasets reach a terminal state or timeout is reached.

    Terminal states: COMPLETED, FAILED, ERROR, CANCELLED.
    Non-terminal (will appear in "pending"): RUNNING, PENDING, PROCESSING.

    Returns JSON:
      {
        "total": N,
        "all_terminal": true/false,
        "by_state": {"COMPLETED": N, "RUNNING": N, ...},
        "pending": [{"dataset_id": "...", "state": "..."}],
        "failed":  [{"dataset_id": "...", "state": "..."}]
      }
    When all_terminal is false, call wait_for_datasets_bulk again with the same jobs.
    Pass the "failed" list items to retry_dataset to re-run failed jobs.
    """
    if len(jobs) > _DATASETS_BULK_MAX:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. "
                    f"Maximum per call is {_DATASETS_BULK_MAX}. "
                    "Split the call into smaller batches."
                )
            }
        )

    deadline = time.monotonic() + timeout_seconds

    while True:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=_DATASETS_BULK_WORKERS
        ) as executor:
            statuses = list(executor.map(_fetch_dataset_state, jobs))

        by_state: Dict[str, int] = dict(Counter(s["state"] for s in statuses))
        pending = [s for s in statuses if s["state"] not in _TERMINAL_STATES]
        failed = [s for s in statuses if s["state"] in ("FAILED", "ERROR")]
        all_terminal = len(pending) == 0

        if all_terminal or time.monotonic() >= deadline:
            return json.dumps(
                {
                    "total": len(jobs),
                    "all_terminal": all_terminal,
                    "by_state": by_state,
                    "pending": pending,
                    "failed": failed,
                },
                indent=2,
            )

        time.sleep(poll_seconds)
