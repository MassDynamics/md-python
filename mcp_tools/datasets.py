import json
from typing import List, Optional

from . import mcp
from ._client import get_client


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
