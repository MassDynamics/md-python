"""List pipeline jobs and datasets."""

import json
from typing import Optional

from .. import mcp
from .._client import get_client


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
