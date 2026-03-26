"""Find initial INTENSITY datasets for uploads."""

import json
from typing import List

from .. import mcp
from .._client import get_client


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
