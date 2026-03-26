"""Retry and delete pipeline datasets."""

from .. import mcp
from .._client import get_client


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
