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


@mcp.tool()
def cancel_dataset(dataset_id: str) -> str:
    """Cancel a pipeline job that is currently running.

    Only valid when the dataset is in a PROCESSING / RUNNING / PENDING state.
    The API rejects cancellation for already-terminal states (COMPLETED,
    FAILED, ERROR, CANCELLED) with an error — the message is surfaced back
    to the caller.

    Use this to stop a runaway job that was submitted by mistake or whose
    parameters turned out to be wrong. After cancelling, call delete_dataset
    if you also want to remove the partial record.
    """
    try:
        ok = get_client().datasets.cancel(dataset_id)
    except Exception as e:
        return f"Failed to cancel dataset: {e}"
    return (
        "Dataset cancellation requested"
        if ok
        else "Failed to cancel dataset (unknown server response)"
    )
