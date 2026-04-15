"""Retry and delete pipeline datasets."""

from .. import mcp
from .._client import get_client


@mcp.tool()
def retry_dataset(dataset_id: str) -> str:
    """Retry a FAILED, ERROR, or CANCELLED pipeline dataset in place.

    Returns: prose. Exactly "Dataset retry triggered successfully" on OK,
    "Failed to retry dataset" on any non-OK response. Details are NOT
    surfaced — if you need them, call list_datasets(upload_id) after.

    Use this when: wait_for_dataset returned a terminal failure state and
    you believe the failure was transient (network, quota, worker crash).
    The same dataset_id is reused and the retried run lands in place of
    the failed one.

    Do NOT use this when: the failure was caused by wrong parameters —
    delete the dataset and re-submit with corrected parameters instead.
    Do NOT retry a RUNNING or PENDING dataset; wait_for_dataset will
    still return a terminal state if you give it time.

    Guardrails:
      - After retrying, call wait_for_dataset(upload_id, dataset_id) to
        monitor the new run.
      - Do not retry more than twice in a row without asking the user to
        investigate the root cause. Repeated failures almost always
        indicate data or parameter issues, not infrastructure.

    See also: wait_for_dataset, delete_dataset, cancel_dataset,
      Workflow H (retry after failure).
    """
    ok = get_client().datasets.retry(dataset_id)
    return "Dataset retry triggered successfully" if ok else "Failed to retry dataset"


@mcp.tool()
def delete_dataset(dataset_id: str) -> str:
    """Permanently delete a pipeline result dataset. DESTRUCTIVE, IRREVERSIBLE.

    Returns: prose. Exactly "Dataset deleted successfully" on 204, or
    "Failed to delete dataset" on any non-OK response.

    Use this when: the user has explicitly confirmed removal of an
    unwanted or failed analysis result, OR when unblocking delete_upload
    (which fails with 409 while datasets exist).

    Do NOT use this when: the dataset is still RUNNING / PROCESSING — use
    cancel_dataset first, then delete once it reaches a terminal state.
    Do NOT delete the initial INTENSITY dataset of an upload unless the
    user has agreed to re-process the whole upload from scratch —
    deletion orphans the upload and downstream pipelines will need to
    re-ingest.

    Guardrails:
      - IRREVERSIBLE. Always echo the dataset_id back to the user and
        state the dataset name + type from a prior list_datasets call
        before calling. Wait for explicit "yes, delete <id>".
      - Never include delete_dataset in a batch() with run_* or wait_*
        tools.

    See also: cancel_dataset (for running jobs), retry_dataset (prefer for
      FAILED jobs), delete_upload (top-level cleanup once all datasets
      are gone).
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
