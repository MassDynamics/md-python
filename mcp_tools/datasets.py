from . import mcp
from ._client import get_client


@mcp.tool()
def list_datasets(upload_id: str) -> str:
    """List all datasets for an upload.

    Returns dataset IDs, names, types, and states.
    """
    datasets = get_client().datasets.list_by_upload(upload_id)
    if not datasets:
        return "No datasets found for this upload"
    lines = [f"Found {len(datasets)} dataset(s):"]
    for ds in datasets:
        lines.append(
            f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
        )
    return "\n".join(lines)


@mcp.tool()
def find_initial_dataset(upload_id: str) -> str:
    """Find the initial INTENSITY dataset for an upload.

    This is the input dataset required to run downstream pipelines
    (normalisation/imputation, pairwise comparison, dose response).
    Returns the dataset ID and details on success.
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
    timeout_seconds: int = 1800,
) -> str:
    """Poll a dataset/pipeline until it reaches a terminal state.

    Returns the final dataset status and details. Default timeout is 30 minutes.
    """
    ds = get_client().datasets.wait_until_complete(
        upload_id, dataset_id, poll_s=poll_seconds, timeout_s=timeout_seconds
    )
    return str(ds)


@mcp.tool()
def retry_dataset(dataset_id: str) -> str:
    """Retry a failed dataset/pipeline job."""
    ok = get_client().datasets.retry(dataset_id)
    return "Dataset retry triggered successfully" if ok else "Failed to retry dataset"


@mcp.tool()
def delete_dataset(dataset_id: str) -> str:
    """Delete a dataset."""
    ok = get_client().datasets.delete(dataset_id)
    return "Dataset deleted successfully" if ok else "Failed to delete dataset"
