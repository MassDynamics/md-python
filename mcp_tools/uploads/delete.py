"""Delete an upload."""

from .. import mcp
from .._client import get_client


@mcp.tool()
def delete_upload(upload_id: str) -> str:
    """Permanently delete an upload and its uploaded files.

    Destructive and irreversible. Only delete an upload the user explicitly
    asked to remove. Prefer leaving failed uploads in place so the user can
    inspect the error rather than silently wiping them.

    Fails with a 409 conflict if the upload still has associated pipeline
    datasets (normalisation_imputation, pairwise_comparison, etc.). In that
    case the tool returns a message telling the caller to delete the
    datasets first via delete_dataset, then retry delete_upload.

    Returns a success message on 204, a friendly "has datasets" message on
    409, and "Failed to delete upload: ..." for any other server error.
    """
    try:
        ok = get_client().uploads.delete(upload_id)
    except Exception as e:
        msg = str(e)
        if "409" in msg:
            return (
                "Cannot delete upload: it has associated datasets. "
                "Delete them first via delete_dataset, then call "
                "delete_upload again."
            )
        return f"Failed to delete upload: {e}"
    return (
        "Upload deleted successfully"
        if ok
        else "Failed to delete upload (unknown server response)"
    )
