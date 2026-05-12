"""Cancel queued uploads and check status of multiple uploads."""

import json
from typing import List

from .. import mcp
from .._client import get_client
from ._executor import _get_executor, _reset_executor


@mcp.tool()
def cancel_upload_queue() -> str:
    """Cancel queued large-file uploads and reset the upload queue.

    Use this when a previous upload has stalled and is blocking new uploads from starting.
    Any transfer that is already in progress (actively sending data to S3) will continue —
    only transfers that are queued but have not yet started are cancelled.

    After calling this tool, new create_upload_from_csv calls will start immediately
    instead of waiting behind a stalled transfer.

    Note: cancelled uploads remain in PENDING state on the server — their upload records
    exist but no data was transferred. Use get_upload to check their status and
    re-submit them with create_upload_from_csv if needed.
    """
    _reset_executor()
    return (
        "Upload queue reset. Queued transfers that had not yet started have been cancelled. "
        "Any transfer already in progress will continue to completion. "
        "New uploads will now start immediately."
    )


@mcp.tool()
def list_uploads_status(upload_ids: List[str], summary: bool = False) -> str:
    """Check the status of multiple uploads in a single call.

    Args:
        upload_ids: list of upload UUIDs to check.
        summary: when True, omits 'source' and returns only {name, status}.
            Use summary=True for large polls (100+ uploads) to reduce token overhead.

    Returns JSON: {upload_id: {name, status, source}} by default, or
    {upload_id: {name, status}} when summary=True.
    Individual fetch errors are recorded inline rather than failing the whole call.
    """
    c = get_client()
    results = {}
    for uid in upload_ids:
        try:
            upload = c.uploads.get_by_id(uid)
            entry: dict = {
                "name": getattr(upload, "name", None),
                "status": getattr(upload, "status", None),
            }
            if not summary:
                entry["source"] = getattr(upload, "source", None)
            results[uid] = entry
        except Exception as e:
            results[uid] = {"error": str(e)}
    return json.dumps(results, indent=2)
