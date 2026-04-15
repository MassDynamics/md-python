"""Get and update upload records."""

import json
from typing import Any, Dict, Optional

from md_python.models.metadata import SampleMetadata

from .. import mcp
from .._client import get_client


def _find_upload_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Look up an upload by exact name via the V2 /uploads/query endpoint."""
    response = get_client().uploads.query(search=name)
    for item in response.get("data", []) or []:
        if item.get("name") == name:
            match: Dict[str, Any] = item
            return match
    return None


@mcp.tool()
def get_upload(
    upload_id: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Get an upload by ID or name.

    Provide either upload_id (UUID string) or name — not both.
    Returns upload details including status, source, and metadata.

    Name lookup uses POST /uploads/query with a search filter and then matches
    the result by exact name. If multiple uploads share the name, the first
    match from the server's first page is returned.
    """
    if not upload_id and not name:
        return "Error: provide either upload_id or name"
    if upload_id:
        upload = get_client().uploads.get_by_id(upload_id)
        return str(upload) if upload else "Upload not found"
    match = _find_upload_by_name(name)  # type: ignore[arg-type]
    return str(match) if match else "Upload not found"


@mcp.tool()
def get_upload_sample_metadata(upload_id: str) -> str:
    """Fetch the sample metadata currently stored on an upload.

    Returns JSON:
      {"sample_metadata": [["sample_name", "condition", ...], ["s1", "ctrl"], ...]}

    The returned 2D array is in the same shape as
    load_metadata_from_csv(...)["sample_metadata"], so the output of this
    tool can be passed straight to update_sample_metadata without any
    reshaping.

    Use this before editing metadata on an existing upload so you can show
    the user what is currently stored and propose diffs, instead of
    overwriting from scratch.

    Returns {"error": "..."} on HTTP failure. Returns
    {"sample_metadata": null} if the upload has no metadata stored yet.
    """
    try:
        metadata = get_client().uploads.get_sample_metadata(upload_id)
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch sample metadata: {e}"})
    if metadata is None:
        return json.dumps({"sample_metadata": None})
    return json.dumps({"sample_metadata": metadata.data})


@mcp.tool()
def update_sample_metadata(
    upload_id: str,
    sample_metadata: list,
) -> str:
    """Update the sample metadata for an existing upload.

    ALWAYS obtain sample_metadata by calling load_metadata_from_csv on the user's
    CSV file. Never construct it manually — sample names must match exactly.

    sample_metadata: 2D array from load_metadata_from_csv["sample_metadata"].
    """
    ok = get_client().uploads.update_sample_metadata(
        upload_id, SampleMetadata(data=sample_metadata)
    )
    return (
        "Sample metadata updated successfully"
        if ok
        else "Failed to update sample metadata"
    )
