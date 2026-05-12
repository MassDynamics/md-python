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
    """Fetch a single upload record by ID or by exact name.

    Returns: prose. A multi-line str(Upload) repr on success (id, name,
    status, source, created_at, file counts). "Upload not found" if no
    record matches. "Error: provide either upload_id or name" when
    neither is supplied. NOT JSON — do not json.loads this.

    Use this when: you have a upload_id or an exact name and need status,
    source, or metadata for a single upload.

    Do NOT use this when: searching by partial name / substring / filters —
    use query_uploads(search=..., ...). For bulk status of several uploads
    use list_uploads_status.

    Args:
      upload_id: UUID of the upload. Mutually exclusive with name.
      name: exact upload name. Mutually exclusive with upload_id. Resolved
        via POST /uploads/query (search filter) then exact-match locally
        on the FIRST PAGE ONLY. If more than ~50 uploads share a prefix
        and the exact match is on page 2+, it will be reported missing.
        Prefer upload_id whenever available.

    Errors:
      - "Error: provide either upload_id or name" — neither supplied.
      - "Upload not found" — 404 or no page-1 exact-name match.
      - Underlying HTTP exceptions propagate (not wrapped).

    Guardrails:
      - Name lookup is best-effort; surface the page-1 limitation to the
        user when ambiguity matters.
      - Echo the upload_id back before any destructive follow-up.

    See also: query_uploads, list_uploads_status, get_upload_sample_metadata.
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
    """Overwrite the sample_metadata stored on an existing upload.

    Returns: prose. Exactly "Sample metadata updated successfully" on
    200 OK, or "Failed to update sample metadata" on any server error.

    Use this when: the user has asked to correct a typo, missing column,
    or extra samples on an upload that has already been created.

    Do NOT use this when: the upload has not been created yet — pass the
    corrected metadata to create_upload[_from_csv] instead. Do NOT call
    while any downstream pipeline is still RUNNING against the old
    metadata — cancel those first.

    Args:
      upload_id: UUID of the upload to update.
      sample_metadata: 2D array with header row including "sample_name".
        MUST come from load_metadata_from_csv or get_upload_sample_metadata
        — never hand-built. Sample names must still match exactly what the
        upload was created with; the backend links files to samples by
        name (workflow/app/models/experiment.rb:107-121).

    Errors:
      - APIError 422: sample_name mismatch between experiment_design and
        the new sample_metadata.
      - APIError 404: upload not found.

    Guardrails:
      - DESTRUCTIVE. Replaces the whole array — there is no cell-level
        patch API. Before calling:
          1. get_upload_sample_metadata(upload_id) to fetch current.
          2. Show the user the diff.
          3. Wait for explicit "yes, overwrite <upload_id>".
      - Re-run any downstream pipelines (NI, pairwise, anova, dose_response)
        whose input relied on the old metadata — they are now analytically
        stale.

    See also: get_upload_sample_metadata, load_metadata_from_csv,
      Workflow I (metadata correction).
    """
    ok = get_client().uploads.update_sample_metadata(
        upload_id, SampleMetadata(data=sample_metadata)
    )
    return (
        "Sample metadata updated successfully"
        if ok
        else "Failed to update sample metadata"
    )
