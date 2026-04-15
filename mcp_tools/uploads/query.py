"""Query uploads with server-side filters and pagination."""

import json
from typing import Dict, List, Optional

from .. import mcp
from .._client import get_client
from .._query import format_query_response

_RECORD_KEYS = ["id", "name", "status", "source", "created_at"]


@mcp.tool()
def query_uploads(
    status: Optional[List[str]] = None,
    source: Optional[List[str]] = None,
    search: Optional[str] = None,
    sample_metadata: Optional[List[Dict[str, str]]] = None,
    page: int = 1,
) -> str:
    """Paginated, filtered search over uploads in the current organisation.

    Use this when you want to **discover** uploads by status, source, free-text
    name match, or sample-metadata column values. For an exact name lookup
    prefer get_upload(name=...), which already handles the common case.

    The server returns 50 results per page. Pass page=2, page=3, ... to walk
    through further pages. The response includes pagination metadata so the
    agent can decide whether another call is needed.

    Args:
        status: filter to uploads in any of these statuses — e.g.
            ["completed"], ["processing", "verifying"], ["processing_failed"].
        source: filter by acquisition software / format used to produce the
            files — e.g. ["diann_tabular"], ["maxquant", "spectronaut"].
        search: case-insensitive free-text match on upload name and sample
            metadata contents. Max 256 characters.
        sample_metadata: structured metadata filter. Each entry is a dict
            with "column" and "value" keys, matching a column in the upload's
            sample_metadata against a value. Server caps at 10 entries.
            Example: [{"column": "condition", "value": "treated"}]
        page: 1-based page number. Defaults to 1.

    Returns JSON:
      {
        "page": 1,
        "total_pages": 4,
        "total_count": 187,
        "uploads": [
          {"id": "...", "name": "...", "status": "...",
           "source": "...", "created_at": "..."},
          ...
        ]
      }

    Returns {"error": "..."} on HTTP failure.
    """
    try:
        response = get_client().uploads.query(
            status=status,
            source=source,
            search=search,
            sample_metadata=sample_metadata,
            page=page,
        )
    except Exception as e:
        return json.dumps({"error": f"Failed to query uploads: {e}"})
    formatted = format_query_response(response, _RECORD_KEYS, "uploads")
    return json.dumps(formatted, indent=2)
