"""Query datasets with server-side filters and pagination."""

import json
from typing import List, Optional

from .. import mcp
from .._client import get_client
from .._query import format_query_response

_RECORD_KEYS = ["id", "name", "type", "state", "experiment_id", "created_at"]


@mcp.tool()
def query_datasets(
    upload_id: Optional[str] = None,
    state: Optional[List[str]] = None,
    type: Optional[List[str]] = None,
    search: Optional[str] = None,
    page: int = 1,
) -> str:
    """Paginated, filtered search over pipeline result datasets.

    Use this to find datasets by state (e.g. all FAILED in the last
    tenant), by type (e.g. all DOSE_RESPONSE), or by name substring,
    across the whole organisation. For "give me all datasets from one
    upload" prefer list_datasets(upload_id=...) — it's simpler and
    surfaces every record.

    The server caps query results at 500 records per scope and returns
    50 per page; pass page=2, 3, ... for subsequent pages. If you are
    looking for something on a tenant with many thousands of datasets,
    filter first (state + type) rather than paging blindly.

    Args:
        upload_id: restrict to datasets belonging to this upload UUID.
        state: filter by dataset state. Common values: COMPLETED, FAILED,
            ERROR, PROCESSING, RUNNING, PENDING, CANCELLED.
        type: filter by dataset type. Common values: INTENSITY,
            NORMALISATION_AND_IMPUTATION, PAIRWISE, ANOVA, DOSE_RESPONSE,
            DOSE_RESPONSE_AGGREGATE, ENRICHMENT, IMPUTATION, DEMO.
        search: case-insensitive substring match on dataset name. 1-256 chars.
        page: 1-based page number. Defaults to 1.

    Returns JSON:
      {
        "page": 1,
        "total_pages": 3,
        "total_count": 112,
        "datasets": [
          {"id": "...", "name": "...", "type": "...", "state": "...",
           "experiment_id": "...", "created_at": "..."},
          ...
        ]
      }

    experiment_id is the upload UUID that owns the dataset. Returns
    {"error": "..."} on HTTP failure.
    """
    try:
        response = get_client().datasets.query(
            upload_id=upload_id,
            state=state,
            type=type,
            search=search,
            page=page,
        )
    except Exception as e:
        return json.dumps({"error": f"Failed to query datasets: {e}"})
    formatted = format_query_response(response, _RECORD_KEYS, "datasets")
    return json.dumps(formatted, indent=2)
