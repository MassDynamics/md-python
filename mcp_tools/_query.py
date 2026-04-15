"""Small shared helper used by query_uploads / query_datasets.

Projects a V2 /query response down to a compact shape that:
- surfaces the pagination block directly on the top level for agent use,
- replaces the raw `data` array with a projected list of only the fields
  the MCP tool wants to expose (so we don't dump the full record per row).
"""

from typing import Any, Dict, List


def format_query_response(
    result: Dict[str, Any],
    record_keys: List[str],
    out_key: str,
) -> Dict[str, Any]:
    """Flatten a query response into {page, total_pages, total_count, <out_key>}.

    result: raw response dict from the V2 /query endpoint. Expected shape:
      {"data": [ {...record...}, ... ], "pagination": {"current_page": ...,
       "per_page": 50, "total_count": ..., "total_pages": ...}}
    record_keys: allowed fields to surface per record. Missing keys are
      silently omitted rather than filled with None — keeps the payload lean.
    out_key: top-level key that will contain the projected record list
      (e.g. "uploads" or "datasets").

    The 50-per-page default is server-imposed. Callers must page explicitly
    via the tool's `page` argument; this helper does not auto-paginate.
    """
    pagination = result.get("pagination") or {}
    projected = [
        {k: record[k] for k in record_keys if k in record}
        for record in result.get("data") or []
    ]
    return {
        "page": pagination.get("current_page"),
        "total_pages": pagination.get("total_pages"),
        "total_count": pagination.get("total_count"),
        out_key: projected,
    }
