"""List pipeline jobs and datasets."""

import json
from typing import Optional

from .. import mcp
from .._client import get_client


@mcp.tool()
def list_jobs(upload_id: Optional[str] = None) -> str:
    """Dual-mode: global pipeline catalog OR executed runs for one upload.

    ⚠ AGENTS: the return shape depends on which mode you call. Branch on
    whether upload_id is set before parsing.

    Returns:
      - Mode A (upload_id omitted): JSON array from the server's /jobs
        endpoint. Typical fields: slug, name, description. Empty case:
        prose string "No jobs available".
      - Mode B (upload_id set): PROSE, NOT JSON. Format:
          "Found N job(s) for upload <uuid>:
             ID: <id> | Name: <name> | Type: <type> | State: <state>
             ..."
        Empty case: "No pipeline jobs found for this upload". Do NOT
        json.loads Mode B — parse line by line.

    Use this when:
      - Mode A — the user wants to know which pipeline slugs exist so you
        can pass them to describe_pipeline or run_*.
      - Mode B — the user wants to see every dataset attached to one
        upload without a type filter.

    Do NOT use this when:
      - You want type-filtered output — use list_datasets(upload_id,
        type_filter="...") instead (same Mode B format, with filtering).
      - You only need the INTENSITY dataset — use find_initial_dataset.

    Args:
      upload_id: optional upload UUID. Presence selects the return mode.

    See also: describe_pipeline, list_datasets, find_initial_dataset.
    """
    if upload_id is not None:
        datasets = get_client().datasets.list_by_upload(upload_id)
        if not datasets:
            return "No pipeline jobs found for this upload"
        lines = [f"Found {len(datasets)} job(s) for upload {upload_id}:"]
        for ds in datasets:
            lines.append(
                f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
            )
        return "\n".join(lines)

    jobs = get_client().jobs.list()
    if not jobs:
        return "No jobs available"
    return json.dumps(jobs, indent=2)


@mcp.tool()
def list_datasets(upload_id: str, type_filter: Optional[str] = None) -> str:
    """List datasets for an upload, optionally filtered by type.

    Returns: prose multi-line table. NOT JSON — parse line by line.
    Format:
      "Found N dataset(s):
         ID: <id> | Name: <name> | Type: <type> | State: <state>
         ..."
    Empty: "No datasets found for this upload".

    Use this when: you want every dataset for one upload, or a single
    type (e.g. all PAIRWISE results).

    Do NOT use this when: you only need the INTENSITY dataset (use
    find_initial_dataset — it handles the type disambiguation). For
    structured JSON + pagination over many uploads use query_datasets.

    Args:
      upload_id: upload UUID to list datasets for.
      type_filter: restrict to one dataset type. Case-insensitive. Common
        values: INTENSITY, NORMALISATION_AND_IMPUTATION, PAIRWISE, ANOVA,
        DOSE_RESPONSE, DOSE_RESPONSE_AGGREGATE, ENRICHMENT, IMPUTATION,
        DEMO.

    NOTE: NI pipeline output datasets are typed INTENSITY, same as the
    raw upload input. The type reflects data format, not which step
    produced it — this is correct, do not flag it.

    See also: find_initial_dataset, query_datasets, list_jobs.
    """
    datasets = get_client().datasets.list_by_upload(upload_id)
    if type_filter:
        datasets = [d for d in datasets if d.type == type_filter.upper()]
    if not datasets:
        return "No datasets found for this upload"
    lines = [f"Found {len(datasets)} dataset(s):"]
    for ds in datasets:
        lines.append(
            f"  ID: {ds.id} | Name: {ds.name} | Type: {ds.type} | State: {ds.state}"
        )
    return "\n".join(lines)
