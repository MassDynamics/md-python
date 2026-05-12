"""Find initial INTENSITY datasets for uploads."""

import json
from typing import List

from .. import mcp
from .._client import get_client


@mcp.tool()
def find_initial_datasets(upload_ids: List[str]) -> str:
    """Find the upload-created INTENSITY dataset for many uploads in one call.

    Bulk variant of find_initial_dataset. Same disambiguation rule applies per
    upload_id: when an upload has multiple INTENSITY datasets (one from the
    raw upload, plus one per NI / filter-only run), the unique INTENSITY
    dataset with no upstream input_dataset_ids is selected
    (md_python.resources.v2.datasets.Datasets.find_initial_dataset:226-262).

    Use this when: you have many upload_ids and need their input dataset IDs
    before kicking off pipelines.

    Do NOT use this when: you have a single upload_id (call find_initial_dataset
    instead — same logic, leaner output) or when the user already gave you
    a dataset_id explicitly.

    Returns JSON: {upload_id: {"dataset_id": "..."} | {"error": "..."}}
    Errors are recorded inline — all upload_ids are checked regardless of
    individual failures, so a single bad id never aborts the batch.

    See also: find_initial_dataset, list_datasets, query_datasets, Workflow E.
    """
    c = get_client()
    results: dict = {}
    for uid in upload_ids:
        try:
            ds = c.datasets.find_initial_dataset(uid)
            if ds:
                results[uid] = {"dataset_id": str(ds.id)}
            else:
                results[uid] = {"error": "No initial dataset found"}
        except Exception as e:
            results[uid] = {"error": str(e)}
    return json.dumps(results, indent=2)


@mcp.tool()
def find_initial_dataset(upload_id: str) -> str:
    """Find the upload-created INTENSITY dataset for an upload.

    Call this after wait_for_upload returns COMPLETED. The dataset id returned
    here is what you pass as input_dataset_ids to every run_* pipeline tool.

    Disambiguation (when an upload has multiple INTENSITY datasets — common
    once an NI or filter-only run has executed): the unique INTENSITY dataset
    whose ``input_dataset_ids`` is empty is the upload-created one. NI/
    filter-only outputs are also typed INTENSITY but always carry a non-empty
    ``input_dataset_ids`` pointing back to the original upload-created
    dataset (md_python.resources.v2.datasets.Datasets.find_initial_dataset:
    226-262). When the disambiguation cannot pick a single record, the
    underlying resource raises ValueError; this tool surfaces that as
    ``Error: <message>`` and lists the candidate ids.

    Use this when: a single upload_id needs its input dataset id resolved.

    Do NOT use this when: looking up many uploads — call find_initial_datasets
    (one round-trip). Do NOT use it when the user already gave you the dataset
    id explicitly.

    Returns: prose. ``Initial dataset found.\\nID: <uuid>\\n<Dataset repr>`` on
    success, or ``Error: <message>`` on disambiguation failure / 404.

    NOTE: Both the raw upload input dataset AND the NI pipeline output dataset
    are typed INTENSITY in this API. This is correct by design — the type
    reflects the data format, not which pipeline step produced it. Do not flag
    INTENSITY on NI output as unexpected or attempt to correct it.

    See also: find_initial_datasets, list_datasets, query_datasets.
    """
    try:
        ds = get_client().datasets.find_initial_dataset(upload_id)
    except ValueError as e:
        return f"Error: {e}"
    return f"Initial dataset found.\nID: {ds.id}\n{ds}"
