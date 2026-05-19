"""Fetch a single dataset by id — including job_slug + job_run_params."""

import json

from .. import mcp
from .._client import get_client


@mcp.tool()
def get_dataset(dataset_id: str) -> str:
    """Fetch one dataset by ID, including the parameters it was run with.

    This is the ONLY MCP tool that returns ``job_slug`` and
    ``job_run_params`` — the dict that records *how* the dataset was
    produced (normalisation method, imputation method, condition
    comparisons, filter criteria, dose-response model, GSEA species,
    etc.). The other dataset tools (``list_datasets``,
    ``query_datasets``, ``find_initial_dataset``, ``list_jobs``) only
    surface summary fields (``id``, ``name``, ``type``, ``state``) and
    will NOT answer "what was this dataset run with?".

    Use this when the user asks:

      * "what parameters was this dataset run with?"
      * "was normalisation applied?" / "which imputation method?"
      * "what comparisons does this pairwise dataset cover?"
      * "audit this NI / ANOVA / dose-response run"
      * "what inputs fed into this dataset?" (via input_dataset_ids)
      * "when did this job start?" / "why did this dataset fail?"

    Wraps ``MDClient.datasets.get_by_id(dataset_id)`` (GET /datasets/:id).

    Args:
        dataset_id: dataset UUID.

    Returns JSON:
        {
          "id": "...",
          "name": "...",
          "type": "INTENSITY | PAIRWISE | ANOVA | DOSE_RESPONSE | ENRICHMENT | ...",
          "state": "COMPLETED | FAILED | ...",
          "job_slug": "normalisation_imputation | pairwise_comparison | ... | \"\"",
          "job_run_params": { ... },
          "input_dataset_ids": ["..."],
          "sample_names": [...] | null,
          "job_run_start_time": "ISO-8601 | null",
          "error_message": "... | null"
        }

    Returns ``{"error": "Dataset not found", "dataset_id": "..."}`` on
    404 and ``{"error": "<message>", "dataset_id": "..."}`` on any
    other HTTP failure.

    Note on ``job_slug``: the API often returns an empty string here
    (the slug lives on the upload-side pipeline record, not the
    dataset). The substantive answer is in ``job_run_params``.

    See also: list_datasets, query_datasets, find_initial_dataset.
    """
    try:
        ds = get_client().datasets.get_by_id(dataset_id)
    except Exception as e:
        return json.dumps({"error": str(e), "dataset_id": dataset_id})

    if ds is None:
        return json.dumps({"error": "Dataset not found", "dataset_id": dataset_id})

    payload = {
        "id": str(ds.id) if ds.id else None,
        "name": ds.name,
        "type": ds.type,
        "state": ds.state,
        "job_slug": ds.job_slug,
        "job_run_params": ds.job_run_params,
        "input_dataset_ids": [str(did) for did in ds.input_dataset_ids],
        "sample_names": ds.sample_names,
        "job_run_start_time": (
            ds.job_run_start_time.isoformat() if ds.job_run_start_time else None
        ),
        "error_message": ds.error_message,
    }
    return json.dumps(payload, indent=2, default=str)
