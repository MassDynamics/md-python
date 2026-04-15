import json
from typing import List

from . import mcp
from ._client import get_client


@mcp.tool()
def query_entities(keyword: str, dataset_ids: List[str]) -> str:
    """Search proteins / genes / peptides by keyword across one or more datasets.

    Returns: JSON. Shape:
      {"results": [ {...server-defined fields...}, ... ]}
    Field names are passed through verbatim from the server (typically
    "gene_name", "dataset_id", "protein_accession", etc.) — do NOT assume
    a fixed schema; parse defensively. Empty "results" is a valid negative
    answer, not an error. On transport / HTTP failure returns
    {"error": "<message>"}.

    Use this when: the user asks whether a specific gene or protein is
    present in an experiment, or wants to confirm an entity of interest
    before running a pipeline or interpreting a result.

    Do NOT use this when: you want to fetch a full dataset table (use
    download_dataset_table); when the dataset ids are not yet known
    (call find_initial_dataset or list_datasets first).

    Args:
      keyword: gene symbol, protein name, or UniProt accession. Minimum
        2 characters — shorter keywords fail server-side with a 400.
        Matching is case-insensitive substring. Examples: "BRCA1",
        "P12345", "EGFR".
      dataset_ids: list of dataset UUIDs to search. Use find_initial_dataset
        or list_datasets to obtain them.

    Guardrails: non-destructive. Safe to batch with other read-only tools.

    See also: find_initial_dataset, list_datasets, download_dataset_table,
      Workflow J (entity lookup).
    """
    try:
        result = get_client().entities.query(keyword=keyword, dataset_ids=dataset_ids)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
