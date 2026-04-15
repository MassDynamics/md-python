import json
from typing import List

from . import mcp
from ._client import get_client


@mcp.tool()
def query_entities(keyword: str, dataset_ids: List[str]) -> str:
    """Search for proteins, genes, or peptides by keyword across one or more datasets.

    Use this to find which proteins/genes exist in an experiment before building
    comparisons or interpreting results.

    Args:
        keyword: Gene symbol, protein name, or UniProt ID (min 2 chars).
                 Examples: "BRCA1", "P12345", "EGFR".
        dataset_ids: List of dataset IDs to search across. Use find_initial_dataset
                     or list_datasets to obtain these.

    Returns JSON of the server response, which has a "results" key whose value is
    a list of matching entity records. Field names come straight from the server
    (e.g. gene_name, dataset_id) — do not assume a fixed schema.

    Returns {"error": "..."} on transport/HTTP errors.
    """
    try:
        result = get_client().entities.query(keyword=keyword, dataset_ids=dataset_ids)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
