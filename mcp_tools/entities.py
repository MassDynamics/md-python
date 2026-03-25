import json
from typing import List

from . import mcp
from ._client import get_client


@mcp.tool()
def search_entities(keyword: str, dataset_ids: List[str]) -> str:
    """Search for proteins, genes, or peptides by keyword across one or more datasets.

    Use this to find which proteins/genes exist in an experiment before building
    comparisons or interpreting results.

    Args:
        keyword: Gene symbol, protein name, or UniProt ID (min 2 chars).
                 Examples: "BRCA1", "P12345", "EGFR".
        dataset_ids: List of dataset IDs to search across. Use find_initial_dataset
                     or list_datasets to obtain these. Accepts 1–500 IDs.

    Returns JSON list of results grouped by dataset:
      [
        {
          "dataset_id": "...",
          "entity_type": "protein|gene|peptide",
          "items": [
            {
              "ProteinIds": ["P12345"],
              "GeneNames": ["BRCA1"],
              "Description": "Breast cancer type 1 susceptibility protein",
              "GroupId": "1"
            }
          ]
        }
      ]

    Returns {"error": "..."} if:
      - keyword is shorter than 2 characters (400)
      - a dataset ID is not found (404)
      - entity search is not enabled on your account (403) — contact support
      - the upstream search service is unavailable (502)
    """
    try:
        results = get_client().entities.search(keyword=keyword, dataset_ids=dataset_ids)
        return json.dumps(results, indent=2)
    except PermissionError as e:
        return json.dumps({"error": str(e), "code": 403})
    except ValueError as e:
        return json.dumps({"error": str(e), "code": 400})
    except Exception as e:
        return json.dumps({"error": str(e)})
