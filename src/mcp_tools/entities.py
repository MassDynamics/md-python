import json
from typing import List

from . import mcp
from ._client import get_client


@mcp.tool()
def map_protein_to_protein(dataset_ids: List[str], entity_ids: List[str]) -> str:
    """Return a graph of protein groups linked through shared individual proteins.

    Returns: JSON. Shape:
      {"nodes": [ {...server-defined fields...}, ... ],
       "edges": [ {...server-defined fields...}, ... ]}
    Field names are passed through verbatim from the server — do NOT assume
    a fixed schema; parse defensively. An empty graph (``nodes`` and ``edges``
    both empty) is a valid negative answer, not an error. On transport / HTTP
    failure returns {"error": "<message>"}.

    Use this when: the user wants to know which other protein groups share
    individual proteins with a given protein group across one or more datasets
    — useful for shared-peptide ambiguity inspection, isoform-family discovery,
    or cross-experiment protein-group provenance.

    Do NOT use this when: the user wants a free-text keyword search (use
    query_entities); when the user has not yet identified the protein groups
    to query (use query_entities to resolve a keyword to entity_ids first);
    when the user wants the underlying intensities (use download_dataset_table).

    Args:
      dataset_ids: list of dataset UUIDs to scope the graph to. 1–500 entries
        (workflow/app/api/api/v2/entities/map/protein_to_protein.rb:23
        length: { min: 1, max: 500 }, uuid_array: true). Use
        find_initial_dataset / list_datasets / query_datasets to obtain them.
        The endpoint returns 400 if any of the supplied dataset_ids is not a
        valid UUID, and 403 if the caller does not have read access.
      entity_ids: protein-group IDs to query. Minimum 1 entry
        (workflow/app/api/api/v2/entities/map/protein_to_protein.rb:28
        length: { min: 1 }). The service returns nodes and edges reachable
        from these groups through their individual proteins. Resolve free-text
        keywords (gene symbols, UniProt accessions) to protein-group IDs via
        query_entities first.

    Guardrails: non-destructive. Safe to batch with other read-only tools.

    See also: query_entities, find_initial_dataset, download_dataset_table.
    """
    try:
        result = get_client().entities.mappings.protein_to_protein(
            dataset_ids=dataset_ids, entity_ids=entity_ids
        )
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


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
        2 characters, maximum 1024 characters
        (workflow/app/api/api/v2/entities/query.rb:18 length: { min: 2,
        max: 1024 }) — shorter keywords fail server-side with a 400.
        Matching is case-insensitive substring. Examples: "BRCA1",
        "P12345", "EGFR".
      dataset_ids: list of dataset UUIDs to search. 1–500 entries
        (workflow/app/api/api/v2/entities/query.rb:23 length: { min: 1,
        max: 500 }). Use find_initial_dataset / find_initial_datasets /
        list_datasets / query_datasets to obtain them. The endpoint returns
        404 if any of the supplied dataset_ids is unknown — split your batch
        if you cannot guarantee every id is valid.

    Guardrails: non-destructive. Safe to batch with other read-only tools.

    See also: find_initial_dataset, list_datasets, download_dataset_table,
      Workflow J (entity lookup).
    """
    try:
        result = get_client().entities.query(keyword=keyword, dataset_ids=dataset_ids)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
