"""Discover — and verify — the tables a dataset actually has."""

import json
from typing import Optional

from md_python.resources.v2.datasets import (
    REASON_DATASET_NOT_FOUND,
    DatasetNotFoundError,
)

from .. import mcp
from .._client import get_client


@mcp.tool()
def list_dataset_tables(
    dataset_id: str,
    verify: bool = True,
    upload_id: Optional[str] = None,
) -> str:
    """Check which tables a dataset ACTUALLY has, before downloading them.

    Call this FIRST, always. It answers "is the data there?" — it does not
    just list names. Three things it settles that a raw 404 cannot:

      1. Does the DATASET still exist? Datasets can be deleted in the web UI
         and this MCP is never notified, so an id that worked earlier in the
         session can be dead. A dead id returns
         ``{"error": ..., "reason": "dataset_not_found"}`` — re-discover it
         with list_datasets / query_datasets / find_initial_dataset. Do NOT
         try table names against it.
      2. Which omics modality is it? An INTENSITY dataset holds ONLY its own
         entity's tables. A metabolomics dataset has no Protein_Intensity —
         asking for one is a guaranteed 404. The modality is resolved from
         the dataset's entity_type (or the upload's source), and the
         candidates are narrowed to it (``entity``, ``entity_resolved_from``).
      3. Do those tables really resolve? With ``verify=True`` (default) every
         candidate is probed (a presigned-URL resolve, no file transfer) and
         the answer is split into what IS there and what is NOT.

    data-set-service stores tables under CAPITALISED, entity-specific names
    ("Protein_Intensity", "Gene_Metadata") and has NO list-tables endpoint;
    names are CASE-SENSITIVE and a lowercase guess 404s.

    Catalogued types:
      INTENSITY (incl. normalisation/imputation output):
        protein -> "Protein_Intensity", "Protein_Metadata"
        peptide -> "Peptide_*"; gene -> "Gene_*";
        metabolite -> "Metabolite_*"; ptm -> "PTM_*"
      PAIRWISE:      "output_comparisons", "runtime_metadata"
      DOSE_RESPONSE: "output_curves", "output_volcanoes", "input_drc",
                     "runtime_metadata"
      ENRICHMENT (run_gsea / run_ora):
                     "output_comparisons"  <- the GSEA RESULTS table. Yes,
                     the SAME name PAIRWISE uses — that is correct, not a
                     mix-up, and "output_gsea"/"output_enrichment"/
                     "output_pathways" do not exist.
                     "database_metadata"   <- gene-set / database metadata
                     "runtime_metadata"
      ORA:           "ora_results", "runtime_metadata"
      ANOVA:         "anova_results", "runtime_metadata"

    A type outside that set returns ``"catalogued": false``: its names cannot
    be enumerated, so there is nothing to verify and nothing to guess. DO NOT
    brute-force names — ask the user for the exact name or use the dataset's
    visualisation module.

    Args:
        dataset_id: dataset UUID.
        verify: probe each candidate for real existence (default true). Costs
            one cheap request per candidate — usually 2 once the modality is
            resolved, at most ~12 when it is not. Pass false when you only
            want the candidate names fast and will accept a possible 404.
        upload_id: parent upload. Optional; used only to resolve the omics
            modality when the dataset itself does not record it.

    Returns JSON (catalogued type, verify=true):
        {
          "dataset_id": "...", "type": "INTENSITY",
          "catalogued": true, "verified": true,
          "entity": "metabolite" | null,          # null = modality unresolved
          "entity_resolved_from": "job_run_params" | "upload_source" | null,
          "candidates": ["Metabolite_Intensity", "Metabolite_Metadata"],
          "tables": ["Metabolite_Intensity"],     # CONFIRMED PRESENT
          "unavailable": ["Metabolite_Metadata"], # probed, absent
          "indeterminate": [{"table": "...", "error": "..."}],  # if any
          "note": "..."
        }

    ``tables`` is the ONLY key that means "this table exists — download it".
    ``candidates`` are unconfirmed names. With ``verify=false`` there is no
    ``tables`` key at all, only ``candidates``. ``unavailable`` was probed and
    is absent. ``indeterminate`` failed for a reason other than a 404
    (network/5xx/auth) — existence UNKNOWN, retry, do not conclude it is
    missing. When ``entity_resolved_from`` is null the modality could not be
    determined and ``candidates`` spans every entity — most of them will not
    exist; verify or pass upload_id.

    Errors: {"error": "...", "reason": "dataset_not_found", "dataset_id":
    "..."} when the dataset does not resolve, {"error": "...", "dataset_id":
    "..."} otherwise.
    """
    try:
        result = get_client().datasets.list_table_names(
            dataset_id, verify=verify, upload_id=upload_id
        )
    except DatasetNotFoundError as e:
        return json.dumps(
            {
                "error": str(e),
                "reason": REASON_DATASET_NOT_FOUND,
                "dataset_id": dataset_id,
            },
            indent=2,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "dataset_id": dataset_id})
    return json.dumps(result, indent=2)
