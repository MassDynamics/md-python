"""Download a dataset table via a presigned URL (or to a local path)."""

import json
from typing import Any, Dict, Optional

import requests

from md_python.resources.v2.datasets import (
    REASON_DATASET_NOT_FOUND,
    REASON_TABLE_NAME_INVALID,
    DatasetNotFoundError,
    TableNotFoundError,
    classify_table_name,
)

from .. import mcp
from .._client import get_client

_VALID_FORMATS = ("csv", "parquet")
_STREAM_CHUNK = 1024 * 1024  # 1 MiB


def _preflight_error(dataset_id: str, table_name: str) -> Optional[Dict[str, Any]]:
    """Reject, before the HTTP call, a request that provably cannot succeed.

    Two provable causes, checked in this order:

      (a) the DATASET does not exist — deleted in the web UI (the MCP is never
          notified) or a stale id. Answered as ``dataset_not_found`` and
          WITHOUT any table-name talk: naming advice about a dead dataset is
          exactly what sent the model guessing.
      (b) the TABLE cannot exist for this dataset — a name outside the type's
          catalogue (``table_name_invalid``, with the case-mismatch hint that
          fixes the most common mistake), or a real name from another omics
          layer (``table_not_in_this_modality`` — Protein_Intensity on a
          metabolomics dataset).

    Uses the cheap, unverified catalogue lookup (verify=False): the download
    itself is the probe. Returns None when the download should proceed —
    including when the type is uncatalogued, where nothing can be proven, and
    when the lookup itself fails, so the real request produces the real error.
    """
    try:
        info = get_client().datasets.list_table_names(dataset_id, verify=False)
    except DatasetNotFoundError as e:
        return {
            "error": str(e),
            "reason": REASON_DATASET_NOT_FOUND,
            "dataset_id": dataset_id,
        }
    except Exception:
        return None

    rejection = classify_table_name(info, table_name)
    if rejection is None:
        return None

    error: Dict[str, Any] = dict(rejection)
    error["dataset_id"] = dataset_id
    error["table_name"] = table_name
    return error


@mcp.tool()
def download_dataset_table(
    dataset_id: str,
    table_name: str,
    format: str = "csv",
    output_path: Optional[str] = None,
) -> str:
    """Get a dataset table as a presigned URL, or download it to disk.

    Two modes:

    1) URL mode (default, output_path omitted) — returns a time-limited
       presigned S3 URL the caller can fetch directly. Use this when an
       agent wants to hand the URL to a user, embed in a notebook, or
       pass to another tool that accepts URLs. The URL is short-lived;
       re-call if it expires.

    2) Download mode (output_path provided) — streams the file to the
       given local path and returns its size. Use this only when the
       caller actually needs the bytes on disk. This tool never inlines
       file contents in the response — very large tables would blow up
       the MCP transport.

    NEVER guess ``table_name``. Call ``list_dataset_tables`` first — it
    confirms the dataset still exists and which tables it ACTUALLY has. If a
    request is rejected, do NOT retry with a different guess — read ``reason``
    and act on it; a wrong name is a 404 every time.

    Args:
        dataset_id: dataset UUID.
        table_name: CAPITALISED, entity-specific name of the table, exactly
            as data-set-service stores it. Names are CASE-SENSITIVE — a
            lowercase guess (e.g. "protein_intensity" for
            "Protein_Intensity") returns a 404. Call ``list_dataset_tables``
            to discover the valid names. Common tables by dataset type:
              INTENSITY (and NORMALISATION_AND_IMPUTATION):
                proteomics     -> "Protein_Intensity", "Protein_Metadata"
                transcriptomics-> "Gene_Intensity", "Gene_Metadata"
                metabolomics   -> "Metabolite_Intensity", "Metabolite_Metadata"
                (also "Peptide_*" / "PTM_*" when those layers are present)
              PAIRWISE: "output_comparisons", "runtime_metadata"
              DOSE_RESPONSE: "output_curves", "output_volcanoes",
                "input_drc", "runtime_metadata"
            Other types (e.g. ENRICHMENT, ANOVA) have NO catalogue: their
            table names cannot be enumerated and must not be guessed — see
            ``list_dataset_tables``.
        format: "csv" or "parquet". Parquet is smaller and faster for
            downstream pandas/polars reads; CSV is easier for quick
            inspection. Defaults to "csv".
        output_path: if set, the table is streamed to this local path
            instead of the URL being returned.

    Returns JSON:
        URL mode:
          {"download_url": "...", "expires_note": "presigned, time-limited",
           "dataset_id": "...", "table_name": "...", "format": "csv"}
        Download mode:
          {"path": "...", "bytes": 12345, "dataset_id": "...",
           "table_name": "...", "format": "csv"}

    Returns {"error": "...", "reason": "..."} on failure. ``reason`` is the
    machine-readable cause — branch on it, never on the prose:

      "dataset_not_found"          the DATASET is gone (deleted in the web UI —
                                   the MCP is not notified — or a stale id).
                                   Table names are irrelevant: re-discover the
                                   dataset with list_datasets / query_datasets
                                   / find_initial_dataset. Do NOT try other
                                   table names.
      "table_name_invalid"         the dataset exists; this name is not one of
                                   its tables. The error carries
                                   {"valid_tables": [...], "case_sensitive":
                                   true, "did_you_mean": "..."} — use one of
                                   those verbatim.
      "table_not_in_this_modality" the name is a real table name, but for an
                                   omics layer this dataset does not have (e.g.
                                   Protein_Intensity on a metabolomics
                                   dataset). "valid_tables" lists what this
                                   dataset does have. No spelling of the
                                   requested table exists here.

    The first two table-level causes are detected BEFORE the HTTP call when the
    dataset's type is catalogued. Errors with no ``reason`` are plain failures
    (invalid format, transport error, streaming error).
    """
    if format not in _VALID_FORMATS:
        return json.dumps(
            {
                "error": (
                    f"Invalid format '{format}'. "
                    f"Expected one of: {', '.join(_VALID_FORMATS)}."
                )
            }
        )

    preflight = _preflight_error(dataset_id, table_name)
    if preflight is not None:
        return json.dumps(preflight, indent=2)

    try:
        url = get_client().datasets.download_table_url(
            dataset_id, table_name, format=format
        )
    except DatasetNotFoundError as e:
        # The dataset died between the pre-flight and now, or the pre-flight
        # lookup failed. Either way this is NOT a table-name problem.
        return json.dumps(
            {
                "error": str(e),
                "reason": REASON_DATASET_NOT_FOUND,
                "dataset_id": dataset_id,
            },
            indent=2,
        )
    except TableNotFoundError as e:
        # Already actionable — lists the valid table names. Don't bury it
        # behind a generic "Failed to get download URL" prefix.
        return json.dumps(
            {
                "error": str(e),
                "reason": getattr(e, "reason", REASON_TABLE_NAME_INVALID),
                "dataset_id": dataset_id,
                "table_name": table_name,
            },
            indent=2,
        )
    except Exception as e:
        return json.dumps({"error": f"Failed to get download URL: {e}"})

    if output_path is None:
        return json.dumps(
            {
                "download_url": url,
                "expires_note": "presigned, time-limited",
                "dataset_id": dataset_id,
                "table_name": table_name,
                "format": format,
            },
            indent=2,
        )

    try:
        total = 0
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=_STREAM_CHUNK):
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)
    except Exception as e:
        return json.dumps({"error": f"Failed to download table: {e}"})

    return json.dumps(
        {
            "path": output_path,
            "bytes": total,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "format": format,
        },
        indent=2,
    )
