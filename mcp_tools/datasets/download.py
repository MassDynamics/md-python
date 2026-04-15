"""Download a dataset table via a presigned URL (or to a local path)."""

import json
from typing import Optional

import requests

from .. import mcp
from .._client import get_client

_VALID_FORMATS = ("csv", "parquet")
_STREAM_CHUNK = 1024 * 1024  # 1 MiB


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

    Args:
        dataset_id: dataset UUID.
        table_name: name of the table exposed by the dataset. Typical
            tables for different dataset types:
              INTENSITY: "protein_intensity", "peptide_intensity"
              NORMALISATION_AND_IMPUTATION: "protein_intensity"
              PAIRWISE: "protein_pairwise_comparison"
              ANOVA: "protein_anova"
              DOSE_RESPONSE: "dose_response"
            Check the dataset record or describe_pipeline output if you
            are not sure which tables exist for a given type.
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

    Returns {"error": "..."} on any failure (invalid format, HTTP error
    from the API, or a streaming error while writing the file).
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

    try:
        url = get_client().datasets.download_table_url(
            dataset_id, table_name, format=format
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
