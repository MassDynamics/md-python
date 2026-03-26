"""Create new uploads (single and from-CSV with background transfer)."""

import json
import os
from typing import List, Optional

from md_python.models.metadata import ExperimentDesign, SampleMetadata
from md_python.models.upload import Upload

from .. import mcp
from .._client import get_client
from ..files import load_metadata_from_csv
from ._executor import _LARGE_UPLOAD_THRESHOLD_BYTES, _get_executor
from .validate import validate_upload_inputs


@mcp.tool()
def create_upload(
    name: str,
    source: str,
    experiment_design: List[List[str]],
    sample_metadata: List[List[str]],
    s3_bucket: Optional[str] = None,
    s3_prefix: Optional[str] = None,
    filenames: Optional[List[str]] = None,
    file_location: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """Create a new upload and trigger the processing workflow.

    PREFER create_upload_from_csv — it loads, validates, and creates in one call.
    Use create_upload directly only when you already have experiment_design and
    sample_metadata arrays from a prior load_metadata_from_csv call.

    MANDATORY PREPARATION — follow these steps in order, every time:
      1. Call load_metadata_from_csv(file_path) on the user's metadata CSV.
         Use the experiment_design and sample_metadata arrays it returns directly.
      2. Call validate_upload_inputs(experiment_design, sample_metadata) to catch
         mismatches before submission.
      3. Only then call create_upload with the validated arrays.

    NEVER construct experiment_design or sample_metadata by hand from filenames,
    column lists, or your own inference — sample names must come verbatim from the
    file. Manual construction causes silent mismatches that make the upload fail.

    experiment_design: 2D array from load_metadata_from_csv["experiment_design"].
    sample_metadata:   2D array from load_metadata_from_csv["sample_metadata"].

    source — the proteomics software that produced the data files. Valid values:
      maxquant       — MaxQuant output (requires proteinGroups.txt + summary.txt)
      diann_tabular  — DIA-NN tabular report (requires report.tsv)
      diann_matrix   — DIA-NN matrix format (requires report.pg_matrix.tsv;
                        optionally report.pr_matrix.tsv for peptide-level)
      tims_diann     — timsTOF DIA-NN output (pg_matrix format, same as diann_matrix)
      spectronaut    — Spectronaut export (report.txt / .tsv / .csv)
      msfragger      — MSFragger/FragPipe output (combined_protein.tsv +
                        combined_modified_peptide.tsv + combined_ion.tsv)
      md_format      — pre-converted MD long-format TSV with columns:
                        ProteinGroupId, ProteinGroup, GeneNames, SampleName,
                        ProteinIntensity, Imputed
      md_format_gene — pre-converted MD gene-level TSV:
                        GeneId, SampleName, GeneExpression
      md_diann_maxlfq — DIA-NN output with MD's MaxLFQ implementation applied
      unknown        — flexible wide-format with a mapping.json descriptor

    NOTE: labelling_method (lfq vs tmt) is not yet exposed in this client —
    LFQ is assumed. Contact support for TMT upload support.

    For S3-backed uploads: provide s3_bucket, s3_prefix, and filenames.
    For local file uploads: provide file_location (directory path). filenames
    is auto-discovered from file_location if omitted (all files in the directory,
    sorted). Provide filenames explicitly to restrict which files are uploaded.

    Returns the new upload ID on success. Note: for local file uploads this call
    blocks while transferring files — for large proteomics files this may exceed
    the 60s MCP client timeout. Use create_upload_from_csv instead, which returns
    immediately and transfers files in the background.
    """
    if file_location and not filenames:
        filenames = sorted(
            f
            for f in os.listdir(file_location)
            if os.path.isfile(os.path.join(file_location, f))
        )
    upload = Upload(
        name=name,
        source=source,
        description=description,
        experiment_design=ExperimentDesign(data=experiment_design),
        sample_metadata=SampleMetadata(data=sample_metadata),
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        filenames=filenames,
        file_location=file_location,
    )
    upload_id = get_client().uploads.create(upload)
    return f"Upload created. ID: {upload_id}"


@mcp.tool()
def create_upload_from_csv(
    name: str,
    source: str,
    metadata_csv_path: str,
    file_location: str,
    filenames: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> str:
    """Create a new upload from a metadata CSV file, with background file transfer.

    This is the recommended tool for uploading local proteomics files. It:
      - Reads experiment_design and sample_metadata directly from a CSV file
        (no large array inlining — saves thousands of tokens per upload)
      - Auto-discovers filenames from file_location if not provided
      - Returns the upload ID immediately after the server accepts the record
      - Transfers data files to S3 in a background thread (does not block)
      - Fits within the 60-second MCP client timeout regardless of file size

    After calling this tool:
      1. Call wait_for_upload(upload_id) to monitor transfer + ingestion progress.
         Call again if it returns a non-terminal status (PENDING/PROCESSING).
      2. When COMPLETED, call find_initial_dataset(upload_id).

    metadata_csv_path: path to a combined or experiment-design CSV file.
      Must contain filename, sample_name, condition columns (plus any extras
      like dose, batch). Processed by load_metadata_from_csv internally.
      LFQ shortcut: if your CSV has sample_name + condition but no filename,
      add a filename column equal to sample_name first.

    file_location: directory containing the data files to upload.

    filenames: list of filenames to upload from file_location. If omitted, all
      files in file_location are auto-discovered (sorted). Provide explicitly
      to restrict which files are included.

    source — proteomics software that produced the data files. Valid values:
      maxquant, diann_tabular, diann_matrix, tims_diann, spectronaut,
      msfragger, md_format, md_format_gene, md_diann_maxlfq, unknown

    Returns the upload ID on success, or a validation/error message.
    """
    # Load and validate metadata from CSV
    metadata_result = json.loads(load_metadata_from_csv(metadata_csv_path))
    if "error" in metadata_result:
        return f"Error reading metadata CSV: {metadata_result['error']}"

    experiment_design = metadata_result.get("experiment_design")
    sample_metadata = metadata_result.get("sample_metadata")

    if experiment_design is None:
        return (
            "Error: could not extract experiment_design from the CSV. "
            "Ensure the file has filename, sample_name, and condition columns. "
            f"Notes: {metadata_result.get('notes', [])}"
        )
    if sample_metadata is None:
        return (
            "Error: could not extract sample_metadata from the CSV. "
            "Ensure the file has a sample_name column. "
            f"Notes: {metadata_result.get('notes', [])}"
        )

    # Validate
    validation = validate_upload_inputs(experiment_design, sample_metadata)
    if not validation.startswith("OK"):
        return f"Metadata validation failed:\n{validation}"

    # Auto-discover filenames
    if not filenames:
        if not os.path.isdir(file_location):
            return f"Error: file_location not found or not a directory: {file_location}"
        filenames = sorted(
            f
            for f in os.listdir(file_location)
            if os.path.isfile(os.path.join(file_location, f))
        )
        if not filenames:
            return f"Error: no files found in file_location: {file_location}"

    # Check total file size to decide whether to use the sequential executor.
    # Large transfers are routed through a single-threaded executor to prevent
    # concurrent multipart uploads from saturating uplink bandwidth and stalling.
    total_bytes = sum(
        os.path.getsize(os.path.join(file_location, f))
        for f in filenames
        if os.path.isfile(os.path.join(file_location, f))
    )
    use_sequential = total_bytes >= _LARGE_UPLOAD_THRESHOLD_BYTES

    upload = Upload(
        name=name,
        source=source,
        description=description,
        experiment_design=ExperimentDesign(data=experiment_design),
        sample_metadata=SampleMetadata(data=sample_metadata),
        filenames=filenames,
        file_location=file_location,
    )

    try:
        upload_id = get_client().uploads.create(
            upload,
            background=True,
            executor=_get_executor() if use_sequential else None,
        )
    except Exception as e:
        return f"Error creating upload: {e}"

    sample_count = metadata_result.get("sample_count", "?")
    transfer_note = (
        "Files queued for sequential background upload (large files — "
        "will start after any running transfer completes)."
        if use_sequential
        else "Files uploading in background (may take several minutes for large files)."
    )
    return (
        f"Upload record created. ID: {upload_id}\n"
        f"Samples: {sample_count} | Files: {len(filenames)} | Source: {source}\n"
        f"{transfer_note}\n"
        f"Next: call wait_for_upload(upload_id='{upload_id}') to monitor progress."
    )
