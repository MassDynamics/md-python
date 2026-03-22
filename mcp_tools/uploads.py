import json
import os
from typing import List, Optional

from md_python.models.metadata import ExperimentDesign, SampleMetadata
from md_python.models.upload import Upload

from . import mcp
from ._client import get_client
from .files import load_metadata_from_csv


@mcp.tool()
def get_upload(
    upload_id: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Get an upload by ID or name.

    Provide either upload_id (UUID string) or name — not both.
    Returns upload details including status, source, and metadata.
    """
    if not upload_id and not name:
        return "Error: provide either upload_id or name"
    c = get_client()
    upload = (
        c.uploads.get_by_id(upload_id) if upload_id else c.uploads.get_by_name(name)
    )
    return str(upload) if upload else "Upload not found"


@mcp.tool()
def validate_upload_inputs(
    experiment_design: List[List[str]],
    sample_metadata: List[List[str]],
) -> str:
    """Validate experiment_design and sample_metadata before calling create_upload.

    Call this BEFORE create_upload to catch mismatches that would cause the upload to fail.

    What each table is:
      experiment_design — maps raw data files to biological samples.
        Required columns: filename (raw file name without extension),
        sample_name (unique sample label), condition (experimental group).
        One row per raw file. For LFQ data, filename usually equals sample_name.
      sample_metadata — per-sample experimental variables used by analysis pipelines.
        Required column: sample_name (must exactly match experiment_design).
        Optional columns: dose, batch, cellline, drug, or any covariate.

    Checks performed:
      - experiment_design has required columns: filename, sample_name, condition
      - sample_metadata has a sample_name column
      - Every sample_name in experiment_design appears in sample_metadata (exact match, case-sensitive)
      - Every sample_name in sample_metadata appears in experiment_design (no orphans)
      - No duplicate sample_names in either table

    Returns "OK: N samples validated" on success, or a detailed error message.
    """
    errors = []

    # Validate experiment_design structure
    if not experiment_design or len(experiment_design) < 2:
        return (
            "Error: experiment_design must have a header row and at least one data row"
        )

    ed_header = [h.strip().lower() for h in experiment_design[0]]
    synonyms = {"file": "filename", "sample": "sample_name", "group": "condition"}
    ed_header_norm = [synonyms.get(h, h) for h in ed_header]

    for col in ["filename", "sample_name", "condition"]:
        if col not in ed_header_norm:
            errors.append(
                f"experiment_design missing required column '{col}' "
                f"(got: {experiment_design[0]})"
            )

    if errors:
        return "\n".join(errors)

    sample_idx = ed_header_norm.index("sample_name")
    ed_samples = [
        row[sample_idx]
        for row in experiment_design[1:]
        if isinstance(row, list) and len(row) > sample_idx
    ]

    # Validate sample_metadata structure
    if not sample_metadata or len(sample_metadata) < 2:
        return "Error: sample_metadata must have a header row and at least one data row"

    sm_header = [h.strip().lower() for h in sample_metadata[0]]
    if "sample_name" not in sm_header:
        errors.append(
            f"sample_metadata must have a 'sample_name' column; got: {sample_metadata[0]}"
        )
        return "\n".join(errors)

    sm_sample_idx = sm_header.index("sample_name")
    sm_samples = [
        row[sm_sample_idx]
        for row in sample_metadata[1:]
        if isinstance(row, list) and len(row) > sm_sample_idx
    ]

    # Cross-check
    ed_set = set(ed_samples)
    sm_set = set(sm_samples)

    missing_from_sm = ed_set - sm_set
    if missing_from_sm:
        errors.append(
            f"sample_names in experiment_design but NOT in sample_metadata: {sorted(missing_from_sm)}"
        )

    missing_from_ed = sm_set - ed_set
    if missing_from_ed:
        errors.append(
            f"sample_names in sample_metadata but NOT in experiment_design: {sorted(missing_from_ed)}"
        )

    # Duplicate check
    ed_dupes = [s for s in ed_set if ed_samples.count(s) > 1]
    if ed_dupes:
        errors.append(
            f"Duplicate sample_names in experiment_design: {sorted(set(ed_dupes))}"
        )

    sm_dupes = [s for s in sm_set if sm_samples.count(s) > 1]
    if sm_dupes:
        errors.append(
            f"Duplicate sample_names in sample_metadata: {sorted(set(sm_dupes))}"
        )

    if errors:
        return "Validation failed:\n" + "\n".join(f"  - {e}" for e in errors)

    return f"OK: {len(ed_samples)} samples validated"


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
def update_sample_metadata(
    upload_id: str,
    sample_metadata: List[List[str]],
) -> str:
    """Update the sample metadata for an existing upload.

    ALWAYS obtain sample_metadata by calling load_metadata_from_csv on the user's
    CSV file. Never construct it manually — sample names must match exactly.

    sample_metadata: 2D array from load_metadata_from_csv["sample_metadata"].
    """
    ok = get_client().uploads.update_sample_metadata(
        upload_id, SampleMetadata(data=sample_metadata)
    )
    return (
        "Sample metadata updated successfully"
        if ok
        else "Failed to update sample metadata"
    )


@mcp.tool()
def wait_for_upload(
    upload_id: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check upload status, polling until a terminal state or the timeout is reached.

    IMPORTANT — MCP CLIENT TIMEOUT: The MCP client enforces a hard 60-second limit
    per tool call. This tool defaults to 45 seconds so it fits within that cap.
    If the upload is still processing when the timeout is reached, this tool returns
    the current status instead of raising an error. Simply call it again to continue
    monitoring. A typical upload may require several calls over a few minutes.

    Terminal states (stops polling):
      COMPLETED  — data ingested; call find_initial_dataset next.
      FAILED / ERROR — ingestion failed; check the returned message for details.
      CANCELLED  — upload was stopped.

    Non-terminal (call again):
      PROCESSING / PENDING — still in progress; call wait_for_upload again.

    For background file uploads started by create_upload_from_csv: the upload
    will initially show PENDING while files are transferring, then transition to
    PROCESSING once the server begins ingestion.
    """
    try:
        upload = get_client().uploads.wait_until_complete(
            upload_id, poll_s=poll_seconds, timeout_s=timeout_seconds
        )
        return str(upload)
    except TimeoutError:
        # Return current status — caller should call again to continue monitoring
        try:
            upload = get_client().uploads.get_by_id(upload_id)
            status = getattr(upload, "status", "UNKNOWN")
            return (
                f"Status: {status}. Upload not yet complete — "
                f"call wait_for_upload again to continue monitoring.\n{upload}"
            )
        except Exception as e:
            return f"Status unknown (could not fetch upload): {e}. Call wait_for_upload again."
    except Exception as e:
        return f"Upload {upload_id} failed: {e}"


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
    validation = _validate_arrays(experiment_design, sample_metadata)
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
        upload_id = get_client().uploads.create(upload, background=True)
    except Exception as e:
        return f"Error creating upload: {e}"

    sample_count = metadata_result.get("sample_count", "?")
    return (
        f"Upload record created. ID: {upload_id}\n"
        f"Samples: {sample_count} | Files: {len(filenames)} | Source: {source}\n"
        f"Files uploading in background (may take several minutes for large files).\n"
        f"Next: call wait_for_upload(upload_id='{upload_id}') to monitor progress."
    )


def _validate_arrays(
    experiment_design: List[List[str]], sample_metadata: List[List[str]]
) -> str:
    """Internal validation — delegates to validate_upload_inputs (same module)."""
    return validate_upload_inputs(experiment_design, sample_metadata)


@mcp.tool()
def list_uploads_status(upload_ids: List[str]) -> str:
    """Check the status of multiple uploads in a single call.

    Use this after submitting several create_upload_from_csv calls to monitor
    all uploads at once without making a separate get_upload call for each.

    Returns a compact JSON summary: {upload_id: {name, status, source}}.
    Individual fetch errors are recorded inline rather than failing the whole call.

    upload_ids: list of upload UUIDs to check.
    """
    c = get_client()
    results = {}
    for uid in upload_ids:
        try:
            upload = c.uploads.get_by_id(uid)
            results[uid] = {
                "name": getattr(upload, "name", None),
                "status": getattr(upload, "status", None),
                "source": getattr(upload, "source", None),
            }
        except Exception as e:
            results[uid] = {"error": str(e)}
    return json.dumps(results, indent=2)
