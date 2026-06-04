"""Create new uploads (single and from-CSV with background transfer)."""

import json
import os
from typing import List, Optional

from md_python.models.metadata import ExperimentDesign, SampleMetadata
from md_python.models.upload import Upload

from .. import mcp
from .._client import get_client
from ..files import load_metadata_from_csv
from ..files._io import _read_header_only, _sniff_delimiter
from ._executor import _LARGE_UPLOAD_THRESHOLD_BYTES, _get_executor
from .validate import validate_upload_inputs

# Tabular extensions md-converter's md_format reader will inspect.
_MD_FORMAT_TABULAR_EXTS = (".tsv", ".txt", ".csv")


def _check_md_format_composition(
    source: Optional[str],
    file_location: Optional[str],
    filenames: Optional[List[str]],
) -> Optional[str]:
    """Reject a peptide-only md_format upload.

    md_format PTM uploads pair a REQUIRED protein table with an OPTIONAL
    peptide table. md-converter always builds the protein output first and
    raises FileNotFoundError if it is absent (md_format/runner.py:27,38,
    reader.py:46-48), so a peptide-only upload fails ingestion. Catch it here
    with a clear message instead. (The get_md_format_spec("peptide") docs
    state the dual-file rule; this is the code-level guard.)

    Bounded header read only (one row per file via _read_header_only) — stays
    inside the ENTITY-DATA BOUNDARY. Returns an error string when the upload is
    peptide-only, or None when it is fine / not applicable (non-md_format
    source, S3-backed upload with no local files, or unreadable headers).
    """
    if source != "md_format" or not file_location or not filenames:
        return None

    peptide_files: List[str] = []
    protein_files: List[str] = []
    for fn in filenames:
        path = os.path.join(file_location, fn)
        if not os.path.isfile(path):
            continue
        if os.path.splitext(fn)[1].lower() not in _MD_FORMAT_TABULAR_EXTS:
            continue
        try:
            header = {
                h.strip() for h in _read_header_only(path, _sniff_delimiter(path))
            }
        except Exception:
            continue  # unreadable header — let the server be the judge
        is_peptide = "ModifiedSequence" in header or "StrippedSequence" in header
        if is_peptide:
            peptide_files.append(fn)
        elif "ProteinIntensity" in header:
            protein_files.append(fn)

    if peptide_files and not protein_files:
        return (
            "Error: peptide-only md_format upload rejected. "
            f"The peptide-level file(s) {peptide_files} have no companion "
            "protein-level md_format file (one with a ProteinIntensity column). "
            "For an md_format PTM/phospho experiment the PROTEIN table is the "
            "REQUIRED base and the peptide table is an OPTIONAL companion — both "
            "must be uploaded together via filenames=[protein_file, peptide_file]. "
            "md-converter builds the protein output first and will fail with "
            "FileNotFoundError('Protein data file not found') on a peptide-only "
            "upload. Add the protein md_format table and retry."
        )
    return None


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
    """Create a Mass Dynamics upload record and trigger processing.

    Returns: prose. Exact string "Upload created. ID: <uuid>".

    Use this when you already have experiment_design and sample_metadata
    arrays in memory from a prior load_metadata_from_csv call, OR when you
    are creating an S3-backed upload (s3_bucket / s3_prefix) where there is
    no local file transfer.

    Do NOT use this for local file uploads of any realistic size. This call
    blocks while transferring files and will exceed the 60s MCP client
    timeout for anything larger than ~30 MB. Use create_upload_from_csv
    instead — it reads, validates, creates, and backgrounds the transfer.

    MANDATORY PREPARATION — follow these steps in order, every time:
      1. load_metadata_from_csv(file_path) on the user's metadata CSV.
         Use the experiment_design and sample_metadata arrays verbatim.
      2. validate_upload_inputs(experiment_design, sample_metadata).
      3. Only then call create_upload.
    NEVER hand-construct experiment_design or sample_metadata — sample names
    must match the source files exactly.

    MD_FORMAT ID-SHAPE PREFLIGHT (md_format / md_format_metabolite only) —
    a BOUNDED, REQUIRED exception to "never read proteomics data files":
    before an md_format* upload, read the HEADER + first ~5 data rows of each
    data file and confirm the primary-ID columns have the expected shape —
    peptide ModifiedSequence is INLINE UniMod (PEPT(UniMod:21)IDE), NOT a tool's
    native annotation like PD's "[K].PEPT.[V] | 1xPhospho [T4]"; protein
    ProteinGroup is UniProt accession(s); SampleName matches experiment_design.
    This is an ID-shape check on a few lines, distinct from ingesting the full
    table (read_csv_preview stays within this boundary). If the IDs look like
    another tool's native output, STOP — name the tool the format resembles and
    what is wrong — instead of uploading malformed data. (A peptide-only
    md_format upload is additionally rejected by this client; see the md_format
    source note below.)

    Args (required):
      name: human-readable experiment name. Must be unique within the
        organisation (enforced at workflow/app/models/experiment.rb:62-66).
      source: proteomics source format. EXACTLY one of — every other value
        is rejected server-side at experiment.rb:68-72 AND refused by the
        client guard in src/md_python/resources/v2/uploads.py::
        ALLOWED_UPLOAD_SOURCES:

          "maxquant"        MaxQuant LFQ or TMT. Requires proteinGroups.txt
                            and summary.txt in file_location
                            (md-converter readers/maxquant/reader.py:19,43).
          "diann_tabular"   DIA-NN matrix export. Requires
                            report.pg_matrix.tsv (+ optional
                            report.pr_matrix.tsv) in file_location.
                            Routed to mdconverter.diann_matrix at
                            experiment_runner.py:16-24.
          "tims_diann"      DIA-NN long-format / timsTOF / PASER. Requires
                            report.tsv plus the DIA-NN log file. Version-
                            detected and routed to mdconverter.diann or
                            mdconverter.paser at experiment_runner.py:26-66.
          "spectronaut"     Spectronaut single report file (.txt/.tsv/.csv)
                            with columns R.FileName, PG.GroupLabel,
                            PG.ProteinGroups, PG.ProteinAccessions,
                            PG.Quantity (readers/spectronaut/reader.py:576).
          "md_format"       MD long-format TSV. PROTEIN-level columns:
                            ProteinGroupId, ProteinGroup, GeneNames, SampleName,
                            ProteinIntensity, Imputed
                            (readers/md_format/reader.py:288). Every row
                            with ProteinIntensity=0 MUST have Imputed=1.
                            PEPTIDE-level (PTM) is a DUAL-FILE upload under this
                            same source: pass BOTH a peptide file AND a protein
                            file in filenames=. A peptide file alone fails with
                            "Protein data file not found" (reader.py:47).
                            Peptide columns: ModifiedSequence, StrippedSequence,
                            Unique, ProteinGroup, ProteinGroupId, GeneNames,
                            SampleName, PeptideIntensity, Imputed (optional:
                            OtherProteinGroupIds, ProteinNames, Description).
                            Unique is a REQUIRED boolean. ProteinGroupId and
                            ProteinGroup MUST use the IDENTICAL mapping in both
                            files (do NOT factorize the two files independently).
                            Call get_md_format_spec("peptide") for the full
                            schema + a dual-file conversion template.
          "md_format_gene"  MD gene-level TSV with columns GeneId,
                            GeneExpression, SampleName
                            (readers/md_format_gene/reader.py:8).
          "md_format_metabolite"
                            MD metabolite-level TSV with columns
                            MetaboliteId, MetaboliteIntensity, SampleName,
                            Imputed (readers/md_format_metabolite/reader.py:8).
                            Imputed is REQUIRED and validated 0/1 — it is
                            NOT auto-derived (unlike md_format_gene).

      ALL md_format* sources are LONG format and MUST be a FULL matrix:
      exactly one row per entity per sample, with EVERY entity x sample
      combination present — NO EXCEPTIONS. md-converter rejects an
      incomplete matrix. A non-measurement is a row with intensity 0.0 and
      Imputed=1, never an absent row. Use plan_wide_to_md_format to generate
      a conversion script that produces this shape by construction.

      experiment_design: 2D array with header row [filename, sample_name,
        condition] (order irrelevant; synonyms normalised client-side).
        Must come from load_metadata_from_csv["experiment_design"].
      sample_metadata: 2D array with header row including "sample_name".
        Every sample_name in experiment_design must also appear here
        (server-side validation at experiment.rb:107-121). Must come
        from load_metadata_from_csv["sample_metadata"].

    Args (optional):
      s3_bucket: S3 bucket for S3-backed uploads. Mutually exclusive with
        file_location.
      s3_prefix: S3 prefix inside s3_bucket.
      filenames: list of filenames. For local uploads auto-discovered from
        file_location if omitted; for S3 uploads always required.
      file_location: local directory containing the data files. Mutually
        exclusive with s3_bucket / s3_prefix.
      description: free-form description.

    Errors:
      - ValueError "source=... is not a supported upload format" — source
        not in the allow-list above.
      - ValueError "Either file_location or s3_bucket must be provided".
      - ValueError "filenames must be provided when using file_location".
      - APIError 422 from the server for design/metadata mismatches or
        duplicate names.

    Guardrails:
      - Labelling is assumed LFQ. TMT uploads are not yet exposed — contact
        support.
      - Never construct metadata arrays by hand.
      - Echo the returned upload_id back to the user before any follow-up.

    See also: create_upload_from_csv (preferred for local files),
      wait_for_upload, find_initial_dataset.
    """
    if file_location and not filenames:
        filenames = sorted(
            f
            for f in os.listdir(file_location)
            if os.path.isfile(os.path.join(file_location, f))
        )
    composition_error = _check_md_format_composition(source, file_location, filenames)
    if composition_error:
        return composition_error
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

    source — proteomics software that produced the data files. EXACTLY one
      of (any other value is rejected client-side and server-side):
        maxquant, diann_tabular, tims_diann, spectronaut,
        md_format, md_format_gene, md_format_metabolite
      Prefer diann_tabular for DIA-NN pg_matrix (+ optional pr_matrix)
      uploads; use tims_diann for report.tsv + DIA-NN log. Full per-format
      file expectations live in create_upload's docstring.

    md_format peptide is a DUAL-FILE upload (protein + peptide companion in
    filenames=); a peptide-only md_format upload is rejected before transfer.
    Run the md_format ID-SHAPE PREFLIGHT first (see create_upload's docstring).

    Returns: prose. Starts with "Upload record created. ID: <uuid>" on
    success, or "Error: ..." / "Metadata validation failed: ..." on
    failure. Branch on the "ID:" sentinel to extract the id.
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

    # Reject a peptide-only md_format upload before transferring anything.
    composition_error = _check_md_format_composition(source, file_location, filenames)
    if composition_error:
        return composition_error

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
