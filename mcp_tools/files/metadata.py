"""Metadata CSV tools: read_csv_preview and load_metadata_from_csv.

ENTITY-DATA BOUNDARY (enforced by all tools in this module):
- Only read METADATA files: experiment-design CSVs, sample-metadata CSVs.
- NEVER read files that contain protein/peptide/gene intensities or expression
  values. Those files are uploaded as-is; the API and md-converter process them.
- If you are unsure whether a file is a metadata file, use read_csv_preview
  first. If entity-data columns are detected, stop and ask the user for the
  correct metadata file instead.
"""

import json
import os
from typing import Dict, List, Optional, Set, Tuple

from .. import mcp
from ._io import _read_full, _read_header_only, _read_preview, _sniff_delimiter

# ──────────────────────────────────────────────────────────────────────────────
# Entity-data column detection — sourced from md-converter format readers
# ──────────────────────────────────────────────────────────────────────────────

# Exact lowercase column names that are always entity data
_ENTITY_EXACT: Set[str] = {
    # MD_Format / MD_Format long tables
    "proteinintensity",
    "peptideintensity",
    "normalisedintensity",
    "geneexpression",
    # DIA-NN tabular (report.tsv) & PASER
    "pg.maxlfq",
    "pg.quantity",
    "pg.normalised",
    "precursor.quantity",
    "precursor.normalised",
    "genes.quantity",
    "genes.normalised",
    "genes.maxlfq",
    # Spectronaut
    "pep.quantity",
    "eg.totalquantity (settings)",
    # Generic
    "intensity",
    "lfq intensity",
}

# Lowercase column prefixes that indicate entity data (MaxQuant dynamic columns)
_ENTITY_PREFIXES: Tuple[str, ...] = (
    "lfq intensity ",  # MaxQuant: "lfq intensity samplename"
    "intensity ",  # MaxQuant: "intensity samplename"
    "reporter intensity ",  # MaxQuant TMT: "reporter intensity 1 samplename"
    "reporter intensity corrected ",
)

# Format-specific structural columns that identify entity-data files.
# If these are present, the file is an entity-data file — stop immediately.
_FORMAT_FINGERPRINTS: List[Tuple[str, str, str]] = [
    # (column_to_detect_lowercase, format_name, what_to_ask_for_instead)
    (
        "majority protein ids",
        "MaxQuant proteinGroups.txt",
        "the experiment design TSV from the MaxQuant 'combined/txt/' folder",
    ),
    (
        "file.name",
        "DIA-NN report.tsv",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "protein.group",
        "DIA-NN matrix or PASER report",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "r.filename",
        "Spectronaut protein/peptide report",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "pg.grouplabel",
        "Spectronaut protein report",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "pep.groupingkey",
        "Spectronaut peptide report",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "proteinintensity",
        "MD_Format protein table",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "peptideintensity",
        "MD_Format peptide table",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "geneexpression",
        "MD_Format_Gene expression table",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "protein id",
        "MSFragger combined_protein.tsv",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "modified sequence",
        "MSFragger or DIA-NN peptide-level output",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
    (
        "modifiedsequence",
        "MD_Format peptide table",
        "the experiment design CSV (filename, sample_name, condition, ...)",
    ),
]

# ──────────────────────────────────────────────────────────────────────────────
# Experiment-design column synonyms
# ──────────────────────────────────────────────────────────────────────────────

_ED_SYNONYMS: Dict[str, str] = {
    "filename": "filename",
    "file": "filename",
    "file_name": "filename",
    "file name": "filename",
    "sample_name": "sample_name",
    "sample": "sample_name",
    "samplename": "sample_name",
    "condition": "condition",
    "group": "condition",
}

_ED_REQUIRED = {"filename", "sample_name", "condition"}


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _check_entity_data(header_stripped: List[str]) -> Optional[str]:
    """Return an error string if the file looks like entity data, else None."""
    header_lower = [h.strip().lower() for h in header_stripped]

    # Check format fingerprints first (most informative)
    for col, fmt, ask_for in _FORMAT_FINGERPRINTS:
        if col in header_lower:
            return (
                f"This looks like a {fmt} — an entity-data file. "
                "Do NOT read, summarise, or process its intensity/expression columns. "
                f"Ask the user for {ask_for} instead."
            )

    # Check exact entity-data column names
    for h in header_lower:
        if h in _ENTITY_EXACT:
            return (
                f"Column '{h}' contains entity data (intensities/expression). "
                "Do NOT read this file. Ask the user for their metadata CSV instead."
            )

    # Check entity-data column prefixes (MaxQuant dynamic names)
    for h in header_lower:
        for prefix in _ENTITY_PREFIXES:
            if h.startswith(prefix):
                return (
                    f"Column '{h}' looks like a MaxQuant intensity column. "
                    "Do NOT read this file. Ask the user for their experiment design TSV instead."
                )

    return None


def _normalise_header(header: List[str]) -> List[str]:
    return [_ED_SYNONYMS.get(h.strip().lower(), h.strip().lower()) for h in header]


def _safe_get(row: List[str], idx: int) -> str:
    """Return row[idx].strip(), or '' if idx is out of bounds."""
    return row[idx].strip() if idx < len(row) else ""


def _build_ed_rows(data_rows: List[List[str]], idx: Dict[str, int]) -> List[List[str]]:
    """Build experiment_design data rows from the column-index map.

    Returns one [filename, sample_name, condition] list per data row.
    """
    return [
        [
            _safe_get(row, idx["filename"]),
            _safe_get(row, idx["sample_name"]),
            _safe_get(row, idx["condition"]),
        ]
        for row in data_rows
    ]


def _sm_column_order(
    normalised: List[str], header_stripped: List[str]
) -> Tuple[List[int], List[str]]:
    """Return (col_indices, headers) for sample_metadata.

    Excludes the 'filename' column. Moves sample_name to position 0 if it
    isn't already there.
    """
    col_indices = [i for i, col in enumerate(normalised) if col != "filename"]
    headers = [header_stripped[i] for i in col_indices]

    sn_pos = next(
        (
            j
            for j, h in enumerate(headers)
            if h.strip().lower() in ("sample_name", "sample", "samplename")
        ),
        None,
    )
    if sn_pos is not None and sn_pos != 0:
        col_indices = [col_indices[sn_pos]] + [
            c for j, c in enumerate(col_indices) if j != sn_pos
        ]
        headers = [header_stripped[i] for i in col_indices]

    return col_indices, headers


def _deduplicate_rows_by_sample_name(
    data_rows: List[List[str]], sn_idx: int, col_indices: List[int]
) -> Tuple[List[List[str]], Set[str]]:
    """Deduplicate data rows by sample_name, keeping the first occurrence.

    Returns (deduplicated_rows, seen_sample_names).
    """
    seen: Set[str] = set()
    result: List[List[str]] = []
    for row in data_rows:
        sn = _safe_get(row, sn_idx)
        if sn and sn not in seen:
            seen.add(sn)
            result.append([_safe_get(row, i) for i in col_indices])
    return result, seen


def _collect_notes(
    has_ed: bool,
    normalised: List[str],
    header_stripped: List[str],
    experiment_design: Optional[List[List[str]]],
    sm_headers: List[str],
) -> List[str]:
    """Generate human-readable notes/warnings for the load_metadata_from_csv result."""
    notes: List[str] = []

    if not has_ed:
        has_condition = "condition" in normalised or "group" in [
            h.strip().lower() for h in header_stripped
        ]
        if has_condition:
            notes.append(
                "No 'filename' column detected — only sample_metadata was built. "
                "LFQ SHORTCUT: for LFQ data where each file = one sample, "
                "add a 'filename' column to your CSV with the same values as "
                "'sample_name', then re-run load_metadata_from_csv. "
                "This will generate both experiment_design and sample_metadata automatically."
            )
        else:
            notes.append(
                "No filename/condition columns detected — only sample_metadata was built. "
                "If you need an experiment_design, add 'filename' and 'condition' columns "
                "to this file and re-run load_metadata_from_csv."
            )

    if has_ed and experiment_design and len(sm_headers) == 1:
        notes.append(
            "sample_metadata only contains sample_name. "
            "Consider asking the user for additional experimental variables "
            "(dose, batch, cellline, drug, …) to add as columns."
        )

    if experiment_design:
        empty_conditions = sum(1 for row in experiment_design[1:] if not row[2])
        if empty_conditions:
            notes.append(
                f"{empty_conditions} row(s) have an empty condition value. "
                "Ask the user to provide the condition for each sample before calling create_upload."
            )

    notes.append(
        "Always run validate_upload_inputs before calling create_upload "
        "to confirm sample_name alignment between experiment_design and sample_metadata."
    )
    return notes


# ──────────────────────────────────────────────────────────────────────────────
# MCP tools
# ──────────────────────────────────────────────────────────────────────────────


@mcp.tool()
def read_csv_preview(
    file_path: str,
    max_rows: int = 5,
    delimiter: Optional[str] = None,
) -> str:
    """Show the column names and first few rows of a CSV or TSV file.

    Args:
        file_path: path to the CSV or TSV file.
        max_rows: number of data rows to return (default 5). Increase to see more context.
        delimiter: column separator (auto-detected from file extension if omitted).

    Use this to inspect a metadata file before loading it. Supported file types:
    experiment design CSVs (filename, sample_name, condition, ...), sample metadata
    CSVs (sample_name, dose, batch, ...), or combined LFQ metadata CSVs.

    ENTITY-DATA BOUNDARY: If this tool reports that the file looks like a proteomics
    output (DIA-NN report, MaxQuant proteinGroups, Spectronaut export, MD_Format
    table, etc.), stop immediately and ask the user for their metadata CSV instead.

    Reads only header + max_rows lines — never loads the full file.
    """
    if not os.path.exists(file_path):
        return f"Error: file not found: {file_path}"

    sep = delimiter or _sniff_delimiter(file_path)

    try:
        # Read header only first for a fast entity-data check
        header = _read_header_only(file_path, sep)
    except Exception as e:
        return f"Error reading file: {e}"

    if not header:
        return "Error: file appears empty"

    entity_err = _check_entity_data(header)
    if entity_err:
        return f"STOP — {entity_err}"

    # Entity check passed — read the preview rows (still bounded)
    _, preview_rows = _read_preview(file_path, sep, max_rows)

    lines = [
        f"File: {os.path.basename(file_path)}",
        f"Delimiter: {'tab' if sep == chr(9) else repr(sep)}",
        f"Columns ({len(header)}): {', '.join(header)}",
        "",
        f"First {len(preview_rows)} data row(s):",
    ]
    for i, row in enumerate(preview_rows):
        lines.append(f"  [{i + 1}] {', '.join(row)}")

    return "\n".join(lines)


@mcp.tool()
def load_metadata_from_csv(
    file_path: str,
    delimiter: Optional[str] = None,
) -> str:
    """Load experiment_design and/or sample_metadata from a CSV or TSV file.

    Args:
        file_path: path to the metadata CSV or TSV file.
        delimiter: column separator (auto-detected from file extension if omitted).

    WHAT EACH TABLE IS:

    experiment_design — maps raw data files to biological samples. Required by
      create_upload. Three required columns:
        filename    : raw data filename (without extension). For LFQ data where
                      each file = one sample, filename is usually the same as
                      sample_name. For TMT/fractionated experiments, multiple rows
                      can share a condition.
        sample_name : unique biological sample label — must match exactly across
                      all downstream tables (sample_metadata, pipeline params).
        condition   : experimental group (e.g. "treated", "control", "WT").

    sample_metadata — per-sample experimental variables used by analysis pipelines.
      Required column: sample_name. Additional columns are used by pipelines:
        dose        : numeric dose value (required for run_dose_response)
        condition   : group label (used by run_pairwise_comparison)
        batch       : batch covariate (can be added as control_variables in limma)
        Any other columns are preserved and available as covariates.

    LFQ SHORTCUT — for LFQ data where each file is a separate sample:
      The experiment_design can always be auto-derived from sample_metadata by
      treating sample_name as filename. If the user's CSV has sample_name and
      condition but no filename column, suggest they add a "filename" column
      equal to sample_name — this is the standard LFQ single-file setup.

    Handles three cases automatically:

    1. COMBINED file (LFQ single-file workflow — most common):
       Has filename + sample_name + condition PLUS extra columns
       (dose, batch, cellline, drug, …). Returns BOTH experiment_design and
       sample_metadata. sample_metadata is deduplicated by sample_name.
       Example: filename, sample_name, condition, dose, batch

    2. EXPERIMENT-DESIGN-ONLY file:
       Has filename, sample_name, condition but no extra columns.
       Returns experiment_design only (sample_metadata is null).

    3. SAMPLE-METADATA-ONLY file:
       Has sample_name and extra columns but no filename/condition columns.
       Returns sample_metadata only (experiment_design is null).
       → If condition is present, you can derive experiment_design via the LFQ
         shortcut (add filename = sample_name column to the file and re-run).

    Column synonyms accepted:
      filename    → filename (also: file, file_name)
      sample_name → sample_name (also: sample, samplename)
      condition   → condition (also: group)

    ENTITY-DATA BOUNDARY: Only use this on metadata/design CSV files.
    Never point it at proteomics data files — DIA-NN reports, MaxQuant
    proteinGroups.txt, Spectronaut exports, MSFragger combined_protein.tsv,
    MD_Format protein/peptide tables, or any file containing intensity,
    expression, or quantification columns. Those files are uploaded directly;
    the API (via md-converter) extracts all measurement data from them.

    Returns JSON with:
    - experiment_design: 2D array — pass directly to create_upload (or null)
    - sample_metadata:   2D array — pass directly to create_upload, run_dose_response,
                         run_pairwise_comparison, generate_pairwise_comparisons (or null)
    - sample_count:      number of unique samples detected
    - columns_found:     all column names from the file
    - notes:             warnings or recommendations

    Always pass these arrays verbatim to downstream tools. Never re-construct,
    filter, or modify them — any manual editing risks sample name mismatches.
    """
    if not os.path.exists(file_path):
        return json.dumps({"error": f"File not found: {file_path}"})

    sep = delimiter or _sniff_delimiter(file_path)

    try:
        # Read header only first — cheap entity-data check before loading the file
        header = _read_header_only(file_path, sep)
    except Exception as e:
        return json.dumps({"error": f"Could not read file: {e}"})

    if not header:
        return json.dumps({"error": "File appears empty"})

    entity_err = _check_entity_data(header)
    if entity_err:
        return json.dumps({"error": f"STOP — {entity_err}"})

    # Entity check passed — this is a metadata CSV, safe to read in full
    try:
        header, data_rows = _read_full(file_path, sep)
    except Exception as e:
        return json.dumps({"error": f"Could not read file: {e}"})

    # Strip and filter blank rows
    header_stripped = [h.strip() for h in header]
    data_rows = [r for r in data_rows if any(c.strip() for c in r)]

    if not data_rows:
        return json.dumps({"error": "File has a header but no data rows"})

    normalised = _normalise_header(header_stripped)

    has_sm_col = "sample_name" in normalised
    if not has_sm_col:
        return json.dumps(
            {
                "error": (
                    "No 'sample_name' column found (also tried synonyms: sample, samplename). "
                    f"Columns found: {header_stripped}"
                )
            }
        )

    # Build a name→index map (first occurrence wins)
    idx: Dict[str, int] = {}
    for i, col in enumerate(normalised):
        if col not in idx:
            idx[col] = i

    has_ed = _ED_REQUIRED.issubset(set(normalised))

    # ── experiment_design ────────────────────────────────────────────────────
    experiment_design: Optional[List[List[str]]] = None
    if has_ed:
        experiment_design = [["filename", "sample_name", "condition"]] + _build_ed_rows(
            data_rows, idx
        )

    # ── sample_metadata ──────────────────────────────────────────────────────
    # Include all columns except 'filename'; sample_name first; deduplicate.
    sm_col_indices, sm_headers = _sm_column_order(normalised, header_stripped)
    sm_rows, seen = _deduplicate_rows_by_sample_name(
        data_rows, idx["sample_name"], sm_col_indices
    )
    sample_metadata: Optional[List[List[str]]] = (
        [sm_headers] + sm_rows if sm_rows else None
    )

    # ── notes ────────────────────────────────────────────────────────────────
    notes = _collect_notes(
        has_ed, normalised, header_stripped, experiment_design, sm_headers
    )

    return json.dumps(
        {
            "experiment_design": experiment_design,
            "sample_metadata": sample_metadata,
            "sample_count": len(seen),
            "columns_found": header_stripped,
            "notes": notes,
        },
        indent=2,
    )
