"""
File-reading tools for extracting upload metadata from local CSV/TSV files.

ENTITY-DATA BOUNDARY (enforced by all tools in this module):
- Only read METADATA files: experiment-design CSVs, sample-metadata CSVs.
- NEVER read files that contain protein/peptide/gene intensities or expression
  values. Those files are uploaded as-is; the API and md-converter process them.
- If you are unsure whether a file is a metadata file, use read_csv_preview
  first. If entity-data columns are detected, stop and ask the user for the
  correct metadata file instead.
"""

import csv
import json
import os
from typing import Dict, List, Optional, Set, Tuple

from . import mcp

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


def _sniff_delimiter(file_path: str, sample_bytes: int = 8192) -> str:
    """Detect delimiter using csv.Sniffer on the first sample_bytes of the file.

    Accepts tab, comma, semicolon, or pipe. Falls back to extension heuristic
    (.tsv/.txt → tab, everything else → comma) if Sniffer fails or returns an
    unexpected character.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            sample = f.read(sample_bytes)
        dialect = csv.Sniffer().sniff(sample, delimiters="\t,;|")
        if dialect.delimiter in ("\t", ",", ";", "|"):
            return dialect.delimiter
    except csv.Error:
        pass
    # Extension fallback
    return "\t" if os.path.splitext(file_path)[1].lower() in (".tsv", ".txt") else ","


def _read_header_only(file_path: str, delimiter: str) -> List[str]:
    """Read just the header row — minimal I/O, used for entity-data detection."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        return next(reader, [])


def _read_preview(
    file_path: str, delimiter: str, max_rows: int
) -> Tuple[List[str], List[List[str]]]:
    """Read header + up to max_rows data rows. Stops early — never reads the full file."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, [])
        rows: List[List[str]] = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)
    return header, rows


def _read_full(file_path: str, delimiter: str) -> Tuple[List[str], List[List[str]]]:
    """Read header + all rows. Only call after entity-data check has passed
    and the file is confirmed to be a small metadata/design CSV."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, [])
        rows = list(reader)
    return header, rows


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

    Use this to inspect a metadata file before loading it.

    Supported metadata file types:
    - Experiment design CSVs (filename, sample_name, condition, ...)
    - Sample metadata CSVs (sample_name, dose, batch, ...)
    - Combined LFQ metadata CSVs (both above merged into one file)

    ENTITY-DATA BOUNDARY: If this tool reports that the file looks like a
    proteomics output (DIA-NN report, MaxQuant proteinGroups, Spectronaut
    export, MD_Format table, etc.), stop immediately and ask the user for
    their metadata CSV instead. Never attempt to read, aggregate, or interpret
    protein/peptide/gene intensity or expression data — the API handles that.

    Returns column names and a preview of the first max_rows data rows.
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


# ──────────────────────────────────────────────────────────────────────────────
# Wide → md_format conversion guide
# ──────────────────────────────────────────────────────────────────────────────

# Known annotation (non-intensity) columns for each format.
# Everything else in the file header is treated as a sample/intensity column.
_FORMAT_ANNOTATION_COLS: Dict[str, Set[str]] = {
    "diann_matrix": {
        "protein.group",
        "protein.ids",
        "protein.names",
        "genes",
        "first.protein.description",
        # pr_matrix extras
        "modified.sequence",
        "stripped.sequence",
        "precursor.charge",
        "precursor.id",
    },
    "maxquant": {
        "protein ids",
        "majority protein ids",
        "peptide counts (all)",
        "peptide counts (razor+unique)",
        "peptide counts (unique)",
        "fasta headers",
        "number of proteins",
        "peptides",
        "razor + unique peptides",
        "unique peptides",
        "sequence coverage [%]",
        "unique sequence coverage [%]",
        "mol. weight [kda]",
        "sequence length",
        "sequence lengths",
        "q-value",
        "score",
        "only identified by site",
        "reverse",
        "potential contaminant",
        "id",
        "peptide ids",
        "mod. peptide ids",
        "evidence ids",
        "ms/ms ids",
        "best ms/ms",
        "gene names",
        "protein names",
        "protein groups",
        "majority protein ids",
    },
    "spectronaut": {
        "pg.grouplabel",
        "pg.proteingroups",
        "pg.proteinaccessions",
        "pg.proteinnames",
        "pg.genes",
        "pg.proteindescriptions",
        "pep.groupingkey",
        "pep.groupingkeytype",
        "pep.isoproteotypic",
        "pep.strippedsequence",
        "pep.modifiedsequence",
        "eg.precursorid",
        "eg.modifiedsequence",
    },
    "md_format_gene": {
        "geneid",
    },
}

_MD_FORMAT_PROTEIN_SPEC = {
    "ProteinGroupId": "integer — unique per protein group (use pd.factorize)",
    "ProteinGroup": "string — primary protein group identifier (e.g. UniProt accession)",
    "GeneNames": "string — gene name(s), empty string if unknown",
    "SampleName": "string — sample identifier (must match experiment_design sample_name)",
    "ProteinIntensity": "float — intensity value; use 0.0 for missing",
    "Imputed": "integer 0 or 1 — 1 if intensity was missing/imputed",
}

_MD_FORMAT_PEPTIDE_SPEC = {
    "ModifiedSequence": "string — peptide sequence with modifications (e.g. PEPT(UniMod:21)IDE)",
    "StrippedSequence": "string — bare amino acid sequence",
    "ProteinGroup": "string — parent protein group identifier",
    "ProteinGroupId": "integer — matches protein-level ProteinGroupId",
    "GeneNames": "string — gene name(s)",
    "SampleName": "string — sample identifier",
    "PeptideIntensity": "float — intensity value; use 0.0 for missing",
    "Imputed": "integer 0 or 1",
}

_MD_FORMAT_GENE_SPEC = {
    "GeneId": "string — gene identifier (e.g. Ensembl ID or gene symbol)",
    "SampleName": "string — sample identifier",
    "GeneExpression": "float — expression value; use 0.0 for missing",
}


def _detect_annotation_cols(
    header_lower: List[str], source_hint: Optional[str]
) -> Set[str]:
    """Return the set of lowercased annotation column names for a given format."""
    if source_hint and source_hint.lower() in _FORMAT_ANNOTATION_COLS:
        return _FORMAT_ANNOTATION_COLS[source_hint.lower()]
    # Auto-detect: return the union of all known annotation columns
    # (conservative — better to misclassify a sample as annotation than vice versa)
    all_known: Set[str] = set()
    for cols in _FORMAT_ANNOTATION_COLS.values():
        all_known |= cols
    return all_known


def _build_protein_script(
    input_file: str,
    annotation_cols: List[str],
    sample_cols: List[str],
    sep: str,
    protein_col: str,
    gene_col: str,
) -> str:
    sep_repr = r"\t" if sep == "\t" else ","
    ann_repr = repr(annotation_cols)
    samp_repr = repr(sample_cols[:3]) + (" + ..." if len(sample_cols) > 3 else "")
    return f"""import pandas as pd

# ── 1. Load the wide-format file (header only shown here for reference) ───────
#  annotation columns : {ann_repr}
#  sample columns     : {samp_repr}

df = pd.read_csv({repr(input_file)}, sep={repr(sep_repr)}, low_memory=False)

annotation_cols = {ann_repr}
sample_cols = [c for c in df.columns if c not in annotation_cols]

# ── 2. Melt to long format ────────────────────────────────────────────────────
long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="ProteinIntensity",
)

# ── 3. Map to md_format columns ───────────────────────────────────────────────
long_df["Imputed"] = long_df["ProteinIntensity"].isna().astype(int)
long_df["ProteinIntensity"] = long_df["ProteinIntensity"].fillna(0.0)
long_df["ProteinGroupId"] = pd.factorize(long_df[{repr(protein_col)}])[0] + 1
long_df["ProteinGroup"] = long_df[{repr(protein_col)}]
long_df["GeneNames"] = long_df[{repr(gene_col)}].fillna("") if {repr(gene_col)} in long_df.columns else ""

result = long_df[[
    "ProteinGroupId", "ProteinGroup", "GeneNames",
    "SampleName", "ProteinIntensity", "Imputed",
]]

# ── 4. Save ───────────────────────────────────────────────────────────────────
out = {repr(input_file.rsplit(".", 1)[0] + "_md_format.tsv")}
result.to_csv(out, sep="\\t", index=False)
print(f"Saved {{len(result)}} rows to {{out}}")
"""


def _build_gene_script(
    input_file: str,
    annotation_cols: List[str],
    sample_cols: List[str],
    sep: str,
    gene_col: str,
) -> str:
    sep_repr = r"\t" if sep == "\t" else ","
    ann_repr = repr(annotation_cols)
    return f"""import pandas as pd

df = pd.read_csv({repr(input_file)}, sep={repr(sep_repr)}, low_memory=False)

annotation_cols = {ann_repr}
sample_cols = [c for c in df.columns if c not in annotation_cols]

long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="GeneExpression",
)

long_df["Imputed"] = long_df["GeneExpression"].isna().astype(int)
long_df["GeneExpression"] = long_df["GeneExpression"].fillna(0.0)
long_df["GeneId"] = long_df[{repr(gene_col)}]

result = long_df[["GeneId", "SampleName", "GeneExpression"]]

out = {repr(input_file.rsplit(".", 1)[0] + "_md_format_gene.tsv")}
result.to_csv(out, sep="\\t", index=False)
print(f"Saved {{len(result)}} rows to {{out}}")
"""


@mcp.tool()
def plan_wide_to_md_format(
    file_path: str,
    target: str = "md_format",
    source_hint: Optional[str] = None,
    annotation_columns: Optional[List[str]] = None,
    delimiter: Optional[str] = None,
) -> str:
    """Generate a Python/pandas conversion plan for a wide-format file → md_format or md_format_gene.

    Wide format: entities (proteins, peptides, genes) on rows; sample intensity
    columns on the right, annotation columns (IDs, gene names, descriptions) on the left.
    This is the native output of DIA-NN matrix files, MaxQuant proteinGroups.txt,
    Spectronaut exports, etc.

    md_format (protein): ProteinGroupId, ProteinGroup, GeneNames, SampleName,
                          ProteinIntensity, Imputed
    md_format (peptide): ModifiedSequence, StrippedSequence, ProteinGroup,
                          ProteinGroupId, GeneNames, SampleName, PeptideIntensity, Imputed
    md_format_gene:       GeneId, SampleName, GeneExpression

    ENTITY-DATA BOUNDARY: This tool reads ONLY the header row of the file.
    It never reads or processes intensity/expression values.
    The conversion script it generates must be run by the user locally —
    do not attempt to execute it yourself or read the resulting data.

    Parameters:
    - file_path:          path to the wide-format input file
    - target:             "md_format" (protein or peptide) or "md_format_gene"
    - source_hint:        optional format name to improve auto-detection:
                          diann_matrix, maxquant, spectronaut, md_format_gene
    - annotation_columns: optional explicit list of annotation column names
                          (everything else will be treated as sample columns)
    - delimiter:          auto-detected from file extension if omitted

    Returns JSON with:
    - detected_annotation_cols: columns identified as entity metadata
    - detected_sample_cols:     columns identified as sample intensity values
    - md_format_spec:           required output columns and their types
    - conversion_script:        ready-to-run Python/pandas script
    - notes:                    warnings and next steps
    """
    if not os.path.exists(file_path):
        return json.dumps({"error": f"File not found: {file_path}"})

    sep = delimiter or _sniff_delimiter(file_path)

    try:
        header = _read_header_only(file_path, sep)
    except Exception as e:
        return json.dumps({"error": f"Could not read file header: {e}"})

    if not header:
        return json.dumps({"error": "File appears empty"})

    header_stripped = [h.strip() for h in header]
    header_lower = [h.lower() for h in header_stripped]

    # Determine annotation columns
    if annotation_columns:
        ann_lower = {c.strip().lower() for c in annotation_columns}
        detected_ann = [h for h in header_stripped if h.lower() in ann_lower]
        detected_samp = [h for h in header_stripped if h.lower() not in ann_lower]
    else:
        known_ann = _detect_annotation_cols(header_lower, source_hint)
        detected_ann = [h for h in header_stripped if h.lower() in known_ann]
        detected_samp = [h for h in header_stripped if h.lower() not in known_ann]

    if not detected_samp:
        return json.dumps(
            {
                "error": (
                    "Could not identify any sample columns. "
                    "All columns matched known annotation names. "
                    "Provide annotation_columns explicitly to specify which columns "
                    "contain entity metadata (the rest will be treated as samples)."
                ),
                "all_columns": header_stripped,
            }
        )

    notes: List[str] = []

    # Pick protein/gene identifier columns for the script
    protein_col = next(
        (
            h
            for h in detected_ann
            if h.lower()
            in (
                "protein.group",
                "majority protein ids",
                "pg.grouplabel",
                "protein ids",
                "proteingroup",
            )
        ),
        detected_ann[0] if detected_ann else "ProteinGroup",
    )
    gene_col = next(
        (
            h
            for h in detected_ann
            if h.lower() in ("genes", "gene names", "pg.genes", "genenames")
        ),
        "",
    )

    if target.lower() == "md_format_gene":
        spec = _MD_FORMAT_GENE_SPEC
        gene_id_col = next(
            (
                h
                for h in detected_ann
                if h.lower() in ("geneid", "gene id", "genes", "gene names", "gene_id")
            ),
            detected_ann[0] if detected_ann else "GeneId",
        )
        script = _build_gene_script(
            file_path, detected_ann, detected_samp, sep, gene_id_col
        )
    else:
        spec = _MD_FORMAT_PROTEIN_SPEC
        script = _build_protein_script(
            file_path,
            detected_ann,
            detected_samp,
            sep,
            protein_col,
            gene_col or protein_col,
        )

    if len(detected_samp) > 50:
        notes.append(
            f"Detected {len(detected_samp)} sample columns — this is a large file. "
            "The pandas melt will produce a very large long-format table. "
            "Consider chunked processing if memory is limited."
        )
    if not gene_col and target.lower() != "md_format_gene":
        notes.append(
            "No gene-name column auto-detected. The script uses the protein column as GeneNames. "
            "Set gene_col manually in the script if your file has a separate gene column."
        )
    notes.append(
        "After converting, upload the output file using source='md_format' "
        "(or 'md_format_gene') in create_upload. "
        "You still need an experiment_design CSV and a sample_metadata CSV — "
        "use load_metadata_from_csv or build them manually."
    )
    notes.append(
        "SampleName values in the output must exactly match sample_name values "
        "in your experiment_design and sample_metadata. "
        "Check that the wide-format column headers match your expected sample names."
    )

    return json.dumps(
        {
            "detected_annotation_cols": detected_ann,
            "detected_sample_cols": detected_samp,
            "sample_col_count": len(detected_samp),
            "md_format_spec": spec,
            "conversion_script": script,
            "notes": notes,
        },
        indent=2,
    )
