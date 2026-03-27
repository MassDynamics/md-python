"""MD format conversion tools: get_md_format_spec and plan_wide_to_md_format."""

import json
import os
from typing import Dict, List, Optional, Set

from .. import mcp
from ._io import _read_header_only, _sniff_delimiter

# ──────────────────────────────────────────────────────────────────────────────
# Known annotation (non-intensity) columns for each format.
# Everything else in the file header is treated as a sample/intensity column.
# ──────────────────────────────────────────────────────────────────────────────

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

# ──────────────────────────────────────────────────────────────────────────────
# MD format column specifications
# ──────────────────────────────────────────────────────────────────────────────

_MD_FORMAT_PROTEIN_SPEC = {
    "ProteinGroupId": "integer — unique per protein group (use pd.factorize)",
    "ProteinGroup": "string — primary protein group identifier (e.g. UniProt accession)",
    "GeneNames": "string — gene name(s), empty string if unknown",
    "SampleName": "string — sample identifier (must match experiment_design sample_name)",
    "ProteinIntensity": (
        "float — measured intensity. Use 0.0 for missing values, "
        "BUT every row with ProteinIntensity=0.0 MUST also have Imputed=1. "
        "A zero with Imputed=0 is treated as a real measured intensity (almost never "
        "correct in proteomics) and will cause downstream pairwise jobs to fail."
    ),
    "Imputed": (
        "integer 0 or 1 — set to 1 for every row where ProteinIntensity=0.0. "
        "If your source uses 0.0 for missing (not NaN), add after melting: "
        "long_df.loc[long_df['ProteinIntensity'] == 0, 'Imputed'] = 1"
    ),
}

_MD_FORMAT_PEPTIDE_SPEC = {
    "ModifiedSequence": "string — peptide sequence with modifications (e.g. PEPT(UniMod:21)IDE)",
    "StrippedSequence": "string — bare amino acid sequence",
    "ProteinGroup": "string — parent protein group identifier",
    "ProteinGroupId": "integer — matches protein-level ProteinGroupId",
    "GeneNames": "string — gene name(s)",
    "SampleName": "string — sample identifier",
    "PeptideIntensity": (
        "float — measured intensity. Use 0.0 for missing values, "
        "BUT every row with PeptideIntensity=0.0 MUST also have Imputed=1."
    ),
    "Imputed": (
        "integer 0 or 1 — set to 1 for every row where PeptideIntensity=0.0. "
        "A zero with Imputed=0 is treated as a real measurement and causes downstream failures."
    ),
}

_MD_FORMAT_GENE_SPEC = {
    "GeneId": "string — gene identifier (e.g. Ensembl ID or gene symbol)",
    "SampleName": "string — sample identifier",
    "GeneExpression": (
        "float — expression value. Use 0.0 for missing values, "
        "BUT every row with GeneExpression=0.0 MUST also have Imputed=1."
    ),
    "Imputed": (
        "integer 0 or 1 — set to 1 for every row where GeneExpression=0.0. "
        "A zero with Imputed=0 is treated as a real measurement and causes downstream failures."
    ),
}

# ──────────────────────────────────────────────────────────────────────────────
# Generic conversion templates (no file path — fill in column names)
# ──────────────────────────────────────────────────────────────────────────────

_GENERIC_PROTEIN_TEMPLATE = """\
import pandas as pd

# Fill in the actual column names from your file
annotation_cols = ["ProteinGroup", "GeneNames"]   # entity metadata columns
# Everything else is treated as sample columns

df = pd.read_csv("your_file.tsv", sep="\\t", low_memory=False)

sample_cols = [c for c in df.columns if c not in annotation_cols]

long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="ProteinIntensity",
)

long_df["Imputed"] = long_df["ProteinIntensity"].isna().astype(int)
long_df["ProteinIntensity"] = long_df["ProteinIntensity"].fillna(0.0)
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment the line below:
# long_df.loc[long_df["ProteinIntensity"] == 0, "Imputed"] = 1
long_df["ProteinGroupId"] = pd.factorize(long_df["ProteinGroup"])[0] + 1
long_df["GeneNames"] = long_df["GeneNames"].fillna("") if "GeneNames" in long_df.columns else ""

result = long_df[["ProteinGroupId", "ProteinGroup", "GeneNames", "SampleName", "ProteinIntensity", "Imputed"]]
result.to_csv("output_md_format.tsv", sep="\\t", index=False)
print(f"Saved {len(result)} rows")
"""

_GENERIC_GENE_TEMPLATE = """\
import pandas as pd

annotation_cols = ["GeneId"]   # entity metadata columns (GeneId required)
# Everything else is treated as sample columns

df = pd.read_csv("your_file.tsv", sep="\\t", low_memory=False)

sample_cols = [c for c in df.columns if c not in annotation_cols]

long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="GeneExpression",
)

long_df["Imputed"] = long_df["GeneExpression"].isna().astype(int)
long_df["GeneExpression"] = long_df["GeneExpression"].fillna(0.0)
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment the line below:
# long_df.loc[long_df["GeneExpression"] == 0, "Imputed"] = 1

result = long_df[["GeneId", "SampleName", "GeneExpression"]]
result.to_csv("output_md_format_gene.tsv", sep="\\t", index=False)
print(f"Saved {len(result)} rows")
"""


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


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
    transpose: bool = False,
) -> str:
    sep_repr = r"\t" if sep == "\t" else ","
    ann_repr = repr(annotation_cols)
    samp_repr = repr(sample_cols[:3]) + (" + ..." if len(sample_cols) > 3 else "")
    transpose_block = (
        (
            "\n# ── 1b. Transpose: samples were in rows, proteins in columns ────────────────\n"
            "df = df.set_index(df.columns[0]).T.reset_index()\n"
            "df = df.rename(columns={'index': 'ProteinGroup'})\n"
        )
        if transpose
        else ""
    )
    return f"""import pandas as pd

# ── 1. Load the wide-format file (header only shown here for reference) ───────
#  annotation columns : {ann_repr}
#  sample columns     : {samp_repr}

df = pd.read_csv({repr(input_file)}, sep={repr(sep_repr)}, low_memory=False)
{transpose_block}
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
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment:
# long_df.loc[long_df["ProteinIntensity"] == 0, "Imputed"] = 1
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
    transpose: bool = False,
) -> str:
    sep_repr = r"\t" if sep == "\t" else ","
    ann_repr = repr(annotation_cols)
    transpose_block = (
        (
            "\n# ── 1b. Transpose: samples were in rows, genes in columns ──────────────────\n"
            "df = df.set_index(df.columns[0]).T.reset_index()\n"
            "df = df.rename(columns={'index': 'GeneId'})\n"
        )
        if transpose
        else ""
    )
    return f"""import pandas as pd

df = pd.read_csv({repr(input_file)}, sep={repr(sep_repr)}, low_memory=False)
{transpose_block}
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
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment:
# long_df.loc[long_df["GeneExpression"] == 0, "Imputed"] = 1
long_df["GeneId"] = long_df[{repr(gene_col)}]

result = long_df[["GeneId", "SampleName", "GeneExpression"]]

out = {repr(input_file.rsplit(".", 1)[0] + "_md_format_gene.tsv")}
result.to_csv(out, sep="\\t", index=False)
print(f"Saved {{len(result)}} rows to {{out}}")
"""


# ──────────────────────────────────────────────────────────────────────────────
# MCP tools
# ──────────────────────────────────────────────────────────────────────────────


@mcp.tool()
def get_md_format_spec(entity_type: str = "protein") -> str:
    """Return the MD format column specification and a generic pandas conversion template.

    Use this when you need to explain MD format to a user, write custom conversion
    code for data already in memory, or understand the target schema before seeing
    a file. For converting an actual wide-format file on disk, use
    plan_wide_to_md_format instead — it reads the header and generates a
    file-specific script automatically.

    Args:
        entity_type: "protein" (default), "peptide", or "gene"

    Returns JSON with:
    - entity_type:          the requested type
    - spec:                 required columns and their types
    - conversion_template:  generic pandas snippet to adapt to your data
    - upload_source:        the source= value to use in create_upload
    - notes:                alignment requirements and next steps
    """
    et = entity_type.lower()
    if et == "gene":
        spec = _MD_FORMAT_GENE_SPEC
        template = _GENERIC_GENE_TEMPLATE
        source = "md_format_gene"
    elif et == "peptide":
        spec = _MD_FORMAT_PEPTIDE_SPEC
        template = _GENERIC_PROTEIN_TEMPLATE.replace(
            "ProteinIntensity", "PeptideIntensity"
        ).replace(
            '["ProteinGroupId", "ProteinGroup", "GeneNames", "SampleName", "ProteinIntensity", "Imputed"]',
            '["ModifiedSequence", "StrippedSequence", "ProteinGroup", "ProteinGroupId", "GeneNames", "SampleName", "PeptideIntensity", "Imputed"]',
        )
        source = "md_format"
    else:
        spec = _MD_FORMAT_PROTEIN_SPEC
        template = _GENERIC_PROTEIN_TEMPLATE
        source = "md_format"

    return json.dumps(
        {
            "entity_type": et,
            "spec": spec,
            "conversion_template": template,
            "upload_source": source,
            "notes": [
                "SampleName values in the output MUST exactly match sample_name values "
                "in your experiment_design and sample_metadata — case-sensitive.",
                "CRITICAL: Every row where the intensity/expression value is 0.0 MUST have "
                "Imputed=1. A zero with Imputed=0 is treated as a real measured value "
                "(almost never correct in proteomics/genomics) and will cause downstream "
                "pairwise analysis to fail silently. If your source uses 0.0 for missing "
                "(not NaN), add after melting: "
                "long_df.loc[long_df['<intensity_col>'] == 0, 'Imputed'] = 1",
                f"After converting, upload with source='{source}' in create_upload.",
                "You still need an experiment_design CSV and a sample_metadata CSV alongside the data file.",
            ],
        },
        indent=2,
    )


@mcp.tool()
def plan_wide_to_md_format(
    file_path: str,
    target: str = "md_format",
    source_hint: Optional[str] = None,
    annotation_columns: Optional[List[str]] = None,
    delimiter: Optional[str] = None,
    transpose: bool = False,
) -> str:
    """Generate a Python/pandas conversion script for a wide-format file → md_format or md_format_gene.

    Works for any wide-format intensity matrix — DIA-NN, MaxQuant, Spectronaut,
    or a generic CSV/TSV. The standard orientation is entities (proteins, genes)
    in rows and samples in columns. Use transpose=True (or omit it to auto-detect)
    when the matrix is flipped: samples in rows and proteins in columns.

    md_format (protein): ProteinGroupId, ProteinGroup, GeneNames, SampleName,
                          ProteinIntensity, Imputed
    md_format (peptide): ModifiedSequence, StrippedSequence, ProteinGroup,
                          ProteinGroupId, GeneNames, SampleName, PeptideIntensity, Imputed
    md_format_gene:       GeneId, SampleName, GeneExpression

    To get the full spec without a file, call get_md_format_spec(entity_type) first.

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
                          (everything else will be treated as sample columns).
                          Use annotation_columns to fix wrong auto-detection.
    - delimiter:          auto-detected from file extension if omitted
    - transpose:          set True when samples are rows and proteins are columns
                          (auto-detected if the first column is named SampleName)

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

    # Auto-detect transposed orientation: first column named "SampleName" means
    # samples are in rows and proteins are in columns.
    auto_transposed = header_lower[0] in ("samplename", "sample_name", "sample name")
    do_transpose = transpose or auto_transposed

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
            file_path, detected_ann, detected_samp, sep, gene_id_col, do_transpose
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
            do_transpose,
        )

    if do_transpose:
        reason = (
            "auto-detected (first column is SampleName)"
            if auto_transposed
            else "transpose=True"
        )
        notes.append(
            f"Transposed orientation {reason}: the script flips rows/columns before melting. "
            "Verify that the column names after transposing match your protein identifiers."
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
