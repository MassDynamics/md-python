"""MD format conversion tools: get_md_format_spec and plan_wide_to_md_format."""

import json
import os
import re
from typing import Dict, List, Optional, Set

from .. import mcp
from ._io import _read_header_only, _read_preview, _sniff_delimiter

# ──────────────────────────────────────────────────────────────────────────────
# Known annotation (non-intensity) columns for each format.
# Everything else in the file header is treated as a sample/intensity column.
# ──────────────────────────────────────────────────────────────────────────────

_FORMAT_ANNOTATION_COLS: Dict[str, Set[str]] = {
    "diann_tabular": {
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
    "md_format_metabolite": {
        "metaboliteid",
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# MD format column specifications
# ──────────────────────────────────────────────────────────────────────────────

_MD_FORMAT_PROTEIN_SPEC = {
    "ProteinGroupId": "integer — unique per protein group (use pd.factorize)",
    "ProteinGroup": (
        "string — primary protein group identifier. MUST be UniProt accession(s) "
        "(e.g. P12345, or P12345;Q67890 for a group) — NOT Ensembl IDs (ENSP/ENSG) "
        "or bare gene symbols. The platform maps PTM sites onto UniProt protein "
        "SEQUENCES; non-UniProt ids resolve to 0 sequences and the upload fails "
        "(silently, as a stuck 'processing' status). If your source uses Ensembl "
        "ids, convert them to UniProt accessions first (e.g. UniProt ID-mapping "
        "Ensembl_Protein→UniProtKB)."
    ),
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
    "ModifiedSequence": "REQUIRED — string — peptide sequence with modifications (e.g. PEPT(UniMod:21)IDE)",
    "StrippedSequence": "REQUIRED — string — bare amino acid sequence",
    "Unique": (
        "REQUIRED — boolean (TRUE/FALSE) — TRUE if the peptide is unique to its "
        "protein group. Compute as: TRUE where the StrippedSequence maps to exactly "
        "one ProteinGroup across the dataset, FALSE otherwise."
    ),
    "ProteinGroup": (
        "REQUIRED — string — parent protein group identifier. MUST be UniProt "
        "accession(s) (e.g. P12345) — NOT Ensembl ids (ENSP/ENSG) or gene symbols: "
        "PTM sites are mapped onto UniProt protein SEQUENCES, and non-UniProt ids "
        "yield 0 sequence matches and a (silent) failed upload. MUST also use the "
        "IDENTICAL ProteinGroup→ProteinGroupId mapping as the companion protein-level "
        "file (see DUAL-FILE note below)."
    ),
    "ProteinGroupId": (
        "REQUIRED — integer — MUST match the protein-level file's ProteinGroupId for "
        "the same ProteinGroup. Do NOT factorize the peptide and protein files "
        "independently — derive the peptide ProteinGroupId from the protein file's "
        "ProteinGroup→ProteinGroupId map (peptide-only groups absent from the protein "
        "file get fresh ids above the protein file's max)."
    ),
    "GeneNames": "REQUIRED — string — gene name(s); same as the protein-level file.",
    "SampleName": "REQUIRED — string — sample identifier; same sample set as the protein file.",
    "PeptideIntensity": (
        "REQUIRED — float — measured intensity. Use 0.0 for missing values, "
        "BUT every row with PeptideIntensity=0.0 MUST also have Imputed=1."
    ),
    "Imputed": (
        "REQUIRED — integer 0 or 1 — set to 1 for every row where PeptideIntensity=0.0. "
        "A zero with Imputed=0 is treated as a real measurement and causes downstream failures."
    ),
    "OtherProteinGroupIds": "OPTIONAL — string — for a nonunique peptide, the other protein group ids (semicolon-separated).",
    "ProteinNames": "OPTIONAL — string — protein name(s), semicolon-separated.",
    "Description": "OPTIONAL — string — protein description(s), semicolon-separated.",
}

_MD_FORMAT_GENE_SPEC = {
    "GeneId": ("REQUIRED — string — gene identifier (e.g. Ensembl ID or gene symbol)."),
    "SampleName": (
        "REQUIRED — string — sample identifier; must match sample_name in "
        "sample_metadata exactly (case-sensitive)."
    ),
    "GeneExpression": (
        "REQUIRED — float — expression value. NaN or 0.0 are both treated as "
        "missing by md-converter; the converter auto-fills missing "
        "GeneId×SampleName combinations with GeneExpression=0 before flagging."
    ),
    "Imputed": (
        "OPTIONAL — integer 0 or 1. Unlike md_format protein/peptide, the "
        "md-converter md_format_gene reader DERIVES this column itself: every "
        "row where GeneExpression is NaN or 0 is auto-flagged as Imputed=1 "
        "(md-converter/src/mdconverter/md_format_gene/reader.py:120-124). "
        "You may include the column if you want to be explicit, but you do "
        "NOT need to set it. The required columns the converter actually "
        "checks for are [GeneId, GeneExpression, SampleName] only "
        "(reader.py:8 REQUIRED_GENE_COLUMNS)."
    ),
}

_MD_FORMAT_METABOLITE_SPEC = {
    "MetaboliteId": (
        "REQUIRED — string — metabolite identifier (e.g. HMDB ID, KEGG ID, "
        "or compound name)."
    ),
    "MetaboliteIntensity": (
        "REQUIRED — float — measured intensity. Use 0.0 for missing values, "
        "BUT every row with MetaboliteIntensity=0.0 MUST also have Imputed=1."
    ),
    "SampleName": (
        "REQUIRED — string — sample identifier; must match sample_name in "
        "sample_metadata exactly (case-sensitive)."
    ),
    "Imputed": (
        "REQUIRED — integer 0 or 1, validated: md-converter rejects any value "
        "other than 0 or 1 (md-converter/src/mdconverter/md_format_metabolite/"
        "reader.py:80-82). Unlike md_format_gene, the md_format_metabolite "
        "reader does NOT auto-derive this column — you MUST set it. Set 1 for "
        "every row where MetaboliteIntensity=0.0. The required columns the "
        "converter checks for are [MetaboliteId, MetaboliteIntensity, "
        "SampleName, Imputed] (reader.py:8 REQUIRED_METABOLITE_COLUMNS)."
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

# Peptide is a DUAL-FILE upload: a peptide-level file PLUS a companion protein-
# level file, both passed to create_upload filenames=. The peptide table needs a
# Unique column and a ProteinGroupId that MATCHES the protein file's
# ProteinGroup->ProteinGroupId mapping (do NOT factorize the two files
# independently). This template produces both that requirement and the protein
# companion's id map.
_GENERIC_PEPTIDE_TEMPLATE = """\
import pandas as pd

# ── 1. Protein companion file (REQUIRED alongside the peptide file) ──────────
# Build (or load) the protein-level md_format table first; its
# ProteinGroup->ProteinGroupId map is the single source of truth for ids.
protein_df = pd.read_csv("output_md_format.tsv", sep="\\t")   # protein md_format
pg_to_id = (
    protein_df[["ProteinGroup", "ProteinGroupId"]]
    .drop_duplicates()
    .set_index("ProteinGroup")["ProteinGroupId"]
    .to_dict()
)

# ── 2. Peptide wide-format file ─────────────────────────────────────────────
annotation_cols = ["ModifiedSequence", "StrippedSequence", "ProteinGroup", "GeneNames"]
df = pd.read_csv("your_peptide_file.tsv", sep="\\t", low_memory=False)
sample_cols = [c for c in df.columns if c not in annotation_cols]

long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="PeptideIntensity",
)

long_df["Imputed"] = long_df["PeptideIntensity"].isna().astype(int)
long_df["PeptideIntensity"] = long_df["PeptideIntensity"].fillna(0.0)
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment:
# long_df.loc[long_df["PeptideIntensity"] == 0, "Imputed"] = 1

# Unique = TRUE if the stripped sequence maps to exactly one protein group.
pg_per_seq = long_df.groupby("StrippedSequence")["ProteinGroup"].transform("nunique")
long_df["Unique"] = pg_per_seq == 1

# ProteinGroupId MUST come from the protein file's map (not an independent
# factorize). Peptide-only groups absent from the protein file get fresh ids
# above the protein file's max.
_next = (max(pg_to_id.values()) + 1) if pg_to_id else 1
for pg in sorted(set(long_df["ProteinGroup"]) - set(pg_to_id)):
    pg_to_id[pg] = _next
    _next += 1
long_df["ProteinGroupId"] = long_df["ProteinGroup"].map(pg_to_id).astype(int)
long_df["GeneNames"] = long_df["GeneNames"].fillna("") if "GeneNames" in long_df.columns else ""

result = long_df[[
    "ModifiedSequence", "StrippedSequence", "Unique", "ProteinGroup",
    "ProteinGroupId", "GeneNames", "SampleName", "PeptideIntensity", "Imputed",
]]
result.to_csv("output_md_format_peptide.tsv", sep="\\t", index=False)
print(f"Saved {len(result)} peptide rows")
# Upload BOTH files together:
#   create_upload(..., source="md_format",
#                 filenames=["output_md_format_peptide.tsv", "output_md_format.tsv"])
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

# md-converter auto-flags Imputed for gene data (NaN or 0 → Imputed=1)
# at md_format_gene/reader.py:120-124, so you do NOT need to write the
# Imputed column yourself. Required columns: [GeneId, GeneExpression,
# SampleName] (md_format_gene/reader.py:8 REQUIRED_GENE_COLUMNS).
long_df["GeneExpression"] = long_df["GeneExpression"].fillna(0.0)

result = long_df[["GeneId", "SampleName", "GeneExpression"]]
result.to_csv("output_md_format_gene.tsv", sep="\\t", index=False)
print(f"Saved {len(result)} rows")
"""

_GENERIC_METABOLITE_TEMPLATE = """\
import pandas as pd

annotation_cols = ["MetaboliteId"]   # entity metadata columns (MetaboliteId required)
# Everything else is treated as sample columns

df = pd.read_csv("your_file.tsv", sep="\\t", low_memory=False)

sample_cols = [c for c in df.columns if c not in annotation_cols]

long_df = df.melt(
    id_vars=annotation_cols,
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="MetaboliteIntensity",
)

# Imputed is REQUIRED for md_format_metabolite and validated as 0/1 by
# md-converter (md_format_metabolite/reader.py:80-82). Unlike md_format_gene
# it is NOT auto-derived — you must emit it explicitly.
long_df["Imputed"] = long_df["MetaboliteIntensity"].isna().astype(int)
long_df["MetaboliteIntensity"] = long_df["MetaboliteIntensity"].fillna(0.0)
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment the line below:
# long_df.loc[long_df["MetaboliteIntensity"] == 0, "Imputed"] = 1

result = long_df[["MetaboliteId", "SampleName", "MetaboliteIntensity", "Imputed"]]
result.to_csv("output_md_format_metabolite.tsv", sep="\\t", index=False)
print(f"Saved {len(result)} rows")
"""


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _detect_annotation_cols(source_hint: Optional[str]) -> Set[str]:
    """Return the set of lowercased annotation column names for a given format.

    When ``source_hint`` matches a known format, return that format's specific
    annotation set; otherwise fall back to the union of all known annotation
    columns (conservative — better to misclassify a sample as annotation than
    vice versa).
    """
    if source_hint and source_hint.lower() in _FORMAT_ANNOTATION_COLS:
        return _FORMAT_ANNOTATION_COLS[source_hint.lower()]
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

# md-converter auto-flags Imputed for gene data (NaN or 0 → Imputed=1)
# at md_format_gene/reader.py:120-124. Required columns are only
# [GeneId, GeneExpression, SampleName] (reader.py:8 REQUIRED_GENE_COLUMNS),
# so we don't need to emit the Imputed column.
long_df["GeneExpression"] = long_df["GeneExpression"].fillna(0.0)
long_df["GeneId"] = long_df[{repr(gene_col)}]

result = long_df[["GeneId", "SampleName", "GeneExpression"]]

out = {repr(input_file.rsplit(".", 1)[0] + "_md_format_gene.tsv")}
result.to_csv(out, sep="\\t", index=False)
print(f"Saved {{len(result)}} rows to {{out}}")
"""


def _build_metabolite_script(
    input_file: str,
    annotation_cols: List[str],
    sep: str,
    metabolite_col: str,
    transpose: bool = False,
) -> str:
    sep_repr = r"\t" if sep == "\t" else ","
    ann_repr = repr(annotation_cols)
    transpose_block = (
        (
            "\n# ── 1b. Transpose: samples were in rows, metabolites in columns ────────────\n"
            "df = df.set_index(df.columns[0]).T.reset_index()\n"
            "df = df.rename(columns={'index': 'MetaboliteId'})\n"
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
    value_name="MetaboliteIntensity",
)

# Imputed is REQUIRED for md_format_metabolite and validated as 0/1 by
# md-converter (md_format_metabolite/reader.py:80-82). Unlike md_format_gene
# it is NOT auto-derived — emit it explicitly.
long_df["Imputed"] = long_df["MetaboliteIntensity"].isna().astype(int)
long_df["MetaboliteIntensity"] = long_df["MetaboliteIntensity"].fillna(0.0)
# CRITICAL: if source uses 0.0 for missing (not NaN), uncomment:
# long_df.loc[long_df["MetaboliteIntensity"] == 0, "Imputed"] = 1
long_df["MetaboliteId"] = long_df[{repr(metabolite_col)}]

result = long_df[["MetaboliteId", "SampleName", "MetaboliteIntensity", "Imputed"]]

out = {repr(input_file.rsplit(".", 1)[0] + "_md_format_metabolite.tsv")}
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
        entity_type: "protein" (default), "peptide", "gene", or "metabolite"

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
    elif et == "metabolite":
        spec = _MD_FORMAT_METABOLITE_SPEC
        template = _GENERIC_METABOLITE_TEMPLATE
        source = "md_format_metabolite"
    elif et == "peptide":
        spec = _MD_FORMAT_PEPTIDE_SPEC
        template = _GENERIC_PEPTIDE_TEMPLATE
        source = "md_format"
    else:
        spec = _MD_FORMAT_PROTEIN_SPEC
        template = _GENERIC_PROTEIN_TEMPLATE
        source = "md_format"

    if et == "gene":
        notes = [
            "SampleName values in the output MUST exactly match sample_name values "
            "in your sample_metadata — case-sensitive.",
            "GENE-SPECIFIC: md-converter auto-derives the Imputed column for gene data. "
            "Every row where GeneExpression is NaN or 0 is auto-flagged as Imputed=1 "
            "(md-converter/src/mdconverter/md_format_gene/reader.py:120-124). The required "
            "columns the converter actually checks for are only [GeneId, GeneExpression, "
            "SampleName] (reader.py:8 REQUIRED_GENE_COLUMNS). You may include an Imputed "
            "column to be explicit, but you do NOT have to.",
            f"After converting, upload with source='{source}' in create_upload.",
            "You still need a sample_metadata CSV alongside the data file. "
            "experiment_design is OPTIONAL for md_format_gene uploads — the workflow "
            "skips the 'experiment_design required' validation for source=md_format_gene "
            "(workflow/app/models/experiment.rb:98-103).",
        ]
    elif et == "metabolite":
        notes = [
            "SampleName values in the output MUST exactly match sample_name values "
            "in your experiment_design and sample_metadata — case-sensitive.",
            "METABOLITE-SPECIFIC: Imputed is REQUIRED and validated — md-converter "
            "rejects any value other than 0 or 1 (md-converter/src/mdconverter/"
            "md_format_metabolite/reader.py:80-82). Unlike md_format_gene it is NOT "
            "auto-derived: set Imputed=1 for every row where MetaboliteIntensity=0.0.",
            "The input must be a FULL matrix — every MetaboliteId x SampleName "
            "combination present as exactly one row "
            "(md_format_metabolite/reader.py:93-107). Melting a wide intensity matrix "
            "satisfies this automatically.",
            f"After converting, upload with source='{source}' in create_upload.",
            "You still need an experiment_design CSV and a sample_metadata CSV "
            "alongside the data file. experiment_design IS required for "
            "md_format_metabolite uploads — only md_format_gene is exempt "
            "(workflow/app/models/experiment.rb:98-103).",
        ]
    else:
        intensity_col = "PeptideIntensity" if et == "peptide" else "ProteinIntensity"
        notes = [
            "SampleName values in the output MUST exactly match sample_name values "
            "in your experiment_design and sample_metadata — case-sensitive.",
            "CRITICAL (protein/peptide only): Every row where the intensity value is 0.0 "
            "MUST have Imputed=1. A zero with Imputed=0 is treated as a real measured "
            "value (almost never correct in proteomics) and will cause downstream "
            "pairwise analysis to fail silently. If your source uses 0.0 for missing "
            f"(not NaN), add after melting: "
            f"long_df.loc[long_df['{intensity_col}'] == 0, 'Imputed'] = 1",
            f"After converting, upload with source='{source}' in create_upload.",
            "You still need an experiment_design CSV and a sample_metadata CSV alongside "
            "the data file.",
        ]
        if et == "peptide":
            notes += [
                "PEPTIDE = DUAL-FILE UPLOAD: a peptide md_format upload MUST include "
                "BOTH a peptide-level file AND a companion protein-level md_format file, "
                "both passed in filenames=. A peptide file alone fails ingestion with "
                "'Protein data file not found' (md-converter md_format/reader.py:47).",
                "PEPTIDE REQUIRES a Unique column (boolean, TRUE if the peptide is "
                "unique to its protein group) that the protein file does not have.",
                "CROSS-TABLE ID RULE: ProteinGroupId and ProteinGroup MUST use the "
                "IDENTICAL ProteinGroup→ProteinGroupId mapping in the peptide and "
                "protein files. Do NOT factorize the two files independently — derive "
                "the peptide ProteinGroupId from the protein file's map (peptide-only "
                "groups get fresh ids above the protein file's max). Independent "
                "factorization yields mismatched ids and a silent ingestion failure.",
                "PROTEINGROUP MUST BE UNIPROT: ProteinGroup must hold UniProt "
                "accession(s) (e.g. P12345), NOT Ensembl ids (ENSP/ENSG) or gene "
                "symbols. PTM sites are located within UniProt protein SEQUENCES; "
                "non-UniProt ids match 0 sequences and the upload fails silently "
                "(stuck 'processing'). VERIFY BEFORE UPLOAD that StrippedSequence "
                "values are substrings of the UniProt sequence for their ProteinGroup "
                "(sample a few hundred; expect >90% — isoform differences explain the "
                "rest). Convert Ensembl→UniProt via UniProt ID-mapping "
                "(Ensembl_Protein→UniProtKB) if needed. Also resolve ';'-joined "
                "ambiguous peptide forms (e.g. 'PEPTIDEK;EPTIDEK') to a single "
                "sequence — joined forms never match a sequence.",
                "MODIFIEDSEQUENCE MUST BE INLINE UNIMOD: residue then (UniMod:NN) "
                "(N-term mods before residue 1, e.g. (UniMod:2016)PEPTIDE); NOT a "
                "tool's native annotation. Proteome Discoverer exports it as "
                "'[K].PEPT.[V] | 1xPhospho [T4]' which is NOT ingestible and must be "
                "converted (Phospho->21, Oxidation->35, Carbamidomethyl->4, Acetyl->1, "
                "TMTpro->2016, Met-loss->765, Met-loss+Acetyl->766; honour the Nx "
                "multiplier). VALIDATE: stripping every (UniMod:NN) must reproduce "
                "StrippedSequence exactly. ~15% of PD rows carry an UNLOCALISED "
                "modification ([S/Y], [S/T], bare [S]) with no defined residue; inline "
                "UniMod needs a specific site, so this is a SCIENTIFIC choice you MUST "
                "put to the user — never auto-decide. Offer at minimum: (a) assign to "
                "the first candidate residue of an allowed type (site inferred — can "
                "collapse distinct precursors), or (b) drop the unlocalised rows; "
                "optionally (c) assign-to-first plus a sidecar flagging inferred sites. "
                "See the md-mcp-ops skill's pd_to_md_format recipe + pd_to_md_peptide.py.",
            ]

    notes.insert(
        0,
        "MD format is LONG format and MUST be a FULL matrix: exactly one row per "
        "entity per sample, with EVERY entity x sample combination present — NO "
        "EXCEPTIONS. A non-measurement is represented as a row with intensity 0.0 "
        "and Imputed=1, never as an absent row. Melting a complete wide intensity "
        "matrix produces this automatically.",
    )

    return json.dumps(
        {
            "entity_type": et,
            "spec": spec,
            "conversion_template": template,
            "upload_source": source,
            "notes": notes,
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
    """Generate a Python/pandas conversion script for a wide-format file → md_format, md_format_gene, or md_format_metabolite.

    Works for any wide-format intensity matrix — DIA-NN, MaxQuant, Spectronaut,
    or a generic CSV/TSV. The standard orientation is entities (proteins, genes)
    in rows and samples in columns. Use transpose=True (or omit it to auto-detect)
    when the matrix is flipped: samples in rows and proteins in columns.

    md_format (protein): ProteinGroupId, ProteinGroup, GeneNames, SampleName,
                          ProteinIntensity, Imputed
    md_format (peptide): ModifiedSequence, StrippedSequence, ProteinGroup,
                          ProteinGroupId, GeneNames, SampleName, PeptideIntensity, Imputed
    md_format_gene:       GeneId, SampleName, GeneExpression
    md_format_metabolite: MetaboliteId, SampleName, MetaboliteIntensity, Imputed

    To get the full spec without a file, call get_md_format_spec(entity_type) first.

    ENTITY-DATA BOUNDARY: This tool reads ONLY the header row of the file.
    It never reads or processes intensity/expression values.
    The conversion script it generates must be run by the user locally —
    do not attempt to execute it yourself or read the resulting data.

    Parameters:
    - file_path:          path to the wide-format input file
    - target:             "md_format" (protein or peptide), "md_format_gene",
                          or "md_format_metabolite"
    - source_hint:        optional format name to improve auto-detection:
                          diann_tabular, maxquant, spectronaut, md_format_gene,
                          md_format_metabolite
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
        known_ann = _detect_annotation_cols(source_hint)
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
            file_path, detected_ann, sep, gene_id_col, do_transpose
        )
    elif target.lower() == "md_format_metabolite":
        spec = _MD_FORMAT_METABOLITE_SPEC
        metabolite_id_col = next(
            (
                h
                for h in detected_ann
                if h.lower()
                in (
                    "metaboliteid",
                    "metabolite id",
                    "metabolite_id",
                    "metabolite",
                    "compound",
                    "compoundid",
                    "compound_id",
                )
            ),
            detected_ann[0] if detected_ann else "MetaboliteId",
        )
        script = _build_metabolite_script(
            file_path, detected_ann, sep, metabolite_id_col, do_transpose
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
    if target.lower() == "md_format_gene":
        notes.append(
            "After converting, upload the output file using source='md_format_gene' "
            "in create_upload. You still need a sample_metadata CSV; experiment_design "
            "is OPTIONAL for md_format_gene "
            "(workflow/app/models/experiment.rb:98-103 skips the required check)."
        )
        notes.append(
            "GENE-SPECIFIC: md-converter auto-flags Imputed for gene data — every row "
            "where GeneExpression is NaN or 0 becomes Imputed=1 "
            "(md_format_gene/reader.py:120-124). The required output columns are only "
            "[GeneId, GeneExpression, SampleName] (reader.py:8 REQUIRED_GENE_COLUMNS). "
            "Do NOT manually emit Imputed for gene data — it will be overwritten."
        )
    elif target.lower() == "md_format_metabolite":
        notes.append(
            "After converting, upload the output file using "
            "source='md_format_metabolite' in create_upload. You still need an "
            "experiment_design CSV and a sample_metadata CSV — use "
            "load_metadata_from_csv or build them manually."
        )
        notes.append(
            "METABOLITE-SPECIFIC: Imputed is REQUIRED and validated as 0/1 by "
            "md-converter (md_format_metabolite/reader.py:80-82). Unlike "
            "md_format_gene it is NOT auto-derived — the generated script emits it. "
            "Every row where MetaboliteIntensity = 0.0 MUST have Imputed=1, otherwise "
            "downstream jobs treat zeros as real measurements."
        )
    else:
        notes.append(
            "After converting, upload the output file using source='md_format' "
            "in create_upload. You still need an experiment_design CSV and a "
            "sample_metadata CSV — use load_metadata_from_csv or build them manually."
        )
        notes.append(
            "CRITICAL (protein/peptide only): every row where ProteinIntensity / "
            "PeptideIntensity = 0.0 MUST have Imputed=1, otherwise downstream pairwise "
            "/ ANOVA jobs will treat zeros as real measurements and fail silently."
        )
    notes.append(
        "MD format is LONG format and MUST be a FULL matrix: exactly one row per "
        "entity per sample, with EVERY entity x sample combination present — NO "
        "EXCEPTIONS. Missing combinations are an error (md-converter rejects an "
        "incomplete matrix); represent a non-measurement as a row with intensity 0.0 "
        "and Imputed=1, never as an absent row. Melting a complete wide matrix (as "
        "this script does) produces a full matrix automatically."
    )
    notes.append(
        "SampleName values in the output must exactly match sample_name values "
        "in your sample_metadata. "
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


# ──────────────────────────────────────────────────────────────────────────────
# Pre-upload guard: catch non-UniProt ProteinGroup ids before they cause a
# silent server-side PTM site-mapping failure (0 sequences -> stuck "processing").
# ──────────────────────────────────────────────────────────────────────────────

# Patterns that are NOT valid UniProt accessions and break PTM site-mapping.
_ENSEMBL_RE = re.compile(r"^ENS[A-Z]*[GPT]\d{6,}", re.IGNORECASE)
# UniProt accession formats (Swiss-Prot 6-char + newer 10-char; allow isoform -N).
_UNIPROT_RE = re.compile(
    r"^[OPQ][0-9][A-Z0-9]{3}[0-9]" r"|^[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}",
    re.IGNORECASE,
)


@mcp.tool()
def validate_md_format_ids(file_path: str, delimiter: Optional[str] = None) -> str:
    """Check that an md_format file's ProteinGroup column holds UniProt accessions.

    Call this on a protein- OR peptide-level md_format data file BEFORE
    create_upload. It catches the most common silent failure for peptide/PTM
    uploads: ProteinGroup populated with Ensembl ids (ENSP/ENSG) or bare gene
    symbols instead of UniProt accessions. The platform maps PTM sites onto
    UniProt protein SEQUENCES; non-UniProt ids match 0 sequences and the upload
    fails silently (it sits in "processing" with no dataset, no surfaced error).

    Reads ONLY the header + a sample of rows (never the full file), so it is safe
    on multi-GB md_format files and respects the entity-data boundary — it looks
    at the ProteinGroup id column only, not at intensity values.

    Returns a prose verdict:
      "OK: ProteinGroup looks like UniProt accessions (N/M sampled)."  on pass, or
      "WARNING: ProteinGroup does not look like UniProt ..."           with the
      offending examples + the Ensembl→UniProt remediation hint, on fail.
    Returns "Error: ..." if the file is unreadable or has no ProteinGroup column.
    """
    if not os.path.exists(file_path):
        return f"Error: file not found: {file_path}"
    delim = delimiter or _sniff_delimiter(file_path)
    header, rows = _read_preview(file_path, delim, max_rows=500)
    if not header:
        return f"Error: could not read a header row from {file_path}"
    norm = [h.strip().lower() for h in header]
    if "proteingroup" not in norm:
        return (
            "Error: no ProteinGroup column found. This check applies to "
            "md_format protein/peptide files (gene/metabolite use a different id)."
        )
    idx = norm.index("proteingroup")
    vals = [r[idx].strip() for r in rows if len(r) > idx and r[idx].strip()]
    if not vals:
        return "Error: ProteinGroup column is present but empty in the sampled rows."

    # A group may be ';'-joined accessions — test the first member of each.
    firsts = [v.split(";")[0] for v in vals]
    ensembl = [v for v in firsts if _ENSEMBL_RE.match(v)]
    uniprot = [v for v in firsts if _UNIPROT_RE.match(v)]
    n = len(firsts)

    if ensembl:
        ex = sorted(set(ensembl))[:5]
        return (
            "WARNING: ProteinGroup contains Ensembl ids, not UniProt accessions "
            f"({len(ensembl)}/{n} sampled look like Ensembl, e.g. {ex}). "
            "PTM/peptide uploads map sites onto UniProt SEQUENCES — Ensembl ids "
            "resolve to 0 sequences and the upload will FAIL SILENTLY (stuck "
            "'processing', no dataset). FIX: convert ProteinGroup to UniProt "
            "accessions (UniProt ID-mapping Ensembl_Protein→UniProtKB), keep the "
            "peptide/protein ProteinGroupId mapping consistent, then re-verify "
            "that StrippedSequence values are substrings of the UniProt sequence "
            "for their ProteinGroup before uploading."
        )
    frac = len(uniprot) / n
    if frac < 0.5:
        ex = sorted(set(firsts))[:5]
        return (
            f"WARNING: only {len(uniprot)}/{n} sampled ProteinGroup values look "
            f"like UniProt accessions (e.g. {ex}). If these are gene symbols or "
            "another identifier, convert to UniProt accessions before upload — "
            "PTM site-mapping requires UniProt sequences."
        )
    return (
        f"OK: ProteinGroup looks like UniProt accessions ({len(uniprot)}/{n} "
        "sampled match). Note: this validates id FORMAT only — for peptide/PTM "
        "uploads also verify StrippedSequence values fall within the UniProt "
        "sequence for their ProteinGroup."
    )
