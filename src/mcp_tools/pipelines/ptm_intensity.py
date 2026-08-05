"""PTM Intensity Table (PTM site summarisation) pipeline tool."""

from typing import List, Optional

from md_python.models.dataset_builders import PtmIntensityTableDataset

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_ptm_intensity_table(
    input_dataset_ids: List[str],
    dataset_name: str,
    entity_type: str = "peptide",
    loc_probability_threshold: float = 0.0,
    modification_types: Optional[List[str]] = None,
    summarization_method: str = "sum",
    include_imputed: bool = False,
    impute_missing_values: bool = True,
    flanking_window: int = 20,
) -> str:
    """Re-run PTM site summarisation (rollup) with custom parameters.

    Collapses the peptide intensities that report the same protein modification
    site — different charge states, missed cleavages, co-occurring mods — into
    ONE row per site per sample. Produces site-level ``PTM_Intensity`` +
    ``PTM_Metadata`` tables, plus a ``PTM_unmapped`` table listing sites that
    could not be placed on a protein sequence.

    Returns: prose. Exact string "PTM intensity table started. Dataset ID:
    <uuid>" on success. The "Dataset ID:" sentinel is stable.

    Use this when: the user wants a DIFFERENT rollup from the one created
    automatically at upload — a stricter localisation threshold, only certain
    modifications, median/TMP instead of sum, or different imputation.

    Do NOT use this when: the user just wants to analyse the PTM dataset that
    already exists. Every upload containing modified peptides ALREADY has a PTM
    dataset built with the defaults below — find it with find_initial_dataset /
    list_datasets rather than re-running this.

    INPUT REQUIREMENTS:
      * input_dataset_ids: exactly ONE **peptide** INTENSITY dataset UUID (a
        DATASET id, not an upload id) that carries a ``PTM_sites`` table — i.e.
        the upload contained modified peptides. The flow raises if that table is
        absent. Confirm with list_dataset_tables before submitting.

    WITH EVERY DEFAULT LEFT ALONE THE OUTPUT REPRODUCES THE UPLOAD-TIME PTM
    TABLES EXACTLY. If the user does not want to change anything, they do not
    need this tool.

    Backend job slug: "ptm_intensity_table" (JOB_NAME "ptm intensity table";
    output dataset type INTENSITY, server-derived from the slug). Parameter
    names, bounds and defaults are from md-converter
    flows/ptm_intensity_dataset_types.py::PTMIntensityTableParamsProperties.
    If the server rejects the slug, call list_jobs() to read the live catalogue
    and pass the correct value — the builder's job_slug is overridable.

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter                  Platform default  Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    entity_type                "peptide"         "peptide" ONLY. Site
                                                 summarisation reads PTM_sites,
                                                 which only peptide datasets
                                                 carry.
    loc_probability_threshold  0.0               float 0.0-1.0. Keeps sites with
                                                 PTMProbSample >= threshold.
                                                 0 keeps everything. 0.75 is the
                                                 conventional phospho starting
                                                 point.
    modification_types         [] (all)          Subset of: Acetylation,
                                                 Oxidation, Phosphorylation,
                                                 Carbamidomethylation,
                                                 Deamidation, Pyro-glu,
                                                 Met-loss, TMTpro. A site is
                                                 kept if it carries AT LEAST ONE
                                                 selected mod. [] keeps all.
    summarization_method       "sum"             "sum" | "median" | "tmp"
                                                 (Tukey Median Polish).
    include_imputed            False             Include imputed PEPTIDE
                                                 intensities in the rollup.
    impute_missing_values      True              Impute missing SITE-level
                                                 intensities after rollup
                                                 (MNAR / Perseus). Off leaves
                                                 zero placeholders.
    flanking_window            20                int 0-30. Residues of sequence
                                                 context kept each side of the
                                                 modified residue
                                                 (LeftFlank / RightFlank).

    Explain each choice in plain language. Only proceed once the user confirms.
    ═══════════════════════════════════════════════════════════════════════════════

    PARAMETERS THAT ARE EASY TO GET WRONG — say these out loud to the user:
      * The threshold filters **PTMProbSample** (best precursor for that site in
        that ONE replicate), NOT PTMProbMax (best across all samples). A site can
        pass in one replicate and fail in another.
      * `include_imputed` and `impute_missing_values` are DIFFERENT and
        independent. The first concerns the PEPTIDE intensities feeding the
        rollup (off by default); the second concerns the SITE-level output after
        it (on by default).
      * A threshold filter does NOTHING on data with no real localisation
        probabilities. Only `diann_tabular` (with report.parquet) and
        `spectronaut` (with the PTM-localisation schema) supply them; an
        `md_format` upload sets every probability to 1.0. Check the upload source
        before recommending a threshold, or the filter will look like quality
        control while removing nothing.
      * "tmp" is not a drop-in for "sum" — it works in log space and is
        exponentiated back, so it yields a different scale. Do not compare
        datasets summarised by different methods.
      * A stricter threshold means fewer sites tested, hence less severe
        multiple-testing correction. DE results across thresholds are not
        directly comparable.

    Errors:
      - ValueError: not exactly 1 input dataset; entity_type != "peptide";
        unsupported modification_types; numeric bounds out of range.
      - APIError 422: input is not a peptide intensity dataset, or carries no
        PTM_sites table.

    Guardrails:
      - input_dataset_ids are DATASET ids, not upload ids.
      - The rollup fetches protein sequences from UniProt at runtime and
        re-fetches on every run, so it is not instantaneous and can fail for
        reasons unrelated to the data. A large PTM_unmapped table usually means
        an accession problem (non-UniProt ids, obsolete accessions), not a
        modification problem.
      - Site GroupIds are re-assigned per run. When comparing two rollups, join
        on PTMProtein, never on GroupId.
    """
    dataset_id = PtmIntensityTableDataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        entity_type=entity_type,
        loc_probability_threshold=loc_probability_threshold,
        modification_types=modification_types,
        summarization_method=summarization_method,
        include_imputed=include_imputed,
        impute_missing_values=impute_missing_values,
        flanking_window=flanking_window,
    ).run(get_client())
    return f"PTM intensity table started. Dataset ID: {dataset_id}"
