"""MOFA+ multi-omics factor analysis pipeline tool."""

from typing import List

from md_python.models.dataset_builders import MOFADataset

from .. import mcp
from .._client import get_client


@mcp.tool()
def run_mofa(
    input_dataset_ids: List[str],
    dataset_name: str,
    num_factors: int = 15,
    convergence_mode: str = "fast",
    scale_views: bool = True,
    center_groups: bool = True,
    max_iter: int = 1000,
    ard_factors: bool = True,
    drop_factor_threshold: float = 0.01,
) -> str:
    """Run a MOFA+ multi-omics factor analysis.

    MOFA+ (Multi-Omics Factor Analysis) integrates two or more INTENSITY
    datasets — "omics views", e.g. protein abundance + phosphoproteomics —
    into a set of latent factors capturing shared and view-specific
    variation. Output is a MOFA dataset with factor scores, factor
    loadings, and per-view variance explained.

    Returns: prose. Exact string "MOFA+ pipeline started. Dataset ID:
    <uuid>" on success. The "Dataset ID:" sentinel is stable.

    Use this when: the user has 2+ INTENSITY datasets on the SAME samples
    (different feature sets / omics layers) and wants an unsupervised
    integration into latent factors.

    Do NOT use this when: there is only one dataset (MOFA needs >= 2
    views), or the views do not share the same sample set — the backend
    fails with an explicit error if sample names do not match across
    views.

    INPUT REQUIREMENTS:
      * input_dataset_ids: >= 2 INTENSITY dataset UUIDs. These are the
        omics views. Each is a DATASET id (not an upload id). NI-output
        INTENSITY datasets are the typical input. Features need not
        overlap across views, but the SAMPLE set must be identical.
      * Input is expected in linear space — the backend log2-transforms
        every view (log2(x+1)) before training.

    Backend job slug: "mofa" (the published MOFA+ job). Parameter
    defaults / bounds are from the live job catalogue (/jobs -> slug
    "mofa", MOFAParams) and md-mofa src/md_mofa/process.py.

    ══ MANDATORY BEFORE CALLING ════════════════════════════════════════════════
    Present this parameter table to the user and wait for explicit confirmation
    before submitting. Do NOT choose any value autonomously.

    Parameter              Platform default   Options / notes
    ──────────────────────────────────────────────────────────────────────────────
    num_factors            15                 int 2-50. Upper bound on latent
                                               factors; MOFA auto-prunes factors
                                               below drop_factor_threshold, so the
                                               final count is data-driven.
    convergence_mode       "fast"             "fast" | "medium" | "slow". fast for
                                               exploratory runs, slow for final.
    scale_views            True               Scale each view to unit variance.
                                               Recommended when views have very
                                               different intensity ranges.
    center_groups          True               Center features per group at zero
                                               mean. Leave on unless already
                                               mean-centered.
    max_iter               1000               int 100-10000. Raise if the model
                                               has not converged.
    ard_factors            True               ARD sparsity prior on factors;
                                               combines with auto-prune so the
                                               factor count emerges from the data.
    drop_factor_threshold  0.01               float 0.0-0.1. Factors explaining
                                               less than this fraction of variance
                                               in EVERY view are dropped. 0
                                               disables auto-pruning.

    Explain each choice in plain language. Only proceed once the user has
    confirmed or explicitly asked you to use the recommended defaults.
    ═══════════════════════════════════════════════════════════════════════════════

    Errors:
      - ValueError: fewer than 2 input_dataset_ids; num_factors / max_iter
        / drop_factor_threshold out of range; bad convergence_mode.
      - APIError 422: views do not share the same sample set, or an input
        dataset is not an INTENSITY dataset.

    Guardrails:
      - MOFA needs >= 2 views — never call with a single dataset.
      - input_dataset_ids are DATASET ids, not upload ids.
    """
    dataset_id = MOFADataset(
        input_dataset_ids=input_dataset_ids,
        dataset_name=dataset_name,
        num_factors=num_factors,
        convergence_mode=convergence_mode,
        scale_views=scale_views,
        center_groups=center_groups,
        max_iter=max_iter,
        ard_factors=ard_factors,
        drop_factor_threshold=drop_factor_threshold,
    ).run(get_client())
    return f"MOFA+ pipeline started. Dataset ID: {dataset_id}"
