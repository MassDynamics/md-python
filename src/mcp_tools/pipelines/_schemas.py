"""Pipeline parameter schemas — single source of truth for all pipeline types.

WIRE FORMAT
-----------
The dataset service validates ``job_run_params`` against the converter's flat
Pydantic models (e.g. ``NormalisationAndImputationParamsProperties`` in
md-converter/src/flows/intensity_imputation_types.py). All keys are emitted at
the top level of ``job_run_params`` — there is no nested
``{"normalisation_methods": {"method": "median"}}`` form. Method literal
strings use the converter's canonical (spaced) form: ``"batch correction"``,
``"by missing values"``, ``"by ptm localization probability"``,
``"by minimum abundance"``, ``"limma remove batch effect"``, ``"combat seq"``.
Underscored aliases on input are accepted for backward compatibility (see
NormalisationImputationDataset._METHOD_ALIAS_MAP) but are deprecated; the
wire payload always uses the spaced form.

Source-of-truth references
--------------------------
- NI: md-converter/src/flows/intensity_imputation_types.py
  (NormalisationAndImputationParamsProperties).
- Pairwise / ANOVA:
  MDFlexiComparisons/src/md_flexi_comparisons/process_r.py.
- Dose response:
  data-set-service/src/flows/utils/type_defs.py (DoseResponseParams).
- Workflow API: workflow/app/api/api/v2/datasets/create.rb.
- CAMERA GSEA `sets` (species-conditional knowledge bases):
  md_python/models/dataset_builders/_gsea_sets.py, transcribed from the live
  /jobs catalogue (slug camera_gsea -> sets.parameters.options.cases).

Update this file whenever the converter or data-set-service adds new methods
or parameters; keep the citations above current.
"""

from typing import Any, Dict, List

from md_python.models.dataset_builders._gsea_sets import (
    GSEA_DEFAULT_SETS,
    GSEA_SETS_BY_SPECIES,
)


def _gsea_sets_union() -> List[str]:
    """Every camera_gsea knowledge base, any species, catalogue order, deduped."""
    union: List[str] = []
    for values in GSEA_SETS_BY_SPECIES.values():
        union.extend(v for v in values if v not in union)
    return union


# All job slugs available on this account (from GET /api/v2/jobs).
# Standard user-facing pipelines are marked with *.
AVAILABLE_JOB_SLUGS: Dict[str, str] = {
    "normalisation_imputation": "* Normalise and impute intensity data",
    "pairwise_comparison": "* Pairwise differential abundance (limma)",
    "anova": "* ANOVA differential abundance across 3+ conditions (limma)",
    "dose_response": "* Dose-response curve fitting (4PL log-logistic)",
    "dose_response_aggregate": "Aggregate multiple dose-response results into a summary table",
    "mofa": "* MOFA+ multi-omics factor analysis (integrate 2+ INTENSITY views)",
    "ora": "* Over-Representation Analysis of a foreground entity list (hypergeometric)",
    "camera_gsea": "* Gene-set enrichment analysis (CAMERA, competitive test)",
    "wgcna": "* WGCNA weighted co-expression network + module-trait correlation",
    "knn_tn_imputation": "Custom KNN-TN (truncated normal) imputation",
    "intensity": "Internal: raw intensity ingestion",
    "initial_job": "Internal: initial dataset creation",
    "md_dataset_custom_r": "Custom R script dataset",
    "demo_flow": "Demo/test job",
    "md_hello_world": "Demo/test job",
    "basic_r_to_say_hi": "Demo/test job",
    "jnj_dose_response": "Custom dose-response variant",
}

_SHARED_FILTER_BLOCK: Dict[str, Any] = {
    "filter_valid_values_criteria": {
        "type": "str",
        "required": True,
        "valid_values": ["percentage", "count"],
        "default": "percentage",
        "description": (
            "Whether the threshold is expressed as a fraction or an absolute count "
            "of valid (non-NA, non-imputed) values per entity."
        ),
    },
    "filter_threshold_proportion": {
        "type": "float",
        "required": False,
        "default": 0.5,
        "range": "0.0–1.0",
        "description": "Minimum proportion of valid values per entity (when criteria='percentage').",
    },
    "filter_threshold_count": {
        "type": "int",
        "required": False,
        "default": 3,
        "range": "≥1",
        "description": "Minimum count of valid values per entity (when criteria='count').",
    },
    "filter_valid_values_logic": {
        "type": "str",
        "required": False,
        "default": "at least one condition",
        "valid_values": [
            "all conditions",
            "at least one condition",
            "full experiment",
        ],
        "description": (
            "How the threshold is applied across conditions. The first two values "
            "require filter_based_on_condition."
        ),
    },
    "filter_based_on_condition": {
        "type": "str",
        "required": False,
        "description": (
            "Column name (from experiment_design) that defines conditions. "
            "REQUIRED when filter_valid_values_logic ∈ "
            "{'all conditions', 'at least one condition'}."
        ),
    },
    "experiment_design": {
        "type": "Dict[str, List]  (SampleMetadata.to_columns())",
        "required": True,
        "description": (
            "Sample metadata as column dict. Pass SampleMetadata(...).to_columns() "
            "from load_metadata_from_csv output."
        ),
    },
}


_PIPELINE_SCHEMAS: Dict[str, Any] = {
    "normalisation_imputation": {
        "description": (
            "Filter, normalise, and impute an intensity dataset. The job runs "
            "filtration → normalisation → imputation (in that order); each step "
            "can be skipped independently. Output is an INTENSITY dataset. "
            "Filter-only is a valid pattern: pass normalisation_method='skip' + "
            "imputation_method='skip' + a filtration_method."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "entity_type",
            "normalisation_method",
            "imputation_method",
        ],
        "guidance": (
            "Always present ALL method parameters to the user and ask whether to "
            "keep defaults or change them. For standard DDA proteomics, 'mnar' "
            "imputation is preferred. Wire-format method strings use the converter "
            "canonical (spaced) form (e.g. 'batch correction', 'by missing values'); "
            "underscored aliases are accepted for backward compatibility but are "
            "deprecated. Pairwise additions (HR, edgeR, DESeq2) are NOT exposed by "
            "this MCP — pairwise ships as limma-only."
        ),
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "entity_type": {
                "type": "str",
                "required": True,
                "valid_values": ["protein", "peptide", "gene", "metabolite", "ptm"],
                "description": (
                    "Entity level of the input dataset. Must match the upstream INTENSITY "
                    "dataset (e.g. md_format_gene uploads → 'gene', md_format_metabolite "
                    "→ 'metabolite', phospho uploads → 'ptm'). Drives which normalisation "
                    "/ filtration methods are valid. Wire format is lowercase — the UI "
                    "shows 'PTM' / 'Metabolite' but the backend stores them lowercase "
                    "(confirmed against live job_run_params 2026-05-27). Metabolite NI "
                    "is currently upstream-gated by md-converter and may 422."
                ),
            },
            "normalisation_method": {
                "type": "str",
                "valid_values": [
                    "skip",
                    "median",
                    "quantile",
                    "sum",
                    "batch correction",
                    "cpm",
                ],
                "valid_values_per_entity_type": {
                    "protein": [
                        "skip",
                        "median",
                        "quantile",
                        "sum",
                        "batch correction",
                    ],
                    "peptide": [
                        "skip",
                        "median",
                        "quantile",
                        "sum",
                        "batch correction",
                    ],
                    "gene": [
                        "skip",
                        "median",
                        "quantile",
                        "sum",
                        "batch correction",
                        "cpm",
                    ],
                    "ptm": [
                        "skip",
                        "median",
                        "quantile",
                        "sum",
                        "batch correction",
                    ],
                    "metabolite": [
                        "skip",
                        "median",
                        "quantile",
                        "sum",
                        "batch correction",
                    ],
                },
                "method_params": {
                    "skip": "No extra parameters. Skips normalisation entirely.",
                    "median": {
                        "median_normalisation_centre_at_zero": {
                            "type": "bool",
                            "required": False,
                            "default": True,
                            "description": (
                                "When True (default): centres each sample's median at zero "
                                "in log2 space. When False: preserves the overall intensity "
                                "level after normalisation."
                            ),
                        },
                        "include_imputed_values": {
                            "type": "bool",
                            "required": False,
                            "default": False,
                            "description": (
                                "Include previously-imputed values in the per-sample median "
                                "calculation. Default False (use only observed values)."
                            ),
                        },
                    },
                    "quantile": {
                        "include_imputed_values": {
                            "type": "bool",
                            "required": False,
                            "default": False,
                            "description": (
                                "Include previously-imputed values in the quantile mapping."
                            ),
                        },
                    },
                    "sum": {
                        "include_imputed_values": {
                            "type": "bool",
                            "required": False,
                            "default": False,
                            "description": (
                                "Include previously-imputed values in the per-sample sum."
                            ),
                        },
                    },
                    "batch correction": {
                        "batch_correction_technique": {
                            "type": "str",
                            "required": True,
                            "valid_values": [
                                "limma remove batch effect",
                                "combat",
                                "combat seq",
                            ],
                            "valid_values_per_entity_type": {
                                "protein": ["limma remove batch effect", "combat"],
                                "peptide": ["limma remove batch effect", "combat"],
                                "gene": [
                                    "limma remove batch effect",
                                    "combat",
                                    "combat seq",
                                ],
                            },
                            "description": (
                                "Engine for batch correction. Decision rules: "
                                "'combat' — empirical-Bayes, single batch column, "
                                "use when batches are confounded but each batch has ≥3 samples. "
                                "'limma remove batch effect' — linear-model adjustment, "
                                "supports multiple batch columns; use when batches are "
                                "well-separated and a fast adjustment suffices. "
                                "'combat seq' — count-data ComBat, gene/RNA-seq only."
                            ),
                        },
                        "batch_variables": {
                            "type": "List[Dict]",
                            "required": "limma only",
                            "description": (
                                "Required for limma remove batch effect. List of "
                                "{column: str, type: 'categorical'} entries — one per batch column."
                            ),
                        },
                        "batch_variable_combat": {
                            "type": "str",
                            "required": "combat / combat seq only",
                            "description": (
                                "Single batch column name. Required for combat and combat seq."
                            ),
                        },
                        "mean_only": {
                            "type": "bool",
                            "required": False,
                            "default": False,
                            "description": (
                                "ComBat only. When True corrects only the mean, not variance. "
                                "Use for low-feature datasets where empirical-Bayes variance "
                                "estimation is unreliable."
                            ),
                        },
                        "reference_batch_combat": {
                            "type": "str",
                            "required": False,
                            "description": (
                                "ComBat only. Optional reference batch value (must appear in "
                                "the batch column). Other batches are corrected toward this one."
                            ),
                        },
                        "design_variables": {
                            "type": "List[Dict]",
                            "required": False,
                            "description": (
                                "Optional biological covariates to preserve (e.g. condition). "
                                "Each entry: {column: str, type: 'categorical'|'numerical'}."
                            ),
                        },
                        "include_imputed_values": {
                            "type": "bool",
                            "required": False,
                            "default": False,
                            "description": "Use previously-imputed values during correction.",
                        },
                        "experiment_design": {
                            "type": "Dict[str, List]  (SampleMetadata.to_columns())",
                            "required": True,
                            "description": (
                                "Sample metadata as column dict. Pass "
                                "SampleMetadata(...).to_columns() from load_metadata_from_csv."
                            ),
                        },
                    },
                    "cpm": {
                        "prior_count": {
                            "type": "float",
                            "required": False,
                            "default": 0,
                            "range": "0–10",
                            "description": (
                                "Prior count added before CPM (edgeR convention). "
                                "Use 0 for plain CPM. Gene entity_type only."
                            ),
                        },
                    },
                },
                "description": (
                    "Normalisation algorithm. 'median': subtract per-sample median in log2 "
                    "space — robust, recommended. 'quantile': force samples to the same "
                    "quantile distribution. 'sum': scale by per-sample sum. 'batch correction': "
                    "remove batch effects (requires batch_correction_technique sub-selection). "
                    "'cpm': Counts Per Million — gene data only. 'skip': no normalisation."
                ),
            },
            "imputation_method": {
                "type": "str",
                "valid_values": [
                    "skip",
                    "mnar",
                    "knn",
                    "knn_tn",
                    "global_median",
                    "median_by_entity",
                    "mindet",
                    "set to constant",
                    "set to missing",
                ],
                "method_params": {
                    "mnar": {
                        "std_position": {
                            "type": "float",
                            "required": False,
                            "default": 1.8,
                            "range": "0.0–3.0",
                            "description": (
                                "Mean shift in standard deviations below the observed mean. "
                                "Higher = more aggressive left-shift."
                            ),
                        },
                        "std_width": {
                            "type": "float",
                            "required": False,
                            "default": 0.3,
                            "range": "0.0–1.0",
                            "description": (
                                "Width of the imputed Gaussian as a fraction of the observed std."
                            ),
                        },
                    },
                    "knn": {
                        "n_neighbors": {
                            "type": "int",
                            "required": False,
                            "default": 3,
                            "range": "1–10",
                            "description": "Number of nearest neighbours.",
                        },
                        "weights": {
                            "type": "str",
                            "required": False,
                            "default": "uniform",
                            "valid_values": ["uniform", "distance"],
                            "description": (
                                "'uniform' (default): equal-weighted neighbours. "
                                "'distance': closer neighbours contribute more."
                            ),
                        },
                    },
                    "knn_tn": {
                        "knn_tn_k": {
                            "type": "int",
                            "required": False,
                            "default": 5,
                            "range": "1–10",
                            "description": "Adaptive k for truncated-normal KNN imputation.",
                        },
                        "knn_tn_distance": {
                            "type": "str",
                            "required": False,
                            "default": "truncation",
                            "valid_values": ["truncation", "correlation"],
                            "description": (
                                "Distance metric used by KNN-TN. 'truncation' (default) is "
                                "the truncated-normal MLE distance; 'correlation' is the "
                                "Pearson-based variant."
                            ),
                        },
                    },
                    "global_median": (
                        "No extra parameters. Replaces all missing values with the global "
                        "median intensity."
                    ),
                    "median_by_entity": (
                        "No extra parameters. Replaces each missing value with that entity's "
                        "own median intensity."
                    ),
                    "mindet": {
                        "q": {
                            "type": "float",
                            "required": False,
                            "default": 0.01,
                            "range": "0.0–0.5",
                            "description": (
                                "Quantile used to estimate the minimum detectable value per "
                                "sample (Perseus-style)."
                            ),
                        },
                    },
                    "set to constant": {
                        "constant_value": {
                            "type": "int",
                            "required": False,
                            "default": 0,
                            "range": "0–100",
                            "description": "Fixed value substituted for every missing entry.",
                        },
                    },
                    "set to missing": (
                        "No extra parameters. Output NaN for every imputed position."
                    ),
                    "skip": "No extra parameters. Leaves data unchanged.",
                },
                "description": (
                    "Imputation algorithm. 'mnar' (PREFERRED for standard DDA): Perseus-style "
                    "left-tail Gaussian. 'knn' / 'knn_tn': K-nearest-neighbour imputation; "
                    "knn_tn uses a truncated-normal MLE and an adaptive k (recommended for "
                    "small experiments). 'mindet': impute at a low per-sample quantile. "
                    "'global_median' / 'median_by_entity': simple median fills. "
                    "'set to constant': replace with a fixed value. 'set to missing': output NaN. "
                    "'skip': leave unchanged."
                ),
            },
            "filtration_method": {
                "type": "str",
                "required": False,
                "default": "skip",
                "valid_values": [
                    "skip",
                    "by missing values",
                    "by ptm localization probability",
                    "by minimum abundance",
                ],
                "valid_values_per_entity_type": {
                    "protein": ["skip", "by missing values"],
                    "peptide": [
                        "skip",
                        "by missing values",
                        "by ptm localization probability",
                    ],
                    "gene": ["skip", "by minimum abundance"],
                    "ptm": [
                        "skip",
                        "by missing values",
                        "by ptm localization probability",
                    ],
                    "metabolite": ["skip", "by missing values"],
                },
                "method_params": {
                    "skip": "No extra parameters. Filtration is skipped.",
                    "by missing values": _SHARED_FILTER_BLOCK,
                    "by ptm localization probability": {
                        "threshold": {
                            "type": "float",
                            "required": False,
                            "default": 0.5,
                            "range": "0.0–1.0",
                            "description": (
                                "PTM sites with PTMProbMax ≥ threshold are retained. "
                                "Peptide entity_type only."
                            ),
                        },
                    },
                    "by minimum abundance": {
                        "minimum_abundance_threshold": {
                            "type": "float",
                            "required": False,
                            "default": 0,
                            "range": "0–100",
                            "description": (
                                "Values strictly above this threshold are valid. Gene only — "
                                "typically used after CPM."
                            ),
                        },
                        **_SHARED_FILTER_BLOCK,
                    },
                },
                "description": (
                    "Optional filtration applied BEFORE normalisation. The output is still an "
                    "INTENSITY dataset. 'by missing values' (protein/peptide): drop entities "
                    "that fail a completeness criterion. 'by ptm localization probability' "
                    "(peptide): drop low-confidence PTM sites. 'by minimum abundance' (gene): "
                    "drop low-count genes after CPM."
                ),
            },
            "normalisation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Forward-compat escape hatch for normalisation params. Typed kwargs "
                    "(median_normalisation_centre_at_zero, include_imputed_values, "
                    "batch_correction_technique, batch_variables, batch_variable_combat, "
                    "mean_only, reference_batch_combat, design_variables, experiment_design, "
                    "prior_count) are preferred. Anything passed here is merged LAST and "
                    "overrides typed values."
                ),
            },
            "imputation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Forward-compat escape hatch for imputation params. See method_params for "
                    "per-method keys. Merged LAST and overrides typed values."
                ),
            },
            "filtration_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Forward-compat escape hatch for filtration params. See method_params for "
                    "per-method keys. Merged LAST and overrides typed values."
                ),
            },
        },
    },
    "anova": {
        "description": (
            "Run ANOVA-based differential abundance analysis across multiple conditions "
            "using limma linear models. Use when comparing 3+ groups simultaneously. "
            "Supports both 'all comparisons' and custom comparison subsets."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_metadata",
            "condition_column",
            "filter_values_criteria",
        ],
        "guidance": "Always ask the user which parameters to use before calling this tool.",
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs (normalised/imputed).",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and condition_column.",
            },
            "condition_column": {
                "type": "str",
                "description": "Column in sample_metadata defining groups to compare.",
            },
            "comparisons_type": {
                "type": "str",
                "default": "all",
                "valid_values": ["all", "custom"],
                "description": (
                    "'all': test all pairwise comparisons between condition levels. "
                    "'custom': specify a subset via condition_comparisons."
                ),
            },
            "condition_comparisons": {
                "type": "List[List[str]]",
                "required": False,
                "default": [],
                "description": (
                    "Custom [case, control] pairs to test. Only used when comparisons_type='custom'."
                ),
            },
            "filter_values_criteria": {
                "type": "Dict[str, Any]",
                "required": True,
                "default": {"method": "percentage", "filter_threshold_percentage": 0.5},
                "description": (
                    "Valid-value completeness filter. "
                    "{'method': 'percentage', 'filter_threshold_percentage': 0.0–1.0} "
                    "or {'method': 'count', 'filter_threshold_count': int ≥ 1}."
                ),
            },
            "filter_valid_values_logic": {
                "type": "str",
                "default": "at least one condition",
                "valid_values": [
                    "all conditions",
                    "at least one condition",
                    "full experiment",
                ],
                "description": (
                    "Logic for applying the valid-value filter. "
                    "'at least one condition' (default): keep rows that pass the threshold "
                    "in at least one condition. "
                    "'all conditions': must pass in every condition. "
                    "'full experiment': must pass across the whole dataset."
                ),
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": (
                    "Allow intensity-dependent trend for prior variances (limma-trend, "
                    "Law et al. 2014). Recommended: True."
                ),
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": (
                    "Use robust empirical Bayes moderation (Phipson et al. 2016). "
                    "Protects against hyper/hypo-variable proteins. Recommended: True."
                ),
            },
            "filter_method": {
                "type": "str",
                "default": "percentage",
                "valid_values": ["percentage", "count"],
                "description": (
                    "Completeness criterion: 'percentage' uses "
                    "filter_threshold_percentage in [0,1]; 'count' uses "
                    "filter_threshold_count (int >= 1)."
                ),
            },
            "filter_threshold_count": {
                "type": "Optional[int]",
                "required": "when filter_method='count'",
                "default": None,
                "range": ">= 1",
                "description": (
                    "Minimum count of valid samples per condition. Required when "
                    "filter_method='count'. Source-of-truth: "
                    "ANOVAParamsProperties.filter_threshold_count "
                    "(process_r.py:518-529)."
                ),
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "peptide", "gene", "metabolite", "ptm"],
                "description": (
                    "Entity level to analyse. Wire format is lowercase (UI shows "
                    "'PTM' / 'Metabolite' but the backend stores them lowercase)."
                ),
            },
            "de_method": {
                "type": "str",
                "default": "limma",
                "valid_values": ["limma", "edgeR", "DESeq2"],
                "valid_values_per_entity_type": {
                    "protein": ["limma"],
                    "peptide": ["limma"],
                    "gene": ["limma", "edgeR", "DESeq2"],
                    "metabolite": ["limma"],
                    "ptm": ["limma"],
                },
                "description": (
                    "Differential-expression engine. Same shape as pairwise — only "
                    "entity_type='gene' allows edgeR / DESeq2. Wire field is "
                    "entity-keyed: ``de_method_<entity_type>``."
                ),
            },
            "edger_norm_method": {
                "type": "str",
                "default": "TMM",
                "valid_values": ["TMM", "RLE", "upperquartile", "none"],
                "description": (
                    "Library size normalisation method for edgeR. Only used when "
                    "de_method='edgeR' (entity_type='gene' only)."
                ),
            },
            "deseq2_lfc_shrinkage": {
                "type": "str",
                "default": "none",
                "valid_values": ["none", "apeglm", "ashr", "normal"],
                "description": (
                    "Log-fold-change shrinkage method for DESeq2. Only used when "
                    "de_method='DESeq2' (entity_type='gene' only)."
                ),
            },
            "deseq2_alpha": {
                "type": "float",
                "default": 0.05,
                "range": "0.0–1.0",
                "description": (
                    "DESeq2 independent-filtering FDR threshold. Set this to the "
                    "downstream FDR threshold you intend to apply. Only used when "
                    "de_method='DESeq2'."
                ),
            },
            "apeglm_seed": {
                "type": "int",
                "default": 1,
                "description": (
                    "RNG seed for apeglm. Only used when de_method='DESeq2' AND "
                    "deseq2_lfc_shrinkage='apeglm'."
                ),
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": (
                    "Covariates to add to the limma design matrix (e.g. batch, sex). "
                    "Each entry: {'column': str, 'type': 'categorical'|'numerical'}. "
                    "Wrapped on the wire as {'control_variables': [...]}. "
                    "Source-of-truth: process_r.py:69-71 (entry shape) and "
                    "process_r.py:380-386 (ANOVAParamsProperties.control_variables)."
                ),
            },
        },
    },
    "dose_response": {
        "description": (
            "Fit dose-response curves to intensity data using a four-parameter "
            "log-logistic (4PL) model. "
            "MINIMUM DATA REQUIREMENTS: at least 3 distinct dose levels and at "
            "least 5 total replicates across all doses (3+ replicates per dose "
            "recommended). The pipeline will fail if these minimums are not met."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_names",
            "control_samples",
            "sample_metadata",
            "dose_column",
        ],
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "sample_names": {
                "type": "List[str]",
                "description": "All sample names included in the analysis. Must match sample_name values in sample_metadata exactly.",
            },
            "control_samples": {
                "type": "List[str]",
                "default": [],
                "description": (
                    "Samples nominated as controls (dose = 0). Used to anchor the baseline. "
                    "Ask the user which samples are controls — never guess."
                ),
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and dose_column. Dose values are converted to numbers.",
            },
            "dose_column": {
                "type": "str",
                "default": "dose",
                "description": "Column in sample_metadata containing dose values. All values must be numeric.",
            },
            "log_intensities": {
                "type": "bool",
                "default": True,
                "description": "Apply log2 transformation to intensities before fitting. Recommended: True.",
            },
            "use_imputed_intensities": {
                "type": "bool",
                "default": False,
                "description": (
                    "Use imputed intensity values from a prior normalisation_imputation step. "
                    "Platform default False (data-set-service DoseResponseParams.use_imputed_intensities). "
                    "Set True to include NI-imputed values; False uses only observed values."
                ),
            },
            "normalise": {
                "type": "str",
                "default": "none",
                "valid_values": ["none", "sum", "median"],
                "description": (
                    "Within-sample normalisation applied before curve fitting. "
                    "'none' (default): recommended when data has already been normalised upstream. "
                    "'median': normalizeMedianAbsValues() from limma (Ritchie et al. 2015). "
                    "'sum': scale by ratio of median sum to per-sample sum (Zecha et al. 2018)."
                ),
            },
            "span_rollmean_k": {
                "type": "int",
                "default": 1,
                "range": "1 to N distinct dose values",
                "description": (
                    "Rolling mean window for computing observed span. "
                    "k=1 (default): no smoothing — fine detail. "
                    "k=3: typically adequate smoothing. "
                    "Max k is capped at the number of distinct dose values."
                ),
            },
            "prop_required_in_protein": {
                "type": "float",
                "default": 0.5,
                "range": "0.0–1.0",
                "description": (
                    "Minimum proportion of replicates with non-missing values required per protein. "
                    "Use 0 to include all proteins regardless of missing data."
                ),
            },
        },
    },
    "pairwise_comparison": {
        "description": "Run limma-based pairwise differential expression analysis.",
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_metadata",
            "condition_column",
            "condition_comparisons",
        ],
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Non-empty list of input dataset UUIDs.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output dataset.",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and condition_column.",
            },
            "condition_column": {
                "type": "str",
                "description": "Column in sample_metadata defining groups to compare (e.g. 'condition').",
            },
            "condition_comparisons": {
                "type": "List[List[str]]",
                "description": "List of [case, control] pairs. Use generate_pairwise_comparisons to build these.",
            },
            "filter_valid_values_logic": {
                "type": "str",
                "default": "at least one condition",
                "valid_values": [
                    "all conditions",
                    "at least one condition",
                    "full experiment",
                ],
                "description": (
                    "Logic for the valid-value completeness filter. "
                    "'at least one condition' (default): keep rows that pass in at least one compared condition. "
                    "'all conditions': must pass in every condition — more stringent. "
                    "'full experiment': must pass across the entire experiment — most stringent."
                ),
            },
            "filter_method": {
                "type": "str",
                "default": "percentage",
                "valid_values": ["percentage", "count"],
                "description": "Completeness filter method: fraction of valid values ('percentage') or absolute count ('count').",
            },
            "filter_threshold_percentage": {
                "type": "float",
                "default": 0.5,
                "range": "0.0–1.0",
                "description": "Minimum fraction of valid values required (used when filter_method='percentage').",
            },
            "fit_separate_models": {
                "type": "bool",
                "default": True,
                "description": (
                    "Fit a separate limma model per comparison. "
                    "True (default): each comparison filters entities independently — "
                    "reduces impact of conditions with many missing values."
                ),
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": "Allow intensity-dependent trend for prior variances (Law et al. 2014). Recommended: True.",
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": "Robust empirical Bayes moderation (Phipson et al. 2016). Recommended: True.",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "peptide", "gene", "metabolite", "ptm"],
                "description": (
                    "Entity level to analyse. Wire format is lowercase (UI shows "
                    "'PTM' / 'Metabolite' but the backend stores them lowercase — "
                    "confirmed against live job_run_params 2026-05-27)."
                ),
            },
            "de_method": {
                "type": "str",
                "default": "limma",
                "valid_values": ["limma", "edgeR", "DESeq2"],
                "valid_values_per_entity_type": {
                    "protein": ["limma"],
                    "peptide": ["limma"],
                    "gene": ["limma", "edgeR", "DESeq2"],
                    "metabolite": ["limma"],
                    "ptm": ["limma"],
                },
                "description": (
                    "Differential-expression engine. Only entity_type='gene' "
                    "exposes a real choice; every other entity_type is hard-pinned "
                    "to 'limma' by the MDFlexiComparisons Pydantic schema. The MCP "
                    "rejects edgeR / DESeq2 for any non-gene entity_type "
                    "client-side. Wire field is entity-keyed: ``de_method_<entity_type>``."
                ),
            },
            "edger_norm_method": {
                "type": "str",
                "default": "TMM",
                "valid_values": ["TMM", "RLE", "upperquartile", "none"],
                "description": (
                    "Library size normalisation method for edgeR. Only used when "
                    "de_method='edgeR' (entity_type='gene' only)."
                ),
            },
            "deseq2_lfc_shrinkage": {
                "type": "str",
                "default": "none",
                "valid_values": ["none", "apeglm", "ashr", "normal"],
                "description": (
                    "Log-fold-change shrinkage method for DESeq2. Only used when "
                    "de_method='DESeq2' (entity_type='gene' only). 'apeglm' is the "
                    "recommended modern choice for ranking."
                ),
            },
            "deseq2_alpha": {
                "type": "float",
                "default": 0.05,
                "range": "0.0–1.0",
                "description": (
                    "DESeq2 independent-filtering FDR threshold (alpha). SET TO THE "
                    "FDR THRESHOLD YOU WILL APPLY DOWNSTREAM — DESeq2's independent "
                    "filtering optimises the rejection set at this alpha, so "
                    "mismatched values silently lose statistical power. Only used "
                    "when de_method='DESeq2'."
                ),
            },
            "apeglm_seed": {
                "type": "int",
                "default": 1,
                "description": (
                    "RNG seed for apeglm shrinkage. Only used when "
                    "de_method='DESeq2' AND deseq2_lfc_shrinkage='apeglm'. apeglm's "
                    "posterior optimisation uses random initialisation for some "
                    "genes; fixing this seed guarantees reproducible results."
                ),
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": (
                    "Covariates to include in the limma model (e.g. batch, age). "
                    "Each item: {'column': str, 'type': 'numerical'|'categorical'}. "
                    "Source-of-truth: MDFlexiComparisons ControlValue (process_r.py:69-71). "
                    "Pass only the list; the MCP wraps it as "
                    "{'control_variables': [...]} on the wire."
                ),
            },
        },
    },
    "ora": {
        "description": (
            "Over-Representation Analysis (ORA). Tests whether a user-supplied "
            "foreground entity list is enriched for any pathway / gene-set in the "
            "chosen database, using the hypergeometric test with Benjamini-Hochberg "
            "correction (clusterProfiler, Wu et al. 2021). Output dataset type 'ORA'. "
            "Source-of-truth: live /jobs catalogue, slug 'ora', MDORAParamsProperties."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "foreground_ids",
            "species",
        ],
        "guidance": "Always ask the user which parameters to use before calling this tool.",
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Exactly one INTENSITY dataset UUID.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output ORA dataset.",
            },
            "foreground_ids": {
                "type": "List[str]",
                "description": (
                    "Entity IDs (of the chosen entity_type) forming the foreground "
                    "tested for over-representation."
                ),
            },
            "species": {
                "type": "str",
                "required": True,
                "valid_values": ["human", "mouse", "yeast", "chinese_hamster"],
                "description": "Organism for the chosen gene-set database.",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "gene"],
                "description": "Entity type the foreground list contains.",
            },
            "database": {
                "type": "str",
                "default": "GO - Biological Process",
                "description": (
                    "Pathway / gene-set collection. Options depend on species "
                    "(Reactome, GO BP/CC/MF, MSigDB collections)."
                ),
            },
            "background": {
                "type": "str",
                "default": "Detected features in this dataset",
                "valid_values": [
                    "Detected features in this dataset",
                    "Custom Background List",
                    "Selected Database",
                ],
                "description": (
                    "Reference universe the foreground is tested against. "
                    "'Detected features in this dataset' is recommended."
                ),
            },
            "custom_background_ids": {
                "type": "Optional[List[str]]",
                "required": "when background='Custom Background List'",
                "default": None,
                "description": (
                    "Entity IDs forming the background universe. Required only when "
                    "background='Custom Background List'."
                ),
            },
            "min_gene_set_size": {
                "type": "int",
                "default": 5,
                "range": ">= 1",
                "description": "Sets with fewer members in the background are dropped.",
            },
            "max_gene_set_size": {
                "type": "int",
                "default": 500,
                "range": ">= 1",
                "description": "Sets with more members in the background are dropped.",
            },
        },
    },
    "camera_gsea": {
        "description": (
            "CAMERA gene-set enrichment analysis (Wu & Smyth 2012). Competitive "
            "gene-set test accounting for inter-gene correlation; tests whether genes "
            "in a set are differentially expressed relative to genes outside it, for "
            "each pairwise comparison. Output dataset type 'ENRICHMENT'. "
            "Source-of-truth: live /jobs catalogue, slug 'camera_gsea', "
            "EnrichmentParamsProperties."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_metadata",
            "condition_column",
            "condition_comparisons",
            "species",
        ],
        "guidance": "Always ask the user which parameters to use before calling this tool.",
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Exactly one INTENSITY dataset UUID.",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output ENRICHMENT dataset.",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and condition_column.",
            },
            "condition_column": {
                "type": "str",
                "description": "Column in sample_metadata defining groups to compare.",
            },
            "condition_comparisons": {
                "type": "List[List[str]]",
                "description": "List of [case, control] pairs.",
            },
            "species": {
                "type": "str",
                "required": True,
                "valid_values": ["Human", "Mouse", "Chinese hamster", "Yeast"],
                "description": "Species of the dataset (title-cased on the wire).",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["gene", "protein"],
                "description": "Entity type of the intensity dataset to run enrichment on.",
            },
            "sets": {
                "type": "Optional[List[str]]",
                "default": list(GSEA_DEFAULT_SETS),
                # Genuinely species-conditional (the live catalogue publishes
                # sets.parameters.options as {"ref": "species", "cases": {...}}),
                # so the per-species map is authoritative; valid_values is the
                # union, exposed for parity with every other constrained param.
                "valid_values": _gsea_sets_union(),
                "valid_values_per_species": {
                    species: list(values)
                    for species, values in GSEA_SETS_BY_SPECIES.items()
                },
                "description": (
                    "Knowledge bases for enrichment. Multiple may be selected. "
                    "Options depend on species — use valid_values_per_species, "
                    "keyed by the `species` you pass. Mouse MSigDB collections "
                    "use MH/M1/M2/M3/M5/M7/M8 prefixes, NOT the Human C-numbers; "
                    "Chinese hamster has no Reactome. Values must be verbatim: "
                    "'Hallmark' is NOT valid — the Human string is "
                    "'MSigDB-H (hallmark gene sets)' and the Mouse string is "
                    "'MSigDB-MH (hallmark gene sets)'. HAZARD: the backend "
                    "silently drops an unrecognised value (job still reports "
                    "COMPLETED without running that database); run_gsea now "
                    "rejects unknown values before submission."
                ),
            },
            "filter_method": {
                "type": "str",
                "default": "percentage",
                "valid_values": ["percentage", "count"],
                "description": (
                    "Completeness criterion: 'percentage' uses "
                    "filter_threshold_percentage; 'count' uses filter_threshold_count."
                ),
            },
            "filter_threshold_percentage": {
                "type": "float",
                "default": 0.5,
                "range": "0.0-1.0",
                "description": "Minimum fraction of valid values (when filter_method='percentage').",
            },
            "filter_threshold_count": {
                "type": "Optional[int]",
                "required": "when filter_method='count'",
                "default": None,
                "range": ">= 1",
                "description": "Minimum count of valid values (when filter_method='count').",
            },
            "filter_valid_values_logic": {
                "type": "str",
                "default": "at least one condition",
                "valid_values": [
                    "all conditions",
                    "at least one condition",
                    "full experiment",
                ],
                "description": "Logic for applying the valid-value filter across conditions.",
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": "Allow intensity-dependent trend for prior variances (Law et al. 2014).",
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": "Robust empirical Bayes moderation (Phipson et al. 2016).",
            },
            "fit_separate_models": {
                "type": "bool",
                "default": True,
                "description": "Fit a separate limma model per pairwise comparison.",
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": (
                    "Covariates to include in the model (e.g. batch, age). Each item: "
                    "{'column': str, 'type': 'categorical'|'numerical'}. Wrapped on the "
                    "wire as {'control_variables': [...]}."
                ),
            },
        },
    },
    "wgcna": {
        "description": (
            "WGCNA weighted co-expression network analysis (PyWGCNA, Rezaie et al. "
            "2023). Builds a weighted correlation network over entities, detects "
            "co-expression modules, summarises each with an eigenentity, and "
            "correlates module eigenentities with sample-metadata trait columns. "
            "Output dataset type 'WGCNA'. Requires complete numeric input — run "
            "Normalisation & Imputation first. Source-of-truth: live /jobs catalogue, "
            "slug 'wgcna', WGCNAParams."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
        ],
        "guidance": "Always ask the user which parameters to use before calling this tool.",
        "parameters": {
            "input_dataset_ids": {
                "type": "List[str]",
                "description": "Exactly one INTENSITY dataset UUID (complete / imputed).",
            },
            "dataset_name": {
                "type": "str",
                "description": "Name for the output WGCNA dataset.",
            },
            "sample_metadata": {
                "type": "Optional[List[List[str]]]",
                "default": None,
                "description": (
                    "2D array with header row. Optional; needed only for "
                    "module-trait correlations against trait_columns."
                ),
            },
            "trait_columns": {
                "type": "Optional[List[str]]",
                "default": None,
                "description": "Sample-metadata columns to correlate module eigengenes against.",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "peptide", "gene"],
                "description": "Entity level of the input dataset.",
            },
            "log_transform": {
                "type": "bool",
                "default": True,
                "description": "Apply log2(intensity) before building the network.",
            },
            "network_type": {
                "type": "str",
                "default": "signed",
                "valid_values": ["unsigned", "signed", "signed hybrid"],
                "description": "How correlations become edge weights. 'signed' recommended.",
            },
            "min_module_size": {
                "type": "int",
                "default": 30,
                "range": ">= 2",
                "description": "Smallest module kept by dynamic tree cut.",
            },
            "merge_cut_height": {
                "type": "float",
                "default": 0.25,
                "range": "0.0-1.0",
                "description": "Eigengene-dissimilarity threshold below which modules merge.",
            },
            "soft_power": {
                "type": "Optional[int]",
                "default": None,
                "range": "1-30 or None",
                "description": (
                    "Manually pin the soft-thresholding power β. None auto-selects "
                    "the lowest power meeting rsquared_cut and mean_connectivity_cut."
                ),
            },
            "rsquared_cut": {
                "type": "float",
                "default": 0.9,
                "range": "0.0-1.0",
                "description": "Minimum scale-free topology fit R² used during auto-β selection.",
            },
            "mean_connectivity_cut": {
                "type": "int",
                "default": 100,
                "range": ">= 1",
                "description": "Upper bound on mean network connectivity at the chosen β.",
            },
            "deep_split": {
                "type": "int",
                "default": 2,
                "range": "0-4",
                "description": "Sensitivity of dynamic tree cut. Higher = more, smaller modules.",
            },
            "filter_method": {
                "type": "Optional[str]",
                "default": None,
                "valid_values": [None, "goodSamplesGenes"],
                "description": (
                    "Iterative good-samples/genes filter. None skips it. When set to "
                    "'goodSamplesGenes', the min_fraction / min_n_samples / min_n_genes "
                    "/ min_relative_weight / tol sub-params apply."
                ),
            },
            "min_fraction": {
                "type": "float",
                "default": 0.5,
                "range": "0.0-1.0",
                "description": "Min fraction of non-missing samples (goodSamplesGenes only).",
            },
            "min_n_samples": {
                "type": "int",
                "default": 4,
                "range": ">= 1",
                "description": "Min samples an entity must be observed in (goodSamplesGenes only).",
            },
            "min_n_genes": {
                "type": "int",
                "default": 4,
                "range": ">= 1",
                "description": "Min good entities after filtering or the run fails (goodSamplesGenes only).",
            },
            "min_relative_weight": {
                "type": "float",
                "default": 0.1,
                "range": "0.0-1.0",
                "description": "Relative-weight threshold below which observations are missing (goodSamplesGenes only).",
            },
            "tol": {
                "type": "Optional[float]",
                "default": None,
                "range": ">= 0.0 or None",
                "description": "Variance threshold for declaring an entity constant (goodSamplesGenes only). None auto-computes.",
            },
        },
    },
}
