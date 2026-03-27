"""Pipeline parameter schemas — single source of truth for all pipeline types.

API NOTE — state of flux
------------------------
The dataset service is transitioning between two parameter formats:
  Old (flat, still accepted by Rails API):
    job_run_params = {"entity_type": "protein", "normalisation_methods": {"method": "median"}, ...}
  New (Pydantic/JSON-Schema, returned by GET /api/v2/jobs required_params):
    params = {"entity_type": "protein", "normalisation_methods": {"method": "median"}, ...}

The Python client currently uses the flat/old format which the API still accepts.
Method name strings (e.g. "mnar", "skip", "set to constant") come from the live API.

Update here when the API adds new methods or options.
"""

from typing import Any, Dict

# All job slugs available on this account (from GET /api/v2/jobs).
# Standard user-facing pipelines are marked with *.
AVAILABLE_JOB_SLUGS: Dict[str, str] = {
    "normalisation_imputation": "* Normalise and impute intensity data",
    "pairwise_comparison": "* Pairwise differential abundance (limma)",
    "anova": "* ANOVA differential abundance across 3+ conditions (limma)",
    "dose_response": "* Dose-response curve fitting (4PL log-logistic)",
    "dose_response_aggregate": "Aggregate multiple dose-response results into a summary table",
    "camera_gsea": "Gene-set enrichment analysis (CAMERA)",
    "knn_tn_imputation": "Custom KNN-TN (truncated normal) imputation",
    "intensity": "Internal: raw intensity ingestion",
    "initial_job": "Internal: initial dataset creation",
    "md_dataset_custom_r": "Custom R script dataset",
    "demo_flow": "Demo/test job",
    "md_hello_world": "Demo/test job",
    "basic_r_to_say_hi": "Demo/test job",
    "jnj_dose_response": "Custom dose-response variant",
}

_PIPELINE_SCHEMAS: Dict[str, Any] = {
    "normalisation_imputation": {
        "description": (
            "Normalise and impute missing values in an intensity dataset. "
            "Optional pre-processing filtration step is also available."
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
            "keep defaults or change them. "
            "For standard DDA proteomics, 'mnar' imputation is preferred."
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
                "valid_values": ["protein", "peptide", "gene"],
                "description": "Entity level of the input dataset. Must match the data type.",
            },
            "normalisation_method": {
                "type": "str",
                "valid_values": [
                    "median",
                    "quantile",
                    "skip",
                    "batch_correction",
                    "cpm",
                ],
                "method_params": {
                    "median": "No extra parameters.",
                    "quantile": "No extra parameters.",
                    "skip": "No extra parameters. Skips normalisation entirely.",
                    "batch_correction": {
                        "batch_variables": {
                            "type": "List[str]",
                            "required": True,
                            "description": (
                                "Column names in sample_metadata that define batch "
                                "membership (e.g. ['batch']). Must have ≥2 distinct values."
                            ),
                        },
                        "design_variables": {
                            "type": "List[str]",
                            "required": False,
                            "default": None,
                            "description": (
                                "Column names encoding biological design to preserve "
                                "(e.g. ['condition']). Protects biological signal."
                            ),
                        },
                        "experiment_design": {
                            "type": "SampleMetadata (from load_metadata_from_csv)",
                            "required": True,
                            "description": (
                                "The sample metadata dict (column→values) needed to apply "
                                "batch correction. Pass sample_metadata from load_metadata_from_csv."
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
                                "Prior count added before CPM calculation (edgeR convention). "
                                "Use 0 for plain CPM. Gene entity_type only."
                            ),
                        },
                    },
                },
                "description": (
                    "Normalisation algorithm to apply. "
                    "'median': subtract per-sample median in log2 space; robust, recommended for most proteomics. "
                    "'quantile': force all samples to the same quantile distribution; stronger assumption. "
                    "'skip': skip normalisation (use when data is already normalised upstream). "
                    "'batch_correction': ComBat-style batch effect removal; requires batch_variables + experiment_design. "
                    "'cpm': Counts Per Million; gene data only."
                ),
            },
            "imputation_method": {
                "type": "str",
                "valid_values": [
                    "mnar",
                    "knn",
                    "global_median",
                    "median_by_entity",
                    "set to constant",
                    "set to missing",
                    "skip",
                ],
                "method_params": {
                    "mnar": {
                        "std_position": {
                            "type": "float",
                            "required": False,
                            "default": 1.8,
                            "range": "0.0–3.0",
                            "description": (
                                "Mean shift factor: how many standard deviations below the observed "
                                "mean to centre the imputed distribution. Higher = more aggressive "
                                "left-shift (more separation from observed values)."
                            ),
                        },
                        "std_width": {
                            "type": "float",
                            "required": False,
                            "default": 0.3,
                            "range": "0.0–1.0",
                            "description": (
                                "Standard deviation scaling factor: width of the imputed Gaussian "
                                "as a fraction of the observed std. Smaller = tighter imputed cluster."
                            ),
                        },
                    },
                    "knn": {
                        "n_neighbors": {
                            "type": "int",
                            "required": True,
                            "default": 3,
                            "range": "1–10",
                            "description": "Number of nearest neighbours to use for imputation.",
                        },
                        "weights": {
                            "type": "str | null",
                            "required": False,
                            "valid_values": [None, "distance"],
                            "default": None,
                            "description": (
                                "null (default): all neighbours weighted equally (uniform). "
                                "'distance': closer neighbours contribute more."
                            ),
                        },
                    },
                    "global_median": (
                        "No extra parameters. Replaces all missing values with the "
                        "global median intensity across all proteins and samples."
                    ),
                    "median_by_entity": (
                        "No extra parameters. Replaces each missing value with that "
                        "protein/gene's own median intensity across samples."
                    ),
                    "set to constant": {
                        "constant_value": {
                            "type": "int",
                            "required": True,
                            "default": 0,
                            "range": "0–100",
                            "description": "Fixed integer value to substitute for every missing entry.",
                        },
                    },
                    "set to missing": (
                        "No extra parameters. Sets all imputed positions to NaN "
                        "(outputs missing values, no imputation performed)."
                    ),
                    "skip": (
                        "No extra parameters. Returns data unchanged "
                        "(preserves existing imputed values as-is)."
                    ),
                },
                "description": (
                    "Imputation algorithm to apply. "
                    "'mnar' (PREFERRED for standard DDA proteomics): Perseus-style left-tail Gaussian draw; "
                    "models MNAR data where low-abundance proteins are systematically absent. "
                    "Requires std_position (default 1.8) and std_width (default 0.3) — ask user. "
                    "'knn': K-nearest neighbours; better for MAR data; requires n_neighbors (default 3) and weights. "
                    "'global_median': fast, simple; replaces all missing with the global median. "
                    "'median_by_entity': per-protein/gene median; better than global_median for heterogeneous data. "
                    "'set to constant': replace all missing with a fixed integer value (0–100). "
                    "'set to missing': output NaN for all imputed positions. "
                    "'skip': leave imputed flags unchanged."
                ),
            },
            "filtration_method": {
                "type": "str",
                "required": False,
                "default": "skip",
                "valid_values": [
                    "skip",
                    "minimum_abundance",
                    "ptm_localization_probability",
                ],
                "method_params": {
                    "skip": "No extra parameters. No pre-processing filtration applied.",
                    "minimum_abundance": {
                        "minimum_abundance_threshold": {
                            "type": "int",
                            "required": False,
                            "default": 0,
                            "range": "0–100",
                            "description": (
                                "Values strictly above this threshold are considered valid. "
                                "Typically used after CPM normalisation for gene data."
                            ),
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
                                "Logic for applying the abundance threshold across conditions."
                            ),
                        },
                        "filter_threshold_proportion": {
                            "type": "float",
                            "required": False,
                            "default": 0.5,
                            "range": "0.0–1.0",
                            "description": "Minimum proportion of valid values required per entity.",
                        },
                        "filtration_experiment_design": {
                            "type": "SampleMetadata (from load_metadata_from_csv)",
                            "required": True,
                            "description": (
                                "Sample metadata needed to group samples by condition for filtering."
                            ),
                        },
                    },
                    "ptm_localization_probability": {
                        "threshold": {
                            "type": "float",
                            "required": False,
                            "default": 0.5,
                            "range": "0.0–1.0",
                            "description": (
                                "PTM sites with PTMProbMax >= threshold are retained. "
                                "Peptide entity_type only."
                            ),
                        },
                    },
                },
                "description": (
                    "Optional pre-processing filtration step applied before normalisation. "
                    "'skip' (default): no filtering. "
                    "'minimum_abundance': filter by minimum abundance threshold — typically for gene CPM data. "
                    "'ptm_localization_probability': filter PTM sites by localisation probability — peptide data only."
                ),
            },
            "normalisation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Additional parameters for the chosen normalisation method. "
                    "See method_params in normalisation_method for per-method keys. "
                    "Example for batch_correction: "
                    "{'batch_variables': ['batch'], 'design_variables': ['condition'], "
                    "'experiment_design': <sample_metadata dict>}."
                ),
            },
            "imputation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Additional parameters for the chosen imputation method. "
                    "See method_params in imputation_method for per-method keys. "
                    "Example for mnar: {'std_position': 1.8, 'std_width': 0.3}. "
                    "Example for knn: {'n_neighbors': 3} (weights defaults to null)."
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
                "default": True,
                "description": (
                    "Use imputed intensity values from a prior normalisation_imputation step. "
                    "Default True. Set False to use only observed (non-imputed) values."
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
                "valid_values": ["protein", "peptide"],
                "description": (
                    "Entity level to analyse. Use 'protein' unless user requests peptide-level results. "
                    "NOTE: gene-level pairwise is not yet supported — use run_anova for gene data."
                ),
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": (
                    "Covariates to include in the limma model (e.g. batch, age). "
                    "Each item: {'column': str, 'type': 'numerical'|'categorical'}. "
                    "Helps account for known sources of variation."
                ),
            },
        },
    },
}
