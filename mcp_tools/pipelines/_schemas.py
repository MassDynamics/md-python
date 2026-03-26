"""Pipeline parameter schemas — single source of truth for all pipeline types.

Update here when the API adds new methods or options.
"""

from typing import Any, Dict

_PIPELINE_SCHEMAS: Dict[str, Any] = {
    "normalisation_imputation": {
        "description": "Normalise and impute missing values in an intensity dataset.",
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "normalisation_method",
            "imputation_method",
        ],
        "guidance": (
            "Always present ALL method parameters to the user and ask whether to keep defaults or change them. "
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
                "default": "protein",
                "valid_values": ["protein", "peptide", "gene"],
                "description": "Entity level of the input dataset. Must match the data type.",
            },
            "normalisation_method": {
                "type": "str",
                "valid_values": [
                    "median",
                    "quantile",
                    "none",
                    "batch_correction",
                    "cpm",
                ],
                "method_params": {
                    "median": "No extra parameters.",
                    "quantile": "No extra parameters.",
                    "none": "No extra parameters. Skips normalisation entirely.",
                    "batch_correction": {
                        "batch_variables": {
                            "type": "List[str]",
                            "required": True,
                            "description": "Column names in sample_metadata that identify batch membership (e.g. ['batch']). Must have ≥2 distinct values.",
                        },
                        "design_variables": {
                            "type": "List[str]",
                            "required": False,
                            "default": None,
                            "description": "Column names that encode the biological design to preserve (e.g. ['condition']). Protects biological signal from being removed by batch correction.",
                        },
                    },
                    "cpm": {
                        "prior_count": {
                            "type": "float",
                            "required": False,
                            "default": 0,
                            "description": "Prior count added to raw counts before CPM calculation (edgeR convention). Use 0 for plain CPM. Gene entity_type only.",
                        },
                    },
                },
                "description": (
                    "Normalisation algorithm to apply. "
                    "'median': subtract per-sample median in log2 space; robust, recommended for most proteomics. "
                    "'quantile': force all samples to the same quantile distribution; stronger assumption. "
                    "'none': skip normalisation (use when data is already normalised upstream). "
                    "'batch_correction': ComBat-style batch effect removal; requires batch_variables. "
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
                    "constant",
                    "none",
                ],
                "method_params": {
                    "mnar": {
                        "std_position": {
                            "type": "float",
                            "required": False,
                            "default": 1.8,
                            "description": "How many standard deviations below the observed mean to centre the imputed distribution. Higher = more left-shifted (more aggressive imputation).",
                        },
                        "std_width": {
                            "type": "float",
                            "required": False,
                            "default": 0.3,
                            "description": "Width of the imputed Gaussian as a fraction of the observed standard deviation. Smaller = tighter imputed cluster.",
                        },
                    },
                    "knn": {
                        "n_neighbors": {
                            "type": "int",
                            "required": True,
                            "description": "Number of nearest neighbours to use for imputation. Typical range: 2–10.",
                        },
                        "weights": {
                            "type": "str",
                            "required": True,
                            "valid_values": ["uniform", "distance"],
                            "default": "uniform",
                            "description": "'uniform': all neighbours weighted equally. 'distance': closer neighbours weighted more.",
                        },
                    },
                    "global_median": "No extra parameters. Replaces all missing values with the global median intensity.",
                    "median_by_entity": "No extra parameters. Replaces each missing value with that protein/gene's own median.",
                    "constant": {
                        "constant_value": {
                            "type": "float",
                            "required": True,
                            "description": "Fixed numeric value to substitute for every missing entry.",
                        },
                    },
                    "none": "No extra parameters. Sets all imputed positions to NaN (no imputation performed).",
                },
                "description": (
                    "Imputation algorithm to apply. "
                    "'mnar' (PREFERRED for standard DDA proteomics): Perseus-style left-tail Gaussian draw, "
                    "models Missing Not At Random data where low-abundance proteins are systematically absent. "
                    "'knn': K-nearest neighbours; better when data is MAR (missing at random); requires n_neighbors and weights. "
                    "'global_median': fast, simple; replaces all missing with the global median. "
                    "'median_by_entity': per-protein/gene median; better than global_median for heterogeneous data. "
                    "'constant': replace all missing with a fixed value; requires constant_value. "
                    "'none': skip imputation (leaves NaN in output)."
                ),
            },
            "normalisation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Additional parameters for the chosen normalisation method. "
                    "See method_params in normalisation_method for per-method keys. "
                    "Example for batch_correction: "
                    "{'batch_variables': ['batch'], 'design_variables': ['condition']}."
                ),
            },
            "imputation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Additional parameters for the chosen imputation method. "
                    "See method_params in imputation_method for per-method keys. "
                    "Example for mnar: {'std_position': 1.8, 'std_width': 0.3}. "
                    "Example for knn: {'n_neighbors': 5, 'weights': 'uniform'}."
                ),
            },
        },
    },
    "anova": {
        "description": (
            "Run ANOVA-based differential abundance analysis across multiple conditions "
            "using limma linear models. Use when comparing 3+ groups simultaneously."
        ),
        "required": [
            "input_dataset_ids",
            "dataset_name",
            "sample_metadata",
            "condition_column",
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
                "valid_values": ["all"],
                "description": "Type of comparisons to generate. Currently only 'all' is supported.",
            },
            "filter_values_criteria": {
                "type": "Dict[str, Any]",
                "default": {"method": "percentage", "filter_threshold_percentage": 0.5},
                "description": "Valid-value filter. {'method': 'percentage', 'filter_threshold_percentage': 0–1} or {'method': 'count', 'filter_threshold_count': int}.",
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": "Apply limma trend (intensity-dependent prior variance).",
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": "Apply robust empirical Bayes moderation.",
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
                "description": "Subset of sample_names used as controls (dose = 0).",
            },
            "sample_metadata": {
                "type": "List[List[str]]",
                "description": "2D array with header row. Must include sample_name and dose_column. Dose values are converted to numbers.",
            },
            "dose_column": {
                "type": "str",
                "default": "dose",
                "description": "Column in sample_metadata containing dose values.",
            },
            "log_intensities": {
                "type": "bool",
                "default": True,
                "description": "Log-transform intensities before fitting.",
            },
            "use_imputed_intensities": {
                "type": "bool",
                "default": True,
                "description": "Use imputed intensity values.",
            },
            "normalise": {
                "type": "str",
                "default": "none",
                "valid_values": ["none", "sum", "median"],
                "description": (
                    "Normalisation to apply before fitting. "
                    "'none' is the standard choice (recommended when data has already been "
                    "normalised upstream, e.g. via run_normalisation_imputation). "
                    "'sum' and 'median' apply within-sample normalisation at the dose-response stage."
                ),
            },
            "span_rollmean_k": {
                "type": "int",
                "default": 1,
                "description": "Rolling mean window size (>= 1). Use 1 to disable smoothing.",
            },
            "prop_required_in_protein": {
                "type": "float",
                "default": 0.5,
                "description": "Minimum fraction of non-missing values required per protein [0, 1].",
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
                "description": "Controls which rows pass the valid-value filter.",
            },
            "filter_method": {
                "type": "str",
                "default": "percentage",
                "valid_values": ["percentage", "count"],
                "description": "Method for the valid-value filter.",
            },
            "filter_threshold_percentage": {
                "type": "float",
                "default": 0.5,
                "description": "Fraction [0, 1] of valid values required (used when filter_method='percentage').",
            },
            "fit_separate_models": {
                "type": "bool",
                "default": True,
                "description": "Fit a separate limma model per comparison.",
            },
            "limma_trend": {
                "type": "bool",
                "default": True,
                "description": "Apply limma trend (intensity-dependent prior variance).",
            },
            "robust_empirical_bayes": {
                "type": "bool",
                "default": True,
                "description": "Apply robust empirical Bayes moderation.",
            },
            "entity_type": {
                "type": "str",
                "default": "protein",
                "valid_values": ["protein", "peptide"],
                "description": "Entity level to analyse.",
            },
            "control_variables": {
                "type": "Optional[List[Dict[str, str]]]",
                "default": None,
                "description": "Covariates to include in the model. Each item: {'column': str, 'type': 'numerical'|'categorical'}.",
            },
        },
    },
}
