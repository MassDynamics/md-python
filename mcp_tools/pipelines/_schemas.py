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
            "Always ask the user which methods to use. "
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
                "valid_values": ["median", "quantile", "none", "batch_correction"],
                "description": (
                    "Normalisation algorithm. "
                    "'median': robust, recommended for most proteomics. "
                    "'quantile': stronger, assumes similar distributions. "
                    "'none': skip (use if data already normalised). "
                    "'batch_correction': correct batch effects; requires batch_variables "
                    "and design_variables in normalisation_extra_params."
                ),
            },
            "imputation_method": {
                "type": "str",
                "valid_values": ["mnar", "knn", "global_median", "median_by_entity"],
                "description": (
                    "Imputation algorithm. "
                    "'mnar' (PREFERRED for DDA proteomics): left-tail Gaussian draw for MNAR data; "
                    "accepts std_position (default 1.8) and std_width (default 0.3) in imputation_extra_params. "
                    "'knn': K-nearest neighbours for MAR data; accepts k in imputation_extra_params. "
                    "'global_median': replace missing with global median intensity. "
                    "'median_by_entity': replace missing with per-entity median."
                ),
            },
            "normalisation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Extra kwargs merged into the normalisation method dict (optional). "
                    "For batch_correction: {'batch_variables': [...], 'design_variables': [...]}."
                ),
            },
            "imputation_extra_params": {
                "type": "Dict[str, Any]",
                "default": None,
                "description": (
                    "Extra kwargs merged into the imputation method dict (optional). "
                    "For knn: {'k': 5}. "
                    "For mnar: {'std_position': 1.8, 'std_width': 0.3}."
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
