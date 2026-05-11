# Changelog

All notable changes to `md-python` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project loosely adheres to [Semantic Versioning](https://semver.org/).

## [0.3.2]

- `client.entities.mappings.peptide_to_protein_same_dataset` map peptides to their protein groups within a single dataset.

## [0.3.1]

- `client.entities.mappings.protein_to_peptide_same_dataset` map protein groups to their peptides within a single dataset.

## [0.3.0]

### Added
- `NormalisationImputationDataset` now exposes the full surface of the MD Converter
  `normalisation_and_imputation` flow:
  - **Filtration for `entity_type="protein"`** via `filtration_method="by missing values"`
    (previously blocked).
  - **`knn_tn` imputation** with `knn_tn_k` (1–10, default 5) and `knn_tn_distance`
    (`truncation` | `correlation`, default `truncation`).
  - **`mindet` imputation** parameter `q` (0–0.5, default 0.01).
  - **Batch-correction sub-technique selector** `batch_correction_technique`
    (`limma remove batch effect` | `combat` | `combat seq` — `combat seq` is gene-only)
    with the matching parameter blocks (`batch_variables` for limma, `batch_variable_combat`
    + `mean_only` + `reference_batch_combat` for ComBat, `batch_variable_combat` only for
    ComBat-Seq, plus `design_variables` and `experiment_design`).
  - **`include_imputed_values`** (default `False`) on median / quantile / sum /
    batch correction.
  - **`median_normalisation_centre_at_zero`** (default `True`) on median normalisation.
  - **Shared filter block** for `by missing values` and `by minimum abundance`:
    `filter_valid_values_criteria` (`percentage` | `count`),
    `filter_threshold_proportion`, `filter_threshold_count`,
    `filter_valid_values_logic`, `filter_based_on_condition`.
  - **`NormalisationImputationDataset.filter_only(...)`** classmethod for
    filtration-only jobs (`normalisation=skip`, `imputation=skip`, output remains
    INTENSITY).
  - **`NormalisationImputationDataset.help()`** classmethod with an in-process
    method-and-parameter reference.

### Changed
- Wire-format strings emitted by `NormalisationImputationDataset` now use the
  MD Converter canonical (spaced) form: `"batch correction"`, `"by missing values"`,
  `"by ptm localization probability"`, `"by minimum abundance"`,
  `"limma remove batch effect"`, `"combat seq"`. Underscored variants
  (`"batch_correction"`, `"minimum_abundance"`, `"ptm_localization_probability"`,
  `"by_missing_values"`, `"limma_remove_batch_effect"`, `"combat_seq"`) are still
  accepted on input and normalised to canonical on output.
- `NormalisationImputationDataset` constructor moved from the legacy nested
  `normalisation_methods` / `imputation_methods` dicts to flat typed kwargs
  (`normalisation_method`, `imputation_method`, `entity_type`, `filtration_method`,
  plus method-specific kwargs). The `extra_params` escape hatch is retained and
  is merged last, so caller-supplied keys override typed defaults.

### Notes
- The output dataset type for the NI flow is `INTENSITY` — including the
  filter-only pattern via `filter_only(...)`. An upload may therefore have
  multiple INTENSITY datasets after running NI; `find_initial_dataset` does not
  yet disambiguate this and will raise. A follow-up release will refine this.

## [0.2.4]
- Prior releases — see git history.
