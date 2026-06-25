from uuid import UUID

from md_python.models import SampleMetadata
from md_python.models.dataset_builders import (
    DoseResponseDataset,
    GseaDataset,
    MinimalDataset,
    MOFADataset,
    NormalisationImputationDataset,
    OraDataset,
    PairwiseComparisonDataset,
    WgcnaDataset,
)


def test_dose_response_dataset_build_and_run(mocker):
    drc = DoseResponseDataset(
        input_dataset_ids=[str(UUID(int=4))],
        dataset_name="Test doseresponse dataset",
        sample_names=["1", "2", "3", "4", "5", "6"],
        control_samples=["1", "3"],
        log_intensities=True,
        use_imputed_intensities=True,
        normalise="none",
        span_rollmean_k=1,
        prop_required_in_protein=0.5,
    )
    ds = drc.to_dataset()
    assert ds.name == "Test doseresponse dataset"
    assert ds.job_slug == "dose_response"
    assert ds.sample_names == ["1", "2", "3", "4", "5", "6"]
    assert ds.job_run_params["control_samples"] == ["1", "3"]
    assert ds.job_run_params["log_intensities"] is True
    assert ds.job_run_params["normalise"] == "none"
    assert ds.job_run_params["prop_required_in_protein"] == 0.5

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "drc-id"
    out = drc.run(client)
    assert out == "drc-id"


def test_pairwise_comparison_dataset_class_build_and_run(mocker):
    sm = SampleMetadata(data=[["group"], ["a"], ["b"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="Pairwise",
        sample_metadata=sm,
        condition_column="group",
        condition_comparisons=[["a", "b"]],
        # filter_values_criteria={"method": "percentage", "filter_threshold_percentage": 0.5},
    )
    ds = pw.to_dataset()
    assert ds.name == "Pairwise"

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "new-id"
    out = pw.run(client)
    assert out == "new-id"


def test_minimal_dataset_build_and_run(mocker):
    md = MinimalDataset(
        input_dataset_ids=[str(UUID(int=2))],
        dataset_name="Min DS",
        job_slug="demo_flow",
    )
    ds = md.to_dataset()
    assert ds.name == "Min DS"
    assert ds.job_slug == "demo_flow"
    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "min-id"
    out = md.run(client)
    assert out == "min-id"


def test_mofa_dataset_build_and_run(mocker):
    mofa = MOFADataset(
        input_dataset_ids=[str(UUID(int=7)), str(UUID(int=8))],
        dataset_name="MOFA integration",
    )
    ds = mofa.to_dataset()
    assert ds.name == "MOFA integration"
    assert ds.job_slug == "mofa"
    assert ds.job_run_params == {
        "num_factors": 15,
        "convergence_mode": "fast",
        "scale_views": True,
        "center_groups": True,
        "max_iter": 1000,
        "ard_factors": True,
        "drop_factor_threshold": 0.01,
    }
    assert len(ds.input_dataset_ids) == 2
    # dataset_name is NOT placed in job_run_params — the dataset-service
    # injects it server-side, same as pairwise / NI builders.
    assert "dataset_name" not in ds.job_run_params

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "mofa-id"
    assert mofa.run(client) == "mofa-id"


def test_mofa_dataset_custom_params_round_trip():
    mofa = MOFADataset(
        input_dataset_ids=[str(UUID(int=7)), str(UUID(int=8))],
        dataset_name="MOFA tuned",
        num_factors=25,
        convergence_mode="slow",
        scale_views=False,
        center_groups=False,
        max_iter=5000,
        ard_factors=False,
        drop_factor_threshold=0.0,
    )
    params = mofa.to_dataset().job_run_params
    assert params["num_factors"] == 25
    assert params["convergence_mode"] == "slow"
    assert params["scale_views"] is False
    assert params["center_groups"] is False
    assert params["max_iter"] == 5000
    assert params["ard_factors"] is False
    assert params["drop_factor_threshold"] == 0.0


def test_mofa_dataset_validation_errors():
    two_ids = [str(UUID(int=7)), str(UUID(int=8))]

    # fewer than 2 views
    try:
        MOFADataset(input_dataset_ids=[str(UUID(int=7))], dataset_name="x").validate()
        raise AssertionError("expected ValueError for <2 input datasets")
    except ValueError as e:
        assert "at least 2" in str(e)

    # num_factors out of range
    try:
        MOFADataset(
            input_dataset_ids=two_ids, dataset_name="x", num_factors=1
        ).validate()
        raise AssertionError("expected ValueError for num_factors=1")
    except ValueError as e:
        assert "num_factors" in str(e)

    # bad convergence_mode
    try:
        MOFADataset(
            input_dataset_ids=two_ids, dataset_name="x", convergence_mode="turbo"
        ).validate()
        raise AssertionError("expected ValueError for bad convergence_mode")
    except ValueError as e:
        assert "convergence_mode" in str(e)

    # max_iter out of range
    try:
        MOFADataset(input_dataset_ids=two_ids, dataset_name="x", max_iter=50).validate()
        raise AssertionError("expected ValueError for max_iter=50")
    except ValueError as e:
        assert "max_iter" in str(e)

    # drop_factor_threshold out of range
    try:
        MOFADataset(
            input_dataset_ids=two_ids, dataset_name="x", drop_factor_threshold=0.5
        ).validate()
        raise AssertionError("expected ValueError for drop_factor_threshold=0.5")
    except ValueError as e:
        assert "drop_factor_threshold" in str(e)

    # empty dataset_name
    try:
        MOFADataset(input_dataset_ids=two_ids, dataset_name="").validate()
        raise AssertionError("expected ValueError for empty dataset_name")
    except ValueError as e:
        assert "dataset_name" in str(e)


ONE_ID = [str(UUID(int=9))]


def test_ora_dataset_build_and_run(mocker):
    ora = OraDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="ORA enrichment",
        foreground_ids=["P1", "P2", "P3"],
        species="human",
    )
    ds = ora.to_dataset()
    assert ds.name == "ORA enrichment"
    assert ds.job_slug == "ora"
    assert ds.job_run_params == {
        "entity_type": "protein",
        "foreground_ids": ["P1", "P2", "P3"],
        "species": "human",
        "database": "GO - Biological Process",
        "background": "Detected features in this dataset",
        "min_gene_set_size": 5,
        "max_gene_set_size": 500,
    }
    assert len(ds.input_dataset_ids) == 1
    # output_dataset_type is NOT in job_run_params — the server derives the
    # output type from the job slug's run_type (mirrors MOFADataset).
    assert "output_dataset_type" not in ds.job_run_params
    assert "dataset_name" not in ds.job_run_params

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "ora-id"
    assert ora.run(client) == "ora-id"


def test_ora_dataset_custom_background_round_trip():
    ora = OraDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="ORA custom bg",
        foreground_ids=["G1"],
        species="mouse",
        entity_type="gene",
        database="Reactome",
        background="Custom Background List",
        custom_background_ids=["G2", "G3"],
        min_gene_set_size=10,
        max_gene_set_size=300,
    )
    params = ora.to_dataset().job_run_params
    assert params["entity_type"] == "gene"
    assert params["database"] == "Reactome"
    assert params["background"] == "Custom Background List"
    assert params["custom_background_ids"] == ["G2", "G3"]
    assert params["min_gene_set_size"] == 10
    assert params["max_gene_set_size"] == 300


def test_ora_dataset_validation_errors():
    # not exactly one input dataset
    try:
        OraDataset(
            input_dataset_ids=[str(UUID(int=1)), str(UUID(int=2))],
            dataset_name="x",
            foreground_ids=["P1"],
            species="human",
        ).validate()
        raise AssertionError("expected ValueError for !=1 input datasets")
    except ValueError as e:
        assert "exactly 1" in str(e)

    # empty foreground
    try:
        OraDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            foreground_ids=[],
            species="human",
        ).validate()
        raise AssertionError("expected ValueError for empty foreground_ids")
    except ValueError as e:
        assert "foreground_ids" in str(e)

    # bad species
    try:
        OraDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            foreground_ids=["P1"],
            species="rat",
        ).validate()
        raise AssertionError("expected ValueError for bad species")
    except ValueError as e:
        assert "species" in str(e)

    # custom background without ids
    try:
        OraDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            foreground_ids=["P1"],
            species="human",
            background="Custom Background List",
        ).validate()
        raise AssertionError("expected ValueError for missing custom_background_ids")
    except ValueError as e:
        assert "custom_background_ids" in str(e)


def test_gsea_dataset_build_and_run(mocker):
    sm = SampleMetadata(data=[["sample_name", "condition"], ["s1", "a"], ["s2", "b"]])
    gsea = GseaDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="GSEA enrichment",
        sample_metadata=sm,
        condition_column="condition",
        condition_comparisons=[["a", "b"]],
        species="Human",
    )
    ds = gsea.to_dataset()
    assert ds.name == "GSEA enrichment"
    assert ds.job_slug == "camera_gsea"
    params = ds.job_run_params
    assert params["entity_type"] == "protein"
    assert params["species"] == "Human"
    assert params["sets"] == [
        "GO - Biological Process",
        "GO - Cellular Component",
        "GO - Molecular Function",
    ]
    assert params["condition_comparisons"] == {
        "condition_comparison_pairs": [["a", "b"]]
    }
    assert params["filter_values_criteria"] == {
        "method": "percentage",
        "filter_threshold_percentage": 0.5,
    }
    assert params["filter_valid_values_logic"] == "at least one condition"
    # output_dataset_type is NOT in job_run_params (mirrors MOFADataset).
    assert "output_dataset_type" not in params
    assert "dataset_name" not in params

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "gsea-id"
    assert gsea.run(client) == "gsea-id"


def test_gsea_dataset_custom_params_round_trip():
    sm = SampleMetadata(data=[["sample_name", "condition"], ["s1", "a"], ["s2", "b"]])
    gsea = GseaDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="GSEA tuned",
        sample_metadata=sm,
        condition_column="condition",
        condition_comparisons=[["a", "b"]],
        species="Mouse",
        entity_type="gene",
        sets=["Reactome"],
        filter_values_criteria={"method": "count", "filter_threshold_count": 3},
        limma_trend=False,
        fit_separate_models=False,
    )
    params = gsea.to_dataset().job_run_params
    assert params["entity_type"] == "gene"
    assert params["sets"] == ["Reactome"]
    assert params["filter_values_criteria"] == {
        "method": "count",
        "filter_threshold_count": 3,
    }
    assert params["limma_trend"] is False
    assert params["fit_separate_models"] is False


def test_gsea_dataset_validation_errors():
    sm = SampleMetadata(data=[["sample_name", "condition"], ["s1", "a"], ["s2", "b"]])

    # bad species
    try:
        GseaDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            sample_metadata=sm,
            condition_column="condition",
            condition_comparisons=[["a", "b"]],
            species="human",
        ).validate()
        raise AssertionError("expected ValueError for bad species casing")
    except ValueError as e:
        assert "species" in str(e)

    # empty comparisons
    try:
        GseaDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            sample_metadata=sm,
            condition_column="condition",
            condition_comparisons=[],
            species="Human",
        ).validate()
        raise AssertionError("expected ValueError for empty comparisons")
    except ValueError as e:
        assert "condition_comparisons" in str(e)

    # >1 input dataset
    try:
        GseaDataset(
            input_dataset_ids=[str(UUID(int=1)), str(UUID(int=2))],
            dataset_name="x",
            sample_metadata=sm,
            condition_column="condition",
            condition_comparisons=[["a", "b"]],
            species="Human",
        ).validate()
        raise AssertionError("expected ValueError for !=1 input datasets")
    except ValueError as e:
        assert "exactly 1" in str(e)


def test_wgcna_dataset_build_and_run(mocker):
    wgcna = WgcnaDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="WGCNA network",
    )
    ds = wgcna.to_dataset()
    assert ds.name == "WGCNA network"
    assert ds.job_slug == "wgcna"
    params = ds.job_run_params
    assert params["entity_type"] == "protein"
    assert params["network_type"] == "signed"
    assert params["min_module_size"] == 30
    assert params["merge_cut_height"] == 0.25
    assert params["soft_power"] is None
    assert params["rsquared_cut"] == 0.9
    assert params["mean_connectivity_cut"] == 100
    assert params["deep_split"] == 2
    assert params["filter_method"] is None
    # output_dataset_type is NOT in job_run_params (mirrors MOFADataset).
    assert "output_dataset_type" not in params
    # goodSamplesGenes sub-params are NOT emitted when filter_method is None
    assert "min_fraction" not in params
    assert "tol" not in params
    # experiment_design omitted when no sample_metadata
    assert "experiment_design" not in params
    assert "dataset_name" not in params

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "wgcna-id"
    assert wgcna.run(client) == "wgcna-id"


def test_wgcna_dataset_filter_emits_subparams():
    sm = SampleMetadata(
        data=[["sample_name", "treatment"], ["s1", "ctrl"], ["s2", "drug"]]
    )
    wgcna = WgcnaDataset(
        input_dataset_ids=ONE_ID,
        dataset_name="WGCNA filtered",
        sample_metadata=sm,
        trait_columns=["treatment"],
        entity_type="gene",
        network_type="unsigned",
        soft_power=12,
        deep_split=4,
        filter_method="goodSamplesGenes",
        min_fraction=0.75,
        min_n_samples=2,
        tol=0.001,
    )
    params = wgcna.to_dataset().job_run_params
    assert params["entity_type"] == "gene"
    assert params["network_type"] == "unsigned"
    assert params["soft_power"] == 12
    assert params["deep_split"] == 4
    assert params["filter_method"] == "goodSamplesGenes"
    assert params["min_fraction"] == 0.75
    assert params["min_n_samples"] == 2
    assert params["tol"] == 0.001
    assert params["trait_columns"] == ["treatment"]
    assert params["experiment_design"]["sample_name"] == ["s1", "s2"]


def test_wgcna_dataset_validation_errors():
    # >1 input dataset
    try:
        WgcnaDataset(
            input_dataset_ids=[str(UUID(int=1)), str(UUID(int=2))],
            dataset_name="x",
        ).validate()
        raise AssertionError("expected ValueError for !=1 input datasets")
    except ValueError as e:
        assert "exactly 1" in str(e)

    # bad network_type
    try:
        WgcnaDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            network_type="bipartite",
        ).validate()
        raise AssertionError("expected ValueError for bad network_type")
    except ValueError as e:
        assert "network_type" in str(e)

    # deep_split out of range
    try:
        WgcnaDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            deep_split=9,
        ).validate()
        raise AssertionError("expected ValueError for deep_split out of range")
    except ValueError as e:
        assert "deep_split" in str(e)

    # soft_power out of range
    try:
        WgcnaDataset(
            input_dataset_ids=ONE_ID,
            dataset_name="x",
            soft_power=99,
        ).validate()
        raise AssertionError("expected ValueError for soft_power out of range")
    except ValueError as e:
        assert "soft_power" in str(e)


def test_builders_validation_errors():
    # MinimalDataset validation
    md = MinimalDataset(input_dataset_ids=[], dataset_name="", job_slug="")
    try:
        md.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert (
            "input_dataset_ids" in str(e)
            or "dataset_name" in str(e)
            or "job_slug" in str(e)
        )

    # PairwiseComparisonDataset validation
    sm = SampleMetadata(data=[["group"], ["a"]])
    pw = PairwiseComparisonDataset(
        input_dataset_ids=[],
        dataset_name="",
        sample_metadata=sm,
        condition_column="",
        condition_comparisons=[],
        filter_values_criteria={
            "method": "percentage",
            "filter_threshold_percentage": 0.5,
        },
    )
    try:
        pw.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert any(
            k in str(e)
            for k in [
                "input_dataset_ids",
                "dataset_name",
                "condition_column",
                "condition_comparisons",
            ]
        )

    # DoseResponseDataset validation: control_samples must be subset of sample_names
    drc = DoseResponseDataset(
        input_dataset_ids=[str(UUID(int=0))],
        dataset_name="DRC",
        sample_names=["a", "b"],
        control_samples=["c"],
    )
    try:
        drc.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "control_samples" in str(e) and "sample_names" in str(e)

    # NormalisationImputationDataset validation
    ni = NormalisationImputationDataset(
        input_dataset_ids=[],
        dataset_name="",
        normalisation_method="quantile",
        imputation_method="mnar",
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert any(k in str(e) for k in ["input_dataset_ids", "dataset_name"])


def test_normalisation_imputation_builder_build_and_run(mocker):
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=3))],
        dataset_name="NI DS",
        normalisation_method="quantile",
        imputation_method="mnar",
        std_position=1.8,
        std_width=0.3,
    )
    ds = ni.to_dataset()
    assert ds.name == "NI DS"
    assert ds.job_slug == "normalisation_imputation"
    assert ds.job_run_params["entity_type"] == "protein"
    assert ds.job_run_params["normalisation_methods_proteomics"] == "quantile"
    assert ds.job_run_params["filtration_methods_protein"] == "skip"
    assert ds.job_run_params["imputation_methods"] == "mnar"
    assert ds.job_run_params["std_position"] == 1.8
    assert ds.job_run_params["std_width"] == 0.3
    # Entity-specific keys for other entities must NOT leak in.
    assert "normalisation_methods_gene" not in ds.job_run_params
    assert "filtration_methods_peptide" not in ds.job_run_params
    assert "filtration_methods_gene" not in ds.job_run_params

    client = mocker.Mock()
    client.datasets = mocker.Mock()
    client.datasets.create.return_value = "new-id"
    out = ni.run(client)
    assert out == "new-id"


def test_normalisation_imputation_builder_gene_entity_canonical_aliases():
    """Legacy underscored 'minimum_abundance' is accepted and emitted as canonical."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=4))],
        dataset_name="NI gene",
        entity_type="gene",
        normalisation_method="cpm",
        imputation_method="skip",
        filtration_method="minimum_abundance",
        minimum_abundance_threshold=1.0,
        filter_valid_values_criteria="percentage",
        filter_threshold_proportion=0.5,
        filter_valid_values_logic="full experiment",
        experiment_design={"sample_name": ["s1", "s2"]},
        prior_count=2,
    )
    ds = ni.to_dataset()
    assert ds.job_run_params["normalisation_methods_gene"] == "cpm"
    assert ds.job_run_params["filtration_methods_gene"] == "by minimum abundance"
    assert ds.job_run_params["minimum_abundance_threshold"] == 1.0
    assert ds.job_run_params["prior_count"] == 2
    # Entity keys for protein/peptide must not be emitted on a gene job.
    assert "normalisation_methods_proteomics" not in ds.job_run_params
    assert "filtration_methods_protein" not in ds.job_run_params
    assert "filtration_methods_peptide" not in ds.job_run_params


# --- New NI-A through NI-S coverage --------------------------------------------------


def _design():
    return {"sample_name": ["s1", "s2", "s3", "s4"], "condition": ["a", "a", "b", "b"]}


def test_ni_protein_combat_emits_combat_keys():
    """NI-A: protein + combat technique emits combat-specific keys."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="combat protein",
        entity_type="protein",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="combat",
        batch_variable_combat="batch",
        mean_only=True,
        reference_batch_combat="b1",
        design_variables=[{"column": "condition", "type": "categorical"}],
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["normalisation_methods_proteomics"] == "batch correction"
    assert p["batch_correction_technique_proteomics"] == "combat"
    assert p["batch_variable_combat"] == "batch"
    assert p["mean_only"] is True
    assert p["reference_batch_combat"] == "b1"
    assert p["experiment_design"] == _design()
    assert "batch_variables" not in p
    assert "batch_correction_technique_gene" not in p


def test_ni_protein_limma_emits_limma_keys():
    """NI-B: protein + limma technique emits batch_variables (no combat keys)."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="limma protein",
        entity_type="protein",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="limma remove batch effect",
        batch_variables=[{"column": "batch", "type": "categorical"}],
        design_variables=[{"column": "condition", "type": "categorical"}],
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["batch_correction_technique_proteomics"] == "limma remove batch effect"
    assert p["batch_variables"] == [{"column": "batch", "type": "categorical"}]
    assert "batch_variable_combat" not in p
    assert "mean_only" not in p
    assert "reference_batch_combat" not in p


def test_ni_gene_combat_seq_emits_combat_seq():
    """NI-C: gene + combat seq technique."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="combat seq gene",
        entity_type="gene",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="combat seq",
        batch_variable_combat="batch",
        design_variables=[{"column": "condition", "type": "categorical"}],
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["batch_correction_technique_gene"] == "combat seq"
    assert p["batch_variable_combat"] == "batch"
    assert "mean_only" not in p
    assert "reference_batch_combat" not in p


def test_ni_protein_filtration_by_missing_values():
    """NI-D: protein filtration via 'by missing values' (newly unblocked)."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="protein filt",
        entity_type="protein",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by missing values",
        filter_valid_values_criteria="percentage",
        filter_threshold_proportion=0.7,
        filter_valid_values_logic="at least one condition",
        filter_based_on_condition="condition",
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["filtration_methods_protein"] == "by missing values"
    assert p["filter_valid_values_criteria"] == "percentage"
    assert p["filter_threshold_proportion"] == 0.7
    assert p["filter_valid_values_logic"] == "at least one condition"
    assert p["filter_based_on_condition"] == "condition"
    assert p["experiment_design"] == _design()


def test_ni_peptide_filtration_by_missing_values_count_logic():
    """NI-E: peptide filtration via 'by missing values' with count criteria."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="peptide filt count",
        entity_type="peptide",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by missing values",
        filter_valid_values_criteria="count",
        filter_threshold_count=3,
        filter_valid_values_logic="full experiment",
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["filtration_methods_peptide"] == "by missing values"
    assert p["filter_valid_values_criteria"] == "count"
    assert p["filter_threshold_count"] == 3
    assert "filter_threshold_proportion" not in p


def test_ni_peptide_filtration_ptm_threshold():
    """NI-F: peptide + by ptm localization probability emits threshold."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="peptide PTM",
        entity_type="peptide",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by ptm localization probability",
        threshold=0.75,
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["filtration_methods_peptide"] == "by ptm localization probability"
    assert p["threshold"] == 0.75


def test_ni_imputation_knn_tn_emits_flat_keys():
    """NI-G: knn_tn imputation method emits flat keys."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="knn_tn",
        normalisation_method="skip",
        imputation_method="knn_tn",
        knn_tn_k=4,
        knn_tn_distance="correlation",
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["imputation_methods"] == "knn_tn"
    assert p["knn_tn_k"] == 4
    assert p["knn_tn_distance"] == "correlation"


def test_ni_imputation_knn_tn_defaults_applied():
    """NI-G': knn_tn with no overrides applies the converter defaults."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="knn_tn defaults",
        normalisation_method="skip",
        imputation_method="knn_tn",
    )
    p = ni.to_dataset().job_run_params
    assert p["knn_tn_k"] == 5
    assert p["knn_tn_distance"] == "truncation"


def test_ni_imputation_mindet_q():
    """NI-H: mindet imputation emits q."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="mindet",
        normalisation_method="skip",
        imputation_method="mindet",
        q=0.05,
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["imputation_methods"] == "mindet"
    assert p["q"] == 0.05


def test_ni_normalisation_median_centre_at_zero_default_true():
    """NI-I: median_normalisation_centre_at_zero defaults to True; override round-trips."""
    ni_default = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="median default",
        normalisation_method="median",
        imputation_method="skip",
    )
    p = ni_default.to_dataset().job_run_params
    assert p["median_normalisation_centre_at_zero"] is True
    assert p["include_imputed_values"] is False

    ni_off = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="median off",
        normalisation_method="median",
        imputation_method="skip",
        median_normalisation_centre_at_zero=False,
    )
    p2 = ni_off.to_dataset().job_run_params
    assert p2["median_normalisation_centre_at_zero"] is False


def test_ni_normalisation_include_imputed_values_default_false():
    """NI-J: include_imputed_values defaults to False on median/quantile/sum/batch correction."""
    for method in ("median", "quantile", "sum"):
        ni = NormalisationImputationDataset(
            input_dataset_ids=[str(UUID(int=1))],
            dataset_name=f"iv {method}",
            normalisation_method=method,
            imputation_method="skip",
        )
        p = ni.to_dataset().job_run_params
        assert p["include_imputed_values"] is False, method

    ni_bc = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="iv bc",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="limma remove batch effect",
        batch_variables=[{"column": "batch", "type": "categorical"}],
        experiment_design=_design(),
    )
    assert ni_bc.to_dataset().job_run_params["include_imputed_values"] is False


def test_ni_filter_only_classmethod_runs():
    """NI-K: filter_only sets normalisation/imputation to skip and runs."""
    ni = NormalisationImputationDataset.filter_only(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="filter only",
        entity_type="protein",
        filtration_method="by missing values",
        filter_valid_values_criteria="percentage",
        filter_threshold_proportion=0.5,
        filter_valid_values_logic="at least one condition",
        filter_based_on_condition="condition",
        experiment_design=_design(),
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["normalisation_methods_proteomics"] == "skip"
    assert p["imputation_methods"] == "skip"
    assert p["filtration_methods_protein"] == "by missing values"


def test_ni_batch_correction_requires_technique():
    """NI-L: batch correction without technique raises."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="bc no tech",
        normalisation_method="batch correction",
        imputation_method="skip",
        experiment_design=_design(),
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "batch_correction_technique" in str(e)


def test_ni_combat_requires_batch_variable_combat():
    """NI-M: combat without batch_variable_combat raises."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="combat no var",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="combat",
        experiment_design=_design(),
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "batch_variable_combat" in str(e)


def test_ni_limma_requires_batch_variables():
    """NI-N: limma without batch_variables raises."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="limma no vars",
        normalisation_method="batch correction",
        imputation_method="skip",
        batch_correction_technique="limma remove batch effect",
        experiment_design=_design(),
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "batch_variables" in str(e)


def test_ni_filtration_by_missing_values_requires_criteria():
    """NI-O: by missing values without criteria raises."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="missing no crit",
        entity_type="protein",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by missing values",
        experiment_design=_design(),
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "filter_valid_values_criteria" in str(e)


def test_ni_filtration_by_missing_values_rejected_for_gene():
    """NI-P: by missing values is not allowed for gene."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="missing on gene",
        entity_type="gene",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by missing values",
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "filtration_method" in str(e)


def test_ni_filtration_minimum_abundance_rejected_for_protein():
    """NI-Q: by minimum abundance is not allowed for protein."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="min abundance on protein",
        entity_type="protein",
        normalisation_method="skip",
        imputation_method="skip",
        filtration_method="by minimum abundance",
    )
    try:
        ni.validate()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "filtration_method" in str(e)


def test_ni_legacy_underscore_aliases_normalised():
    """NI-R: underscored values are accepted on input and emitted as canonical."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="legacy aliases",
        entity_type="peptide",
        normalisation_method="batch_correction",
        imputation_method="skip",
        filtration_method="ptm_localization_probability",
        batch_correction_technique="limma_remove_batch_effect",
        batch_variables=[{"column": "batch", "type": "categorical"}],
        experiment_design=_design(),
        threshold=0.5,
    )
    ni.validate()
    p = ni.to_dataset().job_run_params
    assert p["normalisation_methods_proteomics"] == "batch correction"
    assert p["filtration_methods_peptide"] == "by ptm localization probability"
    assert p["batch_correction_technique_proteomics"] == "limma remove batch effect"


def test_ni_extra_params_overrides_typed_field():
    """NI-S: extra_params merged last, so caller can override any typed value."""
    ni = NormalisationImputationDataset(
        input_dataset_ids=[str(UUID(int=1))],
        dataset_name="override",
        normalisation_method="skip",
        imputation_method="mnar",
        std_position=1.8,
        extra_params={"std_position": 9.9, "custom_future_field": "x"},
    )
    p = ni.to_dataset().job_run_params
    assert p["std_position"] == 9.9
    assert p["custom_future_field"] == "x"
