"""Tests for mcp_tools.workspaces.registry."""

import json
from unittest.mock import patch

from mcp_tools.workspaces._renderable import RENDERABLE_MODULE_IDS, is_renderable
from mcp_tools.workspaces.registry import describe_module_type, list_module_types
from md_python.models import RegisteredModule


def _heading():
    return RegisteredModule(
        id="heading",
        name="Heading",
        group="General",
        icon="md-icon-text",
        input_settings={
            "text": {
                "fieldType": "String",
                "rules": [{"name": "is_required"}],
            },
            "size": {"fieldType": "String", "default": "h1"},
        },
    )


def _pca():
    return RegisteredModule(
        id="dimensionality_reduction_plot",
        name="Dimensionality Reduction Plot",
        group="Experiment",
        icon="x",
        input_settings={
            "datasetsSearch": {
                "fieldType": "Datasets",
                "rules": [{"name": "is_required"}],
            },
            "entityType": {
                "fieldType": "EntityType",
                "rules": [{"name": "is_required"}],
            },
            "colourBy": {
                "fieldType": "DatasetSampleMetadata",
                "default": "sample_name",
            },
        },
    )


def _box_plot():
    """A module the vis-service CAN render (in its frozen REGISTRY)."""
    return RegisteredModule(
        id="box_plot",
        name="Box Plot",
        group="Experiment",
        icon="x",
        input_settings={
            "datasetsSearch": {
                "fieldType": "Datasets",
                "rules": [{"name": "is_required"}],
            },
        },
    )


class TestListModuleTypes:
    def test_returns_grouped_summary(self, mock_client):
        mock_client.module_registry.list.return_value = [_heading(), _pca()]

        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            result = list_module_types()

        data = json.loads(result)
        assert data["total"] == 2
        assert data["groups"] == {"General": 1, "Experiment": 1}
        ids = [m["id"] for m in data["data"]]
        assert sorted(ids) == ["dimensionality_reduction_plot", "heading"]

    def test_required_no_default_surfaced(self, mock_client):
        mock_client.module_registry.list.return_value = [_heading()]

        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(list_module_types())

        heading = data["data"][0]
        # text is required + no default; size has a default
        assert heading["required_keys_no_default"] == ["text"]
        assert heading["has_registry_defaults"] is True


class TestListModuleTypesEntityTypeContract:
    """entity_type is required by most plot modules and rejected by the
    rest. The LLM reads this index far more often than it describes a
    single module, so the contract MUST be published here — otherwise it
    is only discoverable by a failed add_module_to_tab."""

    def test_entity_type_input_published_per_module(self, mock_client):
        mock_client.module_registry.list.return_value = [_heading(), _pca()]
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(list_module_types())

        by_id = {m["id"]: m for m in data["data"]}
        # A module with no EntityType field publishes null — "do not pass".
        assert by_id["heading"]["entity_type_input"] is None
        # A module with one publishes the key, the requiredness, and the
        # values IT accepts.
        eti = by_id["dimensionality_reduction_plot"]["entity_type_input"]
        assert eti["settings_key"] == "entityType"
        assert eti["required"] is True
        assert eti["valid_values"] == ["protein", "peptide", "gene", "metabolite"]
        assert eti["tool_arg"] == "entity_type"

    def test_valid_values_come_from_the_module_when_it_declares_them(self, mock_client):
        narrowed = RegisteredModule(
            id="ptm_intensity_scatter",
            name="PTM Intensity Scatter",
            group="Experiment",
            icon="x",
            input_settings={
                "entityType": {
                    "fieldType": "EntityType",
                    "parameters": {
                        "options": [
                            {"value": "protein", "name": "Protein"},
                            {"value": "peptide", "name": "Peptide"},
                        ]
                    },
                    "rules": [{"name": "is_required"}],
                },
            },
        )
        mock_client.module_registry.list.return_value = [narrowed]
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(list_module_types())

        eti = data["data"][0]["entity_type_input"]
        assert eti["valid_values"] == ["protein", "peptide"]
        assert eti["valid_values_source"] == "registry_options"


class TestListModuleTypesRenderable:
    def test_renderable_flag_published_per_module(self, mock_client):
        mock_client.module_registry.list.return_value = [_box_plot(), _pca()]
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(list_module_types())

        by_id = {m["id"]: m for m in data["data"]}
        assert by_id["box_plot"]["renderable"] is True
        # Placeable and valid — but drawn client-side only.
        assert by_id["dimensionality_reduction_plot"]["renderable"] is False


class TestRenderableCatalogue:
    """Mirrors Visualisations::ServiceClient::REGISTRY in the Rails app
    (workflow/app/services/visualisations/service_client.rb). That file is
    the source of truth and is not editable from here — this test locks
    the mirror so it cannot drift silently."""

    def test_exact_registry_contents(self):
        assert RENDERABLE_MODULE_IDS == [
            "pairwise_volcano_plot",
            "instrument_qc_bar_chart",
            "box_plot",
            "cv_distribution_plot",
            "cv_distribution_violin_plot",
            "missing_values_by_sample_plot",
            "missing_values_by_feature_plot",
            "intensity_distribution_plot",
            "missing_values_heatmap",
            "entity_detection_coverage_plot",
            "ptm_intensity_scatter",
            "entity_abundance_plot",
        ]

    def test_known_ui_only_modules_are_not_renderable(self):
        for item_id in (
            "gsea_dot_plot",
            "pairwise_heatmap",
            "qc_summary_table",
            "entity_rank_plot",
            "dimensionality_reduction_plot",
            "quality_control_report_classic",
            "heading",
        ):
            assert is_renderable(item_id) is False


class TestDescribeModuleType:
    def test_full_description_returned(self, mock_client):
        mock_client.module_registry.get.return_value = _pca()

        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            result = describe_module_type("dimensionality_reduction_plot")

        data = json.loads(result)
        # Top-level keys
        assert data["id"] == "dimensionality_reduction_plot"
        assert isinstance(data["parameters"], list)
        # PCA has datasetsSearch + colourBy → both surface as data deps
        deps = " ".join(data["data_dependencies"]).lower()
        assert "dataset" in deps
        assert "sample_metadata" in deps
        # required_keys_no_default catches datasetsSearch (required, no default)
        assert "datasetsSearch" in data["required_keys_no_default"]
        # registry_defaults catches the default colourBy
        assert data["registry_defaults"] == {"colourBy": "sample_name"}

    def test_renderable_and_note_surfaced(self, mock_client):
        mock_client.module_registry.get.return_value = _pca()
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(describe_module_type("dimensionality_reduction_plot"))
        assert data["renderable"] is False
        assert "render_module_visualisation" in data["render_note"]

    def test_renderable_module_has_no_render_note(self, mock_client):
        mock_client.module_registry.get.return_value = _box_plot()
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(describe_module_type("box_plot"))
        assert data["renderable"] is True
        assert data["render_note"] is None

    def test_entity_type_input_surfaced(self, mock_client):
        mock_client.module_registry.get.return_value = _pca()
        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            data = json.loads(describe_module_type("dimensionality_reduction_plot"))
        assert data["entity_type_input"]["required"] is True
        assert data["entity_type_input"]["settings_key"] == "entityType"

    def test_unknown_id_returns_error_envelope(self, mock_client):
        mock_client.module_registry.get.return_value = None

        with patch(
            "mcp_tools.workspaces.registry.get_client", return_value=mock_client
        ):
            result = describe_module_type("does_not_exist")

        data = json.loads(result)
        assert "error" in data
        assert "does_not_exist" in data["error"]
        assert "list_module_types" in data["error"]
