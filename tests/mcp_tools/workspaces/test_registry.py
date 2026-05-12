"""Tests for mcp_tools.workspaces.registry."""

import json
from unittest.mock import patch

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
            "colourBy": {
                "fieldType": "DatasetSampleMetadata",
                "default": "sample_name",
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
