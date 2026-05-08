from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.resources.v2.module_registry import ModuleRegistry


class TestModuleRegistry:

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def registry(self, mock_client):
        return ModuleRegistry(mock_client)

    def test_list_returns_registered_modules(self, registry, mock_client):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {
                    "id": "anova_volcano_plot",
                    "name": "ANOVA Volcano Plot",
                    "group": "ANOVA",
                    "icon": "md-icon-plot-volcano",
                    "input_settings": [{"key": "datasetsSearch"}],
                }
            ]
        }
        mock_client._make_request.return_value = response

        modules = registry.list()

        assert len(modules) == 1
        assert modules[0].id == "anova_volcano_plot"
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "GET"
        assert call[1]["endpoint"] == "/module_registry/modules"

    def test_list_unwraps_bare_array(self, registry, mock_client):
        # Defensive: if the server ever returns a plain list, still parse it.
        response = Mock()
        response.status_code = 200
        response.json.return_value = [
            {"id": "heading", "name": "Heading", "group": "General", "icon": "x"}
        ]
        mock_client._make_request.return_value = response

        modules = registry.list()
        assert len(modules) == 1
        assert modules[0].id == "heading"

    def test_get_returns_none_on_404(self, registry, mock_client):
        response = Mock()
        response.status_code = 404
        mock_client._make_request.return_value = response

        assert registry.get("missing") is None

    def test_get_success(self, registry, mock_client):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "id": "heading",
            "name": "Heading",
            "group": "General",
            "icon": "x",
        }
        mock_client._make_request.return_value = response

        mod = registry.get("heading")
        assert mod is not None
        assert mod.id == "heading"
        call = mock_client._make_request.call_args
        assert call[1]["endpoint"] == "/module_registry/modules/heading"

    def test_list_failure_raises(self, registry, mock_client):
        response = Mock()
        response.status_code = 503
        response.text = "Manifest unavailable"
        mock_client._make_request.return_value = response

        with pytest.raises(Exception, match="503"):
            registry.list()
