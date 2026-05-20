from unittest.mock import Mock

import pytest

from md_python.client_v2 import MDClientV2
from md_python.models import RegisteredModule
from md_python.resources.v2.workspaces import (
    RenderVisualisationError,
    TabModules,
    Tabs,
    Workspaces,
)

WS_ID = "11111111-1111-1111-1111-111111111111"
TAB_ID = "22222222-2222-2222-2222-222222222222"
MOD_ID = "33333333-3333-3333-3333-333333333333"


def _ws_payload(**overrides):
    base = {
        "id": WS_ID,
        "name": "Project A",
        "description": "desc",
        "created_at": "2026-05-01T00:00:00Z",
        "updated_at": "2026-05-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _tab_payload(**overrides):
    base = {
        "id": TAB_ID,
        "workspace_id": WS_ID,
        "name": "Overview",
        "settings": {},
        "tab_index": 1,
        "locked": False,
        "created_at": "2026-05-01T00:00:00Z",
        "updated_at": "2026-05-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _module_payload(**overrides):
    # Server returns persistence shape: id/i, itemId, h/w, x, y, settings.
    base = {
        "id": MOD_ID,
        "i": MOD_ID,
        "itemId": "heading",
        "h": 1,
        "w": 12,
        "x": 0,
        "y": 0,
        "settings": {},
    }
    base.update(overrides)
    return base


def _response(status_code, json_body=None, text=""):
    response = Mock()
    response.status_code = status_code
    if json_body is not None:
        response.json.return_value = json_body
    response.text = text
    return response


class TestWorkspaces:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def workspaces(self, mock_client):
        return Workspaces(mock_client)

    def test_create(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(201, _ws_payload())

        ws = workspaces.create(name="Project A", description="desc")

        assert ws.name == "Project A"
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "POST"
        assert call[1]["endpoint"] == "/workspaces"
        assert call[1]["json"] == {"name": "Project A", "description": "desc"}

    def test_create_omits_description_when_none(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(
            201, _ws_payload(description=None)
        )
        workspaces.create(name="Bare")
        assert mock_client._make_request.call_args[1]["json"] == {"name": "Bare"}

    def test_list_paginated(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(
            200,
            {
                "data": [_ws_payload(), _ws_payload(name="B")],
                "pagination": {
                    "current_page": 1,
                    "per_page": 50,
                    "total_count": 2,
                    "total_pages": 1,
                },
            },
        )
        body = workspaces.list(page=2)
        assert len(body["data"]) == 2
        assert body["pagination"]["total_pages"] == 1
        assert mock_client._make_request.call_args[1]["params"] == {"page": 2}

    def test_list_all_pages_through(self, workspaces, mock_client):
        mock_client._make_request.side_effect = [
            _response(
                200,
                {
                    "data": [_ws_payload()],
                    "pagination": {
                        "current_page": 1,
                        "per_page": 50,
                        "total_count": 2,
                        "total_pages": 2,
                    },
                },
            ),
            _response(
                200,
                {
                    "data": [_ws_payload(name="B")],
                    "pagination": {
                        "current_page": 2,
                        "per_page": 50,
                        "total_count": 2,
                        "total_pages": 2,
                    },
                },
            ),
        ]
        all_ws = workspaces.list_all()
        assert len(all_ws) == 2
        assert mock_client._make_request.call_count == 2

    def test_get_returns_none_on_404(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(404)
        assert workspaces.get(WS_ID) is None

    def test_update_partial(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(
            200, _ws_payload(name="Renamed")
        )
        workspaces.update(WS_ID, name="Renamed")
        call = mock_client._make_request.call_args
        assert call[1]["json"] == {"name": "Renamed"}
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}"

    def test_delete_returns_true_on_204(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(204)
        assert workspaces.delete(WS_ID) is True

    def test_create_failure_raises(self, workspaces, mock_client):
        mock_client._make_request.return_value = _response(400, text="bad name")
        with pytest.raises(Exception, match="400"):
            workspaces.create(name="x")


class TestTabs:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def tabs(self, mock_client):
        return Tabs(mock_client)

    def test_create_omits_settings_by_default(self, tabs, mock_client):
        mock_client._make_request.return_value = _response(201, _tab_payload())
        tabs.create(WS_ID, name="Overview")
        call = mock_client._make_request.call_args
        assert call[1]["json"] == {"name": "Overview"}
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/tabs"

    def test_create_with_settings(self, tabs, mock_client):
        mock_client._make_request.return_value = _response(
            201, _tab_payload(settings={"reportMode": True})
        )
        tabs.create(WS_ID, name="x", settings={"reportMode": True})
        assert mock_client._make_request.call_args[1]["json"] == {
            "name": "x",
            "settings": {"reportMode": True},
        }

    def test_list(self, tabs, mock_client):
        mock_client._make_request.return_value = _response(
            200,
            {
                "data": [_tab_payload()],
                "pagination": {"current_page": 1, "total_pages": 1},
            },
        )
        body = tabs.list(WS_ID)
        assert len(body["data"]) == 1
        assert body["data"][0].name == "Overview"

    def test_update_partial_with_layout(self, tabs, mock_client):
        mock_client._make_request.return_value = _response(200, _tab_payload())
        tabs.update(WS_ID, TAB_ID, layout={"modules": []})
        call = mock_client._make_request.call_args
        assert call[1]["json"] == {"layout": {"modules": []}}
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/tabs/{TAB_ID}"

    def test_delete(self, tabs, mock_client):
        mock_client._make_request.return_value = _response(204)
        assert tabs.delete(WS_ID, TAB_ID) is True


class TestTabModules:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def modules(self, mock_client):
        return TabModules(mock_client)

    def test_create_sends_full_grid_payload(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            201, _module_payload(itemId="heading", h=1, w=12)
        )
        mod = modules.create(
            workspace_id=WS_ID,
            tab_id=TAB_ID,
            item_id="heading",
            x=0,
            y=0,
            width=12,
            height=1,
            settings={"text": "Hi"},
        )
        assert mod.item_id == "heading"
        assert mod.height == 1
        assert mod.width == 12

        call = mock_client._make_request.call_args
        assert call[1]["method"] == "POST"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/tabs/{TAB_ID}/modules"
        assert call[1]["json"] == {
            "item_id": "heading",
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 1,
            "settings": {"text": "Hi"},
        }

    def test_create_default_settings(self, modules, mock_client):
        mock_client._make_request.return_value = _response(201, _module_payload())
        modules.create(WS_ID, TAB_ID, item_id="heading", x=0, y=0, width=1, height=1)
        assert mock_client._make_request.call_args[1]["json"]["settings"] == {}

    def test_list(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            200, {"data": [_module_payload()]}
        )
        out = modules.list(WS_ID, TAB_ID)
        assert len(out) == 1
        assert out[0].item_id == "heading"

    def test_get_returns_none_on_404(self, modules, mock_client):
        mock_client._make_request.return_value = _response(404)
        assert modules.get(WS_ID, TAB_ID, MOD_ID) is None

    def test_update_only_sends_provided_fields(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            200, _module_payload(x=5, y=2)
        )
        modules.update(WS_ID, TAB_ID, MOD_ID, x=5, y=2)
        call = mock_client._make_request.call_args
        assert call[1]["json"] == {"x": 5, "y": 2}
        assert call[1]["method"] == "PUT"

    def test_delete(self, modules, mock_client):
        mock_client._make_request.return_value = _response(204)
        assert modules.delete(WS_ID, TAB_ID, MOD_ID) is True

    def test_create_text_sends_settings_text(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            201, _module_payload(itemId="text", h=3, w=12)
        )
        modules.create_text(
            workspace_id=WS_ID,
            tab_id=TAB_ID,
            text="<p>hello world</p>",
        )
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "POST"
        assert call[1]["endpoint"] == f"/workspaces/{WS_ID}/tabs/{TAB_ID}/modules"
        assert call[1]["json"] == {
            "item_id": "text",
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 3,
            "settings": {"text": "<p>hello world</p>"},
        }

    def test_create_text_passes_layout_overrides(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            201, _module_payload(itemId="text")
        )
        modules.create_text(WS_ID, TAB_ID, text="hi", x=2, y=4, width=6, height=2)
        body = mock_client._make_request.call_args[1]["json"]
        assert (body["x"], body["y"], body["width"], body["height"]) == (2, 4, 6, 2)

    def test_update_text_only_sends_settings(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            200, _module_payload(itemId="text")
        )
        modules.update_text(WS_ID, TAB_ID, MOD_ID, text="<p>new body</p>")
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "PUT"
        assert call[1]["endpoint"] == (
            f"/workspaces/{WS_ID}/tabs/{TAB_ID}/modules/{MOD_ID}"
        )
        assert call[1]["json"] == {"settings": {"text": "<p>new body</p>"}}


def _render_response(status_code, json_body=None, retry_after=None):
    response = _response(status_code, json_body=json_body)
    response.headers = {}
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
    return response


class TestRenderVisualisation:
    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def modules(self, mock_client):
        return TabModules(mock_client)

    def test_returns_body_on_200(self, modules, mock_client):
        mock_client._make_request.return_value = _render_response(
            200, json_body={"data": [], "layout": {}}
        )
        body = modules.render_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        assert body == {"data": [], "layout": {}}
        call = mock_client._make_request.call_args
        assert call[1]["method"] == "GET"
        assert call[1]["endpoint"] == (
            f"/workspaces/{WS_ID}/tabs/{TAB_ID}/modules/{MOD_ID}/visualisation"
        )

    def test_returns_rendering_envelope_when_poll_false(self, modules, mock_client):
        mock_client._make_request.return_value = _render_response(202, retry_after=4)
        out = modules.render_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        assert out == {"status": "rendering", "retry_after": 4}

    def test_polls_until_200(self, modules, mock_client):
        mock_client._make_request.side_effect = [
            _render_response(202, retry_after=1),
            _render_response(202, retry_after=1),
            _render_response(200, json_body={"ok": True}),
        ]
        sleeps = []
        body = modules.render_visualisation(
            WS_ID,
            TAB_ID,
            MOD_ID,
            poll=True,
            timeout_s=60,
            sleep=sleeps.append,
        )
        assert body == {"ok": True}
        assert sleeps == [1.0, 1.0]
        assert mock_client._make_request.call_count == 3

    def test_clamps_retry_after_to_max(self, modules, mock_client):
        mock_client._make_request.side_effect = [
            _render_response(202, retry_after=999),
            _render_response(200, json_body={"ok": True}),
        ]
        sleeps = []
        modules.render_visualisation(
            WS_ID,
            TAB_ID,
            MOD_ID,
            poll=True,
            timeout_s=60,
            max_retry_s=5,
            sleep=sleeps.append,
        )
        assert sleeps == [5.0]

    def test_polling_times_out(self, modules, mock_client):
        mock_client._make_request.return_value = _render_response(202, retry_after=10)
        with pytest.raises(TimeoutError, match="still 202"):
            modules.render_visualisation(
                WS_ID,
                TAB_ID,
                MOD_ID,
                poll=True,
                timeout_s=0.0,
                sleep=lambda _s: None,
            )

    def test_non_2xx_raises_render_error_with_status_and_body(
        self, modules, mock_client
    ):
        mock_client._make_request.return_value = _response(404, text="missing module")
        with pytest.raises(RenderVisualisationError) as exc_info:
            modules.render_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        err = exc_info.value
        assert err.status_code == 404
        # Non-JSON body — error and body fall back to the raw text.
        assert err.error == "missing module"
        assert err.body == "missing module"

    def test_non_2xx_parses_json_error_body(self, modules, mock_client):
        mock_client._make_request.return_value = _response(
            400,
            text='{"error": "Visualisation not supported for module type \'x\'"}',
        )
        with pytest.raises(RenderVisualisationError) as exc_info:
            modules.render_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        err = exc_info.value
        assert err.status_code == 400
        assert err.error == "Visualisation not supported for module type 'x'"
        assert err.body == {"error": "Visualisation not supported for module type 'x'"}


class TestCreateWithDefaults:
    """create_with_defaults bakes in registry defaults so the persisted
    module always carries a complete settings hash."""

    HEADING = RegisteredModule(
        id="heading",
        name="Heading",
        group="General",
        icon="x",
        input_settings=[
            {"key": "text", "default": None, "required": True},
            {"key": "size", "default": "h1", "required": True},
            {"key": "horizontalPosition", "default": "left", "required": True},
            {"key": "verticalPosition", "default": "middle", "required": True},
        ],
    )

    @pytest.fixture
    def mock_client(self):
        return Mock(spec=MDClientV2)

    @pytest.fixture
    def modules(self, mock_client):
        # registry pre-injected so the helper doesn't try to make a network call
        # for the lookup
        registry = Mock()
        registry.get.return_value = self.HEADING
        return TabModules(mock_client, registry=registry)

    def test_user_settings_layer_on_top_of_defaults(self, modules, mock_client):
        mock_client._make_request.return_value = _response(201, _module_payload())

        modules.create_with_defaults(
            workspace_id=WS_ID,
            tab_id=TAB_ID,
            item_id="heading",
            x=0,
            y=0,
            width=12,
            height=1,
            settings={"text": "Hi", "size": "h2"},
        )

        sent = mock_client._make_request.call_args[1]["json"]
        # User's text + h2 win, the rest filled from defaults
        assert sent["settings"] == {
            "text": "Hi",
            "size": "h2",
            "horizontalPosition": "left",
            "verticalPosition": "middle",
        }
        assert sent["item_id"] == "heading"

    def test_missing_required_no_default_fails_fast(self, modules, mock_client):
        # text is required and has no default; if the user doesn't supply it
        # we should raise BEFORE hitting the API.
        with pytest.raises(ValueError, match="required key.*text"):
            modules.create_with_defaults(
                workspace_id=WS_ID,
                tab_id=TAB_ID,
                item_id="heading",
                x=0,
                y=0,
                width=12,
                height=1,
                settings={},
            )
        mock_client._make_request.assert_not_called()

    def test_unknown_item_id_fails_fast(self, mock_client):
        registry = Mock()
        registry.get.return_value = None
        modules = TabModules(mock_client, registry=registry)

        with pytest.raises(ValueError, match="not in the module registry"):
            modules.create_with_defaults(
                workspace_id=WS_ID,
                tab_id=TAB_ID,
                item_id="bogus",
                x=0,
                y=0,
                width=1,
                height=1,
                settings={"text": "x"},
            )
        mock_client._make_request.assert_not_called()

    def test_caller_supplied_registered_module_skips_lookup(self, mock_client):
        # When the caller already has the RegisteredModule, no GET is issued.
        registry = Mock()
        registry.get.side_effect = AssertionError("registry.get must not be called")
        modules = TabModules(mock_client, registry=registry)
        mock_client._make_request.return_value = _response(201, _module_payload())

        modules.create_with_defaults(
            workspace_id=WS_ID,
            tab_id=TAB_ID,
            item_id="heading",
            x=0,
            y=0,
            width=12,
            height=1,
            settings={"text": "Hi"},
            registered_module=self.HEADING,
        )
        registry.get.assert_not_called()

    def test_id_mismatch_with_supplied_registered_module(self, mock_client):
        modules = TabModules(mock_client, registry=Mock())
        with pytest.raises(ValueError, match="does not match"):
            modules.create_with_defaults(
                workspace_id=WS_ID,
                tab_id=TAB_ID,
                item_id="heading",
                x=0,
                y=0,
                width=1,
                height=1,
                settings={"text": "Hi"},
                registered_module=RegisteredModule(
                    id="text",  # wrong id
                    name="Text",
                    group="General",
                    icon="x",
                ),
            )


class TestWorkspacesNested:
    def test_workspaces_exposes_tabs_and_modules(self):
        client = Mock(spec=MDClientV2)
        ws = Workspaces(client)
        assert isinstance(ws.tabs, Tabs)
        assert isinstance(ws.modules, TabModules)

    def test_workspaces_forwards_registry_to_modules(self):
        client = Mock(spec=MDClientV2)
        registry = Mock()
        ws = Workspaces(client, registry=registry)
        assert ws.modules._registry is registry
