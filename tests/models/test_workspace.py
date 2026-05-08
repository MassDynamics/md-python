from datetime import datetime, timezone
from uuid import UUID

import pytest

from md_python.models import Tab, TabModule, Workspace

WS_ID = "11111111-1111-1111-1111-111111111111"
TAB_ID = "22222222-2222-2222-2222-222222222222"
MOD_ID = "33333333-3333-3333-3333-333333333333"


class TestWorkspaceFromJson:
    def test_full_payload(self):
        ws = Workspace.from_json(
            {
                "id": WS_ID,
                "name": "Project A",
                "description": "Notes",
                "created_at": "2026-05-01T12:00:00Z",
                "updated_at": "2026-05-02T08:30:00Z",
            }
        )
        assert ws.id == UUID(WS_ID)
        assert ws.name == "Project A"
        assert ws.description == "Notes"
        assert ws.created_at == datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        assert ws.updated_at == datetime(2026, 5, 2, 8, 30, tzinfo=timezone.utc)

    def test_optional_fields_missing(self):
        ws = Workspace.from_json({"id": WS_ID, "name": "Bare"})
        assert ws.description is None
        assert ws.created_at is None
        assert ws.updated_at is None


class TestTabFromJson:
    def test_full_payload(self):
        tab = Tab.from_json(
            {
                "id": TAB_ID,
                "workspace_id": WS_ID,
                "name": "Overview",
                "settings": {"reportMode": True},
                "tab_index": 3,
                "locked": True,
                "created_at": "2026-05-01T00:00:00Z",
                "updated_at": "2026-05-01T00:00:00Z",
            }
        )
        assert tab.id == UUID(TAB_ID)
        assert tab.workspace_id == UUID(WS_ID)
        assert tab.tab_index == 3
        assert tab.locked is True
        assert tab.settings == {"reportMode": True}

    def test_settings_defaults_to_empty_dict(self):
        tab = Tab.from_json(
            {
                "id": TAB_ID,
                "workspace_id": WS_ID,
                "name": "x",
                "tab_index": 0,
                "locked": False,
            }
        )
        assert tab.settings == {}


class TestTabModuleFromJson:
    def test_persistence_shape_with_short_keys(self):
        # Server returns mod.attributes which uses short h/w keys
        # (react-grid-layout shorthand) and itemId.
        mod = TabModule.from_json(
            {
                "id": MOD_ID,
                "i": MOD_ID,
                "itemId": "anova_volcano_plot",
                "h": 6,
                "w": 8,
                "x": 0,
                "y": 0,
                "settings": {"datasetsSearch": ["abc"]},
            }
        )
        assert mod.item_id == "anova_volcano_plot"
        assert mod.height == 6
        assert mod.width == 8

    def test_explicit_height_width(self):
        # If a future endpoint returns the full key names, we accept them too.
        mod = TabModule.from_json(
            {
                "id": MOD_ID,
                "item_id": "heading",
                "height": 1,
                "width": 12,
                "x": 0,
                "y": 0,
            }
        )
        assert mod.height == 1
        assert mod.width == 12
        assert mod.settings == {}

    def test_missing_dimensions_raises(self):
        with pytest.raises(ValueError, match="height/width"):
            TabModule.from_json({"id": MOD_ID, "itemId": "heading", "x": 0, "y": 0})

    def test_missing_item_id_raises(self):
        with pytest.raises(ValueError, match="item_id"):
            TabModule.from_json({"id": MOD_ID, "h": 1, "w": 1, "x": 0, "y": 0})
