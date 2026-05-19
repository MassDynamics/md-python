"""Tests for mcp_tools.workspaces.entity_lists."""

import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import UUID

from mcp_tools.workspaces.entity_lists import (
    create_entity_list,
    delete_entity_list,
    get_entity_list,
    list_entity_lists,
    update_entity_list,
)
from md_python.models import EntityList, EntityListItem

WS_ID = "11111111-1111-1111-1111-111111111111"
LIST_ID = "22222222-2222-2222-2222-222222222222"
DATASET_ID = "33333333-3333-3333-3333-333333333333"


def _entity_list(**overrides):
    base = dict(
        id=UUID(LIST_ID),
        name="Top hits",
        type="protein",
        experiment_id=UUID("44444444-4444-4444-4444-444444444444"),
        items_count=1,
        owner=True,
        items=[
            EntityListItem(
                entity_id="P12345",
                group_id=1,
                dataset_id=DATASET_ID,
                id=UUID("55555555-5555-5555-5555-555555555555"),
            )
        ],
        created_at=datetime(2026, 5, 8, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 8, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return EntityList(**base)


class TestCreateEntityList:
    def test_passes_through_to_resource(self, mock_client):
        mock_client.workspaces.entity_lists.create.return_value = _entity_list()
        items = [{"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID}]
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = create_entity_list(
                workspace_id=WS_ID,
                name="Top hits",
                entity_type="protein",
                items=items,
            )

        assert result.startswith("Entity list created.")
        assert LIST_ID in result
        body = json.loads(result.split("\n", 1)[1])
        assert body["name"] == "Top hits"
        assert body["type"] == "protein"
        assert body["items"][0]["entity_id"] == "P12345"

        kwargs = mock_client.workspaces.entity_lists.create.call_args.kwargs
        assert kwargs == {
            "workspace_id": WS_ID,
            "name": "Top hits",
            "entity_type": "protein",
            "items": items,
        }

    def test_resource_error_returns_error_prose(self, mock_client):
        mock_client.workspaces.entity_lists.create.side_effect = ValueError(
            "entity_type must be one of: gene, metabolite, peptide, protein"
        )
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = create_entity_list(
                workspace_id=WS_ID,
                name="x",
                entity_type="lipid",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )
        assert result.startswith("Error: ")
        assert "entity_type must be one of" in result


class TestListEntityLists:
    def test_returns_paginated_json(self, mock_client):
        mock_client.workspaces.entity_lists.list.return_value = {
            "data": [_entity_list()],
            "pagination": {
                "current_page": 1,
                "per_page": 50,
                "total_count": 1,
                "total_pages": 1,
            },
        }
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = list_entity_lists(WS_ID)
        body = json.loads(result)
        assert body["pagination"]["total_count"] == 1
        assert len(body["data"]) == 1
        assert body["data"][0]["id"] == LIST_ID

        kwargs = mock_client.workspaces.entity_lists.list.call_args.kwargs
        assert kwargs == {"workspace_id": WS_ID, "page": 1}

    def test_passes_page_arg(self, mock_client):
        mock_client.workspaces.entity_lists.list.return_value = {
            "data": [],
            "pagination": {
                "current_page": 3,
                "per_page": 50,
                "total_count": 0,
                "total_pages": 0,
            },
        }
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            list_entity_lists(WS_ID, page=3)
        kwargs = mock_client.workspaces.entity_lists.list.call_args.kwargs
        assert kwargs == {"workspace_id": WS_ID, "page": 3}

    def test_resource_error_returns_error_envelope(self, mock_client):
        mock_client.workspaces.entity_lists.list.side_effect = Exception(
            "Failed to list entity lists: 500 - boom"
        )
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = list_entity_lists(WS_ID)
        body = json.loads(result)
        assert "error" in body
        assert "500" in body["error"]


class TestGetEntityList:
    def test_returns_json_on_hit(self, mock_client):
        mock_client.workspaces.entity_lists.get.return_value = _entity_list()
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = get_entity_list(WS_ID, LIST_ID)
        body = json.loads(result)
        assert body["id"] == LIST_ID
        assert body["items"][0]["dataset_id"] == DATASET_ID

    def test_returns_error_envelope_on_404(self, mock_client):
        mock_client.workspaces.entity_lists.get.return_value = None
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = get_entity_list(WS_ID, LIST_ID)
        body = json.loads(result)
        assert "error" in body
        assert LIST_ID in body["error"]


class TestUpdateEntityList:
    def test_passes_through_to_resource(self, mock_client):
        mock_client.workspaces.entity_lists.update.return_value = _entity_list()
        items = [{"entity_id": "P12345", "group_id": 1, "dataset_id": DATASET_ID}]
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = update_entity_list(
                workspace_id=WS_ID,
                list_id=LIST_ID,
                name="Top hits",
                entity_type="protein",
                items=items,
            )
        body = json.loads(result)
        assert body["id"] == LIST_ID
        assert body["name"] == "Top hits"

        kwargs = mock_client.workspaces.entity_lists.update.call_args.kwargs
        assert kwargs == {
            "workspace_id": WS_ID,
            "list_id": LIST_ID,
            "name": "Top hits",
            "entity_type": "protein",
            "items": items,
        }

    def test_resource_error_returns_error_prose(self, mock_client):
        mock_client.workspaces.entity_lists.update.side_effect = ValueError(
            "entity_type must be one of: gene, metabolite, peptide, protein"
        )
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = update_entity_list(
                workspace_id=WS_ID,
                list_id=LIST_ID,
                name="x",
                entity_type="lipid",
                items=[{"entity_id": "P1", "group_id": 1, "dataset_id": DATASET_ID}],
            )
        assert result.startswith("Error: ")
        assert "entity_type must be one of" in result


class TestDeleteEntityList:
    def test_success_returns_prose(self, mock_client):
        mock_client.workspaces.entity_lists.delete.return_value = None
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = delete_entity_list(WS_ID, LIST_ID)
        assert result == f"Entity list deleted successfully. ID: {LIST_ID}"
        mock_client.workspaces.entity_lists.delete.assert_called_once_with(
            WS_ID, LIST_ID
        )

    def test_resource_error_returns_error_prose(self, mock_client):
        mock_client.workspaces.entity_lists.delete.side_effect = Exception(
            "Failed to delete entity list: 404 - missing"
        )
        with patch(
            "mcp_tools.workspaces.entity_lists.get_client",
            return_value=mock_client,
        ):
            result = delete_entity_list(WS_ID, LIST_ID)
        assert result.startswith("Error: ")
        assert "404" in result
