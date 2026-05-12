"""Tests for mcp_tools.workspaces.crud (workspace-level CRUD)."""

import json
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID

from mcp_tools.workspaces.crud import (
    create_workspace,
    delete_workspace,
    get_workspace,
    list_workspaces,
    update_workspace,
)
from md_python.models import Workspace

WS_ID = UUID("11111111-1111-1111-1111-111111111111")


def _ws(name="Project A"):
    return Workspace(
        id=WS_ID,
        name=name,
        description="desc",
        created_at=datetime(2026, 5, 7, 0, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 7, 0, 0, tzinfo=timezone.utc),
    )


class TestCreateWorkspace:
    def test_returns_prose_with_id(self, mock_client):
        mock_client.workspaces.create.return_value = _ws()

        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            result = create_workspace(name="Project A", description="desc")

        assert result.startswith("Workspace created.")
        assert str(WS_ID) in result
        mock_client.workspaces.create.assert_called_once_with(
            name="Project A", description="desc"
        )


class TestListWorkspaces:
    def test_paginated_envelope(self, mock_client):
        mock_client.workspaces.list.return_value = {
            "data": [_ws()],
            "pagination": {
                "current_page": 1,
                "per_page": 50,
                "total_count": 1,
                "total_pages": 1,
            },
        }

        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            data = json.loads(list_workspaces(page=1))

        assert data["pagination"]["total_count"] == 1
        assert data["data"][0]["id"] == str(WS_ID)


class TestGetWorkspace:
    def test_404_returns_error_envelope(self, mock_client):
        mock_client.workspaces.get.return_value = None

        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            data = json.loads(get_workspace(str(WS_ID)))

        assert "error" in data


class TestUpdateWorkspace:
    def test_no_args_returns_error(self, mock_client):
        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            data = json.loads(update_workspace(str(WS_ID)))
        assert "error" in data
        # Did not actually call the API
        mock_client.workspaces.update.assert_not_called()

    def test_partial_update(self, mock_client):
        mock_client.workspaces.update.return_value = _ws(name="Renamed")

        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            data = json.loads(update_workspace(str(WS_ID), name="Renamed"))

        mock_client.workspaces.update.assert_called_once_with(
            str(WS_ID), name="Renamed", description=None
        )
        assert data["name"] == "Renamed"


class TestDeleteWorkspace:
    def test_returns_prose(self, mock_client):
        with patch("mcp_tools.workspaces.crud.get_client", return_value=mock_client):
            result = delete_workspace(str(WS_ID))
        assert result.startswith("Workspace deleted")
        assert str(WS_ID) in result

    def test_destructive_mandate_attached(self):
        # delete_workspace should carry the destructive mandate.
        wrapped = getattr(delete_workspace, "fn", None) or delete_workspace
        doc = (delete_workspace.__doc__ or "") + (wrapped.__doc__ or "")
        assert "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in doc
