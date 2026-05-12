"""Shared fixtures for workspaces MCP tool tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_client():
    """Return a MagicMock standing in for MDClientV2.

    Tests patch ``mcp_tools.workspaces.<tool>.get_client`` to return this.
    """
    return MagicMock()
