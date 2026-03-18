from unittest.mock import MagicMock, patch

from mcp_tools.health import health_check


def test_health_check_ok():
    mock_client = MagicMock()
    mock_client.health.check.return_value = {"status": "ok"}

    with patch("mcp_tools.health.get_client", return_value=mock_client):
        result = health_check()

    assert '"status": "ok"' in result


def test_health_check_error():
    mock_client = MagicMock()
    mock_client.health.check.return_value = {
        "status": "error",
        "message": "unreachable",
    }

    with patch("mcp_tools.health.get_client", return_value=mock_client):
        result = health_check()

    assert "error" in result
    assert "unreachable" in result
