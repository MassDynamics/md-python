"""
Smoke tests for the Streamable HTTP transport.

Starts the FastMCP app in-process via uvicorn (test server) and verifies
that the /mcp endpoint responds correctly to MCP protocol requests.
"""

import json
import threading

import httpx
import pytest
import uvicorn

from mcp_tools import mcp

INITIALIZE_REQUEST = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test-client", "version": "0.0.1"},
    },
}


@pytest.fixture(scope="module")
def http_server():
    """Start the FastMCP streamable-http app on a free port for the test module."""
    app = mcp.streamable_http_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=18765, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait until the server is accepting connections
    import time

    for _ in range(50):
        try:
            httpx.get("http://127.0.0.1:18765/mcp", timeout=0.2)
            break
        except Exception:
            time.sleep(0.1)

    yield "http://127.0.0.1:18765"

    server.should_exit = True
    thread.join(timeout=5)


def test_mcp_endpoint_rejects_plain_get(http_server):
    """GET /mcp without SSE Accept header should be rejected (406 Not Acceptable)."""
    resp = httpx.get(f"{http_server}/mcp", timeout=5)
    assert resp.status_code == 406


def test_mcp_initialize(http_server):
    """POST /mcp with a valid initialize request should return a JSON-RPC result."""
    resp = httpx.post(
        f"{http_server}/mcp",
        json=INITIALIZE_REQUEST,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=10,
    )
    assert resp.status_code == 200

    # Response may be JSON or SSE-like; parse the first JSON object we find
    body = resp.text
    # For streamable-http the body is a JSON-RPC response
    data = json.loads(body) if body.strip().startswith("{") else _parse_sse_json(body)

    assert data.get("jsonrpc") == "2.0"
    assert data.get("id") == 1
    assert "result" in data
    assert "serverInfo" in data["result"]
    assert data["result"]["serverInfo"]["name"] == "mass-dynamics"


def _parse_sse_json(body: str) -> dict:
    """Extract the first JSON payload from an SSE stream body."""
    for line in body.splitlines():
        if line.startswith("data:"):
            return json.loads(line[len("data:") :].strip())
    raise ValueError(f"No JSON found in SSE body: {body!r}")
