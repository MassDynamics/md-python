#!/usr/bin/env python3
"""
Mass Dynamics MCP Server — HTTP (Streamable HTTP transport)

Runs the same MCP server as mcp_server.py but over HTTP instead of stdio.
This allows Claude Desktop (or any MCP client) to connect via URL rather
than spawning a local subprocess.

Claude desktop config (~/.../Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "mass-dynamics": {
          "url": "http://127.0.0.1:8000/mcp"
        }
      }
    }

For remote deployment replace 127.0.0.1 with the server's address and set:
    FASTMCP_HOST=0.0.0.0   # bind to all interfaces

WARNING: HTTP transport sends MD_AUTH_TOKEN in plaintext. For remote/production
deployments, always place a TLS-terminating reverse proxy (nginx, Caddy, etc.)
in front of this server so the token is protected in transit.

Config via env vars (or .env file):
    FASTMCP_HOST=127.0.0.1   (default)
    FASTMCP_PORT=8000         (default)
"""

from pathlib import Path

# Resolve .env relative to THIS script's directory, not the process cwd.
# Same rationale as mcp_server.py — see src/mcp_tools/_env.py.
from mcp_tools._env import load_env_from  # noqa: E402

load_env_from(Path(__file__).resolve().parent)

# Optional internal telemetry plugin. The private ``md_mcp_telemetry`` package is
# installed only on authorised internal machines and is absent for external users
# (this block is then a silent no-op). Installed HERE — AFTER load_env_from so
# MD_MCP_LOG from .env is visible, and BEFORE the tool modules import so the
# ``mcp.tool`` wrap covers every ``@mcp.tool()`` registration.
from mcp_tools import mcp as _mcp  # noqa: E402

try:
    import md_mcp_telemetry  # noqa: E402

    md_mcp_telemetry.install(_mcp)
except ImportError:
    pass

import mcp_tools.batch  # noqa: F401, E402
import mcp_tools.datasets  # noqa: F401
import mcp_tools.entities  # noqa: F401
import mcp_tools.entity_meta  # noqa: F401
import mcp_tools.files  # noqa: F401
import mcp_tools.health  # noqa: F401
import mcp_tools.pipelines  # noqa: F401
import mcp_tools.uploads  # noqa: F401
import mcp_tools.workspaces  # noqa: F401
from mcp_tools import mcp

# NOTE: Global server state — not user-isolated
#
# The large-file upload executor (_large_upload_executor) is a process-level
# singleton. In a multi-user HTTP deployment, cancel_upload_queue() resets it
# for ALL users, not just the caller. Acceptable for single-user deployments.
#
# TODO(auth): Per-request authentication for multi-user deployments
#
# Currently MD_AUTH_TOKEN is read from the environment (or .env) as a
# module-level singleton in mcp_tools/client.py. This is fine for
# single-user / single-token servers where the token is set via env var:
#
#   export MD_AUTH_TOKEN=<token> && python mcp_server_http.py
#
# For multi-user deployments, Claude Desktop can pass the token per-user
# via the "headers" field in claude_desktop_config.json:
#
#   { "mcpServers": { "mass-dynamics": {
#       "url": "http://your-server:8000/mcp",
#       "headers": { "Authorization": "Bearer <user-token>" }
#   }}}
#
# To support this, get_client() needs to become request-scoped:
#   - Add Starlette middleware to extract the Authorization header
#   - Store it in a context variable (contextvars.ContextVar)
#   - Update get_client() to read from the context var instead of os.environ
# The MCP spec also defines a full OAuth 2.0 flow (FastMCP has transport_security
# settings for this) if a proper auth server is available.

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
