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

Config via env vars (or .env file):
    FASTMCP_HOST=127.0.0.1   (default)
    FASTMCP_PORT=8000         (default)
"""

from dotenv import load_dotenv

load_dotenv()

import mcp_tools.batch  # noqa: F401
import mcp_tools.datasets  # noqa: F401
import mcp_tools.files  # noqa: F401
import mcp_tools.health  # noqa: F401
import mcp_tools.pipelines  # noqa: F401
import mcp_tools.uploads  # noqa: F401
from mcp_tools import mcp

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
