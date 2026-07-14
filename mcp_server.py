#!/usr/bin/env python3
"""
Mass Dynamics MCP Server

Exposes the Mass Dynamics API as MCP tools for use with Claude desktop (Chat and Cowork).
Reads MD_AUTH_TOKEN and MD_API_BASE_URL from environment or .env file.

Claude desktop config (~/.../Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "mass-dynamics": {
          "command": "python",
          "args": ["/path/to/md-python/mcp_server.py"]
        }
      }
    }
"""

from pathlib import Path

# Resolve .env relative to THIS script's directory, not the process cwd.
# Claude Desktop (and other MCP clients) launch us with an arbitrary cwd,
# so the historical bare ``load_dotenv()`` would silently miss the .env
# sitting next to this file. ``override=False`` keeps explicit env vars
# winning over the file. See src/mcp_tools/_env.py for the rationale.
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
import mcp_tools.task  # noqa: F401
import mcp_tools.uploads  # noqa: F401
import mcp_tools.workspaces  # noqa: F401
from mcp_tools import mcp

if __name__ == "__main__":
    mcp.run()
