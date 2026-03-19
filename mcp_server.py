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

from dotenv import load_dotenv

load_dotenv()

import mcp_tools.batch  # noqa: F401
import mcp_tools.datasets  # noqa: F401
import mcp_tools.files  # noqa: F401
import mcp_tools.health  # noqa: F401
import mcp_tools.pipelines  # noqa: F401
import mcp_tools.uploads  # noqa: F401
from mcp_tools import mcp

if __name__ == "__main__":
    mcp.run()
