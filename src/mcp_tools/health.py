import json

from . import mcp
from ._client import get_client
from ._workflow_guide import _WORKFLOW_GUIDE

__all__ = ["health_check", "get_workflow_guide", "_WORKFLOW_GUIDE"]


@mcp.tool()
def health_check() -> str:
    """Check the Mass Dynamics API health status."""
    result = get_client().health.check()
    return json.dumps(result, indent=2)


@mcp.tool()
def get_workflow_guide() -> str:
    """Return step-by-step guidance for every common Mass Dynamics workflow.

    Call this at the start of any new session to orient yourself before using
    other tools. Returns a structured guide with workflow steps, tool index,
    batch usage patterns, and critical constraints.
    """
    return json.dumps(_WORKFLOW_GUIDE, indent=2)
