"""Workspaces / Tabs / Modules MCP tools.

Mass Dynamics workspaces are the visual environment of the app — every tab
hosts a react-grid-layout grid of dashboard "modules" (volcano plot,
heatmap, dose-response curves, …). This package wraps the v2 workspaces
API behind MCP tools and adds rich registry introspection so the LLM can
reason about every parameter a module accepts before placing it.

Subpackages:
    workspaces.crud   — workspace CRUD tools
    workspaces.tabs   — tab CRUD tools
    workspaces.modules — module CRUD + create_with_defaults
    workspaces.registry — list_module_types / describe_module_type
    workspaces.entity_lists — create/get named protein/peptide/gene lists
    workspaces._introspect — per-module parameter introspection helpers
    workspaces._mandates  — visualisation behavioural mandate fragment
"""

from . import crud  # noqa: F401
from . import entity_lists  # noqa: F401
from . import modules  # noqa: F401
from . import registry  # noqa: F401
from . import tabs  # noqa: F401
