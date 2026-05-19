"""
Workspaces resource sub-package for the MD Python v2 client.

Maps the `/api/workspaces`, `/api/workspaces/:id/tabs`, and
`/api/workspaces/:id/tabs/:id/modules` endpoints
(see `app/api/api/v2/workspaces/`).

The visual environment of the app is structured as
``Workspace → Tab → Module``. A tab holds a ``layout`` of modules placed on a
react-grid-layout grid (``x``, ``y``, ``width``, ``height`` in grid units).

Layout:
    workspaces.py    — Workspaces class (workspace CRUD)
    tabs.py          — Tabs class (tab CRUD, nested under workspace)
    tab_modules.py   — TabModules class (module CRUD on a tab)
    _common.py       — shared ``_check`` helper / constants

``TabModules``, ``Tabs`` and ``Workspaces`` are re-exported so
``from md_python.resources.v2.workspaces import TabModules, Tabs, Workspaces``
continues to work after the split.
"""

from .tab_modules import TabModules
from .tabs import Tabs
from .workspaces import Workspaces

__all__ = ["TabModules", "Tabs", "Workspaces"]
