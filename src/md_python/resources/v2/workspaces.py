"""
Workspaces resource for the MD Python v2 client.

Maps the `/api/workspaces`, `/api/workspaces/:id/tabs`, and
`/api/workspaces/:id/tabs/:id/modules` endpoints
(see `app/api/api/v2/workspaces/`).

The visual environment of the app is structured as
``Workspace → Tab → Module``. A tab holds a ``layout`` of modules placed on a
react-grid-layout grid (``x``, ``y``, ``width``, ``height`` in grid units).
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...models import Page, RegisteredModule, Tab, TabModule, Workspace
from .entity_lists import EntityLists

if TYPE_CHECKING:
    from ...base_client import BaseMDClient
    from .module_registry import ModuleRegistry


_JSON_HEADERS = {"Content-Type": "application/json"}


def _check(response: Any, expected: int, action: str) -> None:
    if response.status_code != expected:
        raise Exception(f"Failed to {action}: {response.status_code} - {response.text}")


class TabModules:
    """Modules placed on a tab's grid.

    Reached via ``client.workspaces.modules`` and always parameterised by both
    ``workspace_id`` and ``tab_id`` since modules live under both.
    """

    def __init__(
        self,
        client: "BaseMDClient",
        registry: Optional["ModuleRegistry"] = None,
    ):
        self._client = client
        # Optional injection so create_with_defaults() can resolve registry
        # entries. None until first use; resolved lazily to avoid an import
        # cycle and so the registry isn't fetched unless needed.
        self._registry = registry

    def _base(self, workspace_id: str, tab_id: str) -> str:
        return f"/workspaces/{workspace_id}/tabs/{tab_id}/modules"

    def _get_registry(self) -> "ModuleRegistry":
        if self._registry is None:
            from .module_registry import ModuleRegistry

            self._registry = ModuleRegistry(self._client)
        return self._registry

    def create(
        self,
        workspace_id: str,
        tab_id: str,
        item_id: str,
        x: int,
        y: int,
        width: int,
        height: int,
        settings: Optional[Dict[str, Any]] = None,
    ) -> TabModule:
        """Add a module to a tab. The id is server-assigned.

        ``item_id`` must be present in the module registry (use
        ``client.module_registry.list()`` to discover available ids).
        ``settings`` keys must be a subset of the module's declared
        ``input_settings`` keys; the server returns 400 otherwise.
        """
        payload: Dict[str, Any] = {
            "item_id": item_id,
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "settings": settings or {},
        }
        response = self._client._make_request(
            method="POST",
            endpoint=self._base(workspace_id, tab_id),
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create module")
        return TabModule.from_json(response.json())

    def create_with_defaults(
        self,
        workspace_id: str,
        tab_id: str,
        item_id: str,
        x: int,
        y: int,
        width: int,
        height: int,
        settings: Optional[Dict[str, Any]] = None,
        registered_module: Optional[RegisteredModule] = None,
    ) -> TabModule:
        """Create a module with a *complete* settings payload baked in.

        The API does not merge registry-declared defaults server-side, so a
        module persisted with a partial ``settings`` hash will render as a
        broken widget in the app (e.g. "Please provide Size") even though the
        registry declares defaults for those keys.

        This helper resolves that by always sending the full payload:

        1. Fetch the registry entry for ``item_id`` (or use the one the
           caller supplied via ``registered_module=`` to skip the GET).
        2. Build a defaults dict from every ``input_settings`` entry that
           declares a non-null ``default``.
        3. Merge the caller's ``settings`` on top — explicit user values win.
        4. Validate that every ``required: true`` key is satisfied (either by
           the user's settings or by the registry default), or fail fast with
           a clear error before hitting the API.
        5. POST the resulting full settings hash via the regular
           :meth:`create` endpoint.

        Use this when you want the rendered widget to match the app's
        defaults out of the box. Use :meth:`create` when you specifically
        want to send a partial payload (e.g. testing the API's behaviour, or
        when you're constructing the full settings yourself).
        """
        if registered_module is None:
            registered_module = self._get_registry().get(item_id)
            if registered_module is None:
                raise ValueError(
                    f"item_id {item_id!r} is not in the module registry "
                    "(or is not available to the current user)"
                )
        elif registered_module.id != item_id:
            raise ValueError(
                f"registered_module.id ({registered_module.id!r}) does not "
                f"match item_id ({item_id!r})"
            )

        # Defaults first, user settings layered on top.
        full_settings: Dict[str, Any] = registered_module.defaults()
        if settings:
            full_settings.update(settings)

        missing = registered_module.missing_required_keys(full_settings)
        if missing:
            raise ValueError(
                f"Cannot create {item_id!r}: required key(s) not provided "
                f"and no registry default exists for them: {missing}"
            )

        return self.create(
            workspace_id=workspace_id,
            tab_id=tab_id,
            item_id=item_id,
            x=x,
            y=y,
            width=width,
            height=height,
            settings=full_settings,
        )

    def create_text(
        self,
        workspace_id: str,
        tab_id: str,
        text: str,
        x: int = 0,
        y: int = 0,
        width: int = 12,
        height: int = 3,
    ) -> TabModule:
        """Create a text module with its content in a single call.

        The workflow webapp's text module accepts and returns its body via
        the standard module endpoints under ``settings.text``: there is no
        separate "set content" step. ``text`` is a plain string with HTML
        allowed (including embedded base64 ``<img>`` tags).

        The server validates ``settings.text`` against the registry's
        ``parameters.maxLength``; on overflow the API returns a 4xx and
        the client surfaces the error verbatim.
        """
        return self.create(
            workspace_id=workspace_id,
            tab_id=tab_id,
            item_id="text",
            x=x,
            y=y,
            width=width,
            height=height,
            settings={"text": text},
        )

    def update_text(
        self,
        workspace_id: str,
        tab_id: str,
        module_id: str,
        text: str,
    ) -> TabModule:
        """Update a text module's body in place.

        Sends ``{"settings": {"text": text}}``; layout keys (x/y/width/
        height) are preserved server-side because they are not in the
        payload. Use :meth:`update` directly if you also need to move or
        resize the module.
        """
        return self.update(
            workspace_id=workspace_id,
            tab_id=tab_id,
            module_id=module_id,
            settings={"text": text},
        )

    def list(self, workspace_id: str, tab_id: str) -> List[TabModule]:
        """List all modules on a tab (no pagination)."""
        response = self._client._make_request(
            method="GET",
            endpoint=self._base(workspace_id, tab_id),
        )
        _check(response, 200, "list modules")
        data = response.json().get("data", [])
        return [TabModule.from_json(m) for m in data]

    def get(
        self, workspace_id: str, tab_id: str, module_id: str
    ) -> Optional[TabModule]:
        """Get a single module, or ``None`` if not found."""
        response = self._client._make_request(
            method="GET",
            endpoint=f"{self._base(workspace_id, tab_id)}/{module_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get module")
        return TabModule.from_json(response.json())

    def update(
        self,
        workspace_id: str,
        tab_id: str,
        module_id: str,
        item_id: Optional[str] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> TabModule:
        """Partial update — only fields you pass are sent."""
        payload: Dict[str, Any] = {}
        if item_id is not None:
            payload["item_id"] = item_id
        if x is not None:
            payload["x"] = x
        if y is not None:
            payload["y"] = y
        if width is not None:
            payload["width"] = width
        if height is not None:
            payload["height"] = height
        if settings is not None:
            payload["settings"] = settings

        response = self._client._make_request(
            method="PUT",
            endpoint=f"{self._base(workspace_id, tab_id)}/{module_id}",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 200, "update module")
        return TabModule.from_json(response.json())

    def delete(self, workspace_id: str, tab_id: str, module_id: str) -> bool:
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"{self._base(workspace_id, tab_id)}/{module_id}",
        )
        _check(response, 204, "delete module")
        return True


class Tabs:
    """Tabs inside a workspace. Reached via ``client.workspaces.tabs``."""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def _base(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/tabs"

    def create(
        self,
        workspace_id: str,
        name: str,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Tab:
        """Create a tab. ``tab_index`` is auto-assigned to ``max + 1``."""
        payload: Dict[str, Any] = {"name": name}
        if settings is not None:
            payload["settings"] = settings

        response = self._client._make_request(
            method="POST",
            endpoint=self._base(workspace_id),
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create tab")
        return Tab.from_json(response.json())

    def list(self, workspace_id: str, page: int = 1) -> Page[Tab]:
        """List tabs in a workspace, paginated and ordered by ``tab_index`` asc.

        Returns the paginated envelope ``{"data": [...], "pagination": {...}}``
        with the ``data`` items decoded into :class:`Tab` objects.
        """
        response = self._client._make_request(
            method="GET",
            endpoint=self._base(workspace_id),
            params={"page": page},
        )
        _check(response, 200, "list tabs")
        body = response.json()
        return {
            "data": [Tab.from_json(t) for t in body.get("data", [])],
            "pagination": body.get("pagination", {}),
        }

    def list_all(self, workspace_id: str) -> List[Tab]:
        """Convenience: page through all tabs in a workspace."""
        out: List[Tab] = []
        page = 1
        while True:
            body = self.list(workspace_id=workspace_id, page=page)
            out.extend(body["data"])
            pagination = body["pagination"]
            if page >= int(pagination.get("total_pages", page)):
                break
            page += 1
        return out

    def get(self, workspace_id: str, tab_id: str) -> Optional[Tab]:
        response = self._client._make_request(
            method="GET",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get tab")
        return Tab.from_json(response.json())

    def update(
        self,
        workspace_id: str,
        tab_id: str,
        name: Optional[str] = None,
        layout: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Tab:
        """Partial update. Locked tabs reject updates."""
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if layout is not None:
            payload["layout"] = layout
        if settings is not None:
            payload["settings"] = settings

        response = self._client._make_request(
            method="PUT",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 200, "update tab")
        return Tab.from_json(response.json())

    def delete(self, workspace_id: str, tab_id: str) -> bool:
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"{self._base(workspace_id)}/{tab_id}",
        )
        _check(response, 204, "delete tab")
        return True


class Workspaces:
    """V2 workspaces resource — the entry point for the visual environment.

    Tabs and tab-modules are reached via the nested resources::

        client.workspaces.create(name="...")
        client.workspaces.tabs.create(workspace_id, name="...")
        client.workspaces.modules.create(workspace_id, tab_id, ...)
    """

    def __init__(
        self,
        client: "BaseMDClient",
        registry: Optional["ModuleRegistry"] = None,
    ):
        self._client = client
        self.tabs = Tabs(client)
        self.modules = TabModules(client, registry=registry)
        self.entity_lists = EntityLists(client)

    def create(self, name: str, description: Optional[str] = None) -> Workspace:
        payload: Dict[str, Any] = {"name": name}
        if description is not None:
            payload["description"] = description

        response = self._client._make_request(
            method="POST",
            endpoint="/workspaces",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 201, "create workspace")
        return Workspace.from_json(response.json())

    def list(self, page: int = 1) -> Page[Workspace]:
        """List workspaces accessible to the current user, 50 per page."""
        response = self._client._make_request(
            method="GET",
            endpoint="/workspaces",
            params={"page": page},
        )
        _check(response, 200, "list workspaces")
        body = response.json()
        return {
            "data": [Workspace.from_json(w) for w in body.get("data", [])],
            "pagination": body.get("pagination", {}),
        }

    def list_all(self) -> List[Workspace]:
        """Convenience: page through all accessible workspaces."""
        out: List[Workspace] = []
        page = 1
        while True:
            body = self.list(page=page)
            out.extend(body["data"])
            pagination = body["pagination"]
            if page >= int(pagination.get("total_pages", page)):
                break
            page += 1
        return out

    def get(self, workspace_id: str) -> Optional[Workspace]:
        response = self._client._make_request(
            method="GET",
            endpoint=f"/workspaces/{workspace_id}",
        )
        if response.status_code == 404:
            return None
        _check(response, 200, "get workspace")
        return Workspace.from_json(response.json())

    def update(
        self,
        workspace_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Workspace:
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description

        response = self._client._make_request(
            method="PUT",
            endpoint=f"/workspaces/{workspace_id}",
            json=payload,
            headers=_JSON_HEADERS,
        )
        _check(response, 200, "update workspace")
        return Workspace.from_json(response.json())

    def delete(self, workspace_id: str) -> bool:
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/workspaces/{workspace_id}",
        )
        _check(response, 204, "delete workspace")
        return True
