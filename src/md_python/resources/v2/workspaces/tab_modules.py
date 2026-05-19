"""Tab-modules resource — modules placed on a tab's react-grid-layout grid."""

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ....models import RegisteredModule, TabModule
from ._common import _JSON_HEADERS, _check

if TYPE_CHECKING:
    from ....base_client import BaseMDClient
    from ..module_registry import ModuleRegistry


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
            from ..module_registry import ModuleRegistry

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

    def render_visualisation(
        self,
        workspace_id: str,
        tab_id: str,
        module_id: str,
        *,
        poll: bool = True,
        timeout_s: float = 300.0,
        min_retry_s: float = 1.0,
        max_retry_s: float = 30.0,
        sleep: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Fetch the rendered visualisation JSON for a module.

        Endpoint: ``GET /workspaces/:ws/tabs/:tab/modules/:id/visualisation``.

        The server may answer with either:
          * 200 — the visualisation JSON in the body.
          * 202 — render still in flight; ``Retry-After`` (seconds) hints
            at when to re-request the same URL.

        When ``poll=True`` (default), this method keeps re-requesting
        until a 200 lands or ``timeout_s`` elapses. When ``poll=False``,
        a 202 is returned to the caller via the envelope::

            {"status": "rendering", "retry_after": int}

        Args:
          workspace_id, tab_id, module_id: Identify the module to render.
          poll: Follow 202 replies until 200 or timeout. Default True.
          timeout_s: Maximum total wall-clock time to spend polling.
          min_retry_s, max_retry_s: Clamp for the server-supplied
            ``Retry-After`` (when missing, defaults to ``min_retry_s``).
          sleep: Optional callable ``f(seconds) -> None`` to override
            ``time.sleep`` — useful for tests.

        Returns:
          The visualisation JSON body (dict), or the rendering envelope
          when ``poll=False`` and the server is still working.

        Raises:
          TimeoutError: ``timeout_s`` exceeded while polling.
          Exception: any non-200/202 response.
        """
        endpoint = f"{self._base(workspace_id, tab_id)}/{module_id}/visualisation"
        sleeper = sleep if sleep is not None else time.sleep
        deadline = time.monotonic() + timeout_s

        while True:
            response = self._client._make_request(method="GET", endpoint=endpoint)
            if response.status_code == 200:
                return response.json()
            if response.status_code != 202:
                raise Exception(
                    f"Failed to render visualisation: "
                    f"{response.status_code} - {response.text}"
                )

            retry_raw = response.headers.get("Retry-After") if hasattr(
                response, "headers"
            ) else None
            try:
                retry_after = float(retry_raw) if retry_raw is not None else min_retry_s
            except (TypeError, ValueError):
                retry_after = min_retry_s
            retry_after = max(min_retry_s, min(max_retry_s, retry_after))

            if not poll:
                return {"status": "rendering", "retry_after": int(retry_after)}
            if time.monotonic() + retry_after > deadline:
                raise TimeoutError(
                    f"render_visualisation: still 202 after {timeout_s:.0f}s "
                    f"(workspace_id={workspace_id}, tab_id={tab_id}, "
                    f"module_id={module_id})"
                )
            sleeper(retry_after)
