"""Environment loading helpers for the MCP server entrypoints.

The MCP server entrypoint scripts (``mcp_server.py`` / ``mcp_server_http.py``)
sit at the repo root and historically called ``python-dotenv``'s
``load_dotenv()`` with no arguments. ``load_dotenv()`` then walks UP from the
process's *current working directory* looking for a ``.env`` file.

That cwd-relative behaviour is fragile when the server is launched by a
parent process (e.g. Claude Desktop) that does not control cwd: the ``.env``
sitting next to ``mcp_server.py`` is silently missed and the tools fail at
first API call with an opaque "missing token" error.

This module provides a small, testable helper that resolves ``.env``
relative to a caller-supplied base path (the entrypoint's own directory),
and loads it with ``override=False`` so an explicit environment variable
exported by the parent process always wins over the file.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

__all__ = ["load_env_from"]


def load_env_from(base: str | Path, *, filename: str = ".env") -> bool:
    """Load ``<base>/<filename>`` into ``os.environ``.

    Parameters
    ----------
    base:
        Directory (or a file inside that directory; its parent will be
        used) to look for the dotenv file in. Callers should typically
        pass ``Path(__file__).resolve().parent`` so resolution is anchored
        to the script file regardless of cwd.
    filename:
        Name of the dotenv file. Defaults to ``.env``.

    Returns
    -------
    bool
        ``True`` if the file existed and was parsed, ``False`` otherwise.
        Mirrors :func:`dotenv.load_dotenv`'s return contract.

    Notes
    -----
    Always called with ``override=False``: if the parent process has
    already exported a variable (e.g. ``MD_AUTH_TOKEN``), that wins over
    whatever is in the file. This matches the conventional precedence
    "explicit env var beats config file".
    """
    base_path = Path(base)
    if base_path.is_file():
        base_path = base_path.parent
    env_path = base_path / filename
    return load_dotenv(dotenv_path=env_path, override=False)
