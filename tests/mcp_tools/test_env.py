"""Tests for ``mcp_tools._env.load_env_from``.

The helper exists so the MCP server entrypoints can resolve ``.env``
relative to the script file rather than ``os.getcwd()``. The cwd-based
default (``dotenv.load_dotenv()`` with no args) silently misses the file
when Claude Desktop or another MCP client launches the server with an
unrelated cwd, which manifests as opaque "missing token" errors at first
API call.

These tests pin the contract:

    1. The file is found relative to the supplied base path, not cwd.
    2. ``override=False`` semantics: an already-exported env var wins over
       the file. (This is the conventional precedence.)
    3. Missing file returns ``False`` and does not raise.
"""

from __future__ import annotations

import os
from pathlib import Path

from mcp_tools._env import load_env_from


def test_loads_env_from_base_dir_ignoring_cwd(tmp_path: Path, monkeypatch) -> None:
    """Helper finds the file relative to ``base``, not the process cwd."""
    # Arrange: write a .env in one tempdir, change cwd to a *different*
    # tempdir that has no .env. The bare load_dotenv() default would walk
    # up from cwd and find nothing; load_env_from() should still resolve.
    env_dir = tmp_path / "server_dir"
    env_dir.mkdir()
    (env_dir / ".env").write_text("MD_AUTH_TOKEN=test_value_xyz\n")

    other_dir = tmp_path / "elsewhere"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)

    # The autouse conftest fixture clears MD_AUTH_TOKEN. Sanity-check.
    assert "MD_AUTH_TOKEN" not in os.environ

    # Act
    loaded = load_env_from(env_dir)

    # Assert
    assert loaded is True
    assert os.environ.get("MD_AUTH_TOKEN") == "test_value_xyz"


def test_accepts_file_path_and_uses_its_parent(tmp_path: Path, monkeypatch) -> None:
    """Passing a file path (e.g. ``__file__``) uses its parent directory."""
    env_dir = tmp_path / "server_dir"
    env_dir.mkdir()
    (env_dir / ".env").write_text("MD_AUTH_TOKEN=from_file_parent\n")
    fake_script = env_dir / "mcp_server.py"
    fake_script.write_text("# placeholder\n")

    monkeypatch.chdir(tmp_path)
    assert "MD_AUTH_TOKEN" not in os.environ

    loaded = load_env_from(fake_script)

    assert loaded is True
    assert os.environ.get("MD_AUTH_TOKEN") == "from_file_parent"


def test_explicit_env_var_wins_over_file(tmp_path: Path, monkeypatch) -> None:
    """``override=False``: a pre-set env var is not overwritten by .env."""
    env_dir = tmp_path / "server_dir"
    env_dir.mkdir()
    (env_dir / ".env").write_text("MD_AUTH_TOKEN=from_file\n")

    monkeypatch.setenv("MD_AUTH_TOKEN", "from_environment")
    monkeypatch.chdir(tmp_path)

    loaded = load_env_from(env_dir)

    assert loaded is True
    assert os.environ.get("MD_AUTH_TOKEN") == "from_environment"


def test_missing_file_returns_false_and_does_not_raise(
    tmp_path: Path, monkeypatch
) -> None:
    """No .env at the resolved path is a no-op; returns False."""
    monkeypatch.chdir(tmp_path)
    assert "MD_AUTH_TOKEN" not in os.environ

    loaded = load_env_from(tmp_path)

    assert loaded is False
    assert "MD_AUTH_TOKEN" not in os.environ


def test_custom_filename(tmp_path: Path, monkeypatch) -> None:
    """``filename`` override resolves a non-default dotenv name."""
    (tmp_path / ".env.production").write_text("MD_AUTH_TOKEN=production_token\n")
    monkeypatch.chdir(tmp_path.parent)
    assert "MD_AUTH_TOKEN" not in os.environ

    loaded = load_env_from(tmp_path, filename=".env.production")

    assert loaded is True
    assert os.environ.get("MD_AUTH_TOKEN") == "production_token"
