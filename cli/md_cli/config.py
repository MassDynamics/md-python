"""
Persistent configuration for the MD CLI.

Credential resolution order:
  1. Environment variables: MD_API_TOKEN, MD_API_BASE_URL
  2. Config file: ~/.md-cli/config.json
  3. Defaults (base URL only)

The config file is created/updated by `md auth login`.
"""
import json
import os
from pathlib import Path

CONFIG_DIR = Path.home() / ".md-cli"
CONFIG_FILE = CONFIG_DIR / "config.json"
DEFAULT_BASE_URL = "https://app.massdynamics.com/api"


def _ensure_config_dir():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _load_file() -> dict:
    """Load config from disk, or return empty dict."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_config(token: str | None = None, base_url: str | None = None) -> dict:
    """Persist credentials to ~/.md-cli/config.json.

    Only overwrites fields that are explicitly provided.
    Returns the full config dict after saving.
    """
    _ensure_config_dir()
    config = _load_file()
    if token is not None:
        config["token"] = token
    if base_url is not None:
        config["base_url"] = base_url
    elif "base_url" not in config:
        config["base_url"] = DEFAULT_BASE_URL
    CONFIG_FILE.write_text(json.dumps(config, indent=2))
    # Restrict permissions (owner-only read/write)
    try:
        CONFIG_FILE.chmod(0o600)
    except OSError:
        pass
    return config


def get_config() -> dict:
    """Return the merged config (env vars take precedence over file).

    Returns dict with keys: token, base_url
    """
    file_cfg = _load_file()
    return {
        "token": os.environ.get("MD_API_TOKEN") or file_cfg.get("token"),
        "base_url": (
            os.environ.get("MD_API_BASE_URL")
            or file_cfg.get("base_url")
            or DEFAULT_BASE_URL
        ),
    }


def clear_config():
    """Remove stored credentials."""
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
