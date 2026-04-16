"""
Fixture recording and replay for eval testing.

Records all HTTP request/response pairs to a JSON file during a live run,
then replays them deterministically without hitting the network.

Usage (env vars):
    MD_RECORD_FIXTURES=./fixtures/eval-1.json  md experiments get ...
    MD_REPLAY_FIXTURES=./fixtures/eval-1.json  md experiments get ...

The module patches ``requests.request`` globally when either env var is set.
It is completely inert otherwise — zero overhead in production.

Fixture format::

    [
      {
        "seq": 0,
        "request": {
          "method": "GET",
          "url": "https://dev.massdynamics.com/api/health",
          "headers": {"accept": "...", "Authorization": "Bearer [REDACTED]"},
          "json": null
        },
        "response": {
          "status_code": 200,
          "headers": {"content-type": "application/json"},
          "json": { ... },
          "text": null,
          "elapsed_ms": 142
        }
      }
    ]
"""

from __future__ import annotations

import atexit
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock
from urllib.parse import urlparse

import requests
import requests.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _redact_auth(headers: dict) -> dict:
    """Replace Bearer tokens with [REDACTED] so fixtures are safe to commit."""
    out = dict(headers)
    auth = out.get("Authorization", "")
    if auth.startswith("Bearer "):
        out["Authorization"] = "Bearer [REDACTED]"
    return out


def _serialise_response(resp: requests.Response, elapsed_ms: float) -> dict:
    """Extract the parts of a Response we need to replay."""
    try:
        body = resp.json()
        text = None
    except (ValueError, json.JSONDecodeError):
        body = None
        text = resp.text

    return {
        "status_code": resp.status_code,
        "headers": dict(resp.headers),
        "json": body,
        "text": text,
        "elapsed_ms": round(elapsed_ms, 1),
    }


def _build_fake_response(entry: dict) -> requests.Response:
    """Reconstruct a requests.Response from a fixture entry."""
    resp_data = entry["response"]
    resp = MagicMock(spec=requests.Response)
    resp.status_code = resp_data["status_code"]
    resp.headers = resp_data.get("headers", {})
    resp.ok = 200 <= resp_data["status_code"] < 400

    if resp_data.get("json") is not None:
        resp.json.return_value = resp_data["json"]
        resp.text = json.dumps(resp_data["json"])
        resp.content = resp.text.encode("utf-8")
    else:
        resp.json.side_effect = ValueError("No JSON in fixture")
        resp.text = resp_data.get("text", "")
        resp.content = resp.text.encode("utf-8") if resp.text else b""

    if resp_data["status_code"] >= 400:
        http_error = requests.HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_error
    else:
        resp.raise_for_status.return_value = None

    return resp


# URL matching: strip known base URLs to get the endpoint path.
_KNOWN_BASES = (
    "https://dev.massdynamics.com/api",
    "https://app.massdynamics.com/api",
)


def _url_to_endpoint(url: str) -> str:
    """Best-effort extraction of the API path from a full URL."""
    for base in _KNOWN_BASES:
        if url.startswith(base):
            return url[len(base):]
    # Fall back to just the path component
    return urlparse(url).path


def _match_key(method: str, url_or_endpoint: str,
               json_body: Any = None, params: Any = None) -> str:
    """Stable lookup key for matching requests to recorded fixtures."""
    endpoint = _url_to_endpoint(url_or_endpoint)
    parts = [method.upper(), endpoint]
    if params:
        parts.append(json.dumps(params, sort_keys=True))
    if json_body:
        parts.append(json.dumps(json_body, sort_keys=True))
    return "|".join(parts)


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------

class FixtureRecorder:
    """Intercepts requests.request() to record request/response pairs."""

    def __init__(self, output_path: str | Path):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: list[dict] = []
        self._seq = 0
        atexit.register(self.flush)

    def record(self, real_request_fn: Any,
               method: str, url: str, **kwargs: Any) -> requests.Response:
        """Call the real API, record the exchange, return the real response."""
        session = kwargs.pop("session", None)
        t0 = time.monotonic()
        if session is not None:
            resp = real_request_fn(session, method, url, **kwargs)
        else:
            resp = real_request_fn(method, url, **kwargs)
        elapsed_ms = (time.monotonic() - t0) * 1000

        hdrs = kwargs.get("headers") or {}
        entry = {
            "seq": self._seq,
            "request": {
                "method": method.upper(),
                "url": url,
                "headers": _redact_auth(dict(hdrs)),
                "json": kwargs.get("json"),
                "params": kwargs.get("params"),
            },
            "response": _serialise_response(resp, elapsed_ms),
        }
        self._entries.append(entry)
        self._seq += 1
        self.flush()  # eager flush — Click's sys.exit() skips atexit
        return resp

    def flush(self) -> None:
        if not self._entries:
            return
        with open(self.output_path, "w") as f:
            json.dump(self._entries, f, indent=2, default=str)
        print(
            f"[fixtures] Recorded {len(self._entries)} exchanges "
            f"-> {self.output_path}",
            file=sys.stderr,
        )

    def __del__(self) -> None:
        try:
            self.flush()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Replayer
# ---------------------------------------------------------------------------

class FixtureReplayer:
    """Intercepts requests.request() to replay from a fixture file."""

    def __init__(self, fixture_path: str | Path):
        self.fixture_path = Path(fixture_path)
        if not self.fixture_path.exists():
            raise FileNotFoundError(f"Fixture file not found: {self.fixture_path}")

        with open(self.fixture_path) as f:
            entries = json.load(f)

        self._ordered: list[dict] = list(entries)
        self._cursor = 0
        self._by_key: dict[str, list[dict]] = {}
        for entry in entries:
            req = entry["request"]
            key = _match_key(
                req["method"], req.get("url", req.get("endpoint", "")),
                req.get("json"), req.get("params"),
            )
            self._by_key.setdefault(key, []).append(entry)
        self._replay_count = 0

    def replay(self, real_request_fn: Any,
               method: str, url: str, **kwargs: Any) -> requests.Response:
        """Return a canned response without hitting the network."""
        kwargs.pop("session", None)  # not needed for replay
        json_body = kwargs.get("json")
        params = kwargs.get("params")
        key = _match_key(method, url, json_body, params)

        # Strategy 1: sequential match
        entry = None
        if self._cursor < len(self._ordered):
            candidate = self._ordered[self._cursor]
            req = candidate["request"]
            cand_key = _match_key(
                req["method"], req.get("url", req.get("endpoint", "")),
                req.get("json"), req.get("params"),
            )
            if cand_key == key:
                entry = candidate
                self._cursor += 1

        # Strategy 2: key-based fallback
        if entry is None and key in self._by_key:
            matches = self._by_key[key]
            if matches:
                entry = matches.pop(0)

        if entry is None:
            raise LookupError(
                f"[fixtures] No matching fixture for: {method.upper()} {url}\n"
                f"  key={key}\n"
                f"  Available: {list(self._by_key.keys())[:5]}..."
            )

        self._replay_count += 1
        return _build_fake_response(entry)

    @property
    def stats(self) -> dict:
        return {
            "total_fixtures": len(self._ordered),
            "replayed": self._replay_count,
            "remaining": len(self._ordered) - self._cursor,
        }


# ---------------------------------------------------------------------------
# Monkey-patch: patch requests.request globally
# ---------------------------------------------------------------------------

_original_request = None
_active_interceptor = None


def install() -> Optional[str]:
    """Check env vars and install the appropriate interceptor.

    Patches ``requests.api.request`` (which ``requests.get/post/put/delete``
    all delegate to) so that every HTTP call in the process is intercepted.

    Safe to call multiple times — subsequent calls are no-ops.

    Returns "record", "replay", or None.
    """
    global _original_request, _active_interceptor

    # Guard against double-install (e.g. __init__.py auto + manual call)
    if _original_request is not None:
        if _active_interceptor is not None:
            return "replay" if isinstance(_active_interceptor, FixtureReplayer) else "record"
        return None

    record_path = os.environ.get("MD_RECORD_FIXTURES")
    replay_path = os.environ.get("MD_REPLAY_FIXTURES")

    if not record_path and not replay_path:
        return None

    if record_path and replay_path:
        print(
            "[fixtures] WARNING: both env vars set — using REPLAY.",
            file=sys.stderr,
        )

    if replay_path:
        interceptor = FixtureReplayer(replay_path)
        mode = "replay"
        print(f"[fixtures] REPLAY mode from {replay_path}", file=sys.stderr)
    else:
        interceptor = FixtureRecorder(record_path)
        mode = "record"
        print(f"[fixtures] RECORD mode -> {record_path}", file=sys.stderr)

    _active_interceptor = interceptor

    # Save the REAL requests.api.request before patching.
    # We capture it from the requests.sessions module which is the true
    # implementation — requests.api.request is just a wrapper.
    import requests.sessions
    _original_request = requests.sessions.Session.request

    def patched_session_request(self, method, url, **kwargs):
        if mode == "replay":
            return interceptor.replay(_original_request, method, url,
                                      session=self, **kwargs)
        else:
            return interceptor.record(_original_request, method, url,
                                      session=self, **kwargs)

    requests.sessions.Session.request = patched_session_request  # type: ignore[assignment]

    return mode


def uninstall() -> None:
    """Restore original Session.request and flush any recorded fixtures."""
    global _original_request, _active_interceptor

    if _original_request is None:
        return

    import requests.sessions
    requests.sessions.Session.request = _original_request  # type: ignore[assignment]

    if isinstance(_active_interceptor, FixtureRecorder):
        _active_interceptor.flush()

    _original_request = None
    _active_interceptor = None


def get_interceptor() -> Optional[FixtureRecorder | FixtureReplayer]:
    """Get the active interceptor (if any), e.g. for stats."""
    return _active_interceptor
