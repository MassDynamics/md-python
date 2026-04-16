"""Tests for the fixture recording/replay system."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from md_python.fixtures import (
    FixtureRecorder,
    FixtureReplayer,
    _build_fake_response,
    _match_key,
    _redact_auth,
    _url_to_endpoint,
    install,
    uninstall,
)


# ---------------------------------------------------------------------------
# Unit: helpers
# ---------------------------------------------------------------------------

class TestRedactAuth:
    def test_redacts_bearer_token(self):
        headers = {"Authorization": "Bearer eyJhbGciOi...", "accept": "application/json"}
        result = _redact_auth(headers)
        assert result["Authorization"] == "Bearer [REDACTED]"
        assert result["accept"] == "application/json"

    def test_leaves_non_bearer_alone(self):
        headers = {"Authorization": "Basic abc123"}
        result = _redact_auth(headers)
        assert result["Authorization"] == "Basic abc123"

    def test_no_auth_header(self):
        headers = {"accept": "application/json"}
        result = _redact_auth(headers)
        assert "Authorization" not in result


class TestUrlToEndpoint:
    def test_strips_dev_base(self):
        assert _url_to_endpoint("https://dev.massdynamics.com/api/health") == "/health"

    def test_strips_prod_base(self):
        assert _url_to_endpoint("https://app.massdynamics.com/api/uploads/abc") == "/uploads/abc"

    def test_unknown_base_uses_path(self):
        assert _url_to_endpoint("https://custom.example.com/api/foo") == "/api/foo"


class TestMatchKey:
    def test_simple_get(self):
        key = _match_key("GET", "https://dev.massdynamics.com/api/health")
        assert key == "GET|/health"

    def test_post_with_json(self):
        key = _match_key("POST", "https://dev.massdynamics.com/api/datasets",
                         json_body={"name": "test"})
        assert "POST|/datasets|" in key

    def test_deterministic(self):
        k1 = _match_key("POST", "/x", json_body={"b": 2, "a": 1})
        k2 = _match_key("POST", "/x", json_body={"a": 1, "b": 2})
        assert k1 == k2


class TestBuildFakeResponse:
    def test_json_response(self):
        entry = {
            "response": {
                "status_code": 200,
                "headers": {"content-type": "application/json"},
                "json": {"id": "abc", "name": "Test"},
                "text": None,
                "elapsed_ms": 50,
            }
        }
        resp = _build_fake_response(entry)
        assert resp.status_code == 200
        assert resp.json() == {"id": "abc", "name": "Test"}
        assert resp.ok is True

    def test_error_response(self):
        entry = {
            "response": {
                "status_code": 404,
                "headers": {},
                "json": {"error": "not found"},
                "text": None,
                "elapsed_ms": 10,
            }
        }
        resp = _build_fake_response(entry)
        assert resp.status_code == 404
        assert resp.ok is False
        with pytest.raises(requests.HTTPError):
            resp.raise_for_status()

    def test_text_response(self):
        entry = {
            "response": {
                "status_code": 200,
                "headers": {},
                "json": None,
                "text": "col1,col2\na,b",
                "elapsed_ms": 30,
            }
        }
        resp = _build_fake_response(entry)
        assert resp.text == "col1,col2\na,b"
        with pytest.raises(ValueError):
            resp.json()


# ---------------------------------------------------------------------------
# Integration: Recorder
# ---------------------------------------------------------------------------

class TestRecorder:
    def test_records_exchange(self, tmp_path):
        output = tmp_path / "fixtures.json"
        recorder = FixtureRecorder(output)

        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.json.return_value = {"status": "ok"}

        def fake_request(method, url, **kwargs):
            return mock_response

        recorder.record(
            fake_request, "GET",
            "https://dev.massdynamics.com/api/health",
            headers={"Authorization": "Bearer secret"},
        )

        assert output.exists()
        data = json.loads(output.read_text())
        assert len(data) == 1
        assert data[0]["request"]["method"] == "GET"
        assert data[0]["request"]["url"] == "https://dev.massdynamics.com/api/health"
        assert data[0]["request"]["headers"]["Authorization"] == "Bearer [REDACTED]"
        assert data[0]["response"]["status_code"] == 200

    def test_records_multiple(self, tmp_path):
        output = tmp_path / "multi.json"
        recorder = FixtureRecorder(output)

        for i in range(3):
            mock_resp = MagicMock(spec=requests.Response)
            mock_resp.status_code = 200
            mock_resp.headers = {}
            mock_resp.json.return_value = {"seq": i}

            recorder.record(
                lambda m, u, **kw: mock_resp,
                "GET", f"https://dev.massdynamics.com/api/item/{i}",
            )

        data = json.loads(output.read_text())
        assert len(data) == 3
        assert [e["seq"] for e in data] == [0, 1, 2]


# ---------------------------------------------------------------------------
# Integration: Replayer
# ---------------------------------------------------------------------------

class TestReplayer:
    def _write_fixtures(self, path, entries):
        path.write_text(json.dumps(entries))

    def test_sequential_replay(self, tmp_path):
        fixture_file = tmp_path / "fix.json"
        self._write_fixtures(fixture_file, [{
            "seq": 0,
            "request": {
                "method": "GET",
                "url": "https://dev.massdynamics.com/api/health",
                "json": None, "params": None,
            },
            "response": {
                "status_code": 200, "headers": {},
                "json": {"status": "ok"}, "text": None, "elapsed_ms": 5,
            },
        }])

        replayer = FixtureReplayer(fixture_file)
        resp = replayer.replay(
            None, "GET", "https://dev.massdynamics.com/api/health",
        )
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert replayer.stats["replayed"] == 1

    def test_no_match_raises(self, tmp_path):
        fixture_file = tmp_path / "fix.json"
        self._write_fixtures(fixture_file, [{
            "seq": 0,
            "request": {"method": "GET", "url": "https://dev.massdynamics.com/api/health",
                        "json": None, "params": None},
            "response": {"status_code": 200, "json": {}, "elapsed_ms": 10},
        }])

        replayer = FixtureReplayer(fixture_file)
        with pytest.raises(LookupError, match="No matching fixture"):
            replayer.replay(None, "GET", "https://dev.massdynamics.com/api/WRONG")

    def test_key_based_fallback(self, tmp_path):
        """Out-of-order requests still match via key lookup."""
        fixture_file = tmp_path / "fix.json"
        self._write_fixtures(fixture_file, [
            {"seq": 0,
             "request": {"method": "GET", "url": "https://dev.massdynamics.com/api/a",
                         "json": None, "params": None},
             "response": {"status_code": 200, "json": {"r": "a"}, "elapsed_ms": 10}},
            {"seq": 1,
             "request": {"method": "GET", "url": "https://dev.massdynamics.com/api/b",
                         "json": None, "params": None},
             "response": {"status_code": 200, "json": {"r": "b"}, "elapsed_ms": 10}},
        ])

        replayer = FixtureReplayer(fixture_file)
        # Request /b first (out of recorded order)
        resp = replayer.replay(None, "GET", "https://dev.massdynamics.com/api/b")
        assert resp.json() == {"r": "b"}
        resp = replayer.replay(None, "GET", "https://dev.massdynamics.com/api/a")
        assert resp.json() == {"r": "a"}

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            FixtureReplayer(tmp_path / "nonexistent.json")


# ---------------------------------------------------------------------------
# Integration: install / uninstall
# ---------------------------------------------------------------------------

class TestInstallUninstall:
    def test_no_env_vars_returns_none(self, monkeypatch):
        monkeypatch.delenv("MD_RECORD_FIXTURES", raising=False)
        monkeypatch.delenv("MD_REPLAY_FIXTURES", raising=False)
        result = install()
        assert result is None

    def test_install_replay_patches_session(self, monkeypatch, tmp_path):
        fixture_file = tmp_path / "test.json"
        fixture_file.write_text(json.dumps([{
            "seq": 0,
            "request": {"method": "GET", "url": "https://dev.massdynamics.com/api/health",
                        "json": None, "params": None},
            "response": {"status_code": 200, "json": {"status": "ok"}, "elapsed_ms": 5},
        }]))

        import requests.sessions
        original = requests.sessions.Session.request

        monkeypatch.delenv("MD_RECORD_FIXTURES", raising=False)
        monkeypatch.setenv("MD_REPLAY_FIXTURES", str(fixture_file))

        try:
            result = install()
            assert result == "replay"
            assert requests.sessions.Session.request is not original
        finally:
            uninstall()
            assert requests.sessions.Session.request is original

    def test_install_record_mode(self, monkeypatch, tmp_path):
        output = tmp_path / "record.json"
        monkeypatch.setenv("MD_RECORD_FIXTURES", str(output))
        monkeypatch.delenv("MD_REPLAY_FIXTURES", raising=False)

        try:
            result = install()
            assert result == "record"
        finally:
            uninstall()

    def test_uninstall_restores_original(self, monkeypatch, tmp_path):
        import requests.sessions
        original = requests.sessions.Session.request

        output = tmp_path / "record.json"
        monkeypatch.setenv("MD_RECORD_FIXTURES", str(output))
        monkeypatch.delenv("MD_REPLAY_FIXTURES", raising=False)

        install()
        assert requests.sessions.Session.request is not original

        uninstall()
        assert requests.sessions.Session.request is original
