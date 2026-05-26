"""Tests for the bounded 5xx retry helper."""

import pytest

from mcp_tools._retry import _is_retryable, retry_on_5xx


class TestIsRetryable:
    @pytest.mark.parametrize(
        "msg",
        [
            "Failed to get dataset: 500 - {}",
            "Failed to query datasets: 502 - {}",
            "Service Unavailable: 503",
            "Gateway timeout: 504",
        ],
    )
    def test_classifies_5xx_as_retryable(self, msg):
        assert _is_retryable(Exception(msg))

    @pytest.mark.parametrize(
        "msg",
        [
            "Failed to get dataset: 404 - {}",
            "Bad request: 400 - {}",
            "Forbidden: 403",
            "Validation error",
            "ConnectionError: name or service not known",
        ],
    )
    def test_non_5xx_is_not_retryable(self, msg):
        assert not _is_retryable(Exception(msg))


class TestRetryOnFiveXx:
    def test_returns_value_on_first_success(self):
        calls = []

        def ok():
            calls.append(1)
            return "ok"

        assert retry_on_5xx(ok) == "ok"
        assert len(calls) == 1

    def test_retries_then_succeeds(self):
        calls = []
        sleeps: list[float] = []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise Exception("Failed to get dataset: 500 - upstream error")
            return "ok"

        assert (
            retry_on_5xx(
                flaky,
                max_attempts=3,
                base_delay=0.01,
                sleep=sleeps.append,
            )
            == "ok"
        )
        assert len(calls) == 3
        # base_delay * 2**0 then base_delay * 2**1
        assert sleeps == [0.01, 0.02]

    def test_exhausts_retries_then_reraises(self):
        sleeps: list[float] = []

        def always_500():
            raise Exception("Failed to get dataset: 503 - persistent")

        with pytest.raises(Exception, match="503"):
            retry_on_5xx(
                always_500,
                max_attempts=3,
                base_delay=0.01,
                sleep=sleeps.append,
            )
        # Two backoffs between three attempts.
        assert len(sleeps) == 2

    def test_4xx_raises_immediately_without_sleep(self):
        sleeps: list[float] = []

        def four_oh_four():
            raise Exception("Failed to get dataset: 404 - missing")

        with pytest.raises(Exception, match="404"):
            retry_on_5xx(
                four_oh_four,
                max_attempts=5,
                base_delay=0.01,
                sleep=sleeps.append,
            )
        assert sleeps == []

    def test_max_attempts_must_be_positive(self):
        with pytest.raises(ValueError):
            retry_on_5xx(lambda: None, max_attempts=0)
