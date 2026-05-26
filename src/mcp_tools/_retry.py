"""Bounded retry-with-backoff for transient 5xx HTTP failures.

The md_python resource layer raises bare ``Exception`` with messages of the
form ``"Failed to <verb> <noun>: 500 - <body>"`` (and similar for 502, 503,
504). The platform occasionally returns these on otherwise-healthy dataset
GET endpoints during load spikes — the user reported persistent 500s on
``get_dataset`` / ``list_datasets`` / ``query_datasets`` for 15-30 minutes
while the underlying dataset was perfectly fine and visible in the UI.

This helper wraps a callable and retries only when the raised message looks
like a transient server-side failure (5xx). 4xx and non-HTTP errors raise
immediately — they will not heal by waiting.

Scope: applied only to read-only dataset GETs from the MCP tool layer. Do
NOT wrap mutating calls (POST/PUT/DELETE) — at-most-once delivery is the
sane default for writes.
"""

import re
import time
from typing import Callable, TypeVar

T = TypeVar("T")

# Server-side HTTP statuses worth retrying. 500 is most common in practice;
# 502/503/504 cover gateway timeouts and proxy hiccups.
_RETRYABLE_STATUSES = (500, 502, 503, 504)
_STATUS_RE = re.compile(r"\b(5\d\d)\b")


def _is_retryable(exc: BaseException) -> bool:
    msg = str(exc)
    match = _STATUS_RE.search(msg)
    if match is None:
        return False
    try:
        return int(match.group(1)) in _RETRYABLE_STATUSES
    except ValueError:
        return False


def retry_on_5xx(
    fn: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn`` and retry on transient 5xx with exponential backoff.

    Sleeps ``base_delay * 2**attempt`` between attempts (1s, 2s, 4s by default
    — total wall time ≤ 7s for max_attempts=3). On non-5xx exceptions, raises
    immediately. On exhausted retries, re-raises the final exception.

    The ``sleep`` injection point is for tests; production code should leave
    the default.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be ≥ 1")

    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc) or attempt == max_attempts - 1:
                raise
            sleep(base_delay * (2**attempt))

    # Unreachable — the loop either returns or raises.
    assert last_exc is not None
    raise last_exc
