"""Bulk polling for many pipeline datasets concurrently."""

import concurrent.futures
import json
import time
from collections import Counter
from typing import Dict, List

from .. import mcp
from .wait import _fetch_dataset_state

_DATASETS_BULK_MAX = 500
_DATASETS_BULK_WORKERS = 20
_TERMINAL_STATES = {"COMPLETED", "FAILED", "ERROR", "CANCELLED"}


@mcp.tool()
def wait_for_datasets_bulk(
    jobs: List[Dict[str, str]],
    poll_seconds: int = 5,
    timeout_seconds: int = 45,
) -> str:
    """Check the status of multiple pipeline datasets, polling until all are terminal.

    Args:
        jobs: list of job dicts. Max 500. Each dict must have "dataset_id".
            "upload_id" is optional — omit it to look up datasets directly by ID
            (simpler, avoids upload_id mapping errors). Include it only if needed.
            Examples:
              [{"dataset_id": "abc"}]                          # preferred
              [{"upload_id": "up-1", "dataset_id": "abc"}]    # also valid
        poll_seconds: seconds between status sweeps (default 5).
        timeout_seconds: max seconds before returning current summary (default 45).
            Keep below 60 — the MCP client enforces a hard 60-second per-call limit.
            If timeout is reached, call again to continue monitoring.

    Fetches all dataset states concurrently (up to 20 parallel connections).
    Polls until all datasets reach a terminal state or timeout is reached.

    Terminal states: COMPLETED, FAILED, ERROR, CANCELLED.
    Non-terminal (will appear in "pending"): RUNNING, PENDING, PROCESSING.

    Returns JSON:
      {
        "total": N,
        "all_terminal": true/false,
        "by_state": {"COMPLETED": N, "RUNNING": N, ...},
        "pending": [{"dataset_id": "...", "state": "..."}],
        "failed":  [{"dataset_id": "...", "state": "..."}]
      }
    When all_terminal is false, call wait_for_datasets_bulk again with the same jobs.
    Pass the "failed" list items to retry_dataset to re-run failed jobs.
    """
    if len(jobs) > _DATASETS_BULK_MAX:
        return json.dumps(
            {
                "error": (
                    f"Too many jobs: {len(jobs)}. "
                    f"Maximum per call is {_DATASETS_BULK_MAX}. "
                    "Split the call into smaller batches."
                )
            }
        )

    deadline = time.monotonic() + timeout_seconds

    while True:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=_DATASETS_BULK_WORKERS
        ) as executor:
            statuses = list(executor.map(_fetch_dataset_state, jobs))

        by_state: Dict[str, int] = dict(Counter(s["state"] for s in statuses))
        pending = [s for s in statuses if s["state"] not in _TERMINAL_STATES]
        failed = [s for s in statuses if s["state"] in ("FAILED", "ERROR")]
        all_terminal = len(pending) == 0

        if all_terminal or time.monotonic() >= deadline:
            return json.dumps(
                {
                    "total": len(jobs),
                    "all_terminal": all_terminal,
                    "by_state": by_state,
                    "pending": pending,
                    "failed": failed,
                },
                indent=2,
            )

        time.sleep(poll_seconds)
