"""Background file-upload executor — module-level state shared by create and queue."""

import concurrent.futures
import os

_LARGE_UPLOAD_THRESHOLD_BYTES = (
    int(os.environ.get("MD_LARGE_UPLOAD_THRESHOLD_MB", "100")) * 1024 * 1024
)
_large_upload_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.environ.get("MD_MAX_CONCURRENT_LARGE_UPLOADS", "1"))
)


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Return the module-level large-upload executor.

    Indirected via a function so tests can patch this single point
    rather than the bare module variable.
    """
    return _large_upload_executor


def _reset_executor() -> None:
    """Shut down the current executor (cancelling queued futures) and create a replacement."""
    global _large_upload_executor
    _get_executor().shutdown(wait=False, cancel_futures=True)
    _large_upload_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=int(os.environ.get("MD_MAX_CONCURRENT_LARGE_UPLOADS", "1"))
    )
