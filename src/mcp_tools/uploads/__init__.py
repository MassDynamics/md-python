"""Upload management MCP tools."""

from .._destructive import _attach_destructive
from ._executor import _get_executor
from .create import create_upload, create_upload_from_csv
from .delete import delete_upload
from .get import get_upload, get_upload_sample_metadata, update_sample_metadata
from .query import query_uploads
from .queue import cancel_upload_queue, list_uploads_status
from .update import update_upload
from .validate import validate_upload_inputs
from .wait import wait_for_upload

# Append the binding LLM-behaviour mandate to every destructive upload tool's
# docstring. Single source of truth lives in mcp_tools._destructive.
_attach_destructive(
    delete_upload,
    update_sample_metadata,
    cancel_upload_queue,
)

__all__ = [
    "get_upload",
    "validate_upload_inputs",
    "create_upload",
    "create_upload_from_csv",
    "delete_upload",
    "get_upload_sample_metadata",
    "update_upload",
    "update_sample_metadata",
    "wait_for_upload",
    "cancel_upload_queue",
    "list_uploads_status",
    "query_uploads",
]
