"""Dataset management MCP tools."""

from .._destructive import _attach_destructive
from .crud import cancel_dataset, delete_dataset, retry_dataset
from .download import download_dataset_table
from .find import find_initial_dataset, find_initial_datasets
from .get import get_dataset
from .list import list_datasets, list_jobs
from .list_tables import list_dataset_tables
from .query import query_datasets
from .wait import wait_for_dataset
from .wait_bulk import wait_for_datasets_bulk

# Append the binding LLM-behaviour mandate to every destructive dataset tool's
# docstring. Single source of truth lives in mcp_tools._destructive.
_attach_destructive(
    delete_dataset,
    cancel_dataset,
)

__all__ = [
    "list_jobs",
    "list_datasets",
    "find_initial_dataset",
    "find_initial_datasets",
    "get_dataset",
    "wait_for_dataset",
    "wait_for_datasets_bulk",
    "retry_dataset",
    "delete_dataset",
    "cancel_dataset",
    "query_datasets",
    "download_dataset_table",
    "list_dataset_tables",
]
