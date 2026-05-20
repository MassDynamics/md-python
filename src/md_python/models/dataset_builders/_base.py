from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset

if TYPE_CHECKING:
    from ...base_client import BaseMDClient


@pydantic_dataclass
class BaseDatasetBuilder(ABC):
    """Abstract base for dataset builders that produce Dataset objects.

    Shared parameters across dataset builders.
    """

    # Shared fields
    input_dataset_ids: List[str]
    dataset_name: str

    @abstractmethod
    def to_dataset(self) -> Dataset: ...

    @abstractmethod
    def validate(self) -> None:
        """Validate input fields; subclasses must implement."""
        ...

    def run(self, client: "BaseMDClient") -> str:
        """Create the dataset via the API and return the new dataset_id."""
        self.validate()
        return client.datasets.create(self.to_dataset())  # type: ignore[attr-defined, no-any-return]
