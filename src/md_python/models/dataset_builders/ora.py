from typing import List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ._base import BaseDatasetBuilder

# Entity types accepted by the ORA params model (MDORAParamsProperties.entity_type).
_ORA_ENTITY_TYPES = {"protein", "gene"}

# Organisms recognised by the ORA gene-set databases
# (MDORAParamsProperties.species enum).
_ORA_SPECIES = {"human", "mouse", "yeast", "chinese_hamster"}

# Background-universe options (MDORAParamsProperties.background enum).
_ORA_BACKGROUNDS = {
    "Detected features in this dataset",
    "Custom Background List",
    "Selected Database",
}


@pydantic_dataclass
class OraDataset(BaseDatasetBuilder):
    """Builder for an ORA (Over-Representation Analysis) dataset.

    ORA tests whether a user-supplied list of entities (the *foreground*)
    is enriched for any pathway / gene-set in the chosen database using the
    hypergeometric test with Benjamini-Hochberg correction (clusterProfiler,
    Wu et al. 2021).

    The backend job slug is ``"ora"`` and the output dataset type is
    ``"ORA"``. Parameter names, enums, defaults and required-ness are taken
    verbatim from the live job catalogue (``/jobs`` -> slug "ora",
    ``MDORAParamsProperties``).

    Required: ``input_dataset_ids`` (exactly one INTENSITY dataset UUID),
    ``dataset_name``, ``foreground_ids``, ``species``, ``background``.

    Optional (all have backend-aligned defaults):
      entity_type        str   protein|gene, default "protein"
      database           str   default "GO - Biological Process"
                               (options depend on species)
      custom_background_ids list[str]  required only when
                               background == "Custom Background List"
      min_gene_set_size  int   >= 1, default 5
      max_gene_set_size  int   >= 1, default 500
    """

    # input_dataset_ids, dataset_name inherited.
    foreground_ids: List[str]
    species: str
    background: str = "Detected features in this dataset"
    entity_type: str = "protein"
    database: str = "GO - Biological Process"
    custom_background_ids: Optional[List[str]] = None
    min_gene_set_size: int = 5
    max_gene_set_size: int = 500
    job_slug: str = "ora"

    def to_dataset(self) -> Dataset:
        # NOTE: the output dataset type ("ORA") is NOT a member of
        # job_run_params. The create path POSTs a flat payload and the server
        # derives the output type from the job slug's run_type. Embedding it
        # here would ship an unexpected key into MDORAParamsProperties, which
        # has a fixed field set — mirror MOFADataset and send params only.
        params: dict = {
            "entity_type": self.entity_type,
            "foreground_ids": self.foreground_ids,
            "species": self.species,
            "database": self.database,
            "background": self.background,
            "min_gene_set_size": self.min_gene_set_size,
            "max_gene_set_size": self.max_gene_set_size,
        }
        # custom_background_ids is only meaningful (and only sent) when the
        # background universe is the user-supplied list.
        if self.background == "Custom Background List":
            params["custom_background_ids"] = self.custom_background_ids
        return Dataset(
            input_dataset_ids=[UUID(x) for x in self.input_dataset_ids],
            name=self.dataset_name,
            job_slug=self.job_slug,
            job_run_params=params,
        )

    @classmethod
    def help(cls) -> str:
        """Return a human-readable description of parameters."""
        lines = [
            "OraDataset parameters:",
            "- input_dataset_ids (List[str]): exactly one INTENSITY dataset UUID",
            "- dataset_name (str): name for the output ORA dataset",
            "- foreground_ids (List[str]): entity IDs forming the foreground",
            "- species (str): human|mouse|yeast|chinese_hamster",
            "- background (str): 'Detected features in this dataset' |"
            " 'Custom Background List' | 'Selected Database',"
            " default 'Detected features in this dataset'",
            "- entity_type (str): protein|gene, default 'protein'",
            "- database (str): gene-set collection, default"
            " 'GO - Biological Process' (options depend on species)",
            "- custom_background_ids (List[str]): required only when"
            " background='Custom Background List'",
            "- min_gene_set_size (int): >= 1, default 5",
            "- max_gene_set_size (int): >= 1, default 500",
            "- job_slug (str): backend job slug, default 'ora'",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if len(self.input_dataset_ids) != 1:
            raise ValueError(
                "ORA requires exactly 1 input dataset (the INTENSITY dataset); "
                f"got {len(self.input_dataset_ids)}"
            )
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if not self.foreground_ids:
            raise ValueError("foreground_ids cannot be empty")
        if self.entity_type not in _ORA_ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of: {sorted(_ORA_ENTITY_TYPES)}")
        if self.species not in _ORA_SPECIES:
            raise ValueError(f"species must be one of: {sorted(_ORA_SPECIES)}")
        if not self.database:
            raise ValueError("database is required")
        if self.background not in _ORA_BACKGROUNDS:
            raise ValueError(f"background must be one of: {sorted(_ORA_BACKGROUNDS)}")
        if (
            self.background == "Custom Background List"
            and not self.custom_background_ids
        ):
            raise ValueError(
                "custom_background_ids is required when "
                "background='Custom Background List'"
            )
        if not isinstance(self.min_gene_set_size, int) or isinstance(
            self.min_gene_set_size, bool
        ):
            raise ValueError("min_gene_set_size must be an int")
        if self.min_gene_set_size < 1:
            raise ValueError("min_gene_set_size must be >= 1")
        if not isinstance(self.max_gene_set_size, int) or isinstance(
            self.max_gene_set_size, bool
        ):
            raise ValueError("max_gene_set_size must be an int")
        if self.max_gene_set_size < 1:
            raise ValueError("max_gene_set_size must be >= 1")
        if self.min_gene_set_size > self.max_gene_set_size:
            raise ValueError(
                "min_gene_set_size must be <= max_gene_set_size "
                f"(got {self.min_gene_set_size} > {self.max_gene_set_size})"
            )
