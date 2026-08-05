from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.dataclasses import dataclass as pydantic_dataclass

from ..dataset import Dataset
from ._base import BaseDatasetBuilder

# The flow accepts peptide datasets ONLY. PTM site summarisation reads the
# PTM_sites table that hangs off a peptide dataset, so there is no meaningful
# protein / gene / metabolite variant (flows/ptm_intensity_dataset.py raises
# for any other entity_type).
_PTM_INTENSITY_ENTITY_TYPES = {"peptide"}

# Summarisation methods (PTMIntensityTableParamsProperties.summarization_method).
# "tmp" is Tukey Median Polish — it works in log space and is exponentiated
# back, so it is NOT interchangeable with "sum".
_PTM_SUMMARISATION_METHODS = {"sum", "median", "tmp"}

# Modification types offered by the flow's Modification Types multi-select.
# Mirrors md-converter's MODIFICATION_TYPE_TO_UNIMOD keys (and the identical
# table in visualisations-service module_sdk ptm_modifications.py). Note the
# labels are the LONG forms — "Phosphorylation", not md-converter's internal
# "Phospho" — because that is what the form and the filtration layer match on.
_PTM_MODIFICATION_TYPES = {
    "Acetylation",
    "Oxidation",
    "Phosphorylation",
    "Carbamidomethylation",
    "Deamidation",
    "Pyro-glu",
    "Met-loss",
    "TMTpro",
}

_FLANKING_WINDOW_MIN = 0
_FLANKING_WINDOW_MAX = 30


@pydantic_dataclass
class PtmIntensityTableDataset(BaseDatasetBuilder):
    """Builder for a PTM Intensity Table (PTM site summarisation) dataset.

    Re-runs the PTM site-summarisation rollup that already ran automatically at
    upload time, but with caller-chosen parameters. Peptide intensities that
    report the same protein modification site (different charge states, missed
    cleavages, co-occurring mods) are collapsed to ONE row per site per sample,
    producing site-level ``PTM_Intensity`` + ``PTM_Metadata`` tables plus a
    ``PTM_unmapped`` table listing sites that could not be placed on a protein
    sequence.

    **With every default left alone the output reproduces the upload-time PTM
    tables exactly** (see flows/ptm_intensity_dataset_types.py).

    The input MUST be a peptide intensity dataset that carries a ``PTM_sites``
    table — i.e. the upload contained modified peptides. The flow raises if that
    table is absent.

    Backend job slug ``"ptm_intensity_table"``; the output dataset type is
    ``INTENSITY`` (DATASET_RUN_TYPE in md-converter's
    infra/local/register-ptm-intensity-flow.sh), server-derived from the slug.

    Required: ``input_dataset_ids`` (exactly one peptide INTENSITY dataset
    UUID), ``dataset_name``.

    Optional (defaults reproduce upload-time output):
      entity_type              str   "peptide" only, default "peptide"
      loc_probability_threshold float 0.0-1.0, default 0.0 (keep everything)
      modification_types       list[str] subset of the 8 supported types;
                                     default [] = keep all
      summarization_method     str   sum|median|tmp, default "sum"
      include_imputed          bool  default False
      impute_missing_values    bool  default True
      flanking_window          int   0-30, default 20

    NOTE the rollup fetches protein sequences from UniProt at runtime, so a run
    is not instantaneous and can fail for reasons unrelated to the input data.
    """

    # input_dataset_ids, dataset_name inherited.
    entity_type: str = "peptide"
    loc_probability_threshold: float = 0.0
    modification_types: Optional[List[str]] = None
    summarization_method: str = "sum"
    include_imputed: bool = False
    impute_missing_values: bool = True
    flanking_window: int = 20
    job_slug: str = "ptm_intensity_table"

    def to_dataset(self) -> Dataset:
        # As with MOFADataset / WgcnaDataset: the output dataset type is NOT a
        # member of job_run_params. The create path POSTs a flat payload and the
        # server derives the output type from the job slug's run_type. Sending
        # it would ship an unexpected key into the params model.
        #
        # Every param is sent unconditionally — the flow has no conditional
        # visibility clause beyond `when entity_type == "peptide"`, which is
        # always true here.
        params: Dict[str, Any] = {
            "entity_type": self.entity_type,
            "loc_probability_threshold": self.loc_probability_threshold,
            # The form's default is an empty list, not null.
            "modification_types": list(self.modification_types or []),
            "summarization_method": self.summarization_method,
            "include_imputed": self.include_imputed,
            "impute_missing_values": self.impute_missing_values,
            "flanking_window": self.flanking_window,
        }
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
            "PtmIntensityTableDataset parameters:",
            "- input_dataset_ids (List[str]): exactly one PEPTIDE intensity"
            " dataset UUID, carrying a PTM_sites table",
            "- dataset_name (str): name for the output PTM dataset",
            "- entity_type (str): 'peptide' only, default 'peptide'",
            "- loc_probability_threshold (float): 0.0-1.0, default 0.0."
            " Filters on PTMProbSample (per-sample), NOT PTMProbMax",
            "- modification_types (List[str]): subset of "
            f"{sorted(_PTM_MODIFICATION_TYPES)}; default [] = keep all",
            "- summarization_method (str): sum|median|tmp, default 'sum'",
            "- include_imputed (bool): include imputed PEPTIDE intensities in"
            " the summarisation, default False",
            "- impute_missing_values (bool): impute missing SITE-level"
            " intensities (MNAR/Perseus) after rollup, default True",
            "- flanking_window (int): 0-30, default 20",
            "- job_slug (str): backend job slug, default 'ptm_intensity_table'",
        ]
        return "\n".join(lines)

    def validate(self) -> None:
        if not self.input_dataset_ids:
            raise ValueError("input_dataset_ids cannot be empty")
        if len(self.input_dataset_ids) != 1:
            raise ValueError(
                "PTM site summarisation requires exactly 1 input dataset (the "
                "peptide INTENSITY dataset carrying PTM_sites); "
                f"got {len(self.input_dataset_ids)}"
            )
        if not self.dataset_name:
            raise ValueError("dataset_name is required")
        if self.entity_type not in _PTM_INTENSITY_ENTITY_TYPES:
            raise ValueError(
                "entity_type must be 'peptide' — PTM site summarisation only "
                "applies to peptide datasets (they carry the PTM_sites table); "
                f"got {self.entity_type!r}"
            )
        if not 0.0 <= self.loc_probability_threshold <= 1.0:
            raise ValueError("loc_probability_threshold must be between 0.0 and 1.0")
        if self.modification_types is not None:
            if not isinstance(self.modification_types, list):
                raise ValueError("modification_types must be a list or None")
            unsupported = [
                m for m in self.modification_types if m not in _PTM_MODIFICATION_TYPES
            ]
            if unsupported:
                raise ValueError(
                    f"Unsupported modification_types {unsupported}. "
                    f"Valid: {sorted(_PTM_MODIFICATION_TYPES)}"
                )
        if self.summarization_method not in _PTM_SUMMARISATION_METHODS:
            raise ValueError(
                "summarization_method must be one of: "
                f"{sorted(_PTM_SUMMARISATION_METHODS)}"
            )
        if not isinstance(self.include_imputed, bool):
            raise ValueError("include_imputed must be a bool")
        if not isinstance(self.impute_missing_values, bool):
            raise ValueError("impute_missing_values must be a bool")
        if not isinstance(self.flanking_window, int) or isinstance(
            self.flanking_window, bool
        ):
            raise ValueError("flanking_window must be an int")
        if not _FLANKING_WINDOW_MIN <= self.flanking_window <= _FLANKING_WINDOW_MAX:
            raise ValueError(
                "flanking_window must be between "
                f"{_FLANKING_WINDOW_MIN} and {_FLANKING_WINDOW_MAX}"
            )
