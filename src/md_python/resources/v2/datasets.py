"""
Datasets resource for the MD Python v2 client
"""

import time
from typing import TYPE_CHECKING, Any, Dict, List, NoReturn, Optional, Sequence, Tuple

from ...models import Dataset

if TYPE_CHECKING:
    from ...base_client import BaseMDClient

# The MCP tool name the model can actually call. The SDK method behind it is
# Datasets.list_table_names, but that name is NOT exposed over MCP — every
# model-facing string must say ``list_dataset_tables`` or the model is being
# pointed at a tool that does not exist.
DISCOVERY_TOOL = "list_dataset_tables"

# Machine-readable discriminators for the THREE distinct causes that all used
# to surface as the same "404 - Not found". Telemetry: the model could not tell
# them apart and fell into a table-name guessing spiral on a dataset that had
# been deleted in the web UI.
REASON_DATASET_NOT_FOUND = "dataset_not_found"
REASON_TABLE_NAME_INVALID = "table_name_invalid"
REASON_TABLE_NOT_IN_MODALITY = "table_not_in_this_modality"
REASON_INDETERMINATE = "indeterminate"


class TableNotFoundError(ValueError):
    """Raised when a dataset table name does not exist (the DATASET does).

    Subclasses ValueError so existing ``except Exception`` callers keep
    working while new callers can catch it specifically. The message lists
    the valid table names for the dataset, or — when the dataset's type has
    no verified catalogue — says so and tells the caller not to guess.

    ``reason`` discriminates the two table-level causes:
    ``table_name_invalid`` (wrong name / wrong case) and
    ``table_not_in_this_modality`` (a real table name, but for an omics layer
    this dataset does not have — e.g. Protein_Intensity on metabolomics).
    """

    def __init__(self, message: str, reason: str = REASON_TABLE_NAME_INVALID) -> None:
        super().__init__(message)
        self.reason = reason


class DatasetNotFoundError(ValueError):
    """Raised when the DATASET itself does not resolve.

    Deliberately NOT a TableNotFoundError: the table name is irrelevant when
    the dataset is gone, and mixing the two is what sent the model guessing
    table names against a dead id. Datasets can be deleted in the web UI and
    the MCP is never notified, so existence is checked at call time, never
    cached.
    """

    reason = REASON_DATASET_NOT_FOUND


def dataset_not_found_message(dataset_id: str) -> str:
    """Message for a dataset id that does not resolve.

    Says nothing about table names on purpose — see DatasetNotFoundError.
    """
    return (
        f"Dataset '{dataset_id}' does not exist (the dataset lookup itself "
        "returned 404). It may have been DELETED in the web UI — the MCP is "
        "NOT notified of UI-side deletions, so an id that worked earlier in "
        "this session can be dead now — or the id is stale/mistyped. This is "
        "NOT a table-name problem: do NOT try other table names, none of them "
        "can work. Re-discover the dataset with list_datasets / "
        "query_datasets / find_initial_dataset(upload_id) and retry with the "
        "id they return."
    )


# Canonical dataset-table names, verified against data-set-service flow source
# (set_up_initial_data_set.py, pairwise_comparison.py, dose_response.py at
# pinned SHA 3e893aa). data-set-service stores tables under CAPITALISED,
# entity-specific names — passing a lowercase guess (e.g. "protein_intensity")
# makes the workflow download_table route's fetch_table_url return blank, which
# surfaces as a misleading 404 {"error":"Not found"}. There is no public
# endpoint to list a dataset's tables, so this catalogue is the source of truth
# for valid ``table_name`` values.
#
# For INTENSITY datasets the tables present depend on the omics modality of the
# upload; the prefix identifies the entity (Protein_/Gene_/Metabolite_/...).
INTENSITY_TABLES_BY_ENTITY: Dict[str, List[str]] = {
    "protein": ["Protein_Intensity", "Protein_Metadata"],
    "peptide": ["Peptide_Intensity", "Peptide_Metadata"],
    "gene": ["Gene_Intensity", "Gene_Metadata"],
    "metabolite": ["Metabolite_Intensity", "Metabolite_Metadata"],
    "ptm": ["PTM_Intensity", "PTM_Metadata", "PTM_sites"],
}

# Flat table-name catalogue keyed by dataset ``type``. NORMALISATION_AND_-
# IMPUTATION runs register as type INTENSITY (see find_initial_dataset), so they
# share the intensity tables. PAIRWISE / DOSE_RESPONSE names are the
# FlowOutPutTable names emitted by their respective flows.
KNOWN_TABLES_BY_DATASET_TYPE: Dict[str, List[str]] = {
    "INTENSITY": [
        name for tables in INTENSITY_TABLES_BY_ENTITY.values() for name in tables
    ],
    "PAIRWISE": ["output_comparisons", "runtime_metadata"],
    "DOSE_RESPONSE": [
        "output_curves",
        "output_volcanoes",
        "input_drc",
        "runtime_metadata",
    ],
}

# Tables that are known to EXIST for a dataset type whose full table list is
# NOT catalogued (the type is absent from KNOWN_TABLES_BY_DATASET_TYPE). These
# lists are NON-EXHAUSTIVE and must never be used to reject a table name — they
# only exist so an uncatalogued type is not a total dead end.
#
# ENRICHMENT (run_gsea / run_ora, backend slug "camera_gsea"): the flow source
# is not determinable — data-set-service's `enrichment` flow is a stub (`pass`)
# and `camera_gsea` lives outside the repo — so the results-table name is
# UNKNOWN and is deliberately NOT guessed here. "runtime_metadata" is the only
# ENRICHMENT table observed to download successfully.
CONFIRMED_TABLES_BY_UNCATALOGUED_TYPE: Dict[str, List[str]] = {
    "ENRICHMENT": ["runtime_metadata"],
}

# Upload sources that map 1:1 onto an omics entity. The proteomics sources
# (md_format / maxquant / diann_tabular / tims_diann / spectronaut) produce
# protein AND possibly peptide, so they infer NOTHING — collapsing them would
# be a guess. Mirrors mcp_tools.workspaces._introspect.dataset_inputs, which
# uses the same signal for entity_type inference.
UNAMBIGUOUS_UPLOAD_SOURCE_ENTITIES: Dict[str, str] = {
    "md_format_gene": "gene",
    "md_format_metabolite": "metabolite",
}

ENTITY_FROM_JOB_RUN_PARAMS = "job_run_params"
ENTITY_FROM_UPLOAD_SOURCE = "upload_source"


def entity_from_job_run_params(job_run_params: Any) -> Optional[str]:
    """Entity/omics modality from a dataset's ``job_run_params``, or None.

    Authoritative: every pipeline builder (NI / pairwise / ANOVA / GSEA / ORA
    / WGCNA) persists ``entity_type``, and it is the same signal the web UI
    reads. Values outside the known entity vocabulary resolve to None rather
    than being trusted blindly.
    """
    if not isinstance(job_run_params, dict):
        return None
    value = job_run_params.get("entity_type")
    if isinstance(value, str) and value in INTENSITY_TABLES_BY_ENTITY:
        return value
    return None


def entity_from_upload_source(source: Any) -> Optional[str]:
    """Entity from an upload's ``source``, or None when the source is ambiguous."""
    if not isinstance(source, str):
        return None
    return UNAMBIGUOUS_UPLOAD_SOURCE_ENTITIES.get(source)


def entity_of_table(table_name: str) -> Optional[str]:
    """The entity an INTENSITY table name belongs to, or None."""
    for entity, tables in INTENSITY_TABLES_BY_ENTITY.items():
        if table_name in tables:
            return entity
    return None


def find_case_insensitive_match(
    table_name: str, tables: Sequence[str]
) -> Optional[str]:
    """Return the catalogued name that differs from ``table_name`` only by case.

    Case is the single most common table-name mistake ("protein_intensity" for
    "Protein_Intensity"), so it gets a dedicated hint.
    """
    lowered = table_name.lower()
    for candidate in tables:
        if candidate.lower() == lowered and candidate != table_name:
            return candidate
    return None


def invalid_table_message(
    dataset_id: str,
    table_name: str,
    dataset_type: str,
    tables: Sequence[str],
) -> str:
    """Message for a bad table name on a dataset type that IS catalogued.

    The valid names are known, so the message is closed: here is the list,
    names are case-sensitive, stop guessing.
    """
    parts = [f"Table '{table_name}' not found in dataset '{dataset_id}'."]

    match = find_case_insensitive_match(table_name, tables)
    if match:
        parts.append(
            f"Did you mean '{match}'? Table names are CASE-SENSITIVE — "
            f"'{table_name}' and '{match}' are not the same table."
        )

    parts.append(
        f"Valid table names for dataset type '{dataset_type}': {list(tables)}. "
        "Names are case-sensitive; use one of these exactly. Do not try other "
        f"names — no other table exists. Call {DISCOVERY_TOOL}(dataset_id) to "
        "list them."
    )
    return " ".join(parts)


def wrong_modality_message(
    dataset_id: str,
    table_name: str,
    entity: str,
    entity_resolved_from: str,
    other_entity: str,
    tables: Sequence[str],
) -> str:
    """Message for a real table name that belongs to another omics layer.

    Cause (c): the name is spelled right, the dataset is alive, but the table
    cannot exist because the dataset has no such layer (Protein_Intensity on a
    metabolomics dataset). Telemetry: the model 404'd on exactly this and then
    abandoned the objective.
    """
    return (
        f"Table '{table_name}' does not exist in dataset '{dataset_id}'. This "
        f"is a {entity} dataset (entity resolved from {entity_resolved_from}) "
        f"and '{table_name}' belongs to the '{other_entity}' layer, which this "
        f"dataset does not have. Tables for this dataset: {list(tables)}. Use "
        f"one of those — no {other_entity} table exists here under any "
        "spelling, so do not retry with another name."
    )


def classify_table_name(
    info: Dict[str, Any], table_name: str
) -> Optional[Dict[str, Any]]:
    """Decide whether ``table_name`` can possibly exist, from a catalogue lookup.

    ``info`` is a ``list_table_names`` result. Returns None when the name is
    acceptable (or when nothing can be proven — an uncatalogued type), else a
    rejection block carrying a machine-readable ``reason``:

      * ``table_name_invalid``        — not a table of this dataset type, or a
        case mismatch of one (``did_you_mean`` is set for the case mismatch).
      * ``table_not_in_this_modality`` — a real table name for this type, but
        for an omics layer this dataset does not have.

    Never returns ``dataset_not_found``: the dataset is known to exist by the
    time a catalogue lookup has succeeded.
    """
    if not info.get("catalogued"):
        return None

    candidates: List[str] = list(info.get("candidates") or [])
    if table_name in candidates:
        return None

    dataset_id = str(info.get("dataset_id", ""))
    dataset_type = str(info.get("type", ""))
    entity = info.get("entity")
    resolved_from = info.get("entity_resolved_from")

    rejection: Dict[str, Any] = {
        "reason": REASON_TABLE_NAME_INVALID,
        "valid_tables": candidates,
        "case_sensitive": True,
        "entity": entity,
        "entity_resolved_from": resolved_from,
    }

    # Case mismatch against a table this dataset DOES have — the single most
    # common mistake, and it must keep winning over the modality check.
    case_match = find_case_insensitive_match(table_name, candidates)
    if case_match:
        rejection["error"] = invalid_table_message(
            dataset_id, table_name, dataset_type, candidates
        )
        rejection["did_you_mean"] = case_match
        return rejection

    # A real name for this dataset TYPE, but for another entity: only provable
    # once the entity has actually been resolved.
    type_tables = KNOWN_TABLES_BY_DATASET_TYPE.get(dataset_type, [])
    known_name = (
        table_name
        if table_name in type_tables
        else find_case_insensitive_match(table_name, type_tables)
    )
    other_entity = entity_of_table(known_name) if known_name else None
    if entity and other_entity and other_entity != entity:
        rejection["reason"] = REASON_TABLE_NOT_IN_MODALITY
        rejection["error"] = wrong_modality_message(
            dataset_id,
            table_name,
            str(entity),
            str(resolved_from),
            other_entity,
            candidates,
        )
        return rejection

    rejection["error"] = invalid_table_message(
        dataset_id, table_name, dataset_type, candidates
    )
    return rejection


def uncatalogued_table_message(
    dataset_id: str, table_name: str, dataset_type: str
) -> str:
    """Message for a bad table name on a dataset type that is NOT catalogued.

    We genuinely cannot enumerate this type's tables, so the message must stop
    the model from brute-forcing: telemetry shows twelve consecutive 404 guesses
    on an ENRICHMENT dataset before the objective was abandoned.
    """
    confirmed = CONFIRMED_TABLES_BY_UNCATALOGUED_TYPE.get(dataset_type, [])
    parts = [
        f"Table '{table_name}' not found in dataset '{dataset_id}'.",
        f"The table names for dataset type '{dataset_type}' CANNOT be "
        "enumerated: there is no list-tables endpoint and no verified "
        "catalogue for this type.",
        "DO NOT brute-force guess table names. Every wrong guess is a 404; "
        "guessing has been observed to burn a dozen calls and still fail.",
    ]
    if confirmed:
        parts.append(
            f"Tables confirmed to exist for this type (NOT an exhaustive list, "
            f"and not necessarily the results table): {confirmed}."
        )
    parts.append(
        "Instead: tell the user this dataset type's tables are not enumerable "
        "and ask them for the exact table name, or view the results through "
        "the dataset's visualisation module."
    )
    return " ".join(parts)


class Datasets:
    """V2 datasets resource — flat payload, no wrapper"""

    def __init__(self, client: "BaseMDClient"):
        self._client = client

    def create(self, dataset: Dataset) -> str:
        """Create a new dataset.

        V2 uses a flat payload (no wrapping 'dataset' key).

        Args:
            dataset: Dataset object with creation parameters

        Returns:
            Created dataset ID
        """
        payload: Dict[str, Any] = {
            "input_dataset_ids": [
                str(dataset_id) for dataset_id in dataset.input_dataset_ids
            ],
            "name": dataset.name,
            "job_slug": dataset.job_slug,
            "job_run_params": dataset.job_run_params or {},
        }

        response = self._client._make_request(
            method="POST",
            endpoint="/datasets",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code in (200, 201):
            return str(response.json()["dataset_id"])
        else:
            raise Exception(
                f"Failed to create dataset: {response.status_code} - {response.text}"
            )

    def list_by_upload(self, upload_id: str) -> List[Dataset]:
        """Get datasets belonging to an upload"""
        response = self._client._make_request(
            method="POST",
            endpoint="/datasets/query",
            json={"upload_id": upload_id},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return [Dataset.from_json(d) for d in response.json().get("data", [])]
        else:
            raise Exception(
                f"Failed to get datasets: {response.status_code} - {response.text}"
            )

    def get_by_id(self, dataset_id: str) -> Optional[Dataset]:
        """Get a single dataset by ID"""
        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets/{dataset_id}",
        )

        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise Exception(
                f"Failed to get dataset: {response.status_code} - {response.text}"
            )
        return Dataset.from_json(response.json())

    def download_table_url(
        self, dataset_id: str, table_name: str, format: str = "csv"
    ) -> str:
        """Get a presigned download URL for a dataset table.

        The API returns a 302 redirect to a presigned URL.

        ``table_name`` must be the CAPITALISED, entity-specific name the
        data-set-service stores the table under — NOT a lowercase guess.
        Call the ``list_dataset_tables`` tool to discover the valid names.
        Common names by dataset type:

          INTENSITY (and NORMALISATION_AND_IMPUTATION):
            proteomics     -> "Protein_Intensity", "Protein_Metadata"
            transcriptomics-> "Gene_Intensity", "Gene_Metadata"
            metabolomics   -> "Metabolite_Intensity", "Metabolite_Metadata"
            (also "Peptide_*"/"PTM_*" when those layers are present)
          PAIRWISE:        "output_comparisons", "runtime_metadata"
          DOSE_RESPONSE:   "output_curves", "output_volcanoes",
                           "input_drc", "runtime_metadata"

        Other types (e.g. ENRICHMENT, ANOVA) have no verified catalogue —
        their table names cannot be enumerated and must not be guessed.

        A 404 has three different causes and they are NOT interchangeable:
        the DATASET is gone (DatasetNotFoundError — it may have been deleted
        in the web UI), the table NAME is wrong/wrong-case, or the table is
        real but belongs to an omics layer this dataset does not have. The
        latter two raise TableNotFoundError, discriminated by ``.reason``.

        Does NOT transfer the file — it only resolves a presigned URL — so it
        doubles as a cheap existence probe for a single table.
        """
        if format not in ("csv", "parquet"):
            raise ValueError(f"format must be 'csv' or 'parquet', got '{format}'")

        response = self._client._make_request(
            method="GET",
            endpoint=f"/datasets/{dataset_id}/tables/{table_name}.{format}",
            allow_redirects=False,
        )

        if response.status_code == 302:
            location = response.headers.get("Location")
            if location:
                return location
            raise Exception("302 response missing Location header")
        elif response.status_code == 404:
            self._raise_not_found(dataset_id, table_name)
        raise Exception(
            f"Failed to get download URL: {response.status_code} - {response.text}"
        )

    def _raise_not_found(self, dataset_id: str, table_name: str) -> NoReturn:
        """Turn a bare table 404 into the specific cause behind it.

        Order matters: the DATASET is checked first. A dataset that has been
        deleted in the web UI 404s every table, and answering that with a list
        of table names is what sends the caller guessing names against a dead
        id.
        """
        try:
            info = self.list_table_names(dataset_id, verify=False)
        except DatasetNotFoundError:
            raise
        except Exception:
            # Catalogue lookup itself failed (network/5xx): we cannot say which
            # cause it is, so say that rather than assert something false.
            raise TableNotFoundError(
                f"Table '{table_name}' not found in dataset '{dataset_id}'. "
                f"Call {DISCOVERY_TOOL}(dataset_id) to discover valid table "
                "names; names are case-sensitive."
            )

        if not info.get("catalogued"):
            raise TableNotFoundError(
                uncatalogued_table_message(dataset_id, table_name, info["type"])
            )

        rejection = classify_table_name(info, table_name)
        if rejection is None:
            # A catalogued, entity-correct name that still 404s: the table was
            # not produced by this run. Not a naming problem — say so.
            raise TableNotFoundError(
                f"Table '{table_name}' is a valid name for dataset "
                f"'{dataset_id}' (type {info['type']}) but the API has no such "
                "table for this dataset — the run did not produce it. Call "
                f"{DISCOVERY_TOOL}(dataset_id, verify=True) to see which of "
                "its tables actually resolve; do not guess other names."
            )
        raise TableNotFoundError(rejection["error"], rejection["reason"])

    def list_table_names(
        self,
        dataset_id: str,
        verify: bool = True,
        upload_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Discover — and optionally VERIFY — the table names of a dataset.

        Exposed over MCP as the ``list_dataset_tables`` tool.

        Three steps, in order:

        1. The DATASET is resolved first. A dataset id can be dead (deleted in
           the web UI; the MCP is never told) — that raises
           DatasetNotFoundError and nothing is said about table names.
        2. Candidates are narrowed with NO network call. data-set-service has
           no list-tables endpoint, so names come from the dataset's ``type``
           (KNOWN_TABLES_BY_DATASET_TYPE). For INTENSITY the tables present
           depend on the omics modality, so the entity is resolved from
           ``job_run_params['entity_type']``, else from the parent upload's
           ``source`` when it maps 1:1 (md_format_gene / md_format_metabolite).
           If neither resolves, the full per-entity union is returned and
           ``entity_resolved_from`` is null — those are CANDIDATES, not tables
           that exist.
        3. ``verify=True`` (default) probes each candidate with
           ``download_table_url`` — a presigned-URL resolve, no file transfer —
           and splits confirmed-present from absent. Cost: one cheap request
           per candidate (2 once the entity is narrowed, at most 12 for an
           unnarrowed INTENSITY dataset, 2-4 elsewhere). Pass ``verify=False``
           to skip every probe and get the candidate list immediately.

        Args:
            dataset_id: dataset UUID.
            verify: probe each candidate for actual existence.
            upload_id: parent upload, used only as the fallback entity signal
                when the dataset carries no ``entity_type``.

        Returns (catalogued type, verify=True):
            {
              "dataset_id": "...", "type": "INTENSITY",
              "catalogued": true, "verified": true,
              "entity": "metabolite" | null,
              "entity_resolved_from": "job_run_params"|"upload_source"|null,
              "candidates": [...],      # probed names
              "tables": [...],          # CONFIRMED PRESENT — downloadable now
              "unavailable": [...],     # probed, absent (clean 404)
              "indeterminate": [{"table": "...", "error": "..."}],  # if any
              "note": "..."
            }
        With ``verify=False`` there is NO ``tables`` key — only ``candidates``,
        which are unconfirmed. Uncatalogued types (ENRICHMENT / ANOVA) have no
        candidate names, so verification is impossible: ``catalogued`` is false,
        ``tables`` is [] and the note says the names cannot be enumerated and
        must not be guessed.

        Raises DatasetNotFoundError if the dataset does not resolve.
        """
        dataset = self.get_by_id(dataset_id)
        if dataset is None:
            raise DatasetNotFoundError(dataset_not_found_message(dataset_id))

        dataset_type = dataset.type or ""
        catalogued = dataset_type in KNOWN_TABLES_BY_DATASET_TYPE

        result: Dict[str, Any] = {
            "dataset_id": dataset_id,
            "type": dataset_type,
            "catalogued": catalogued,
            "verified": False,
        }
        if not catalogued:
            result["tables"] = []
            result.update(self._uncatalogued_fields(dataset_type))
            return result

        result.update(self._candidate_fields(dataset, dataset_type, upload_id))
        if verify:
            result.update(self._verified_fields(dataset_id, result["candidates"]))
        return result

    def _candidate_fields(
        self, dataset: Dataset, dataset_type: str, upload_id: Optional[str]
    ) -> Dict[str, Any]:
        """Candidate table names for a catalogued dataset, narrowed by entity."""
        if dataset_type != "INTENSITY":
            return {
                "entity": None,
                "entity_resolved_from": None,
                "candidates": list(KNOWN_TABLES_BY_DATASET_TYPE[dataset_type]),
                "note": (
                    "Table names are derived from data-set-service flow source "
                    "(no public list-tables endpoint exists). These are the "
                    "only valid names for this dataset type. Names are "
                    "case-sensitive."
                ),
            }

        entity, resolved_from = self._resolve_entity(dataset, upload_id)
        if entity is None:
            return {
                "entity": None,
                "entity_resolved_from": None,
                "candidates": list(KNOWN_TABLES_BY_DATASET_TYPE["INTENSITY"]),
                "tables_by_entity": INTENSITY_TABLES_BY_ENTITY,
                "note": (
                    "The omics modality of this dataset could NOT be resolved "
                    "(it carries no job_run_params.entity_type and no "
                    "unambiguous upload source), so these are the candidates "
                    "for EVERY entity — most of them do not exist for this "
                    "dataset. Verify them (verify=True) or pass upload_id "
                    "before downloading. Names are case-sensitive."
                ),
            }
        return {
            "entity": entity,
            "entity_resolved_from": resolved_from,
            "candidates": list(INTENSITY_TABLES_BY_ENTITY[entity]),
            "note": (
                f"This is a {entity} dataset (resolved from {resolved_from}), "
                f"so only the {entity} tables can exist — the other entities' "
                "tables (Protein_/Peptide_/Gene_/Metabolite_/PTM_) are NOT in "
                "this dataset. Names are case-sensitive."
            ),
        }

    def _resolve_entity(
        self, dataset: Dataset, upload_id: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """The dataset's omics entity and where it came from, or (None, None).

        Strongest signal first: ``job_run_params['entity_type']`` (persisted by
        every pipeline builder), then the parent upload's ``source`` — but only
        for the 1:1 sources. The proteomics sources are protein-and-maybe-
        peptide, so they resolve nothing; guessing "protein" there would
        re-introduce the bug this exists to fix.
        """
        entity = entity_from_job_run_params(dataset.job_run_params)
        if entity:
            return entity, ENTITY_FROM_JOB_RUN_PARAMS

        params = (
            dataset.job_run_params if isinstance(dataset.job_run_params, dict) else {}
        )
        candidate_upload = upload_id or params.get("upload_id")
        if not isinstance(candidate_upload, str) or not candidate_upload:
            return None, None

        entity = entity_from_upload_source(self._upload_source(candidate_upload))
        if entity:
            return entity, ENTITY_FROM_UPLOAD_SOURCE
        return None, None

    def _upload_source(self, upload_id: str) -> Optional[str]:
        """The parent upload's ``source``, or None when it cannot be fetched.

        Entity narrowing is a convenience — a failed lookup degrades to the
        unnarrowed candidate list, never to an exception.
        """
        uploads = getattr(self._client, "uploads", None)
        if uploads is None:
            return None
        try:
            upload = uploads.get_by_id(upload_id)
        except Exception:
            return None
        source = getattr(upload, "source", None)
        return source if isinstance(source, str) and source else None

    def _verified_fields(
        self, dataset_id: str, candidates: Sequence[str]
    ) -> Dict[str, Any]:
        """Probe each candidate and split present / absent / indeterminate.

        A probe that fails for any reason OTHER than a clean 404 (network,
        5xx, auth) is NOT evidence of absence — reporting it as absent would be
        a lie — so it lands in ``indeterminate``. A DatasetNotFoundError
        propagates: the dataset died under us mid-probe.
        """
        present: List[str] = []
        unavailable: List[str] = []
        indeterminate: List[Dict[str, str]] = []

        for name in candidates:
            try:
                self.download_table_url(dataset_id, name)
            except DatasetNotFoundError:
                raise
            except TableNotFoundError:
                unavailable.append(name)
            except Exception as exc:
                indeterminate.append({"table": name, "error": str(exc)})
            else:
                present.append(name)

        fields: Dict[str, Any] = {
            "verified": True,
            "tables": present,
            "unavailable": unavailable,
            "verification_note": (
                "'tables' are CONFIRMED to exist (each resolved to a presigned "
                "URL just now); 'unavailable' were probed and are absent. "
                "Download only from 'tables'."
            ),
        }
        if indeterminate:
            fields["indeterminate"] = indeterminate
            fields["indeterminate_note"] = (
                "These probes failed for a reason OTHER than a 404 (network, "
                "5xx, auth), so their existence is UNKNOWN — they are neither "
                f"confirmed present nor confirmed absent (reason: "
                f"{REASON_INDETERMINATE}). Retry rather than conclude the table "
                "is missing."
            )
        return fields

    def _uncatalogued_fields(self, dataset_type: str) -> Dict[str, Any]:
        """Fields for a dataset type whose tables cannot be enumerated.

        The note is deliberately blunt: telemetry shows a model that received
        an empty table list for an ENRICHMENT dataset then fired twelve
        consecutive 404 guesses before giving up on the objective.
        """
        confirmed = CONFIRMED_TABLES_BY_UNCATALOGUED_TYPE.get(dataset_type, [])
        note = (
            f"The table names for dataset type '{dataset_type}' CANNOT be "
            "enumerated: there is no list-tables endpoint and no verified "
            "catalogue for this type. DO NOT brute-force guess table names — "
            "every wrong guess is a 404, and guessing has been observed to "
            "burn a dozen calls and still fail. Tell the user this dataset "
            "type's tables are not enumerable and ask them for the exact "
            "table name, or view the results through the dataset's "
            "visualisation module."
        )
        fields: Dict[str, Any] = {
            "note": note,
            "tables_note": (
                "'tables' is empty because this type's tables CANNOT be "
                "enumerated — NOT because the dataset has no tables. There are "
                "no candidate names to verify, so verify=True cannot help here."
            ),
        }
        if confirmed:
            fields["confirmed_tables"] = list(confirmed)
            fields["confirmed_tables_note"] = (
                "Observed to download successfully for this dataset type. "
                "NON-EXHAUSTIVE, and not necessarily the results table — the "
                "absence of a name here does not mean it exists under some "
                "other spelling you can guess."
            )
        return fields

    def query(
        self,
        upload_id: Optional[str] = None,
        state: Optional[List[str]] = None,
        type: Optional[List[str]] = None,
        search: Optional[str] = None,
        page: int = 1,
    ) -> Dict[str, Any]:
        """Query datasets with filters"""
        payload: Dict[str, Any] = {"page": page}

        if upload_id is not None:
            payload["upload_id"] = upload_id
        if state is not None:
            payload["state"] = state
        if type is not None:
            payload["type"] = type
        if search is not None:
            payload["search"] = search

        response = self._client._make_request(
            method="POST",
            endpoint="/datasets/query",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            result: Dict[str, Any] = response.json()
            return result
        else:
            raise Exception(
                f"Failed to query datasets: {response.status_code} - {response.text}"
            )

    def delete(self, dataset_id: str) -> bool:
        """Delete a dataset by ID"""
        response = self._client._make_request(
            method="DELETE",
            endpoint=f"/datasets/{dataset_id}",
        )

        if response.status_code == 204:
            return True
        else:
            raise Exception(
                f"Failed to delete dataset: {response.status_code} - {response.text}"
            )

    def retry(self, dataset_id: str) -> bool:
        """Retry a failed dataset"""
        response = self._client._make_request(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/retry",
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to retry dataset: {response.status_code} - {response.text}"
            )

    def cancel(self, dataset_id: str) -> bool:
        """Cancel a processing dataset"""
        response = self._client._make_request(
            method="POST",
            endpoint=f"/datasets/{dataset_id}/cancel",
        )

        if response.status_code == 200:
            return True
        else:
            raise Exception(
                f"Failed to cancel dataset: {response.status_code} - {response.text}"
            )

    def wait_until_complete(
        self,
        upload_id: str,
        dataset_id: str,
        poll_s: int = 5,
        timeout_s: int = 1800,
    ) -> Dataset:
        """Poll the dataset until it reaches a terminal state.

        upload_id is retained for backwards compatibility but is no longer
        used — lookup now goes via get_by_id(dataset_id) directly so the
        caller does not need to know which upload owns the dataset and the
        poll is not capped by the first page of list_by_upload.
        """
        del upload_id  # unused; see docstring
        end = time.monotonic() + timeout_s
        last: Optional[str] = None
        while time.monotonic() < end:
            ds = self.get_by_id(dataset_id)
            if ds:
                state = ds.state
                if state != last:
                    print(f"state={state}")
                    last = state

                if state in {"COMPLETED"}:
                    return ds
                elif state in {"FAILED", "ERROR", "CANCELLED"}:
                    raise Exception(f"Dataset {dataset_id} failed: {state}")
            else:
                if last is None:
                    print("waiting for dataset to appear...")
            time.sleep(poll_s)

        raise TimeoutError(
            f"Dataset {dataset_id} not terminal within {timeout_s}s (last state={last})"
        )

    def find_initial_dataset(self, upload_id: str) -> Optional[Dataset]:
        """Return the upload-created INTENSITY dataset.

        Once a normalisation/imputation/filtration job runs, an upload has
        multiple INTENSITY datasets (the converter registers the NI flow with
        ``run_type=DatasetType.INTENSITY``). The upload-created one is the
        unique INTENSITY dataset with no upstream inputs
        (``input_dataset_ids == []``); NI-produced INTENSITY datasets always
        carry a non-empty ``input_dataset_ids`` pointing back to the original.
        """
        datasets = self.list_by_upload(upload_id=upload_id)

        if not datasets:
            raise ValueError(f"No datasets found for upload {upload_id}")

        intensity = [d for d in datasets if getattr(d, "type", None) == "INTENSITY"]
        if not intensity:
            raise ValueError(f"No intensity dataset found for upload {upload_id}")

        if len(intensity) == 1:
            return intensity[0]

        originals = [d for d in intensity if not getattr(d, "input_dataset_ids", None)]
        if len(originals) == 1:
            return originals[0]
        if len(originals) > 1:
            raise ValueError(
                f"Multiple upload-created intensity datasets found for upload "
                f"{upload_id} (ids: {[str(d.id) for d in originals]}). "
                "Pick one explicitly via list_by_upload / query_datasets."
            )
        raise ValueError(
            f"Multiple intensity datasets found for upload {upload_id} and "
            "none of them is the upload-created one (every INTENSITY dataset "
            "has upstream inputs). Pick one explicitly via list_by_upload / "
            "query_datasets."
        )
