"""
CLI-facing API client — thin facade over the md-python SDK.

The CLI calls flat method names (e.g. ``client.list_experiments()``) while the
SDK uses nested resources (``client.uploads.query()``).  This module bridges
the two so that ``main.py`` never touches SDK internals directly.

Resolution order for credentials: see ``config.py``.
"""

from __future__ import annotations

import os
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

from .config import get_config

# ---------------------------------------------------------------------------
# Lazy SDK import — the SDK is an optional install for CLI-only users, but
# we need it at runtime.  Import it here so errors surface early.
# ---------------------------------------------------------------------------
from md_python import (
    MDClientV2,
    MDClientV1,
    Dataset,
    SampleMetadata,
    PairwiseComparisonDataset,
    DoseResponseDataset,
)

# Try importing Entities resource (present after Aaron's PR #3 merge)
try:
    from md_python.resources.v2.entities import Entities as _EntitiesResource
except ImportError:
    _EntitiesResource = None  # type: ignore[misc, assignment]


class MDClient:
    """Unified client that exposes every operation the CLI needs.

    Internally uses MDClientV2 as the primary SDK client, with MDClientV1
    available for endpoints that only exist on V1 (e.g. workspace/viz calls
    that hit the Rails app rather than the V2 API).

    Not every downstream endpoint is available on every deployment.  Methods
    degrade gracefully where possible — see docstrings.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        cfg = get_config()
        self._token = token or cfg["token"]
        self._base_url = base_url or cfg["base_url"]

        if not self._token:
            raise ValueError(
                "No API token.  Run `md auth login --token <TOKEN>` "
                "or set the MD_API_TOKEN environment variable."
            )

        # Primary V2 client (uploads, datasets, entities, jobs)
        self._v2 = MDClientV2(api_token=self._token, base_url=self._base_url)

        # V1 client for legacy endpoints (experiments, workspace/viz calls)
        self._v1 = MDClientV1(api_token=self._token, base_url=self._base_url)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _headers(self, accept: str = "application/vnd.md-v2+json") -> dict:
        return {
            "accept": accept,
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _get(self, endpoint: str, **kwargs: Any) -> requests.Response:
        return requests.get(
            f"{self._base_url}{endpoint}",
            headers=self._headers(),
            **kwargs,
        )

    def _post(self, endpoint: str, json: Any = None, **kwargs: Any) -> requests.Response:
        return requests.post(
            f"{self._base_url}{endpoint}",
            headers=self._headers(),
            json=json,
            **kwargs,
        )

    def _put(self, endpoint: str, json: Any = None, **kwargs: Any) -> requests.Response:
        return requests.put(
            f"{self._base_url}{endpoint}",
            headers=self._headers(),
            json=json,
            **kwargs,
        )

    def _delete(self, endpoint: str, **kwargs: Any) -> requests.Response:
        return requests.delete(
            f"{self._base_url}{endpoint}",
            headers=self._headers(),
            **kwargs,
        )

    @staticmethod
    def _raise_for(resp: requests.Response, label: str = "Request") -> None:
        if resp.status_code >= 400:
            raise Exception(f"{label} failed: {resp.status_code} — {resp.text}")

    @staticmethod
    def _to_dict(obj: Any) -> Any:
        """Convert an SDK model object to a plain dict for JSON output."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list):
            return [MDClient._to_dict(item) for item in obj]
        if hasattr(obj, "__dict__"):
            d = {}
            for k, v in obj.__dict__.items():
                if k.startswith("_"):
                    continue
                if isinstance(v, UUID):
                    d[k] = str(v)
                elif isinstance(v, list):
                    d[k] = [MDClient._to_dict(i) for i in v]
                elif hasattr(v, "__dict__") and not callable(v):
                    d[k] = MDClient._to_dict(v)
                else:
                    d[k] = v
            return d
        return obj

    # ==================================================================
    # HEALTH / AUTH
    # ==================================================================

    def health(self) -> dict:
        """Check API health."""
        try:
            resp = self._get("/health")
            return resp.json()
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def auth_status(self) -> dict:
        """Verify token validity by hitting a lightweight endpoint."""
        try:
            resp = self._get("/health")
            if resp.status_code == 200:
                return {"authenticated": True, "base_url": self._base_url}
            return {"authenticated": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"authenticated": False, "error": str(e)}

    # ==================================================================
    # EXPERIMENTS / UPLOADS  (V2: "uploads" = V1 "experiments")
    # ==================================================================

    def list_experiments(self, scope: str = "mine") -> list:
        """List experiments (V1 endpoint — requires session auth on some deployments)."""
        resp = self._v1._make_request("GET", f"/experiments?scope={scope}")
        self._raise_for(resp, "List experiments")
        data = resp.json()
        return data if isinstance(data, list) else data.get("experiments", [data])

    def get_experiment(self, experiment_id: str) -> dict:
        """Get experiment/upload by UUID.  Tries V2 first, falls back to V1."""
        try:
            upload = self._v2.uploads.get_by_id(experiment_id)
            return self._to_dict(upload)
        except Exception:
            try:
                exp = self._v1.experiments.get_by_id(experiment_id)
                return self._to_dict(exp)
            except Exception as e:
                raise Exception(f"Could not fetch experiment {experiment_id}: {e}")

    def get_experiment_by_name(self, name: str) -> dict:
        """Search for experiment/upload by name.  Tries V2 query, falls back to V1."""
        try:
            result = self._v2.uploads.query(search=name)
            items = result.get("data", [])
            if items:
                return self._to_dict(items[0])
            raise Exception(f"No upload found with name matching '{name}'")
        except Exception:
            try:
                exp = self._v1.experiments.get_by_name(name)
                return self._to_dict(exp)
            except Exception as e:
                raise Exception(f"Could not find experiment '{name}': {e}")

    def create_experiment(
        self,
        name: str,
        source: str,
        filenames: List[str],
        experiment_design: list,
        sample_metadata: list,
        labelling_method: str = "lfq",
        species: Optional[str] = None,
        description: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
    ) -> dict:
        """Create an experiment/upload.

        Uses V1 endpoint for now because the CLI's payload structure
        matches the V1 experiment wrapper format.
        """
        payload: Dict[str, Any] = {
            "experiment": {
                "name": name,
                "source": source,
                "filenames": filenames,
                "labelling_method": labelling_method,
                "experiment_design": experiment_design,
                "sample_metadata": sample_metadata,
            }
        }
        if species:
            payload["experiment"]["species"] = species
        if description:
            payload["experiment"]["description"] = description

        if s3_bucket:
            payload["experiment"]["s3_bucket"] = s3_bucket
            payload["experiment"]["s3_prefix"] = s3_prefix or ""
        else:
            # Compute file sizes for local upload
            payload["experiment"]["file_location"] = "local"

        resp = self._v1._make_request(
            "POST", "/experiments", json=payload,
            headers={"Content-Type": "application/json"},
        )
        self._raise_for(resp, "Create experiment")
        return resp.json()

    def upload_file(self, presigned_url: str, file_path: str | Path) -> None:
        """Upload a single file to a presigned S3 URL."""
        file_path = Path(file_path)
        with open(file_path, "rb") as f:
            resp = requests.put(presigned_url, data=f)
        if resp.status_code not in (200, 204):
            raise Exception(f"Upload failed: {resp.status_code} — {resp.text}")

    def start_workflow(self, experiment_id: str) -> dict:
        """Start processing workflow for an experiment."""
        resp = self._v2._make_request(
            "POST", f"/uploads/{experiment_id}/start_workflow",
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code >= 400:
            # Fallback to V1
            resp = self._v1._make_request(
                "POST", f"/experiments/{experiment_id}/start_workflow",
                headers={"Content-Type": "application/json"},
            )
        return resp.json() if resp.content else {"status": "ok"}

    def cancel_experiment(self, experiment_id: str) -> dict:
        """Cancel a processing experiment."""
        resp = self._post(f"/uploads/{experiment_id}/cancel")
        if resp.status_code >= 400:
            resp = self._post(f"/experiments/{experiment_id}/cancel")
        self._raise_for(resp, "Cancel experiment")
        return resp.json() if resp.content else {"status": "cancelled"}

    # ==================================================================
    # UPLOADS — V2 query/delete/metadata (from Aaron's PR #3)
    # ==================================================================

    def query_uploads(
        self,
        search: Optional[str] = None,
        status: Optional[List[str]] = None,
        source: Optional[List[str]] = None,
        page: int = 1,
    ) -> dict:
        """Query uploads with filters (V2)."""
        return self._v2.uploads.query(
            search=search, status=status, source=source, page=page,
        )

    def delete_upload(self, upload_id: str) -> bool:
        """Delete an upload (V2)."""
        return self._v2.uploads.delete(upload_id)

    def get_upload_sample_metadata(self, upload_id: str) -> dict:
        """Get sample metadata for an upload (V2)."""
        meta = self._v2.uploads.get_sample_metadata(upload_id)
        return self._to_dict(meta)

    def update_upload_sample_metadata(
        self, upload_id: str, metadata: list
    ) -> bool:
        """Update sample metadata for an upload (V2).

        Args:
            upload_id: Upload UUID
            metadata: Array-of-arrays format, e.g.
                [["sample_name","condition"], ["S1","Control"], ["S2","Treatment"]]
        """
        sm = SampleMetadata(data=metadata)
        return self._v2.uploads.update_sample_metadata(upload_id, sm)

    # ==================================================================
    # DATASETS
    # ==================================================================

    def list_datasets(self, experiment_id: str) -> list:
        """List datasets for an experiment/upload.  Tries V2 query, falls back to V1."""
        try:
            datasets = self._v2.datasets.list_by_upload(experiment_id)
            return self._to_dict(datasets)
        except Exception:
            try:
                datasets = self._v1.datasets.list_by_experiment(experiment_id)
                return self._to_dict(datasets)
            except Exception as e:
                raise Exception(f"Failed to list datasets: {e}")

    def get_dataset(self, dataset_id: str, experiment_id: Optional[str] = None) -> dict:
        """Get a single dataset by ID."""
        try:
            ds = self._v2.datasets.get_by_id(dataset_id)
            if ds is not None:
                return self._to_dict(ds)
        except Exception:
            pass
        # Fallback: if we have the experiment ID, search in the list
        if experiment_id:
            datasets = self.list_datasets(experiment_id)
            for ds in datasets:
                if isinstance(ds, dict) and str(ds.get("id")) == dataset_id:
                    return ds
        raise Exception(f"Dataset {dataset_id} not found")

    def create_dataset(
        self,
        input_dataset_ids: List[str],
        name: str,
        job_slug: str,
        job_run_params: Optional[dict] = None,
        sample_names: Optional[List[str]] = None,
    ) -> dict:
        """Create a dataset (run an analysis job)."""
        ds = Dataset(
            input_dataset_ids=[UUID(x) for x in input_dataset_ids],
            name=name,
            job_slug=job_slug,
            job_run_params=job_run_params or {},
            sample_names=sample_names,
        )
        dataset_id = self._v2.datasets.create(ds)
        return {"dataset_id": dataset_id, "name": name, "job_slug": job_slug}

    def retry_dataset(self, dataset_id: str) -> dict:
        self._v2.datasets.retry(dataset_id)
        return {"dataset_id": dataset_id, "status": "retry_initiated"}

    def delete_dataset(self, dataset_id: str) -> None:
        self._v2.datasets.delete(dataset_id)

    # ==================================================================
    # DATASETS — V2 query/download (from Aaron's PR #3)
    # ==================================================================

    def query_datasets(
        self,
        upload_id: Optional[str] = None,
        state: Optional[List[str]] = None,
        type: Optional[List[str]] = None,
        search: Optional[str] = None,
        page: int = 1,
    ) -> dict:
        """Query datasets with filters (V2)."""
        return self._v2.datasets.query(
            upload_id=upload_id, state=state, type=type,
            search=search, page=page,
        )

    def download_table_url(
        self, dataset_id: str, table_name: str, format: str = "csv"
    ) -> str:
        """Get a presigned download URL for a dataset table (V2)."""
        return self._v2.datasets.download_table_url(dataset_id, table_name, format)

    # ==================================================================
    # ENTITIES (from Aaron's PR #3)
    # ==================================================================

    def query_entities(self, keyword: str, dataset_ids: List[str]) -> dict:
        """Query entity metadata across datasets (V2)."""
        return self._v2.entities.query(keyword=keyword, dataset_ids=dataset_ids)

    # ==================================================================
    # ANALYSIS SHORTCUTS (Pairwise, Dose-Response, ANOVA)
    # ==================================================================

    def create_pairwise_comparison(
        self,
        input_dataset_ids: List[str],
        name: str,
        experiment_design: dict,
        condition_column: str,
        condition_comparisons: dict,
        log_intensities: bool = True,
        use_imputed_intensities: bool = True,
        normalise: str = "median",
        filter_method: str = "percentage",
        filter_threshold: float = 0.66,
        filter_logic: str = "at least one condition",
        sample_names: Optional[List[str]] = None,
    ) -> dict:
        """Create a pairwise comparison dataset using the builder."""
        # Build SampleMetadata from the experiment_design dict
        sm = SampleMetadata(data=experiment_design)

        # Build filter criteria
        if filter_method == "percentage":
            filter_criteria = {
                "method": "percentage",
                "filter_threshold_percentage": filter_threshold,
            }
        else:
            filter_criteria = {
                "method": "count",
                "filter_threshold_count": int(filter_threshold),
            }

        # Extract comparison pairs from the condition_comparisons dict
        pairs = condition_comparisons.get("condition_comparison_pairs", [])

        builder = PairwiseComparisonDataset(
            input_dataset_ids=input_dataset_ids,
            dataset_name=name,
            sample_metadata=sm,
            condition_column=condition_column,
            condition_comparisons=pairs,
            filter_values_criteria=filter_criteria,
            filter_valid_values_logic=filter_logic,
        )
        dataset_id = builder.run(self._v2)
        return {"dataset_id": dataset_id, "name": name, "job_slug": "pairwise_comparison"}

    def create_dose_response(
        self,
        input_dataset_ids: List[str],
        name: str,
        experiment_design: dict,
        control_samples: List[str],
        log_intensities: bool = True,
        use_imputed_intensities: bool = True,
        normalise: str = "sum",
        span_rollmean_k: float = 1,
        prop_required_in_protein: float = 0.5,
        sample_names: Optional[List[str]] = None,
    ) -> dict:
        """Create a dose-response dataset using the builder."""
        sm = SampleMetadata(data=experiment_design)
        # Infer sample_names from the design if not provided
        if not sample_names:
            cols = sm.to_columns()
            sample_names = cols.get("sample_name", [])

        builder = DoseResponseDataset(
            input_dataset_ids=input_dataset_ids,
            dataset_name=name,
            sample_names=sample_names,
            control_samples=control_samples,
            sample_metadata=sm,
            log_intensities=log_intensities,
            use_imputed_intensities=use_imputed_intensities,
            normalise=normalise,
            span_rollmean_k=int(span_rollmean_k),
            prop_required_in_protein=prop_required_in_protein,
        )
        dataset_id = builder.run(self._v2)
        return {"dataset_id": dataset_id, "name": name, "job_slug": "dose_response"}

    def create_anova(
        self,
        input_dataset_ids: List[str],
        name: str,
        experiment_design: dict,
        condition_column: str,
        log_intensities: bool = True,
        use_imputed_intensities: bool = True,
        normalise: str = "median",
        filter_method: str = "percentage",
        filter_threshold: float = 0.66,
        filter_logic: str = "at least one condition",
        sample_names: Optional[List[str]] = None,
    ) -> dict:
        """Create an ANOVA dataset."""
        if filter_method == "percentage":
            filter_criteria = {
                "method": "percentage",
                "filter_threshold_percentage": filter_threshold,
            }
        else:
            filter_criteria = {
                "method": "count",
                "filter_threshold_count": int(filter_threshold),
            }

        params = {
            "condition_column": condition_column,
            "experiment_design": experiment_design,
            "filter_valid_values_logic": filter_logic,
            "filter_values_criteria": filter_criteria,
            "log_intensities": log_intensities,
            "use_imputed_intensities": use_imputed_intensities,
            "normalise": normalise,
        }

        ds = Dataset(
            input_dataset_ids=[UUID(x) for x in input_dataset_ids],
            name=name,
            job_slug="anova",
            job_run_params=params,
            sample_names=sample_names,
        )
        dataset_id = self._v2.datasets.create(ds)
        return {"dataset_id": dataset_id, "name": name, "job_slug": "anova"}

    # ==================================================================
    # JOBS
    # ==================================================================

    def list_dataset_jobs(self) -> list:
        """List available analysis job types."""
        return self._v2.jobs.list()

    # ==================================================================
    # TABLES (data access)
    # ==================================================================

    def get_table_headers(
        self, experiment_id: str, dataset_id: str, table_name: str
    ) -> list:
        """Get column headers for a dataset table."""
        resp = self._get(
            f"/experiments/{experiment_id}/datasets/{dataset_id}/tables/{table_name}/headers"
        )
        if resp.status_code == 404:
            # Try V2 path
            resp = self._get(f"/datasets/{dataset_id}/tables/{table_name}/headers")
        self._raise_for(resp, f"Get table headers for {table_name}")
        data = resp.json()
        return data if isinstance(data, list) else data.get("headers", [])

    def get_table(
        self, experiment_id: str, dataset_id: str, table_name: str, format: str = "csv"
    ) -> requests.Response:
        """Download a dataset table (returns streaming response)."""
        # Try V2 presigned URL first
        try:
            url = self.download_table_url(dataset_id, table_name, format)
            return requests.get(url, stream=True)
        except Exception:
            pass
        # Fallback to V1 direct download
        resp = requests.get(
            f"{self._base_url}/experiments/{experiment_id}/datasets/{dataset_id}/tables/{table_name}.{format}",
            headers=self._headers(),
            stream=True,
        )
        self._raise_for(resp, f"Download table {table_name}")
        return resp

    def query_table(
        self, experiment_id: str, dataset_id: str, table_name: str, sql: str
    ) -> dict:
        """Run a SQL query against a dataset table."""
        resp = self._post(
            f"/experiments/{experiment_id}/datasets/{dataset_id}/tables/{table_name}/query",
            json={"sql": sql},
        )
        if resp.status_code == 404:
            resp = self._post(
                f"/datasets/{dataset_id}/tables/{table_name}/query",
                json={"sql": sql},
            )
        self._raise_for(resp, f"Query table {table_name}")
        return resp.json()

    # ==================================================================
    # WORKSPACES
    # ==================================================================

    def list_workspaces(self) -> list:
        resp = self._get("/workspaces")
        self._raise_for(resp, "List workspaces")
        data = resp.json()
        return data if isinstance(data, list) else data.get("workspaces", [])

    def get_workspace(self, workspace_id: str) -> dict:
        resp = self._get(f"/workspaces/{workspace_id}")
        self._raise_for(resp, "Get workspace")
        return resp.json()

    def create_workspace(self, name: str, description: Optional[str] = None) -> dict:
        payload: Dict[str, Any] = {"workspace": {"name": name}}
        if description:
            payload["workspace"]["description"] = description
        resp = self._post("/workspaces", json=payload)
        self._raise_for(resp, "Create workspace")
        return resp.json()

    def delete_workspace(self, workspace_id: str) -> None:
        resp = self._delete(f"/workspaces/{workspace_id}")
        self._raise_for(resp, "Delete workspace")

    def add_experiment_to_workspace(
        self, workspace_id: str, experiment_id: str
    ) -> dict:
        resp = self._post(
            f"/workspaces/{workspace_id}/experiments",
            json={"experiment_id": experiment_id},
        )
        self._raise_for(resp, "Add experiment to workspace")
        return resp.json()

    def list_workspace_tabs(self, workspace_id: str) -> list:
        resp = self._get(f"/workspaces/{workspace_id}/tabs")
        self._raise_for(resp, "List workspace tabs")
        data = resp.json()
        return data if isinstance(data, list) else data.get("tabs", [])

    def list_workspace_datasets(self, workspace_id: str) -> list:
        resp = self._get(f"/workspaces/{workspace_id}/datasets")
        self._raise_for(resp, "List workspace datasets")
        data = resp.json()
        return data if isinstance(data, list) else data.get("datasets", [])

    # ==================================================================
    # VISUALISATIONS
    # ==================================================================

    def _viz_post(self, endpoint: str, payload: dict) -> dict:
        resp = self._post(endpoint, json=payload)
        self._raise_for(resp, "Visualisation")
        return resp.json()

    def volcano_plot(
        self,
        workspace_id: str,
        dataset_id: str,
        comparison: str,
        fc_threshold: float = 1.0,
        pvalue_threshold: float = 0.05,
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/volcano",
            {
                "dataset_id": dataset_id,
                "comparison": comparison,
                "fc_threshold": fc_threshold,
                "pvalue_threshold": pvalue_threshold,
            },
        )

    def heatmap(
        self,
        workspace_id: str,
        dataset_ids: List[str],
        cluster_dist: float = 0.5,
        z_score: bool = True,
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/heatmap",
            {
                "dataset_ids": dataset_ids,
                "cluster_dist": cluster_dist,
                "z_score": z_score,
            },
        )

    def dimensionality_reduction(
        self,
        workspace_id: str,
        dataset_ids: List[str],
        method: str = "pca",
        colour_by: str = "condition",
        shape_by: Optional[str] = None,
        scaling_method: str = "zscore",
    ) -> dict:
        payload: Dict[str, Any] = {
            "dataset_ids": dataset_ids,
            "method": method,
            "colour_by": colour_by,
            "scaling_method": scaling_method,
        }
        if shape_by:
            payload["shape_by"] = shape_by
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/dimensionality_reduction",
            payload,
        )

    def box_plot(
        self,
        workspace_id: str,
        dataset_ids: List[str],
        proteins: List[str],
        colour_by: str = "condition",
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/box_plot",
            {
                "dataset_ids": dataset_ids,
                "proteins": proteins,
                "colour_by": colour_by,
            },
        )

    def dose_response_plot(
        self,
        experiment_id: str,
        dataset_ids: List[str],
        proteins: Optional[List[str]] = None,
    ) -> dict:
        payload: Dict[str, Any] = {
            "experiment_id": experiment_id,
            "dataset_ids": dataset_ids,
        }
        if proteins:
            payload["proteins"] = proteins
        return self._viz_post(
            f"/experiments/{experiment_id}/visualisations/dose_response",
            payload,
        )

    def anova_volcano_plot(self, workspace_id: str, dataset_id: str) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/anova_volcano",
            {"dataset_id": dataset_id},
        )

    def intensity_distribution(
        self, workspace_id: str, dataset_ids: List[str]
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/intensity_distribution",
            {"dataset_ids": dataset_ids},
        )

    def missing_values_plot(
        self, workspace_id: str, dataset_ids: List[str], by: str = "feature"
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/missing_values",
            {"dataset_ids": dataset_ids, "by": by},
        )

    def cv_distribution(
        self, workspace_id: str, dataset_ids: List[str]
    ) -> dict:
        return self._viz_post(
            f"/workspaces/{workspace_id}/visualisations/cv_distribution",
            {"dataset_ids": dataset_ids},
        )

    # ==================================================================
    # ENRICHMENT
    # ==================================================================

    def reactome_ora(
        self,
        experiment_id: str,
        protein_list_id: Optional[str] = None,
        proteins: Optional[List[str]] = None,
        species: str = "Homo sapiens",
        include_disease: bool = True,
    ) -> dict:
        payload: Dict[str, Any] = {
            "experiment_id": experiment_id,
            "species": species,
            "include_disease": include_disease,
        }
        if protein_list_id:
            payload["protein_list_id"] = protein_list_id
        if proteins:
            payload["proteins"] = proteins
        resp = self._post(
            f"/experiments/{experiment_id}/enrichment/reactome", json=payload
        )
        self._raise_for(resp, "Reactome enrichment")
        return resp.json()

    def string_network(
        self,
        experiment_id: str,
        protein_list_id: Optional[str] = None,
        species: int = 9606,
        network_type: str = "physical",
        required_score: int = 400,
        add_nodes: int = 10,
    ) -> dict:
        payload: Dict[str, Any] = {
            "experiment_id": experiment_id,
            "species": species,
            "network_type": network_type,
            "required_score": required_score,
            "add_nodes": add_nodes,
        }
        if protein_list_id:
            payload["protein_list_id"] = protein_list_id
        resp = self._post(
            f"/experiments/{experiment_id}/enrichment/string", json=payload
        )
        self._raise_for(resp, "STRING network")
        return resp.json()
