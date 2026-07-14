"""Tests for mcp_tools.workspaces.tabs and mcp_tools.workspaces.modules."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

import pytest

from mcp_tools.workspaces.modules import (
    add_module_to_tab,
    add_plotly_json_module,
    add_text_module,
    list_tab_modules,
    remove_module_from_tab,
    render_module_visualisation,
    update_tab_module,
    update_text_module,
)
from mcp_tools.workspaces.tabs import (
    create_tab,
    delete_tab,
    list_tabs,
    update_tab,
)
from md_python.models import Dataset, RegisteredModule, Tab, TabModule

WS_ID = "11111111-1111-1111-1111-111111111111"
TAB_ID = "22222222-2222-2222-2222-222222222222"
MOD_ID = "33333333-3333-3333-3333-333333333333"


def _tab(**overrides):
    base = dict(
        id=UUID(TAB_ID),
        workspace_id=UUID(WS_ID),
        name="Inspection",
        settings={},
        tab_index=1,
        locked=False,
        created_at=datetime(2026, 5, 7, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 7, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return Tab(**base)


def _module(**overrides):
    base = dict(
        id=UUID(MOD_ID),
        item_id="heading",
        x=0,
        y=0,
        width=12,
        height=1,
        settings={"text": "Hello"},
    )
    base.update(overrides)
    return TabModule(**base)


# ── Registered-module fixtures (live-shape) ──
# Heading: no Datasets field — dataset args must NOT be accepted.
HEADING_REG = RegisteredModule(
    id="heading",
    name="Heading",
    group="General",
    icon="md-icon-text",
    input_settings={
        "text": {
            "fieldType": "String",
            "rules": [{"name": "is_required"}],
        },
        "size": {"fieldType": "String", "default": "h1"},
    },
)

# Single-dataset module — multiple=False, dataset_type=INTENSITY.
# Now also has an EntityType field (mirrors the live registry shape).
PCA_REG = RegisteredModule(
    id="dimensionality_reduction_plot",
    name="Dimensionality Reduction Plot",
    group="Experiment",
    icon="x",
    input_settings={
        "datasetsSearch": {
            "fieldType": "Datasets",
            "parameters": {"type": "INTENSITY", "multiple": False},
            "rules": [{"name": "is_required"}],
        },
        "entityType": {
            "fieldType": "EntityType",
            "parameters": {"datasetsSearch": {"ref": "datasetsSearch"}},
            "when": {"property": "datasetsSearch", "not_equals": None},
            "rules": [{"name": "is_required"}],
        },
        "scalingMethod": {"fieldType": "String", "default": "none"},
    },
)

# Renderable module (in the vis-service REGISTRY) whose EntityType field
# enumerates its OWN options — the accepted entity_type set is per-module,
# not a global vocabulary.
PTM_REG = RegisteredModule(
    id="ptm_intensity_scatter",
    name="PTM Intensity Scatter",
    group="Experiment",
    icon="x",
    input_settings={
        "datasetsSearch": {
            "fieldType": "Datasets",
            "parameters": {"type": "INTENSITY", "multiple": False},
            "rules": [{"name": "is_required"}],
        },
        "entityType": {
            "fieldType": "EntityType",
            "parameters": {
                "options": [
                    {"value": "protein", "name": "Protein"},
                    {"value": "peptide", "name": "Peptide"},
                ]
            },
            "rules": [{"name": "is_required"}],
        },
    },
)

# Multi-dataset module — multiple=True, dataset_type=DOSE_RESPONSE.
DR_REG = RegisteredModule(
    id="dose_response_curve_plot",
    name="Dose Response Curve",
    group="Dose-response",
    icon="x",
    input_settings={
        "datasetsSearch": {
            "fieldType": "Datasets",
            "parameters": {"type": "DOSE_RESPONSE", "multiple": True},
            "rules": [{"name": "is_required"}],
        },
    },
)


# Pairwise volcano — single PAIRWISE dataset + EntityType + a
# ConditionComparison field (keyed as in the live registry). The
# ConditionComparison is required-no-default; the tool must resolve it
# from the dataset's job_run_params.
VOLCANO_REG = RegisteredModule(
    id="pairwise_volcano_plot",
    name="Pairwise Volcano Plot",
    group="Pairwise",
    icon="x",
    input_settings={
        "datasetsSearch": {
            "fieldType": "Datasets",
            "parameters": {"type": "PAIRWISE", "multiple": False},
            "rules": [{"name": "is_required"}],
        },
        "experimentAndConditionComparison": {
            "fieldType": "ConditionComparison",
            "default": None,
            "rules": [{"name": "is_required"}],
        },
        "entityType": {
            "fieldType": "EntityType",
            "rules": [{"name": "is_required"}],
        },
    },
)

# job_run_params shape a completed pairwise dataset carries.
_PAIRWISE_JRP = {
    "condition_comparisons": {
        "condition_comparison_pairs": [
            ["Stage 3", "Stage 1"],
            ["Control", "Stage 5"],
        ]
    }
}


def _stub_dataset(
    uid: str,
    name: str = "Demo Dataset",
    type: str = "INTENSITY",
    job_run_params: dict | None = None,
) -> Dataset:
    return Dataset(
        id=UUID(uid),
        input_dataset_ids=[],
        name=name,
        job_slug="demo",
        job_run_params=job_run_params or {},
        type=type,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────────────


class TestCreateTab:
    def test_returns_prose_with_id(self, mock_client):
        mock_client.workspaces.tabs.create.return_value = _tab()
        with patch("mcp_tools.workspaces.tabs.get_client", return_value=mock_client):
            result = create_tab(WS_ID, name="Inspection")
        assert result.startswith("Tab created.")
        assert TAB_ID in result


class TestListTabs:
    def test_paginated_envelope(self, mock_client):
        mock_client.workspaces.tabs.list.return_value = {
            "data": [_tab()],
            "pagination": {"current_page": 1, "total_pages": 1},
        }
        with patch("mcp_tools.workspaces.tabs.get_client", return_value=mock_client):
            data = json.loads(list_tabs(WS_ID))
        assert data["data"][0]["id"] == TAB_ID


class TestUpdateTab:
    def test_partial_update(self, mock_client):
        mock_client.workspaces.tabs.update.return_value = _tab(name="Renamed")
        with patch("mcp_tools.workspaces.tabs.get_client", return_value=mock_client):
            data = json.loads(update_tab(WS_ID, TAB_ID, name="Renamed"))
        assert data["name"] == "Renamed"

    def test_no_args_returns_error(self, mock_client):
        with patch("mcp_tools.workspaces.tabs.get_client", return_value=mock_client):
            data = json.loads(update_tab(WS_ID, TAB_ID))
        assert "error" in data
        mock_client.workspaces.tabs.update.assert_not_called()


class TestDeleteTab:
    def test_destructive_mandate_attached(self):
        wrapped = getattr(delete_tab, "fn", None) or delete_tab
        doc = (delete_tab.__doc__ or "") + (wrapped.__doc__ or "")
        assert "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in doc


class TestReuseFirstMandate:
    """Locks the documentation claim that the LLM must reuse the
    UI-auto-created 'new tab' rather than creating a parallel default
    tab. Without these tests the docstrings can silently rot."""

    def test_create_tab_carries_reuse_first_mandate(self):
        wrapped = getattr(create_tab, "fn", None) or create_tab
        doc = (create_tab.__doc__ or "") + (wrapped.__doc__ or "")
        # The mandate MUST be visible to the LLM.
        assert "REUSE-FIRST MANDATE" in doc
        # And explicitly say to call list_tabs before create_tab.
        assert "list_tabs(workspace_id)" in doc
        # And explain the lazy auto-tab so the LLM understands why.
        assert "WorkspaceTabsRepository" in doc
        assert "new tab" in doc

    def test_create_workspace_carries_auto_tab_note(self):
        # Imported here to avoid a top-level circular dependency in test
        # collection — both tabs and modules tests live in this file.
        from mcp_tools.workspaces.crud import create_workspace

        wrapped = getattr(create_workspace, "fn", None) or create_workspace
        doc = (create_workspace.__doc__ or "") + (wrapped.__doc__ or "")
        assert "AUTO-TAB BEHAVIOUR" in doc
        # The LLM MUST be told that API-created workspaces have ZERO tabs
        # initially — that's the part that distinguishes the API path
        # from the UI path.
        assert "ZERO tabs" in doc
        assert "list_tabs" in doc


# ──────────────────────────────────────────────────────────────────────────────
# Modules
# ──────────────────────────────────────────────────────────────────────────────


class TestAddModuleToTab:
    def test_uses_create_with_defaults_for_dataset_free_module(self, mock_client):
        # Heading has no Datasets field — dataset args are NOT passed.
        mock_client.module_registry.get.return_value = HEADING_REG
        mock_client.workspaces.modules.create_with_defaults.return_value = _module()
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "heading",
                x=0,
                y=0,
                width=12,
                height=1,
                settings={"text": "Hello"},
            )
        assert result.startswith("Module placed.")
        assert MOD_ID in result
        # The settings going through must be the user's settings — no
        # dataset envelope merged in for a heading module.
        call_kwargs = (
            mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        )
        assert call_kwargs["settings"] == {"text": "Hello"}

    def test_value_error_returns_error_prose(self, mock_client):
        mock_client.module_registry.get.return_value = HEADING_REG
        # Missing required-no-default key raises ValueError client-side;
        # tool surfaces it as `Error: ...` prose.
        mock_client.workspaces.modules.create_with_defaults.side_effect = ValueError(
            "Cannot create 'heading': required key(s) not provided: ['text']"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "heading",
                x=0,
                y=0,
                width=12,
                height=1,
            )
        assert result.startswith("Error: ")
        assert "required key" in result

    def test_visualisation_mandate_attached(self):
        wrapped = getattr(add_module_to_tab, "fn", None) or add_module_to_tab
        doc = (add_module_to_tab.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" in doc
        # The mandate must explicitly call out data-dependency disclosure
        # and the never-elide-rows rule.
        assert "DATA-DEPENDENCY DISCLOSURE" in doc
        assert "NO PARAMETER LEFT UNDOCUMENTED" in doc


class TestAddModuleSingleDataset:
    """Single-dataset modules (parameters.multiple=False).

    The tool must accept dataset_id + upload_id, fetch the dataset name
    via module_registry / datasets.get_by_id, and merge the envelope
    under the module's datasetsSearch key BEFORE calling the API.
    """

    DATASET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    UPLOAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    def test_envelope_built_and_merged(self, mock_client):
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, name="Live Dataset", type="INTENSITY"
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dimensionality_reduction_plot"
        )

        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
                settings={"scalingMethod": "zscore"},
            )

        call_kwargs = (
            mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        )
        envelope = call_kwargs["settings"]["datasetsSearch"]
        assert envelope["type"] == "INTENSITY"
        assert envelope["liveUpdate"] is False
        assert envelope["searchResult"] is None
        assert len(envelope["individualResults"]) == 1
        ir = envelope["individualResults"][0]
        assert ir["id"] == self.DATASET_ID
        assert ir["name"] == "Live Dataset"
        assert ir["experimentId"] == self.UPLOAD_ID
        # entity_type lands under the EntityType field's settings_key.
        assert call_kwargs["settings"]["entityType"] == "protein"
        # User's other settings flowed through too.
        assert call_kwargs["settings"]["scalingMethod"] == "zscore"

    def test_dataset_type_mismatch_fails_fast(self, mock_client):
        # PCA wants INTENSITY; we hand it a PAIRWISE dataset by mistake
        # (typical symptom of confusing upload_id with dataset_id).
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, name="Wrong Type", type="PAIRWISE"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
            )
        assert result.startswith("Error:")
        # Error must explicitly tell the LLM what type was found and what's needed.
        assert "PAIRWISE" in result
        assert "INTENSITY" in result
        # And surface the upload-vs-dataset confusion as the primary
        # hypothesis — "the most common cause is the LLM passing an
        # upload_id where a dataset_id is needed".
        assert "UPLOAD" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_missing_entity_type_when_required_fails_fast(self, mock_client):
        # PCA's entityType is required; not passing entity_type must error.
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, type="INTENSITY"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                # entity_type omitted
            )
        assert result.startswith("Error:")
        assert "entity_type" in result
        # Lists the valid values — protein/peptide/gene.
        for v in ("protein", "peptide", "gene"):
            assert v in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_invalid_entity_type_fails_fast(self, mock_client):
        mock_client.module_registry.get.return_value = PCA_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="lipid",  # not a valid value
            )
        assert result.startswith("Error:")
        assert "lipid" in result

    def test_metabolite_entity_type_accepted(self, mock_client):
        # metabolite is a first-class viz entity_type — a metabolite
        # INTENSITY dataset placed on a plot must validate and persist
        # entityType="metabolite" (vis-service is the final arbiter).
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, name="Metabolite Dataset", type="INTENSITY"
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dimensionality_reduction_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="metabolite",
            )
        call_kwargs = (
            mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        )
        assert call_kwargs["settings"]["entityType"] == "metabolite"

    def test_height_defaults_to_16_when_omitted(self, mock_client):
        # height is optional and defaults to 16 — a plot placed without an
        # explicit height must render at full size, not crop.
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, type="INTENSITY"
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dimensionality_reduction_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=6,
                # height omitted -> default 16
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
            )
        call_kwargs = (
            mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        )
        assert call_kwargs["height"] == 16

    def test_entity_type_on_module_without_entity_field_is_dropped(self, mock_client):
        # heading has no EntityType field. There is nothing to set it on,
        # so the correct behaviour is to DROP it and place the module —
        # NOT to fail. The drop must be surfaced as a warning.
        mock_client.module_registry.get.return_value = HEADING_REG
        mock_client.workspaces.modules.create_with_defaults.return_value = _module()
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "heading",
                x=0,
                y=0,
                width=12,
                height=1,
                entity_type="protein",
                settings={"text": "x"},
            )
        assert result.startswith("Module placed.")
        # entity_type never reaches the wire.
        sent = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        assert sent["settings"] == {"text": "x"}
        # …but the LLM is told, loudly, in the success payload.
        body = json.loads(result.split("\n", 1)[1])
        dropped = [w for w in body["warnings"] if "DROPPED" in w]
        assert len(dropped) == 1
        assert "entity_type" in dropped[0]
        assert "entity_type_input" in dropped[0]

    def test_missing_upload_id_fails_fast(self, mock_client):
        mock_client.module_registry.get.return_value = PCA_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_ID,
                entity_type="protein",
            )
        assert result.startswith("Error:")
        assert "upload_id" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_arity_mismatch_dataset_ids_on_single_module(self, mock_client):
        # PCA is multiple=False; passing dataset_ids must fail-fast.
        mock_client.module_registry.get.return_value = PCA_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_ids=[self.DATASET_ID],
                upload_ids=[self.UPLOAD_ID],
                entity_type="protein",
            )
        assert result.startswith("Error:")
        assert "single" in result
        assert "dataset_id" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_missing_dataset_when_required_fails_fast(self, mock_client):
        # PCA's datasetsSearch is required and has no default. Calling
        # without dataset_id MUST fail fast with a tool-level message
        # before hitting create_with_defaults.
        mock_client.module_registry.get.return_value = PCA_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                height=12,
            )
        assert result.startswith("Error:")
        assert "dataset" in result.lower()
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()


class TestEntityTypeInference:
    """entity_type is required by almost every plot module but is NOT
    discoverable from the module id alone. When the LLM omits it, the tool
    infers it from the same signals the web UI uses — and says so — rather
    than failing outright. It only fails when the signals are ambiguous.
    """

    DATASET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    UPLOAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    def _place(self, mock_client, dataset, upload_source=None):
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = dataset
        mock_client.uploads.get_by_id.return_value = (
            SimpleNamespace(source=upload_source) if upload_source else None
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dimensionality_reduction_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            return add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                # entity_type deliberately omitted
            )

    def test_inferred_from_dataset_job_run_params(self, mock_client):
        # Every pipeline-produced dataset persists entity_type in
        # job_run_params — the same key EntityTypeSelectField.vue reads.
        result = self._place(
            mock_client,
            _stub_dataset(self.DATASET_ID, job_run_params={"entity_type": "gene"}),
        )
        assert result.startswith("Module placed.")
        sent = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        assert sent["settings"]["entityType"] == "gene"
        body = json.loads(result.split("\n", 1)[1])
        warning = body["warnings"][0]
        assert "INFERRED" in warning
        assert "gene" in warning
        assert "job_run_params.entity_type" in warning
        # No upload fetch needed — the dataset answered.
        mock_client.uploads.get_by_id.assert_not_called()

    def test_inferred_from_unambiguous_upload_source(self, mock_client):
        # The initial (upload-conversion) dataset carries no entity_type;
        # a md_format_metabolite upload can only be metabolite.
        result = self._place(
            mock_client,
            _stub_dataset(self.DATASET_ID, job_run_params={}),
            upload_source="md_format_metabolite",
        )
        assert result.startswith("Module placed.")
        sent = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        assert sent["settings"]["entityType"] == "metabolite"
        body = json.loads(result.split("\n", 1)[1])
        assert "INFERRED" in body["warnings"][0]
        assert "md_format_metabolite" in body["warnings"][0]

    @pytest.mark.parametrize("source", ["md_format", "diann_tabular", "maxquant"])
    def test_ambiguous_upload_source_still_fails(self, source, mock_client):
        # protein vs peptide cannot be inferred from the source — guessing
        # would render the wrong table. Fail, and name the discovery tools.
        result = self._place(
            mock_client,
            _stub_dataset(self.DATASET_ID, job_run_params={}),
            upload_source=source,
        )
        assert result.startswith("Error:")
        assert "entity_type" in result
        assert "list_module_types" in result
        assert "describe_module_type" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_upload_lookup_failure_degrades_to_error(self, mock_client):
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, job_run_params={}
        )
        mock_client.uploads.get_by_id.side_effect = Exception("403 forbidden")
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
            )
        # A failed inference is never fatal in itself — it degrades to the
        # actionable "pass entity_type" error, not an exception.
        assert result.startswith("Error:")
        assert "requires entity_type" in result

    def test_explicit_entity_type_skips_inference(self, mock_client):
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, job_run_params={"entity_type": "gene"}
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dimensionality_reduction_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
            )
        sent = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        # The caller's value wins over the dataset's — no inference at all.
        assert sent["settings"]["entityType"] == "protein"
        body = json.loads(result.split("\n", 1)[1])
        assert not any("INFERRED" in w for w in body["warnings"])


class TestEntityTypeValidValuesArePerModule:
    """The accepted entity_type set differs between modules. When a
    module's registry spec enumerates its own options, THOSE are the
    values validated and published — never a hard-coded global list."""

    DATASET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    UPLOAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    def test_value_outside_module_options_rejected(self, mock_client):
        mock_client.module_registry.get.return_value = PTM_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "ptm_intensity_scatter",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="gene",  # module only accepts protein/peptide
            )
        assert result.startswith("Error:")
        assert "['protein', 'peptide']" in result
        assert "gene" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_value_inside_module_options_accepted(self, mock_client):
        mock_client.module_registry.get.return_value = PTM_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(self.DATASET_ID)
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="ptm_intensity_scatter"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_module_to_tab(
                WS_ID,
                TAB_ID,
                "ptm_intensity_scatter",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="peptide",
            )
        sent = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        assert sent["settings"]["entityType"] == "peptide"


class TestAddModuleRenderability:
    """A placed module that has no server-side renderer is still a valid
    module — it draws in the web UI. Say so; never block the add."""

    DATASET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    UPLOAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    def _place(self, mock_client, reg, item_id):
        mock_client.module_registry.get.return_value = reg
        mock_client.datasets.get_by_id.return_value = _stub_dataset(self.DATASET_ID)
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id=item_id
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            return add_module_to_tab(
                WS_ID,
                TAB_ID,
                item_id,
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
            )

    def test_non_renderable_module_placed_with_warning(self, mock_client):
        # dimensionality_reduction_plot is NOT in the vis-service REGISTRY.
        result = self._place(mock_client, PCA_REG, "dimensionality_reduction_plot")
        assert result.startswith("Module placed.")  # NOT an error
        body = json.loads(result.split("\n", 1)[1])
        assert body["renderable"] is False
        assert any("render_module_visualisation" in w for w in body["warnings"])
        assert any("browser" in w for w in body["warnings"])

    def test_renderable_module_has_no_warning(self, mock_client):
        result = self._place(mock_client, PTM_REG, "ptm_intensity_scatter")
        body = json.loads(result.split("\n", 1)[1])
        assert body["renderable"] is True
        assert body["warnings"] == []


class TestAddModuleMissingRequiredKeys:
    """Root cause: a required key with no registry default is discovered
    only on failure. The failure message must name the field the LLM can
    read up-front (required_keys_no_default)."""

    def test_missing_required_key_names_the_discovery_field(self, mock_client):
        mock_client.module_registry.get.return_value = HEADING_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID, TAB_ID, "heading", x=0, y=0, width=12, height=1
            )
        assert result.startswith("Error:")
        assert "'text'" in result
        assert "required_keys_no_default" in result
        assert "describe_module_type" in result
        # Fail-fast: nothing left the process.
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()


class TestAddModulePairwiseComparison:
    """Pairwise modules with a ConditionComparison field (volcano).

    The tool must resolve the comparison (conditionPair + left/right
    groups) from the chosen PAIRWISE dataset's job_run_params so the
    rendered volcano carries a real comparison shape — not an empty
    default.
    """

    DATASET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    UPLOAD_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

    def _place(self, mock_client, **kwargs):
        mock_client.module_registry.get.return_value = VOLCANO_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID,
            name="Spike-in pairwise",
            type="PAIRWISE",
            job_run_params=_PAIRWISE_JRP,
        )
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="pairwise_volcano_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            return add_module_to_tab(
                WS_ID,
                TAB_ID,
                "pairwise_volcano_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
                **kwargs,
            )

    def test_comparison_autofilled_from_first_pair(self, mock_client):
        self._place(mock_client)
        settings = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs[
            "settings"
        ]
        cmp = settings["experimentAndConditionComparison"]["comparison"]
        # First pair, oriented case-vs-control.
        assert cmp["conditionPair"] == "Stage 3 - Stage 1"
        assert cmp["left"] == "Stage 3"
        assert cmp["right"] == "Stage 1"

    def test_explicit_comparison_sets_left_right(self, mock_client):
        # Caller flips orientation: left=Stage 1, right=Stage 3.
        self._place(mock_client, comparison=["Stage 1", "Stage 3"])
        cmp = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs[
            "settings"
        ]["experimentAndConditionComparison"]["comparison"]
        # conditionPair stays in stored case-control order; left/right honour
        # the caller's chosen orientation.
        assert cmp["conditionPair"] == "Stage 3 - Stage 1"
        assert cmp["left"] == "Stage 1"
        assert cmp["right"] == "Stage 3"

    def test_explicit_comparison_picks_other_pair(self, mock_client):
        self._place(mock_client, comparison=["Control", "Stage 5"])
        cmp = mock_client.workspaces.modules.create_with_defaults.call_args.kwargs[
            "settings"
        ]["experimentAndConditionComparison"]["comparison"]
        assert cmp["conditionPair"] == "Control - Stage 5"
        assert cmp["left"] == "Control"
        assert cmp["right"] == "Stage 5"

    def test_unknown_comparison_fails_fast(self, mock_client):
        result = self._place(mock_client, comparison=["Stage 1", "Stage 5"])
        assert result.startswith("Error:")
        # Lists the comparisons the dataset actually carries.
        assert "Stage 3 - Stage 1" in result
        assert "Control - Stage 5" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_missing_pairs_in_job_run_params_fails_fast(self, mock_client):
        mock_client.module_registry.get.return_value = VOLCANO_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, type="PAIRWISE", job_run_params={}
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "pairwise_volcano_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
            )
        assert result.startswith("Error:")
        assert "condition_comparison" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()

    def test_comparison_rejected_on_non_pairwise_module(self, mock_client):
        # PCA has no ConditionComparison field — passing comparison errors.
        mock_client.module_registry.get.return_value = PCA_REG
        mock_client.datasets.get_by_id.return_value = _stub_dataset(
            self.DATASET_ID, type="INTENSITY"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dimensionality_reduction_plot",
                x=0,
                y=0,
                width=12,
                dataset_id=self.DATASET_ID,
                upload_id=self.UPLOAD_ID,
                entity_type="protein",
                comparison=["A", "B"],
            )
        assert result.startswith("Error:")
        assert "does not accept a comparison" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()


class TestAddModuleMultipleDatasets:
    """Multi-dataset modules (parameters.multiple=True)."""

    DATASET_IDS = [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    ]
    UPLOAD_IDS = [
        "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa",
        "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb",
    ]

    def test_envelope_built_with_multiple_entries(self, mock_client):
        mock_client.module_registry.get.return_value = DR_REG
        mock_client.datasets.get_by_id.side_effect = [
            _stub_dataset(self.DATASET_IDS[0], name="DR-1", type="DOSE_RESPONSE"),
            _stub_dataset(self.DATASET_IDS[1], name="DR-2", type="DOSE_RESPONSE"),
        ]
        mock_client.workspaces.modules.create_with_defaults.return_value = _module(
            item_id="dose_response_curve_plot"
        )

        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dose_response_curve_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_ids=self.DATASET_IDS,
                upload_ids=self.UPLOAD_IDS,
            )

        call_kwargs = (
            mock_client.workspaces.modules.create_with_defaults.call_args.kwargs
        )
        envelope = call_kwargs["settings"]["datasetsSearch"]
        assert envelope["type"] == "DOSE_RESPONSE"
        assert len(envelope["individualResults"]) == 2
        # Order preserved + experimentId from the paired upload_id
        assert [
            (ir["id"], ir["experimentId"]) for ir in envelope["individualResults"]
        ] == list(zip(self.DATASET_IDS, self.UPLOAD_IDS))
        assert envelope["keywords"] == ["DR-1", "DR-2"]

    def test_arity_mismatch_dataset_id_on_multiple_module(self, mock_client):
        mock_client.module_registry.get.return_value = DR_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dose_response_curve_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_id=self.DATASET_IDS[0],
                upload_id=self.UPLOAD_IDS[0],
            )
        assert result.startswith("Error:")
        assert "multiple" in result
        assert "dataset_ids" in result

    def test_length_mismatch_fails_fast(self, mock_client):
        mock_client.module_registry.get.return_value = DR_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "dose_response_curve_plot",
                x=0,
                y=0,
                width=12,
                height=12,
                dataset_ids=self.DATASET_IDS,
                upload_ids=[self.UPLOAD_IDS[0]],  # only one — mismatched
            )
        assert result.startswith("Error:")
        assert "length" in result or "match length" in result


class TestAddModuleRejectsDatasetForFreeModule:
    """Modules without a Datasets field (heading, page_break, text)
    must REJECT dataset_id / dataset_ids — surfacing this loudly so the
    LLM doesn't silently pollute settings."""

    def test_dataset_id_on_heading_fails_fast(self, mock_client):
        mock_client.module_registry.get.return_value = HEADING_REG
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_module_to_tab(
                WS_ID,
                TAB_ID,
                "heading",
                x=0,
                y=0,
                width=12,
                height=1,
                dataset_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                upload_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                settings={"text": "Hi"},
            )
        assert result.startswith("Error:")
        assert "does not accept a dataset" in result
        mock_client.workspaces.modules.create_with_defaults.assert_not_called()


class TestAddModuleDocstringContract:
    """Locks the docstring claim that height >= 12 is required for
    plot modules — same regression-test pattern as the reuse-first
    mandate test."""

    def test_height_guidance_visible(self):
        wrapped = getattr(add_module_to_tab, "fn", None) or add_module_to_tab
        doc = (add_module_to_tab.__doc__ or "") + (wrapped.__doc__ or "")
        assert "height MUST be at least" in doc
        assert "12" in doc
        # The dataset-binding section is the new contract: must be visible.
        assert "DATASET BINDING" in doc
        assert "dataset_id" in doc and "dataset_ids" in doc
        assert "upload_id" in doc and "upload_ids" in doc


class TestListTabModules:
    def test_returns_data_envelope(self, mock_client):
        mock_client.workspaces.modules.list.return_value = [_module()]
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            data = json.loads(list_tab_modules(WS_ID, TAB_ID))
        assert len(data["data"]) == 1
        assert data["data"][0]["item_id"] == "heading"


class TestUpdateTabModule:
    def test_passes_item_id_through(self, mock_client):
        mock_client.workspaces.modules.update.return_value = _module(x=5, y=2)
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            data = json.loads(
                update_tab_module(WS_ID, TAB_ID, MOD_ID, item_id="heading", x=5, y=2)
            )
        # Tool MUST pass item_id (server-side bug workaround).
        mock_client.workspaces.modules.update.assert_called_once_with(
            WS_ID,
            TAB_ID,
            MOD_ID,
            item_id="heading",
            x=5,
            y=2,
            width=None,
            height=None,
            settings=None,
        )
        assert data["x"] == 5

    def test_visualisation_mandate_attached(self):
        wrapped = getattr(update_tab_module, "fn", None) or update_tab_module
        doc = (update_tab_module.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" in doc


class TestRemoveModuleFromTab:
    def test_destructive_mandate_attached(self):
        wrapped = getattr(remove_module_from_tab, "fn", None) or remove_module_from_tab
        doc = (remove_module_from_tab.__doc__ or "") + (wrapped.__doc__ or "")
        assert "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in doc


# ──────────────────────────────────────────────────────────────────────────────
# Mandate composition — the visualisation mandate must NOT be attached to
# read-only tools like list_tab_modules.
# ──────────────────────────────────────────────────────────────────────────────


class TestMandateScope:
    def test_list_tab_modules_has_no_visualisation_mandate(self):
        wrapped = getattr(list_tab_modules, "fn", None) or list_tab_modules
        doc = (list_tab_modules.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" not in doc

    def test_create_tab_has_no_visualisation_mandate(self):
        # Creating a tab is safe and parameter-light; no mandate needed.
        wrapped = getattr(create_tab, "fn", None) or create_tab
        doc = (create_tab.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" not in doc


class TestAddTextModule:
    def test_calls_create_text_with_defaults(self, mock_client):
        mock_client.workspaces.modules.create_text.return_value = _module(
            item_id="text", height=3, settings={"text": "<p>hello</p>"}
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_text_module(WS_ID, TAB_ID, text="<p>hello</p>")

        assert result.startswith("Text module placed.")
        assert MOD_ID in result
        kwargs = mock_client.workspaces.modules.create_text.call_args.kwargs
        assert kwargs == {
            "workspace_id": WS_ID,
            "tab_id": TAB_ID,
            "text": "<p>hello</p>",
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 3,
        }

    def test_passes_layout_overrides(self, mock_client):
        mock_client.workspaces.modules.create_text.return_value = _module(
            item_id="text"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_text_module(WS_ID, TAB_ID, text="hi", x=2, y=4, width=6, height=2)
        kwargs = mock_client.workspaces.modules.create_text.call_args.kwargs
        assert (kwargs["x"], kwargs["y"], kwargs["width"], kwargs["height"]) == (
            2,
            4,
            6,
            2,
        )

    def test_api_error_returns_error_prose(self, mock_client):
        # Server validation (e.g. text > maxLength) surfaces as the
        # generic Exception path the API client raises.
        mock_client.workspaces.modules.create_text.side_effect = Exception(
            "Failed to create module: 422 - value length violations for text"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_text_module(WS_ID, TAB_ID, text="x" * 10)
        assert result.startswith("Error: ")
        assert "value length violations" in result

    def test_no_visualisation_mandate(self):
        # Text body is user content, not a parameter table — the heavy
        # Q&A mandate would only get in the way.
        wrapped = getattr(add_text_module, "fn", None) or add_text_module
        doc = (add_text_module.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" not in doc


class TestUpdateTextModule:
    def test_calls_update_text(self, mock_client):
        mock_client.workspaces.modules.update_text.return_value = _module(
            item_id="text", settings={"text": "<p>new</p>"}
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = update_text_module(WS_ID, TAB_ID, MOD_ID, text="<p>new</p>")

        kwargs = mock_client.workspaces.modules.update_text.call_args.kwargs
        assert kwargs == {
            "workspace_id": WS_ID,
            "tab_id": TAB_ID,
            "module_id": MOD_ID,
            "text": "<p>new</p>",
        }
        # Result is the JSON-encoded module — no "Text module placed."
        # prefix because this is an update, not a placement.
        body = json.loads(result)
        assert body["item_id"] == "text"
        assert body["settings"]["text"] == "<p>new</p>"

    def test_no_visualisation_mandate(self):
        wrapped = getattr(update_text_module, "fn", None) or update_text_module
        doc = (update_text_module.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LLM BEHAVIOURAL MANDATES — VISUALISATION" not in doc


class TestRenderModuleVisualisation:
    @pytest.fixture(autouse=True)
    def _renderable_module(self, mock_client):
        """Every render test targets a module the vis-service CAN render.

        The tool resolves the module's item_id first (renderable guard),
        so the stub must answer with a real module.
        """
        mock_client.workspaces.modules.get.return_value = _module(item_id="box_plot")

    def test_returns_json_body_on_success(self, mock_client):
        # MCP tool layer always drives polling itself (resource called with
        # poll=False) so the per-call HTTP count stays bounded by the
        # internal cap. First (and only) resource call returns the body.
        mock_client.workspaces.modules.render_visualisation.return_value = {
            "data": [{"type": "scatter"}],
            "layout": {"title": "x"},
        }
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID)
        body = json.loads(result)
        assert body["data"][0]["type"] == "scatter"

        kwargs = mock_client.workspaces.modules.render_visualisation.call_args.kwargs
        assert kwargs == {
            "workspace_id": WS_ID,
            "tab_id": TAB_ID,
            "module_id": MOD_ID,
            "poll": False,
            "timeout_s": 300.0,
        }
        # One MCP call MUST translate to exactly one HTTP request when the
        # server answers 200 on the first try — the MCP layer must not
        # over-poll a ready render.
        assert mock_client.workspaces.modules.render_visualisation.call_count == 1

    def test_timeout_returns_error_prose(self, mock_client):
        mock_client.workspaces.modules.render_visualisation.side_effect = TimeoutError(
            "render_visualisation: still 202 after 60s"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(
                WS_ID, TAB_ID, MOD_ID, poll=True, timeout_s=60
            )
        assert result.startswith("Error:")
        assert "still 202" in result

    def test_returns_envelope_when_poll_false(self, mock_client):
        mock_client.workspaces.modules.render_visualisation.return_value = {
            "status": "rendering",
            "retry_after": 5,
        }
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        body = json.loads(result)
        assert body["status"] == "rendering"
        assert body["retry_after"] == 5

    def test_render_failure_returns_structured_error_envelope(self, mock_client):
        from md_python.resources.v2.workspaces import RenderVisualisationError

        mock_client.workspaces.modules.render_visualisation.side_effect = (
            RenderVisualisationError(
                status_code=400,
                response_text=(
                    '{"error": "Visualisation not supported for module '
                    "type 'missing_values_by_sample_plot'\"}"
                ),
            )
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID)
        body = json.loads(result)
        assert body["status"] == "error"
        assert body["http_status"] == 400
        assert "not supported" in body["error"]
        assert body["module_id"] == MOD_ID
        assert body["workspace_id"] == WS_ID
        assert body["tab_id"] == TAB_ID
        assert body["detail"]["error"].startswith("Visualisation not supported")

    def test_render_failure_envelope_when_poll_false(self, mock_client):
        from md_python.resources.v2.workspaces import RenderVisualisationError

        mock_client.workspaces.modules.render_visualisation.side_effect = (
            RenderVisualisationError(status_code=500, response_text="upstream boom")
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        body = json.loads(result)
        assert body["status"] == "error"
        assert body["http_status"] == 500
        assert body["error"] == "upstream boom"

    def test_internal_poll_cap_returns_rendering_envelope(self, mock_client):
        """When the server keeps answering 202, ONE MCP call makes at most
        _RENDER_MAX_POLLS HTTP requests before surfacing a rendering
        envelope to the LLM — per the polling-discipline mandate.
        """
        from mcp_tools.workspaces.modules import _RENDER_MAX_POLLS

        # Server always says "still rendering" with retry_after=0 so the
        # tool never actually sleeps in the test.
        mock_client.workspaces.modules.render_visualisation.return_value = {
            "status": "rendering",
            "retry_after": 0,
        }
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(
                WS_ID, TAB_ID, MOD_ID, poll=True, timeout_s=300.0
            )

        body = json.loads(result)
        assert body["status"] == "rendering"
        assert body["polls"] == _RENDER_MAX_POLLS
        assert "internal poll cap" in body["reason"]
        # Exactly _RENDER_MAX_POLLS HTTP requests — the hard cap.
        assert (
            mock_client.workspaces.modules.render_visualisation.call_count
            == _RENDER_MAX_POLLS
        )

    def test_internal_poll_succeeds_on_second_try(self, mock_client):
        """Resource answers 202 first, then a real body. Tool unwraps the
        body without surfacing the rendering envelope."""
        mock_client.workspaces.modules.render_visualisation.side_effect = [
            {"status": "rendering", "retry_after": 0},
            {"data": [{"type": "bar"}], "layout": {}},
        ]
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(
                WS_ID, TAB_ID, MOD_ID, poll=True, timeout_s=300.0
            )
        body = json.loads(result)
        assert body["data"][0]["type"] == "bar"
        assert mock_client.workspaces.modules.render_visualisation.call_count == 2

    def test_internal_poll_returns_envelope_when_deadline_exceeded(self, mock_client):
        """A small timeout_s should produce the rendering envelope BEFORE
        the internal poll cap when the next retry_after would blow the
        deadline."""
        mock_client.workspaces.modules.render_visualisation.return_value = {
            "status": "rendering",
            "retry_after": 5,
        }
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(
                WS_ID, TAB_ID, MOD_ID, poll=True, timeout_s=0.0
            )
        body = json.loads(result)
        assert body["status"] == "rendering"
        assert "timeout_s exceeded" in body["reason"]
        # First request was made; loop bailed before sleeping.
        assert mock_client.workspaces.modules.render_visualisation.call_count == 1


class TestRenderModuleVisualisationRenderableGuard:
    """Only 12 module types have a server-side renderer. Calling this tool
    on any other module used to burn an HTTP round-trip and come back with
    "Visualisation not supported for module type 'X'" — 58% of render
    calls failed this way. The guard answers locally instead."""

    def test_non_renderable_module_fails_fast(self, mock_client):
        mock_client.workspaces.modules.get.return_value = _module(
            item_id="gsea_dot_plot"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID)
        body = json.loads(result)
        assert body["status"] == "error"
        assert body["renderable"] is False
        assert body["item_id"] == "gsea_dot_plot"
        # The renderable set is named so the LLM can pick an alternative.
        assert "box_plot" in body["renderable_module_types"]
        assert len(body["renderable_module_types"]) == 12
        # No HTTP round-trip to the vis endpoint.
        mock_client.workspaces.modules.render_visualisation.assert_not_called()

    def test_non_renderable_guard_applies_when_poll_false(self, mock_client):
        mock_client.workspaces.modules.get.return_value = _module(
            item_id="qc_summary_table"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID, poll=False)
        assert json.loads(result)["status"] == "error"
        mock_client.workspaces.modules.render_visualisation.assert_not_called()

    def test_unknown_module_returns_error_envelope(self, mock_client):
        mock_client.workspaces.modules.get.return_value = None
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID)
        body = json.loads(result)
        assert body["status"] == "error"
        assert "not found" in body["error"]
        assert "list_tab_modules" in body["error"]
        mock_client.workspaces.modules.render_visualisation.assert_not_called()

    def test_renderable_module_passes_through(self, mock_client):
        mock_client.workspaces.modules.get.return_value = _module(
            item_id="pairwise_volcano_plot"
        )
        mock_client.workspaces.modules.render_visualisation.return_value = {
            "data": [],
            "layout": {},
        }
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = render_module_visualisation(WS_ID, TAB_ID, MOD_ID)
        assert json.loads(result) == {"data": [], "layout": {}}
        assert mock_client.workspaces.modules.render_visualisation.call_count == 1

    def test_docstring_names_the_renderable_contract(self):
        wrapped = (
            getattr(render_module_visualisation, "fn", None)
            or render_module_visualisation
        )
        doc = (render_module_visualisation.__doc__ or "") + (wrapped.__doc__ or "")
        assert "ONLY 12 MODULE TYPES ARE RENDERABLE" in doc


class TestAddPlotlyJsonModule:
    def test_creates_with_plotly_json_settings(self, mock_client):
        mock_client.workspaces.modules.create.return_value = _module(
            item_id="plotly_json_renderer",
            settings={"plotlyJson": {"data": [], "layout": {}}, "title": "My plot"},
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_plotly_json_module(
                WS_ID,
                TAB_ID,
                plotly_json={"data": [{"type": "scatter"}], "layout": {"title": "x"}},
                title="My plot",
            )

        assert result.startswith("Plotly JSON module placed.")
        kwargs = mock_client.workspaces.modules.create.call_args.kwargs
        assert kwargs["item_id"] == "plotly_json_renderer"
        assert kwargs["settings"] == {
            "plotlyJson": {"data": [{"type": "scatter"}], "layout": {"title": "x"}},
            "title": "My plot",
        }
        assert (kwargs["width"], kwargs["height"]) == (12, 6)

    def test_omits_title_when_empty(self, mock_client):
        mock_client.workspaces.modules.create.return_value = _module(
            item_id="plotly_json_renderer", settings={"plotlyJson": {}}
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            add_plotly_json_module(WS_ID, TAB_ID, plotly_json={})
        kwargs = mock_client.workspaces.modules.create.call_args.kwargs
        assert kwargs["settings"] == {"plotlyJson": {}}

    def test_non_dict_plotly_json_returns_error(self, mock_client):
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_plotly_json_module(
                WS_ID, TAB_ID, plotly_json="{}"  # type: ignore[arg-type]
            )
        assert result.startswith("Error:")
        assert "must be a dict" in result
        mock_client.workspaces.modules.create.assert_not_called()

    def test_resource_error_returns_error_prose(self, mock_client):
        mock_client.workspaces.modules.create.side_effect = Exception(
            "Failed to create module: 422 - feature flag off"
        )
        with patch("mcp_tools.workspaces.modules.get_client", return_value=mock_client):
            result = add_plotly_json_module(
                WS_ID, TAB_ID, plotly_json={"data": [], "layout": {}}
            )
        assert result.startswith("Error:")
        assert "feature flag off" in result

    def test_docstring_carries_local_only_caveat(self):
        wrapped = getattr(add_plotly_json_module, "fn", None) or add_plotly_json_module
        doc = (add_plotly_json_module.__doc__ or "") + (wrapped.__doc__ or "")
        assert "LOCAL-ONLY" in doc.upper()
        assert "dev.massdynamics.com" in doc
