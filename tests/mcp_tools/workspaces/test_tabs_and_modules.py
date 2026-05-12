"""Tests for mcp_tools.workspaces.tabs and mcp_tools.workspaces.modules."""

import json
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID

from mcp_tools.workspaces.modules import (
    add_module_to_tab,
    add_text_module,
    list_tab_modules,
    remove_module_from_tab,
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


def _stub_dataset(
    uid: str, name: str = "Demo Dataset", type: str = "INTENSITY"
) -> Dataset:
    return Dataset(
        id=UUID(uid),
        input_dataset_ids=[],
        name=name,
        job_slug="demo",
        job_run_params={},
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
                entity_type="metabolite",  # not a valid value
            )
        assert result.startswith("Error:")
        assert "metabolite" in result

    def test_entity_type_on_dataset_free_module_rejected(self, mock_client):
        # heading has no EntityType field — passing entity_type must fail.
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
                entity_type="protein",
                settings={"text": "x"},
            )
        assert result.startswith("Error:")
        assert "does not accept entity_type" in result

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
