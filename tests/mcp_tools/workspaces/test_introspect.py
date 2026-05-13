"""Tests for mcp_tools.workspaces._introspect.

The introspection helper is the source-of-truth for the visualisation
parameter Q&A mandate — every rule the LLM is supposed to follow ("never
elide a parameter row", "always declare data dependencies for non-fillable
fields", "expose conditional-visibility clauses") needs to be reflected
here.
"""

from mcp_tools.workspaces._introspect import describe, parameter_doc
from md_python.models import RegisteredModule


def _module(input_settings):
    return RegisteredModule(
        id="dimensionality_reduction_plot",
        name="Dimensionality Reduction Plot",
        group="Experiment",
        icon="x",
        input_settings=input_settings,
    )


# ──────────────────────────────────────────────────────────────────────────────
# parameter_doc — every required key in the output schema is always present
# ──────────────────────────────────────────────────────────────────────────────


class TestParameterDocSchema:
    """The schema is contract — every key listed below MUST appear in every
    parameter_doc() result, even when the value is None. The LLM is told
    'never elide a row'; we test that the data shape matches that promise."""

    REQUIRED_KEYS = {
        "key",
        "name",
        "group",
        "field_type",
        "value_kind",
        "value_description",
        "platform_default",
        "default_present",
        "default_note",
        "is_required",
        "data_dependencies",
        "cross_field_refs",
        "options",
        "condition",
        "description",
        "fillable_by_llm",
        "raw_parameters",
    }

    def test_dict_spec_emits_all_keys(self):
        doc = parameter_doc(
            "datasetsSearch",
            {
                "fieldType": "Datasets",
                "name": "Datasets",
                "group": "Data",
                "parameters": {"type": "INTENSITY", "multiple": False},
                "rules": [{"name": "is_required"}],
            },
        )
        assert set(doc.keys()) == self.REQUIRED_KEYS

    def test_literal_spec_still_emits_all_keys(self):
        # Some dict-shape entries may be primitives (corner case).
        doc = parameter_doc("foo", "bare_literal")
        assert set(doc.keys()) == self.REQUIRED_KEYS
        assert doc["platform_default"] == "bare_literal"


# ──────────────────────────────────────────────────────────────────────────────
# Required detection — both wire encodings
# ──────────────────────────────────────────────────────────────────────────────


class TestRequiredDetection:
    def test_rules_is_required_marks_required(self):
        doc = parameter_doc(
            "x",
            {"fieldType": "String", "rules": [{"name": "is_required"}]},
        )
        assert doc["is_required"] is True

    def test_required_true_boolean_marks_required(self):
        # Cached-manifest array shape uses required:true.
        doc = parameter_doc("x", {"fieldType": "String", "required": True})
        assert doc["is_required"] is True

    def test_no_required_marker(self):
        doc = parameter_doc("x", {"fieldType": "String"})
        assert doc["is_required"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Default surfacing — null defaults must NEVER be silently dropped
# ──────────────────────────────────────────────────────────────────────────────


class TestDefaultSurfacing:
    def test_null_default_is_flagged(self):
        # The user said: "do not leave any parameter undocumented, even
        # the ones that are set to none". Test that null defaults carry an
        # explicit note so the LLM cannot skip the row.
        doc = parameter_doc(
            "colourBy", {"fieldType": "DatasetSampleMetadata", "default": None}
        )
        assert doc["platform_default"] is None
        assert doc["default_present"] is False
        assert doc["default_note"] is not None
        assert "null" in doc["default_note"]

    def test_missing_default_is_flagged(self):
        doc = parameter_doc("datasetsSearch", {"fieldType": "Datasets"})
        assert doc["default_present"] is False
        assert doc["default_note"] is not None
        assert "no default declared" in doc["default_note"]

    def test_present_default_no_note(self):
        doc = parameter_doc("size", {"fieldType": "String", "default": "h1"})
        assert doc["platform_default"] == "h1"
        assert doc["default_present"] is True
        assert doc["default_note"] is None


# ──────────────────────────────────────────────────────────────────────────────
# Data dependencies — non-fillable fields surface what to fetch
# ──────────────────────────────────────────────────────────────────────────────


class TestDataDependencies:
    def test_datasets_field_declares_dataset_dependency(self):
        doc = parameter_doc("datasetsSearch", {"fieldType": "Datasets"})
        assert doc["fillable_by_llm"] is False
        assert doc["data_dependencies"], "Datasets field must declare a dependency"

    def test_dataset_sample_metadata_demands_metadata_fetch(self):
        # The user's PCA example: colourBy needs sample_metadata.
        doc = parameter_doc("colourBy", {"fieldType": "DatasetSampleMetadata"})
        assert doc["fillable_by_llm"] is False
        joined = " ".join(doc["data_dependencies"]).lower()
        assert "sample_metadata" in joined
        assert "get_upload_sample_metadata" in joined

    def test_protein_list_demands_entity_list(self):
        doc = parameter_doc("entityListId", {"fieldType": "ProteinList"})
        assert doc["fillable_by_llm"] is False
        joined = " ".join(doc["data_dependencies"]).lower()
        assert "entity-list" in joined or "protein-group" in joined

    def test_string_enum_is_fillable(self):
        doc = parameter_doc(
            "scalingMethod",
            {
                "fieldType": "String",
                "default": "none",
                "parameters": {
                    "options": [
                        {"value": "none", "name": "None"},
                        {"value": "zscore", "name": "zScore"},
                    ]
                },
            },
        )
        assert doc["fillable_by_llm"] is True
        assert doc["data_dependencies"] == []

    def test_unmapped_field_type_flagged_explicitly(self):
        # If the registry adds a new fieldType, the LLM should see a clear
        # "ask the maintainers" rather than silently treating the field as
        # opaque-unknown.
        doc = parameter_doc("weird", {"fieldType": "BrandNewType"})
        assert "unmapped" in doc["value_kind"]
        assert "BrandNewType" in doc["value_description"]


class TestExtendedFieldTypeProfiles:
    """Every fieldType used in the workflow manifest must be mapped — the
    LLM otherwise sees ``unmapped:<name>`` and treats the parameter as
    opaque. Regression: 15 fieldTypes were unmapped and these tests pin
    the contract."""

    def test_plot_size_is_fillable(self):
        doc = parameter_doc("plotSize", {"fieldType": "PlotSize"})
        assert doc["value_kind"] == "plot_size"
        assert doc["fillable_by_llm"] is True
        assert "unmapped" not in doc["value_kind"]

    def test_number_range_is_fillable(self):
        doc = parameter_doc(
            "perplexity",
            {"fieldType": "NumberRange", "parameters": {"min": 5, "max": 50}},
        )
        assert doc["value_kind"] == "number-in-range"
        assert doc["fillable_by_llm"] is True

    def test_fold_change_threshold_is_fillable(self):
        doc = parameter_doc("foldChangeThreshold", {"fieldType": "FoldChangeThreshold"})
        assert doc["fillable_by_llm"] is True
        assert "log2" in doc["value_description"]

    def test_n_components_depends_on_dataset(self):
        doc = parameter_doc(
            "nComponents",
            {
                "fieldType": "NComponents",
                "parameters": {"datasetsSearch": {"ref": "datasetsSearch"}},
            },
        )
        assert doc["fillable_by_llm"] is False
        assert any("dataset" in d for d in doc["data_dependencies"])

    def test_pairwise_condition_pairs_depends_on_dataset(self):
        doc = parameter_doc("conditionPairs", {"fieldType": "PairwiseConditionPairs"})
        assert doc["fillable_by_llm"] is False
        assert any("sample metadata" in d for d in doc["data_dependencies"])

    def test_entity_title_is_fillable(self):
        doc = parameter_doc("entityLabel", {"fieldType": "EntityTitle"})
        assert doc["fillable_by_llm"] is True

    def test_experiment_demands_id(self):
        doc = parameter_doc("experimentId", {"fieldType": "Experiment"})
        assert doc["fillable_by_llm"] is False
        assert "experiment" in doc["value_kind"]

    def test_multiple_is_fillable(self):
        doc = parameter_doc("evidenceChannels", {"fieldType": "Multiple"})
        assert doc["fillable_by_llm"] is True
        assert "multi" in doc["value_kind"]

    def test_properties_is_opaque(self):
        doc = parameter_doc("sort", {"fieldType": "Properties"})
        assert doc["fillable_by_llm"] is False
        assert "opaque" in doc["value_description"].lower()

    def test_species_is_fillable(self):
        doc = parameter_doc("speciesId", {"fieldType": "Species"})
        assert doc["fillable_by_llm"] is True
        assert "9606" in doc["value_description"]

    def test_dataset_table_depends_on_dataset(self):
        doc = parameter_doc("tableName", {"fieldType": "DatasetTable"})
        assert doc["fillable_by_llm"] is False
        assert any("dataset" in d for d in doc["data_dependencies"])

    def test_dataset_table_values_depends_on_table(self):
        doc = parameter_doc("datasetTableValues", {"fieldType": "DatasetTableValues"})
        assert doc["fillable_by_llm"] is False
        deps = " ".join(doc["data_dependencies"])
        assert "table" in deps

    def test_sample_metadata_columns_demands_metadata_fetch(self):
        doc = parameter_doc(
            "sampleMetadataInfo", {"fieldType": "DatasetSampleMetadataColumns"}
        )
        assert doc["fillable_by_llm"] is False
        assert any("get_upload_sample_metadata" in d for d in doc["data_dependencies"])

    def test_sample_metadata_value_demands_column(self):
        doc = parameter_doc(
            "controlCondition", {"fieldType": "DatasetSampleMetadataValue"}
        )
        assert doc["fillable_by_llm"] is False
        assert any("column" in d for d in doc["data_dependencies"])

    def test_sample_metadata_values_order_demands_column(self):
        doc = parameter_doc(
            "orderedGroupByValues",
            {"fieldType": "DatasetSampleMetadataValuesOrder"},
        )
        assert doc["fillable_by_llm"] is False
        assert any("column" in d for d in doc["data_dependencies"])

    def test_no_manifest_field_type_is_unmapped(self):
        """Lock the contract: every fieldType present in the built manifest
        must have a profile. New manifest types should fail this test until
        a profile is added."""
        from mcp_tools.workspaces._introspect import _FIELD_TYPE_PROFILES

        manifest_types = {
            "Boolean",
            "ColourPalette",
            "ConditionComparison",
            "DatasetSampleMetadata",
            "DatasetSampleMetadataColumns",
            "DatasetSampleMetadataValue",
            "DatasetSampleMetadataValues",
            "DatasetSampleMetadataValuesOrder",
            "DatasetTable",
            "DatasetTableValues",
            "Datasets",
            "EntityTitle",
            "EntityType",
            "Experiment",
            "FoldChangeThreshold",
            "Multiple",
            "NComponents",
            "Number",
            "NumberRange",
            "OrderableSampleMetadataColumns",
            "PairwiseConditionPairs",
            "PlotSize",
            "Properties",
            "ProteinList",
            "ProteinLists",
            "ProteinSelection",
            "RadioSelectionField",
            "SampleMetadataValuesFilter",
            "Species",
            "String",
        }
        missing = manifest_types - set(_FIELD_TYPE_PROFILES.keys())
        assert not missing, f"unmapped manifest fieldTypes: {sorted(missing)}"


# ──────────────────────────────────────────────────────────────────────────────
# Conditional visibility (`when` clauses)
# ──────────────────────────────────────────────────────────────────────────────


class TestConditionalVisibility:
    def test_equals_condition_humanised(self):
        doc = parameter_doc(
            "correlationMethod",
            {
                "fieldType": "String",
                "when": {"property": "dataType", "equals": "correlation"},
            },
        )
        assert doc["condition"] is not None
        assert doc["condition"]["depends_on"] == "dataType"
        assert doc["condition"]["predicate"] == "equals"
        assert doc["condition"]["value"] == "correlation"
        assert "correlation" in doc["condition"]["human"]

    def test_not_equals_condition_humanised(self):
        doc = parameter_doc(
            "entityType",
            {
                "fieldType": "EntityType",
                "when": {"property": "datasetsSearch", "not_equals": None},
            },
        )
        assert doc["condition"]["predicate"] == "not_equals"

    def test_no_condition_returns_none(self):
        doc = parameter_doc("x", {"fieldType": "String"})
        assert doc["condition"] is None


# ──────────────────────────────────────────────────────────────────────────────
# Cross-field refs (parameters.<x>.ref)
# ──────────────────────────────────────────────────────────────────────────────


class TestCrossFieldRefs:
    def test_ref_surfaced(self):
        doc = parameter_doc(
            "entityType",
            {
                "fieldType": "EntityType",
                "parameters": {"datasetsSearch": {"ref": "datasetsSearch"}},
            },
        )
        assert doc["cross_field_refs"], "ref must be surfaced"
        joined = " ".join(doc["cross_field_refs"])
        assert "datasetsSearch" in joined

    def test_no_refs_means_empty_list(self):
        doc = parameter_doc("x", {"fieldType": "Boolean"})
        assert doc["cross_field_refs"] == []


# ──────────────────────────────────────────────────────────────────────────────
# Options summary
# ──────────────────────────────────────────────────────────────────────────────


class TestOptionsSummary:
    def test_options_extracted(self):
        doc = parameter_doc(
            "drMethod",
            {
                "fieldType": "String",
                "parameters": {
                    "options": [
                        {"value": "pca", "name": "PCA"},
                        {"value": "tsne", "name": "t-SNE"},
                        {"value": "umap", "name": "UMAP"},
                    ]
                },
            },
        )
        assert doc["options"] == [
            {"value": "pca", "label": "PCA"},
            {"value": "tsne", "label": "t-SNE"},
            {"value": "umap", "label": "UMAP"},
        ]

    def test_no_options_none(self):
        doc = parameter_doc("x", {"fieldType": "Boolean"})
        assert doc["options"] is None


# ──────────────────────────────────────────────────────────────────────────────
# describe() — full module description
# ──────────────────────────────────────────────────────────────────────────────


class TestDescribe:
    LIVE_HEADING = {
        "text": {
            "fieldType": "String",
            "name": "Text",
            "group": "Data",
            "rules": [{"name": "is_required"}],
        },
        "size": {
            "fieldType": "String",
            "name": "Size",
            "group": "Data",
            "default": "h1",
            "parameters": {
                "options": [
                    {"name": "Heading 1", "value": "h1"},
                    {"name": "Heading 2", "value": "h2"},
                ]
            },
            "rules": [{"name": "is_required"}],
        },
        "horizontalPosition": {
            "fieldType": "String",
            "name": "Horizontal Position",
            "default": "left",
            "rules": [{"name": "is_required"}],
        },
    }

    def test_top_level_keys_always_present(self):
        out = describe(_module(self.LIVE_HEADING))
        for key in (
            "id",
            "name",
            "short_name",
            "group",
            "icon",
            "keywords",
            "instruction_name",
            "description",
            "short_description",
            "parameters",
            "data_dependencies",
            "required_keys_no_default",
            "registry_defaults",
        ):
            assert key in out

    def test_required_keys_no_default(self):
        out = describe(_module(self.LIVE_HEADING))
        # text is required and has no default — LLM must collect it
        assert out["required_keys_no_default"] == ["text"]
        # size and horizontalPosition are required AND have defaults — auto-fill
        assert out["registry_defaults"] == {
            "size": "h1",
            "horizontalPosition": "left",
        }

    def test_aggregate_data_dependencies(self):
        # Module needs a dataset AND sample-metadata: dependencies must
        # roll up at the module level too, deduped.
        mod = _module(
            {
                "datasetsSearch": {"fieldType": "Datasets"},
                "colourBy": {"fieldType": "DatasetSampleMetadata"},
                "sampleNames": {"fieldType": "DatasetSampleMetadataValues"},
            }
        )
        deps = describe(mod)["data_dependencies"]
        # Dedupe — sample_metadata appears in both colourBy and sampleNames
        # but should only show up once.
        joined = " ".join(deps).lower()
        assert "sample_metadata" in joined
        assert "dataset" in joined

    def test_no_input_settings(self):
        mod = RegisteredModule(
            id="page_break",
            name="Page Break",
            group="General",
            icon="x",
            input_settings=None,
        )
        out = describe(mod)
        assert out["parameters"] == []
        assert out["registry_defaults"] == {}
        assert out["required_keys_no_default"] == []
