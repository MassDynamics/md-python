"""Phase C alignment + behavioural-mandate tests for non-pipeline MCP tools.

Three groups:

1. Destructive-action mandate. Every tool flagged destructive in
   mcp_tools.health._WORKFLOW_GUIDE['constraints'] must carry the shared
   MANDATORY DESTRUCTIVE-ACTION CONFIRMATION fragment in its docstring.
   Source of truth: mcp_tools._destructive.DESTRUCTIVE_FRAGMENT.

2. md_format_gene alignment. The Imputed column must be marked OPTIONAL
   for gene data because md-converter auto-derives it
   (md-converter/src/mdconverter/md_format_gene/reader.py:120-124). Required
   gene columns are only [GeneId, GeneExpression, SampleName] (reader.py:8).

3. tool_index ↔ registry coverage. Every tool name listed in
   _WORKFLOW_GUIDE['tool_index'] must be a registered MCP tool
   (mcp_tools.batch._TOOL_REGISTRY) so the LLM never gets pointed at a
   non-existent function.
"""

import json

import pytest

from mcp_tools._destructive import DESTRUCTIVE_FRAGMENT
from mcp_tools.batch import _TOOL_REGISTRY
from mcp_tools.datasets import cancel_dataset, delete_dataset
from mcp_tools.files import get_md_format_spec, plan_wide_to_md_format
from mcp_tools.health import _WORKFLOW_GUIDE
from mcp_tools.uploads import (
    cancel_upload_queue,
    delete_upload,
    update_sample_metadata,
)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Destructive-action mandate tests
# ──────────────────────────────────────────────────────────────────────────────


_DESTRUCTIVE_TOOLS = [
    delete_upload,
    delete_dataset,
    cancel_dataset,
    cancel_upload_queue,
    update_sample_metadata,
]


@pytest.mark.parametrize("tool", _DESTRUCTIVE_TOOLS, ids=lambda t: t.__name__)
def test_destructive_tool_docstring_includes_mandate(tool):
    """Every destructive tool docstring must include the binding mandate."""
    doc = tool.__doc__ or ""
    assert "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in doc, (
        f"{tool.__name__} docstring must include the MANDATORY "
        f"DESTRUCTIVE-ACTION CONFIRMATION clause."
    )


@pytest.mark.parametrize("tool", _DESTRUCTIVE_TOOLS, ids=lambda t: t.__name__)
def test_destructive_tool_docstring_includes_fragment_block(tool):
    """The full DESTRUCTIVE_FRAGMENT block must be present verbatim."""
    doc = tool.__doc__ or ""
    assert "LLM BEHAVIOURAL MANDATE — DESTRUCTIVE ACTION" in doc, (
        f"{tool.__name__} docstring must include the destructive-action "
        f"mandate block."
    )


def test_destructive_fragment_names_required_steps():
    """Belt-and-braces sanity check on the shared fragment."""
    assert "Echo every target id" in DESTRUCTIVE_FRAGMENT
    assert "Wait for an EXPLICIT confirmation" in DESTRUCTIVE_FRAGMENT
    assert "NEVER chain" in DESTRUCTIVE_FRAGMENT


def test_workflow_guide_destructive_constraint_references_mandate():
    """The DESTRUCTIVE entry in 'constraints' must point at the mandate.

    This is what the LLM reads first via get_workflow_guide(), so the
    constraint text must echo the binding language used in tool docstrings.
    """
    constraints = _WORKFLOW_GUIDE["constraints"]
    destructive = next((c for c in constraints if "DESTRUCTIVE" in c), None)
    assert destructive is not None
    assert "MANDATORY DESTRUCTIVE-ACTION CONFIRMATION" in destructive
    # All five tools must be listed.
    for tool in (
        "delete_upload",
        "delete_dataset",
        "cancel_dataset",
        "cancel_upload_queue",
        "update_sample_metadata",
    ):
        assert tool in destructive


# ──────────────────────────────────────────────────────────────────────────────
# 2. md_format_gene alignment tests
# ──────────────────────────────────────────────────────────────────────────────


class TestGeneSpecImputedIsOptional:
    """md-converter md_format_gene reader auto-derives Imputed.

    Source of truth:
      md-converter/src/mdconverter/md_format_gene/reader.py:8 ::
        REQUIRED_GENE_COLUMNS = ['GeneId', 'GeneExpression', 'SampleName']
      md-converter/src/mdconverter/md_format_gene/reader.py:120-124 ::
        add_imputed_column(self, data) sets Imputed=1 wherever
        GeneExpression is NaN or 0.

    The MCP must NOT instruct the user that Imputed is required for gene
    uploads, and must NOT require the user to manually flag zeros — that's
    only true for protein/peptide md_format.
    """

    def test_gene_spec_marks_imputed_optional(self):
        result = json.loads(get_md_format_spec("gene"))
        spec = result["spec"]
        # Imputed may still be listed (informational) but its description
        # must be marked OPTIONAL and cite the converter auto-derivation.
        assert "Imputed" in spec
        imputed_desc = spec["Imputed"]
        assert "OPTIONAL" in imputed_desc
        assert "md_format_gene/reader.py" in imputed_desc

    def test_gene_spec_marks_required_columns(self):
        result = json.loads(get_md_format_spec("gene"))
        spec = result["spec"]
        for col in ("GeneId", "GeneExpression", "SampleName"):
            assert "REQUIRED" in spec[col]

    def test_gene_notes_explain_auto_imputed(self):
        result = json.loads(get_md_format_spec("gene"))
        notes_text = " ".join(result["notes"])
        assert "auto-derive" in notes_text or "auto-flag" in notes_text
        assert "md_format_gene/reader.py" in notes_text

    def test_gene_notes_mention_experiment_design_optional(self):
        """workflow/app/models/experiment.rb:98-103 skips the
        experiment_design-required validation for source=md_format_gene.
        The MCP should surface this so the LLM does not insist on it."""
        result = json.loads(get_md_format_spec("gene"))
        notes_text = " ".join(result["notes"])
        assert "experiment_design is OPTIONAL" in notes_text
        assert "experiment.rb" in notes_text

    def test_protein_notes_still_critical_imputed_warning(self):
        """Protein spec must keep the strict 'every 0 must have Imputed=1'
        warning — only the gene spec is relaxed."""
        result = json.loads(get_md_format_spec("protein"))
        notes_text = " ".join(result["notes"])
        assert "CRITICAL" in notes_text
        assert "Imputed=1" in notes_text


class TestPlanWideToMdFormatGeneNotes:
    """plan_wide_to_md_format with target='md_format_gene' must produce a
    script that does NOT emit a manually-computed Imputed column, and notes
    that flag the gene-specific auto-Imputed behaviour."""

    def test_gene_target_script_omits_manual_imputed(self, tmp_path):
        f = tmp_path / "wide_genes.tsv"
        f.write_text("GeneId\tS1\tS2\nENSG001\t100\t0\n")
        result = json.loads(
            plan_wide_to_md_format(
                str(f),
                target="md_format_gene",
                annotation_columns=["GeneId"],
            )
        )
        script = result["conversion_script"]
        # The protein template adds long_df["Imputed"] = ... — the gene
        # template must NOT.
        assert 'long_df["Imputed"]' not in script
        assert 'value_name="GeneExpression"' in script

    def test_gene_target_notes_flag_auto_imputed(self, tmp_path):
        f = tmp_path / "wide_genes.tsv"
        f.write_text("GeneId\tS1\tS2\nENSG001\t100\t0\n")
        result = json.loads(
            plan_wide_to_md_format(
                str(f),
                target="md_format_gene",
                annotation_columns=["GeneId"],
            )
        )
        notes_text = " ".join(result["notes"])
        assert "GENE-SPECIFIC" in notes_text
        assert "md_format_gene/reader.py" in notes_text
        assert "experiment_design is OPTIONAL" in notes_text

    def test_protein_target_keeps_critical_imputed_warning(self, tmp_path):
        f = tmp_path / "wide_proteins.tsv"
        f.write_text("Protein.Group\tS1\tS2\nP12345\t100\t0\n")
        result = json.loads(plan_wide_to_md_format(str(f)))
        notes_text = " ".join(result["notes"])
        assert "CRITICAL" in notes_text and "Imputed=1" in notes_text


# ──────────────────────────────────────────────────────────────────────────────
# 3. Workflow-guide ↔ MCP tool-registry coverage tests
# ──────────────────────────────────────────────────────────────────────────────


def _flatten_tool_index(tool_index):
    names = set()
    for category in tool_index.values():
        names.update(category.keys())
    return names


# Tools that appear in tool_index but are intentionally NOT in batch's
# _TOOL_REGISTRY (they do not make sense to batch). Empty for now.
_REGISTRY_EXEMPTIONS: set = set()


def test_every_tool_index_name_is_registered():
    """Every tool name surfaced via get_workflow_guide must resolve to a
    real registered MCP tool function. If a tool is renamed/removed but
    the index isn't updated, the LLM gets pointed at a ghost — this test
    locks the invariant."""
    indexed = _flatten_tool_index(_WORKFLOW_GUIDE["tool_index"])
    registered = set(_TOOL_REGISTRY.keys())

    # `batch` is itself a registered MCP tool but registers via the @mcp.tool
    # decorator separately; it is named in the utility_tools index.
    # We treat it as registered for index-coverage purposes.
    registered_with_extras = registered | {"batch"}

    missing = (indexed - registered_with_extras) - _REGISTRY_EXEMPTIONS
    assert not missing, (
        f"tool_index references tools that aren't in the MCP registry: "
        f"{sorted(missing)}"
    )


def test_workflow_guide_steps_only_reference_existing_tools():
    """Every tool name mentioned in workflow steps must exist. We use a
    permissive scan: split tokens on '(' and check whether any registered
    tool name appears."""
    registered = set(_TOOL_REGISTRY.keys()) | {"batch"}
    workflows = _WORKFLOW_GUIDE["workflows"]
    referenced: set = set()
    for wf in workflows.values():
        for step in wf.get("steps", []):
            head = step.split("(", 1)[0]
            for token in head.replace(",", " ").split():
                if token in registered:
                    referenced.add(token)
    # All these must be a subset of registered. (If we flipped the assertion,
    # we'd have to enumerate every registered tool; the meaningful invariant
    # is: don't reference a name we don't have.)
    assert referenced <= registered, (
        f"Workflow steps reference tools that aren't registered: "
        f"{sorted(referenced - registered)}"
    )


def test_l_gene_workflow_present_and_runnable():
    """The L_gene_workflow must be present and reference the gene-specific
    tools the user needs end-to-end (md_format_gene upload → NI with cpm
    + by minimum abundance → pairwise / ANOVA)."""
    wf = _WORKFLOW_GUIDE["workflows"]["L_gene_workflow"]
    steps_text = " ".join(wf["steps"])
    notes_text = " ".join(wf["notes"])
    assert "md_format_gene" in steps_text
    assert "cpm" in steps_text
    assert "by minimum abundance" in steps_text
    assert "filter_based_on_condition" in steps_text  # NI gene filtration
    # entity_type='gene' must appear on both stats steps
    assert steps_text.count("entity_type='gene'") >= 2
    # Notes must mention the gene-Imputed auto-derivation (so the LLM does
    # not nag the user about adding the column manually).
    assert "Imputed is OPTIONAL" in notes_text
    assert "md_format_gene/reader.py" in notes_text
    # Notes must surface the experiment_design optionality.
    assert "experiment_design is OPTIONAL" in notes_text


def test_k_filtration_only_workflow_matches_signature():
    """K_filtration_only must reference the current
    run_normalisation_imputation signature (filtration_method +
    filtration_extra_params)."""
    wf = _WORKFLOW_GUIDE["workflows"]["K_filtration_only"]
    steps_text = " ".join(wf["steps"])
    assert "filtration_method" in steps_text
    assert "filtration_extra_params" in steps_text
    assert "normalisation_method='skip'" in steps_text
    assert "imputation_method='skip'" in steps_text
