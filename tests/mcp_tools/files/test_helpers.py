"""Unit tests for the private helper functions extracted from load_metadata_from_csv."""

from mcp_tools.files.metadata import (
    _build_ed_rows,
    _collect_notes,
    _deduplicate_rows_by_sample_name,
    _safe_get,
    _sm_column_order,
)


class TestSafeGet:
    def test_normal_index(self):
        assert _safe_get(["a", " b ", "c"], 1) == "b"

    def test_out_of_bounds(self):
        assert _safe_get(["a", "b"], 5) == ""

    def test_strips_whitespace(self):
        assert _safe_get(["  hello  "], 0) == "hello"


class TestBuildEdRows:
    def test_basic(self):
        idx = {"filename": 0, "sample_name": 1, "condition": 2}
        rows = _build_ed_rows([["f1", "s1", "ctrl"], ["f2", "s2", "treated"]], idx)
        assert rows == [["f1", "s1", "ctrl"], ["f2", "s2", "treated"]]

    def test_ragged_row_fills_missing_columns_with_empty_string(self):
        idx = {"filename": 0, "sample_name": 1, "condition": 2}
        rows = _build_ed_rows([["f1", "s1"]], idx)  # condition column absent
        assert rows == [["f1", "s1", ""]]


class TestSmColumnOrder:
    def test_excludes_filename_column(self):
        normalised = ["filename", "sample_name", "condition", "dose"]
        stripped = ["filename", "sample_name", "condition", "dose"]
        col_indices, headers = _sm_column_order(normalised, stripped)
        assert "filename" not in headers
        assert 0 not in col_indices

    def test_moves_sample_name_to_first_position(self):
        normalised = ["condition", "sample_name", "dose"]
        stripped = ["condition", "sample_name", "dose"]
        col_indices, headers = _sm_column_order(normalised, stripped)
        assert headers[0] == "sample_name"

    def test_leaves_sample_name_first_when_already_first(self):
        normalised = ["sample_name", "dose"]
        stripped = ["sample_name", "dose"]
        col_indices, headers = _sm_column_order(normalised, stripped)
        assert headers == ["sample_name", "dose"]


class TestDeduplicateRowsBySampleName:
    def test_removes_duplicate_sample_names(self):
        rows = [["s1", "ctrl"], ["s1", "ctrl"], ["s2", "treated"]]
        result, seen = _deduplicate_rows_by_sample_name(
            rows, sn_idx=0, col_indices=[0, 1]
        )
        assert len(result) == 2
        assert seen == {"s1", "s2"}

    def test_preserves_first_occurrence(self):
        rows = [["s1", "first"], ["s1", "second"]]
        result, _ = _deduplicate_rows_by_sample_name(rows, sn_idx=0, col_indices=[0, 1])
        assert result[0][1] == "first"

    def test_skips_blank_sample_names(self):
        rows = [["", "ctrl"], ["s1", "treated"]]
        result, seen = _deduplicate_rows_by_sample_name(
            rows, sn_idx=0, col_indices=[0, 1]
        )
        assert len(result) == 1
        assert "" not in seen


class TestCollectNotes:
    def test_no_filename_col_with_condition_suggests_lfq_shortcut(self):
        notes = _collect_notes(
            has_ed=False,
            normalised=["sample_name", "condition"],
            header_stripped=["sample_name", "condition"],
            experiment_design=None,
            sm_headers=["sample_name", "condition"],
        )
        assert "LFQ SHORTCUT" in " ".join(notes)

    def test_no_filename_col_no_condition_gives_generic_note(self):
        notes = _collect_notes(
            has_ed=False,
            normalised=["sample_name", "dose"],
            header_stripped=["sample_name", "dose"],
            experiment_design=None,
            sm_headers=["sample_name", "dose"],
        )
        combined = " ".join(notes)
        assert "LFQ SHORTCUT" not in combined
        assert "filename" in combined.lower()

    def test_warns_on_empty_condition_values(self):
        ed = [["filename", "sample_name", "condition"], ["f1", "s1", ""]]
        notes = _collect_notes(
            has_ed=True,
            normalised=["filename", "sample_name", "condition"],
            header_stripped=["filename", "sample_name", "condition"],
            experiment_design=ed,
            sm_headers=["sample_name", "condition"],
        )
        assert "empty condition" in " ".join(notes)

    def test_always_ends_with_validate_reminder(self):
        notes = _collect_notes(
            has_ed=True,
            normalised=["filename", "sample_name", "condition"],
            header_stripped=["filename", "sample_name", "condition"],
            experiment_design=[
                ["filename", "sample_name", "condition"],
                ["f1", "s1", "ctrl"],
            ],
            sm_headers=["sample_name", "condition"],
        )
        assert "validate_upload_inputs" in notes[-1]
