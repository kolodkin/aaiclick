"""Tests for arrow_ingest - pyarrow-based ingest schema evaluation."""

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from aaiclick.data.data_context.arrow_ingest import (
    arrow_table_for_insert,
    infer_struct_array,
    leaf_column_info,
    struct_array_to_columns,
    struct_type_to_columns,
)
from aaiclick.data.models import ColumnInfo


def test_schema_nested_dict():
    """Plain nested dicts map to dot columns with no Array level."""
    arr = infer_struct_array([{"a": 2, "x": {"y": {"z": 1}}}])
    cols = struct_type_to_columns(arr.type)
    assert cols["a"].type == "Int64"
    assert int(cols["a"].array) == 0
    assert cols["x.y.z"].type == "Int64"
    assert int(cols["x.y.z"].array) == 0


def test_schema_star_levels():
    """list-of-dicts adds one Array level; a list leaf inside adds another."""
    arr = infer_struct_array([{"b": [{"c": [1, 2], "d": 5}]}])
    cols = struct_type_to_columns(arr.type)
    assert cols["b.*.c"].type == "Int64"
    assert int(cols["b.*.c"].array) == 2
    assert cols["b.*.d"].type == "Int64"
    assert int(cols["b.*.d"].array) == 1


def test_schema_unifies_across_all_records():
    """Inference scans all records, not just the first."""
    arr = infer_struct_array([{"c": 1}, {"c": 2, "d": 3}])
    cols = struct_type_to_columns(arr.type)
    assert "d" in cols


@pytest.mark.parametrize(
    "records",
    [
        # Strict semantics: a key absent in some records is rejected.
        pytest.param([{"c": 1}, {"c": 2, "d": 3}], id="missing-key-across-records"),
        # Strictness applies inside list-of-dicts items too (silent-drop fix).
        pytest.param([{"b": [{"c": 1}, {"c": 2, "d": 3}]}], id="missing-key-inside-list-items"),
        # A dict field that is None in some records is rejected.
        pytest.param([{"m": {"c": 1}}, {"m": None}], id="none-dict-value"),
    ],
)
def test_struct_array_to_columns_non_identical_keys_raises(records):
    arr = infer_struct_array(records)
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_extraction_parallel_arrays():
    """Leaves come out as dot-star parallel arrow arrays with per-row grouping."""
    arr = infer_struct_array([{"a": 1, "b": [{"c": 10}, {"c": 20}]}, {"a": 2, "b": []}])
    cols = struct_array_to_columns(arr)
    assert cols["a"].to_pylist() == [1, 2]
    assert cols["b.*.c"].to_pylist() == [[10, 20], []]


def test_extraction_deep_mixed_nesting():
    """dict inside list items and list inside dict both extract correctly."""
    arr = infer_struct_array([{"x": {"w": 1, "y": [{"z": 1}, {"z": 2}]}}])
    cols = struct_array_to_columns(arr)
    assert cols["x.w"].to_pylist() == [1]
    assert cols["x.y.*.z"].to_pylist() == [[1, 2]]


@pytest.mark.parametrize(
    "records",
    [
        pytest.param([{"b": [{"c": 1}, 5]}], id="non-dict-item"),
        pytest.param([{"a": 1}, {"a": "s"}], id="type-conflict"),
    ],
)
def test_infer_struct_array_non_uniform_schema_raises(records):
    with pytest.raises(ValueError, match="uniform schema"):
        infer_struct_array(records)


@pytest.mark.parametrize(
    "records, match",
    [
        pytest.param([{"a.b": 1}], "must not contain", id="dotted-field-name"),
        pytest.param([{"x": {"a.b": 1}}], "must not contain", id="nested-dotted-field-name"),
        pytest.param([{"x": {}}], "Empty dict", id="empty-struct"),
    ],
)
def test_struct_type_to_columns_raises(records, match):
    arr = infer_struct_array(records)
    with pytest.raises(ValueError, match=match):
        struct_type_to_columns(arr.type)


def test_empty_list_falls_back_to_string_array():
    arr = infer_struct_array([{"b": []}])
    cols = struct_type_to_columns(arr.type)
    assert cols["b"].type == "String"
    assert int(cols["b"].array) == 1
    assert cols["b"].nullable is True


def test_leaf_column_info_mapping():
    assert leaf_column_info(pa.int64()).type == "Int64"
    assert leaf_column_info(pa.float64()).type == "Float64"
    assert leaf_column_info(pa.bool_()).type == "Bool"
    assert leaf_column_info(pa.string()).type == "String"
    assert leaf_column_info(pa.timestamp("us")).type == "DateTime64(3, 'UTC')"
    null_info = leaf_column_info(pa.null())
    assert null_info.type == "String"
    assert null_info.nullable is True
    assert leaf_column_info(pa.int64()).nullable is False
    assert int(leaf_column_info(pa.int64(), array_depth=2).array) == 2


def test_list_of_scalars_extraction():
    """Plain list fields extract as per-row lists."""
    arr = infer_struct_array([{"tags": [1, 2]}, {"tags": [3]}])
    cols = struct_array_to_columns(arr)
    assert cols["tags"].to_pylist() == [[1, 2], [3]]


def test_nested_list_of_scalars_extraction():
    """list<list<T>> extracts with both levels intact."""
    arr = infer_struct_array([{"m": [[1, 2], [3]]}])
    cols = struct_array_to_columns(arr)
    assert cols["m"].to_pylist() == [[[1, 2], [3]]]


def test_intermediate_list_null_raises():
    """A None at an intermediate list level is rejected, not embedded."""
    arr = infer_struct_array([{"m": [[1, 2], None, [3]]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_scalar_null_inside_list_raises():
    """A None element inside a typed list is rejected."""
    arr = infer_struct_array([{"tags": [1, None]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_none_item_in_list_of_dicts_raises():
    """An explicit None item inside a list of dicts is rejected, not fabricated."""
    arr = infer_struct_array([{"b": [{"c": 1}, None]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_none_item_with_all_null_leaf_raises():
    """A None item is rejected even when the only leaf is itself all-null."""
    arr = infer_struct_array([{"b": [{"c": None}, None]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_arrow_table_matching_types_pass_through():
    """Columns whose arrow type already matches the target are not copied."""
    col_map = {"a": pa.array([1, 2]), "s": pa.array(["x", "y"])}
    columns = {"a": ColumnInfo("Int64"), "s": ColumnInfo("String")}
    tbl = arrow_table_for_insert(col_map, columns)
    assert tbl.column_names == ["a", "s"]
    assert tbl.column("a").type == pa.int64()
    assert tbl.column("a").to_pylist() == [1, 2]


def test_arrow_table_casts_type_override():
    """A FieldSpec-style type override casts the arrow leaf to the target type."""
    col_map = {"a": pa.array([1, 2])}
    tbl = arrow_table_for_insert(col_map, {"a": ColumnInfo("Int32")})
    assert tbl.column("a").type == pa.int32()
    assert tbl.column("a").to_pylist() == [1, 2]


def test_arrow_table_truncates_timestamps_to_ms():
    """us-precision arrow timestamps truncate to the DateTime64(3) target."""
    dt = datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
    col_map = {"t": pa.array([dt])}
    tbl = arrow_table_for_insert(col_map, {"t": ColumnInfo("DateTime64(3, 'UTC')")})
    assert tbl.column("t").type == pa.timestamp("ms", tz="UTC")
    assert tbl.column("t").to_pylist() == [dt.replace(microsecond=123000)]


def test_arrow_table_null_leaf_to_nullable_string():
    """All-null leaves (arrow null type) become nullable string columns."""
    col_map = {"n": pa.array([None, None])}
    tbl = arrow_table_for_insert(col_map, {"n": ColumnInfo("String", nullable=True)})
    assert tbl.column("n").type == pa.string()
    assert tbl.column("n").to_pylist() == [None, None]


def test_arrow_table_builds_python_lists():
    """Plain Python list values are constructed directly at the target type."""
    col_map = {"v": [1.5, 2.5], "tags": [["a"], ["b", "c"]]}
    columns = {"v": ColumnInfo("Float64"), "tags": ColumnInfo("String", array=1)}
    tbl = arrow_table_for_insert(col_map, columns)
    assert tbl.column("v").type == pa.float64()
    assert tbl.column("tags").type == pa.list_(pa.string())
    assert tbl.column("tags").to_pylist() == [["a"], ["b", "c"]]


def test_arrow_table_incompatible_cast_raises():
    """Lossy non-timestamp casts (e.g. float -> Int64) are rejected."""
    col_map = {"a": pa.array([1.5])}
    with pytest.raises(pa.ArrowInvalid):
        arrow_table_for_insert(col_map, {"a": ColumnInfo("Int64")})
