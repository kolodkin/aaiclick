"""Tests for arrow_ingest - pyarrow-based ingest schema evaluation."""

import pyarrow as pa
import pytest

from aaiclick.data.data_context.arrow_ingest import (
    infer_struct_array,
    leaf_column_info,
    struct_array_to_columns,
    struct_type_to_columns,
)


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


def test_missing_key_across_records_raises():
    """Strict semantics: a key absent in some records is rejected."""
    arr = infer_struct_array([{"c": 1}, {"c": 2, "d": 3}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_missing_key_inside_list_items_raises():
    """Strictness applies inside list-of-dicts items too (silent-drop fix)."""
    arr = infer_struct_array([{"b": [{"c": 1}, {"c": 2, "d": 3}]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_extraction_parallel_arrays():
    """Leaves come out as dot-star parallel arrays with per-row grouping."""
    arr = infer_struct_array([{"a": 1, "b": [{"c": 10}, {"c": 20}]}, {"a": 2, "b": []}])
    cols = struct_array_to_columns(arr)
    assert cols["a"] == [1, 2]
    assert cols["b.*.c"] == [[10, 20], []]


def test_extraction_deep_mixed_nesting():
    """dict inside list items and list inside dict both extract correctly."""
    arr = infer_struct_array([{"x": {"w": 1, "y": [{"z": 1}, {"z": 2}]}}])
    cols = struct_array_to_columns(arr)
    assert cols["x.w"] == [1]
    assert cols["x.y.*.z"] == [[1, 2]]


def test_non_dict_item_raises():
    with pytest.raises(ValueError, match="uniform schema"):
        infer_struct_array([{"b": [{"c": 1}, 5]}])


def test_type_conflict_raises():
    with pytest.raises(ValueError, match="uniform schema"):
        infer_struct_array([{"a": 1}, {"a": "s"}])


def test_dotted_field_name_raises():
    arr = infer_struct_array([{"a.b": 1}])
    with pytest.raises(ValueError, match="must not contain"):
        struct_type_to_columns(arr.type)


def test_nested_dotted_field_name_raises():
    arr = infer_struct_array([{"x": {"a.b": 1}}])
    with pytest.raises(ValueError, match="must not contain"):
        struct_type_to_columns(arr.type)


def test_empty_struct_raises():
    arr = infer_struct_array([{"x": {}}])
    with pytest.raises(ValueError, match="Empty dict"):
        struct_type_to_columns(arr.type)


def test_empty_list_falls_back_to_string_array():
    arr = infer_struct_array([{"b": []}])
    cols = struct_type_to_columns(arr.type)
    assert cols["b"].type == "String"
    assert int(cols["b"].array) == 1


def test_none_dict_value_raises():
    """A record where a dict field is None in some records is rejected."""
    arr = infer_struct_array([{"m": {"c": 1}}, {"m": None}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_leaf_column_info_mapping():
    assert leaf_column_info(pa.int64()).type == "Int64"
    assert leaf_column_info(pa.float64()).type == "Float64"
    assert leaf_column_info(pa.bool_()).type == "Bool"
    assert leaf_column_info(pa.string()).type == "String"
    assert leaf_column_info(pa.timestamp("us")).type == "DateTime64(3, 'UTC')"
    assert leaf_column_info(pa.null()).type == "String"
    assert int(leaf_column_info(pa.int64(), array_depth=2).array) == 2


def test_list_of_scalars_extraction():
    """Plain list fields extract as per-row lists."""
    arr = infer_struct_array([{"tags": [1, 2]}, {"tags": [3]}])
    cols = struct_array_to_columns(arr)
    assert cols["tags"] == [[1, 2], [3]]


def test_nested_list_of_scalars_extraction():
    """list<list<T>> extracts with both levels intact."""
    arr = infer_struct_array([{"m": [[1, 2], [3]]}])
    cols = struct_array_to_columns(arr)
    assert cols["m"] == [[[1, 2], [3]]]


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
