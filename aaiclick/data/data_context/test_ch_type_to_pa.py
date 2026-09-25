"""Tests for ch_type_to_pa ClickHouse→PyArrow type mapping."""

import pyarrow as pa
import pytest

from aaiclick.data.data_context.arrow_types import ch_type_to_pa


@pytest.mark.parametrize(
    "ch_type, expected",
    [
        pytest.param("Bool", pa.bool_(), id="bool"),
        pytest.param("UInt8", pa.uint8(), id="uint8"),
        pytest.param("UInt16", pa.uint16(), id="uint16"),
        pytest.param("UInt32", pa.uint32(), id="uint32"),
        pytest.param("UInt64", pa.uint64(), id="uint64"),
        pytest.param("Int8", pa.int8(), id="int8"),
        pytest.param("Int16", pa.int16(), id="int16"),
        pytest.param("Int32", pa.int32(), id="int32"),
        pytest.param("Int64", pa.int64(), id="int64"),
        pytest.param("Float32", pa.float32(), id="float32"),
        pytest.param("Float64", pa.float64(), id="float64"),
        pytest.param("String", pa.string(), id="string"),
        pytest.param("DateTime64(3, 'UTC')", pa.timestamp("ms", tz="UTC"), id="datetime64-utc"),
        pytest.param("DateTime64(6)", pa.timestamp("ms", tz="UTC"), id="datetime64-no-tz"),
        # Nullability is not part of the arrow type: the wrapper is stripped.
        pytest.param("Nullable(Int64)", pa.int64(), id="nullable-int64"),
        pytest.param("Nullable(String)", pa.string(), id="nullable-string"),
        pytest.param("Nullable(UInt64)", pa.uint64(), id="nullable-uint64"),
        pytest.param("LowCardinality(String)", pa.string(), id="low-cardinality"),
        pytest.param("LowCardinality(Nullable(String))", pa.string(), id="low-cardinality-nullable"),
        pytest.param("Array(Int64)", pa.list_(pa.int64()), id="array-int64"),
        pytest.param("Array(String)", pa.list_(pa.string()), id="array-string"),
        pytest.param("Array(Array(Int64))", pa.list_(pa.list_(pa.int64())), id="array-of-array"),
        pytest.param("Array(Nullable(Float64))", pa.list_(pa.float64()), id="array-of-nullable"),
        pytest.param("Map(String, String)", pa.map_(pa.string(), pa.string()), id="map-string-string"),
        pytest.param("Map(String, Int64)", pa.map_(pa.string(), pa.int64()), id="map-string-int64"),
        pytest.param("Map(String, Array(Int64))", pa.map_(pa.string(), pa.list_(pa.int64())), id="map-of-array"),
        pytest.param(
            "Map(String, Map(String, Int64))",
            pa.map_(pa.string(), pa.map_(pa.string(), pa.int64())),
            id="map-of-map",
        ),
        pytest.param(
            "Tuple(Int64, String)",
            pa.struct([("f0", pa.int64()), ("f1", pa.string())]),
            id="tuple",
        ),
        pytest.param(
            "Tuple(Int64, Array(String))",
            pa.struct([("f0", pa.int64()), ("f1", pa.list_(pa.string()))]),
            id="tuple-with-array",
        ),
        pytest.param("SomeUnknownType", pa.string(), id="unknown-falls-back-to-string"),
    ],
)
def test_ch_type_to_pa(ch_type, expected):
    """Pure function: the returned arrow type is the contract."""
    assert ch_type_to_pa(ch_type) == expected
