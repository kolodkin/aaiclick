"""Tests for _ch_type_to_pa ClickHouse→PyArrow type mapping."""

import pyarrow as pa
import pytest

from aaiclick.data.data_context.chdb_client import _ch_type_to_pa


@pytest.mark.parametrize("ch_type,expected", [
    ("Bool", pa.bool_()),
    ("UInt8", pa.uint8()),
    ("UInt16", pa.uint16()),
    ("UInt32", pa.uint32()),
    ("UInt64", pa.uint64()),
    ("Int8", pa.int8()),
    ("Int16", pa.int16()),
    ("Int32", pa.int32()),
    ("Int64", pa.int64()),
    ("Float32", pa.float32()),
    ("Float64", pa.float64()),
    ("String", pa.string()),
    ("DateTime64(3, 'UTC')", pa.timestamp("ms", tz="UTC")),
    ("DateTime64(6)", pa.timestamp("ms", tz="UTC")),
])
def test_base_types(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


@pytest.mark.parametrize("ch_type,expected", [
    ("Nullable(Int64)", pa.int64()),
    ("Nullable(String)", pa.string()),
    ("Nullable(UInt64)", pa.uint64()),
])
def test_nullable(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


@pytest.mark.parametrize("ch_type,expected", [
    ("LowCardinality(String)", pa.string()),
    ("LowCardinality(Nullable(String))", pa.string()),
])
def test_low_cardinality(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


@pytest.mark.parametrize("ch_type,expected", [
    ("Array(Int64)", pa.list_(pa.int64())),
    ("Array(String)", pa.list_(pa.string())),
    ("Array(Array(Int64))", pa.list_(pa.list_(pa.int64()))),
    ("Array(Nullable(Float64))", pa.list_(pa.float64())),
])
def test_array(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


@pytest.mark.parametrize("ch_type,expected", [
    ("Map(String, String)", pa.map_(pa.string(), pa.string())),
    ("Map(String, Int64)", pa.map_(pa.string(), pa.int64())),
    ("Map(String, Array(Int64))", pa.map_(pa.string(), pa.list_(pa.int64()))),
    ("Map(String, Map(String, Int64))", pa.map_(pa.string(), pa.map_(pa.string(), pa.int64()))),
])
def test_map(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


@pytest.mark.parametrize("ch_type,expected", [
    ("Tuple(Int64, String)", pa.struct([("f0", pa.int64()), ("f1", pa.string())])),
    ("Tuple(Int64, Array(String))", pa.struct([("f0", pa.int64()), ("f1", pa.list_(pa.string()))])),
])
def test_tuple(ch_type, expected):
    assert _ch_type_to_pa(ch_type) == expected


def test_unknown_falls_back_to_string():
    assert _ch_type_to_pa("SomeUnknownType") == pa.string()
