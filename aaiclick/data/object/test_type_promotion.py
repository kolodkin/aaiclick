"""Tests that our hardcoded type promotion matches ClickHouse behavior."""

import pytest

from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.object.operators import _promote_arithmetic_type, _determine_agg_result_type


@pytest.fixture
async def ctx():
    async with data_context():
        yield


ARITHMETIC_CASES = [
    ("+", "Bool", "Bool"),
    ("+", "Int64", "Int64"),
    ("+", "Float64", "Float64"),
    ("+", "UInt8", "UInt8"),
    ("+", "UInt64", "UInt64"),
    ("+", "Bool", "Int64"),
    ("+", "Bool", "Float64"),
    ("+", "Int64", "Float64"),
    ("-", "Bool", "Bool"),
    ("-", "Int64", "Int64"),
    ("-", "UInt8", "UInt8"),
    ("-", "UInt64", "UInt64"),
    ("-", "Bool", "Int64"),
    ("-", "Bool", "Float64"),
    ("*", "Bool", "Bool"),
    ("*", "Int64", "Int64"),
    ("*", "UInt8", "UInt8"),
    ("*", "Bool", "Int64"),
    ("*", "Bool", "Float64"),
    ("/", "Bool", "Bool"),
    ("/", "Int64", "Int64"),
    ("/", "Float64", "Float64"),
    ("/", "Bool", "Int64"),
    ("**", "Int64", "Int64"),
    ("**", "Bool", "Bool"),
]


@pytest.mark.parametrize("op,type_a,type_b", ARITHMETIC_CASES)
async def test_arithmetic_type_promotion(ctx, op, type_a, type_b):
    """Verify _promote_arithmetic_type matches ClickHouse toTypeName()."""
    ch = get_ch_client()
    if op == "**":
        expr = f"power(CAST(1, '{type_a}'), CAST(1, '{type_b}'))"
    else:
        expr = f"CAST(1, '{type_a}') {op} CAST(1, '{type_b}')"
    result = await ch.query(f"SELECT toTypeName({expr})")
    ch_type = result.result_rows[0][0]

    our_type = _promote_arithmetic_type(op, type_a, type_b)
    assert our_type == ch_type, f"{type_a} {op} {type_b}: ours={our_type}, CH={ch_type}"


AGG_CASES = [
    ("min", "Bool"),
    ("max", "Bool"),
    ("sum", "Bool"),
    ("min", "Int64"),
    ("max", "Int64"),
    ("sum", "Int64"),
    ("sum", "Float64"),
    ("count", "Int64"),
    ("count", "Bool"),
]


@pytest.mark.parametrize("agg_func,source_type", AGG_CASES)
async def test_agg_type_promotion(ctx, agg_func, source_type):
    """Verify _determine_agg_result_type matches ClickHouse toTypeName()."""
    ch = get_ch_client()
    ch_func = {"mean": "avg", "std": "stddevPop", "var": "varPop"}.get(agg_func, agg_func)
    result = await ch.query(
        f"SELECT toTypeName({ch_func}(x)) FROM (SELECT CAST(1, '{source_type}') AS x)"
    )
    ch_type = result.result_rows[0][0]

    our_type = _determine_agg_result_type(agg_func, source_type)
    assert our_type == ch_type, f"{agg_func}({source_type}): ours={our_type}, CH={ch_type}"
