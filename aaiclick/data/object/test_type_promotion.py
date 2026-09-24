"""Tests that our hardcoded type promotion matches ClickHouse behavior."""

import pytest

from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.object.schema_compute import _determine_agg_result_type, _result_value_type


@pytest.fixture
async def ctx():
    async with data_context():
        yield


# (operator, equivalent ClickHouse function, left type, right type)
OPERATOR_CASES = [
    pytest.param("==", "equals", "Int64", "Int64", id="==-Int64-Int64"),
    pytest.param("!=", "notEquals", "Float64", "Float64", id="!=-Float64-Float64"),
    pytest.param("<", "less", "Int64", "Float64", id="<-Int64-Float64"),
    pytest.param("<=", "lessOrEquals", "Bool", "Bool", id="<=-Bool-Bool"),
    pytest.param(">", "greater", "String", "String", id=">-String-String"),
    pytest.param(">=", "greaterOrEquals", "UInt8", "Int64", id=">=-UInt8-Int64"),
    pytest.param("+", "plus", "Bool", "Bool", id="+-Bool-Bool"),
    pytest.param("+", "plus", "Int64", "Int64", id="+-Int64-Int64"),
    pytest.param("+", "plus", "Float64", "Float64", id="+-Float64-Float64"),
    pytest.param("+", "plus", "UInt8", "UInt8", id="+-UInt8-UInt8"),
    pytest.param("+", "plus", "UInt64", "UInt64", id="+-UInt64-UInt64"),
    pytest.param("+", "plus", "Bool", "Int64", id="+-Bool-Int64"),
    pytest.param("+", "plus", "Bool", "Float64", id="+-Bool-Float64"),
    pytest.param("+", "plus", "Int64", "Float64", id="+-Int64-Float64"),
    pytest.param("-", "minus", "Bool", "Bool", id="--Bool-Bool"),
    pytest.param("-", "minus", "Int64", "Int64", id="--Int64-Int64"),
    pytest.param("-", "minus", "UInt8", "UInt8", id="--UInt8-UInt8"),
    pytest.param("-", "minus", "UInt64", "UInt64", id="--UInt64-UInt64"),
    pytest.param("-", "minus", "Bool", "Int64", id="--Bool-Int64"),
    pytest.param("-", "minus", "Bool", "Float64", id="--Bool-Float64"),
    pytest.param("*", "multiply", "Bool", "Bool", id="*-Bool-Bool"),
    pytest.param("*", "multiply", "Int64", "Int64", id="*-Int64-Int64"),
    pytest.param("*", "multiply", "UInt8", "UInt8", id="*-UInt8-UInt8"),
    pytest.param("*", "multiply", "Bool", "Int64", id="*-Bool-Int64"),
    pytest.param("*", "multiply", "Bool", "Float64", id="*-Bool-Float64"),
    pytest.param("/", "divide", "Bool", "Bool", id="/-Bool-Bool"),
    pytest.param("/", "divide", "Int64", "Int64", id="/-Int64-Int64"),
    pytest.param("/", "divide", "Float64", "Float64", id="/-Float64-Float64"),
    pytest.param("/", "divide", "Bool", "Int64", id="/-Bool-Int64"),
    pytest.param("**", "power", "Int64", "Int64", id="**-Int64-Int64"),
    pytest.param("**", "power", "Bool", "Bool", id="**-Bool-Bool"),
    pytest.param("%", "modulo", "Int64", "Int64", id="%-Int64-Int64"),
    pytest.param("%", "modulo", "UInt8", "UInt8", id="%-UInt8-UInt8"),
    pytest.param("%", "modulo", "Int32", "UInt8", id="%-Int32-UInt8"),
    pytest.param("%", "modulo", "Int64", "Float64", id="%-Int64-Float64"),
    # Narrow and mixed-signedness integers widen — the result must not fall
    # through to the left operand's type.
    pytest.param("+", "plus", "Int32", "Int32", id="+-Int32-Int32"),
    pytest.param("*", "multiply", "Int32", "Int32", id="*-Int32-Int32"),
    pytest.param("+", "plus", "Int8", "Int8", id="+-Int8-Int8"),
    pytest.param("+", "plus", "UInt16", "UInt16", id="+-UInt16-UInt16"),
    pytest.param("+", "plus", "UInt32", "Int8", id="+-UInt32-Int8"),
    pytest.param("+", "plus", "UInt64", "Int64", id="+-UInt64-Int64"),
    pytest.param("*", "multiply", "Int64", "UInt64", id="*-Int64-UInt64"),
    pytest.param("-", "minus", "Int32", "Int32", id="--Int32-Int32"),
    pytest.param("-", "minus", "UInt32", "UInt32", id="--UInt32-UInt32"),
    pytest.param("-", "minus", "Int8", "UInt64", id="--Int8-UInt64"),
    pytest.param("+", "plus", "Float32", "Float32", id="+-Float32-Float32"),
    pytest.param("+", "plus", "Int32", "Float32", id="+-Int32-Float32"),
]


@pytest.mark.parametrize("op,ch_func,type_a,type_b", OPERATOR_CASES)
async def test_operator_result_type(ctx, op, ch_func, type_a, type_b):
    """Verify _result_value_type matches ClickHouse toTypeName().

    Pure function: the returned type name is the contract, pinned against the engine.
    """
    ch = get_ch_client()
    result = await ch.query(f"SELECT toTypeName({ch_func}(CAST(1, '{type_a}'), CAST(1, '{type_b}')))")
    ch_type = result.result_rows[0][0]

    our_type = _result_value_type(op, type_a, type_b)
    assert our_type == ch_type, f"{type_a} {op} {type_b}: ours={our_type}, CH={ch_type}"


# (aggregation, equivalent ClickHouse function, source type)
AGG_CASES = [
    pytest.param("min", "min", "Bool", id="min-Bool"),
    pytest.param("max", "max", "Bool", id="max-Bool"),
    pytest.param("sum", "sum", "Bool", id="sum-Bool"),
    pytest.param("min", "min", "Int64", id="min-Int64"),
    pytest.param("max", "max", "Int64", id="max-Int64"),
    pytest.param("sum", "sum", "Int64", id="sum-Int64"),
    pytest.param("sum", "sum", "Float64", id="sum-Float64"),
    pytest.param("sum", "sum", "Float32", id="sum-Float32"),
    # sum() widens every narrow integer to the 64-bit type of its signedness.
    pytest.param("sum", "sum", "UInt8", id="sum-UInt8"),
    pytest.param("sum", "sum", "UInt16", id="sum-UInt16"),
    pytest.param("sum", "sum", "UInt32", id="sum-UInt32"),
    pytest.param("sum", "sum", "UInt64", id="sum-UInt64"),
    pytest.param("sum", "sum", "Int8", id="sum-Int8"),
    pytest.param("sum", "sum", "Int16", id="sum-Int16"),
    pytest.param("sum", "sum", "Int32", id="sum-Int32"),
    pytest.param("mean", "avg", "Int32", id="mean-Int32"),
    pytest.param("std", "stddevPop", "UInt8", id="std-UInt8"),
    pytest.param("var", "varPop", "UInt8", id="var-UInt8"),
    pytest.param("count", "count", "Int64", id="count-Int64"),
    pytest.param("count", "count", "Bool", id="count-Bool"),
]


@pytest.mark.parametrize("agg_func,ch_func,source_type", AGG_CASES)
async def test_agg_type_promotion(ctx, agg_func, ch_func, source_type):
    """Verify _determine_agg_result_type matches ClickHouse toTypeName().

    Pure function: the returned type name is the contract, pinned against the engine.
    """
    ch = get_ch_client()
    result = await ch.query(f"SELECT toTypeName({ch_func}(x)) FROM (SELECT CAST(1, '{source_type}') AS x)")
    ch_type = result.result_rows[0][0]

    our_type = _determine_agg_result_type(agg_func, source_type)
    assert our_type == ch_type, f"{agg_func}({source_type}): ours={our_type}, CH={ch_type}"
