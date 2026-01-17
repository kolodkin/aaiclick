"""
aaiclick.operators - Operator implementations for Object class.

This module contains database-level functions that implement all operators.
Each operator function takes table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from .context import get_context
from .models import Schema, ColumnMeta, QueryInfo, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY


# Operator to SQL expression mapping
OPERATOR_EXPRESSIONS = {
    # Arithmetic
    "+": "a.value + b.value",
    "-": "a.value - b.value",
    "*": "a.value * b.value",
    "/": "a.value / b.value",
    "//": "intDiv(a.value, b.value)",
    "%": "a.value % b.value",
    "**": "power(a.value, b.value)",
    # Comparison
    "==": "a.value = b.value",
    "!=": "a.value != b.value",
    "<": "a.value < b.value",
    "<=": "a.value <= b.value",
    ">": "a.value > b.value",
    ">=": "a.value >= b.value",
    # Bitwise
    "&": "bitAnd(a.value, b.value)",
    "|": "bitOr(a.value, b.value)",
    "^": "bitXor(a.value, b.value)",
}


async def _apply_operator_db(info_a: QueryInfo, info_b: QueryInfo, operator: str, ch_client):
    """
    Apply an operator on two tables at the database level.

    Args:
        info_a: QueryInfo for first operand (contains source and base_table)
        info_b: QueryInfo for second operand (contains source and base_table)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance

    Returns:
        New Object instance pointing to result table
    """
    ctx = get_context()

    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    # Get fieldtype from first table's value column (use base table for metadata)
    fieldtype_query = f"""
    SELECT comment FROM system.columns
    WHERE table = '{info_a.base_table}' AND name = 'value'
    """
    result = await ch_client.query(fieldtype_query)
    fieldtype = FIELDTYPE_SCALAR
    if result.result_rows:
        meta = ColumnMeta.from_yaml(result.result_rows[0][0])
        if meta.fieldtype:
            fieldtype = meta.fieldtype

    # Get value column types from both tables (use base tables for metadata)
    type_query_a = f"""
    SELECT type FROM system.columns
    WHERE table = '{info_a.base_table}' AND name = 'value'
    """
    type_result_a = await ch_client.query(type_query_a)
    type_a = type_result_a.result_rows[0][0] if type_result_a.result_rows else "Float64"

    type_query_b = f"""
    SELECT type FROM system.columns
    WHERE table = '{info_b.base_table}' AND name = 'value'
    """
    type_result_b = await ch_client.query(type_query_b)
    type_b = type_result_b.result_rows[0][0] if type_result_b.result_rows else "Float64"

    # Determine result type: promote to Float64 if mixing integer and float types
    int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
    float_types = {"Float32", "Float64"}

    if (type_a in int_types and type_b in float_types) or (type_a in float_types and type_b in int_types):
        value_type = "Float64"
    elif type_a in float_types or type_b in float_types:
        value_type = "Float64"
    else:
        value_type = type_a

    # Build schema for result table
    schema = Schema(
        fieldtype=fieldtype,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    # Create result object with schema
    result = await ctx.create_object(schema)

    # Insert data based on fieldtype (use sources for data queries)
    if fieldtype == FIELDTYPE_ARRAY:
        # Array operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT a.rn as aai_id, {expression} AS value
        FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {info_a.source}) AS a
        INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {info_b.source}) AS b
        ON a.rn = b.rn
        """
    else:
        # Scalar operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT 1 AS aai_id, {expression} AS value
        FROM {info_a.source} AS a, {info_b.source} AS b
        """

    await ch_client.command(insert_query)

    return result


# Arithmetic Operators

async def add(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Add two sources together at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result of info_a + info_b
    """
    return await _apply_operator_db(info_a, info_b, "+", ch_client)


async def sub(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Subtract one source from another at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result of info_a - info_b
    """
    return await _apply_operator_db(info_a, info_b, "-", ch_client)


async def mul(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "*", ch_client)


async def truediv(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "/", ch_client)


async def floordiv(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "//", ch_client)


async def mod(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "%", ch_client)


async def pow(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "**", ch_client)


# Comparison Operators

async def eq(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "==", ch_client)


async def ne(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "!=", ch_client)


async def lt(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "<", ch_client)


async def le(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "<=", ch_client)


async def gt(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, ">", ch_client)


async def ge(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, ">=", ch_client)


# Bitwise Operators

async def and_(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "&", ch_client)


async def or_(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "|", ch_client)


async def xor(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """
    Apply operator at database level.

    Args:
        info_a: QueryInfo for first operand
        info_b: QueryInfo for second operand
        ch_client: ClickHouse client instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(info_a, info_b, "^", ch_client)
