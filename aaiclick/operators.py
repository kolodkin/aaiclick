"""
aaiclick.operators - Operator implementations for Object class.

This module contains database-level functions that implement all operators.
Each operator function takes table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from .models import Schema, ColumnMeta, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY


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


async def _apply_operator_db(source_a: str, source_b: str, table_a: str, table_b: str, operator: str, ch_client, ctx):
    """
    Apply an operator on two tables at the database level.

    Args:
        source_a: First data source (table name or subquery)
        source_b: Second data source (table name or subquery)
        table_a: First base table name (for metadata queries)
        table_b: Second base table name (for metadata queries)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance
        ctx: Context instance for creating result object

    Returns:
        New Object instance pointing to result table
    """

    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    # Get fieldtype from first table's value column (use base table for metadata)
    fieldtype_query = f"""
    SELECT comment FROM system.columns
    WHERE table = '{table_a}' AND name = 'value'
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
    WHERE table = '{table_a}' AND name = 'value'
    """
    type_result_a = await ch_client.query(type_query_a)
    type_a = type_result_a.result_rows[0][0] if type_result_a.result_rows else "Float64"

    type_query_b = f"""
    SELECT type FROM system.columns
    WHERE table = '{table_b}' AND name = 'value'
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
        FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {source_a}) AS a
        INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {source_b}) AS b
        ON a.rn = b.rn
        """
    else:
        # Scalar operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT 1 AS aai_id, {expression} AS value
        FROM {source_a} AS a, {source_b} AS b
        """

    await ch_client.command(insert_query)

    return result


# Arithmetic Operators

async def add(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Add two sources together at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of source_a + source_b
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "+", ch_client, ctx)


async def sub(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Subtract one source from another at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of source_a - source_b
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "-", ch_client, ctx)


async def mul(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "*", ch_client, ctx)


async def truediv(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "/", ch_client, ctx)


async def floordiv(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "//", ch_client, ctx)


async def mod(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "%", ch_client, ctx)


async def pow(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "**", ch_client, ctx)


# Comparison Operators

async def eq(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "==", ch_client, ctx)


async def ne(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "!=", ch_client, ctx)


async def lt(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "<", ch_client, ctx)


async def le(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "<=", ch_client, ctx)


async def gt(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, ">", ch_client, ctx)


async def ge(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, ">=", ch_client, ctx)


# Bitwise Operators

async def and_(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "&", ch_client, ctx)


async def or_(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "|", ch_client, ctx)


async def xor(source_a: str, source_b: str, table_a: str, table_b: str, ch_client, ctx):
    """
    Apply operator at database level.

    Args:
        source_a: First data source (table or subquery)
        source_b: Second data source (table or subquery)
        table_a: First base table name (for metadata)
        table_b: Second base table name (for metadata)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result
    """
    return await _apply_operator_db(source_a, source_b, table_a, table_b, "^", ch_client, ctx)
