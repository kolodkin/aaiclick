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


async def _apply_operator_db(table_a: str, table_b: str, operator: str, ch_client, ctx):
    """
    Apply an operator on two tables at the database level.

    Args:
        table_a: First table name
        table_b: Second table name
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance
        ctx: Context instance for creating result object

    Returns:
        New Object instance pointing to result table
    """

    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    # Get fieldtype from first table's value column
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

    # Get value column types from both tables
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

    # Insert data based on fieldtype
    if fieldtype == FIELDTYPE_ARRAY:
        # Array operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT a.rn as aai_id, {expression} AS value
        FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {table_a}) AS a
        INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {table_b}) AS b
        ON a.rn = b.rn
        """
    else:
        # Scalar operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT 1 AS aai_id, {expression} AS value
        FROM {table_a} AS a, {table_b} AS b
        """

    await ch_client.command(insert_query)

    return result


# Arithmetic Operators

async def add(table_a: str, table_b: str, ch_client, ctx):
    """
    Add two tables together at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a + table_b
    """
    return await _apply_operator_db(table_a, table_b, "+", ch_client, ctx)


async def sub(table_a: str, table_b: str, ch_client, ctx):
    """
    Subtract one table from another at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a - table_b
    """
    return await _apply_operator_db(table_a, table_b, "-", ch_client, ctx)


async def mul(table_a: str, table_b: str, ch_client, ctx):
    """
    Multiply two tables together at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a * table_b
    """
    return await _apply_operator_db(table_a, table_b, "*", ch_client, ctx)


async def truediv(table_a: str, table_b: str, ch_client, ctx):
    """
    Divide one table by another at database level.

    Args:
        table_a: First table name (numerator)
        table_b: Second table name (denominator)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a / table_b
    """
    return await _apply_operator_db(table_a, table_b, "/", ch_client, ctx)


async def floordiv(table_a: str, table_b: str, ch_client, ctx):
    """
    Floor divide one table by another at database level.

    Args:
        table_a: First table name (numerator)
        table_b: Second table name (denominator)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a // table_b
    """
    return await _apply_operator_db(table_a, table_b, "//", ch_client, ctx)


async def mod(table_a: str, table_b: str, ch_client, ctx):
    """
    Modulo operation between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a % table_b
    """
    return await _apply_operator_db(table_a, table_b, "%", ch_client, ctx)


async def pow(table_a: str, table_b: str, ch_client, ctx):
    """
    Raise one table to the power of another at database level.

    Args:
        table_a: First table name (base)
        table_b: Second table name (exponent)
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a ** table_b
    """
    return await _apply_operator_db(table_a, table_b, "**", ch_client, ctx)


# Comparison Operators

async def eq(table_a: str, table_b: str, ch_client, ctx):
    """
    Check equality between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a == table_b
    """
    return await _apply_operator_db(table_a, table_b, "==", ch_client, ctx)


async def ne(table_a: str, table_b: str, ch_client, ctx):
    """
    Check inequality between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a != table_b
    """
    return await _apply_operator_db(table_a, table_b, "!=", ch_client, ctx)


async def lt(table_a: str, table_b: str, ch_client, ctx):
    """
    Check if one table is less than another at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a < table_b
    """
    return await _apply_operator_db(table_a, table_b, "<", ch_client, ctx)


async def le(table_a: str, table_b: str, ch_client, ctx):
    """
    Check if one table is less than or equal to another at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a <= table_b
    """
    return await _apply_operator_db(table_a, table_b, "<=", ch_client, ctx)


async def gt(table_a: str, table_b: str, ch_client, ctx):
    """
    Check if one table is greater than another at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a > table_b
    """
    return await _apply_operator_db(table_a, table_b, ">", ch_client, ctx)


async def ge(table_a: str, table_b: str, ch_client, ctx):
    """
    Check if one table is greater than or equal to another at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with boolean result of table_a >= table_b
    """
    return await _apply_operator_db(table_a, table_b, ">=", ch_client, ctx)


# Bitwise Operators

async def and_(table_a: str, table_b: str, ch_client, ctx):
    """
    Bitwise AND operation between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a & table_b
    """
    return await _apply_operator_db(table_a, table_b, "&", ch_client, ctx)


async def or_(table_a: str, table_b: str, ch_client, ctx):
    """
    Bitwise OR operation between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a | table_b
    """
    return await _apply_operator_db(table_a, table_b, "|", ch_client, ctx)


async def xor(table_a: str, table_b: str, ch_client, ctx):
    """
    Bitwise XOR operation between two tables at database level.

    Args:
        table_a: First table name
        table_b: Second table name
        ch_client: ClickHouse client instance
        ctx: Context instance

    Returns:
        New Object with result of table_a ^ table_b
    """
    return await _apply_operator_db(table_a, table_b, "^", ch_client, ctx)
