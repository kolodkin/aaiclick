"""
aaiclick.data.operators - Operator implementations for Object class.

This module contains database-level functions that implement all operators.
Each operator function takes table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from .data_context import create_object
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
    result = await create_object(schema)

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


# Aggregation functions mapping
AGGREGATION_FUNCTIONS = {
    "min": "min",
    "max": "max",
    "sum": "sum",
    "mean": "avg",
    "std": "stddevPop",
}


async def _apply_aggregation_db(info: QueryInfo, agg_func: str, ch_client):
    """
    Apply an aggregation function on a table at the database level.

    Creates a new Object with a scalar result containing the aggregated value.
    All computation happens within ClickHouse - no data round-trips to Python.

    Args:
        info: QueryInfo for the source (contains source and base_table)
        agg_func: Aggregation function key (e.g., 'min', 'max', 'sum', 'mean', 'std')
        ch_client: ClickHouse client instance

    Returns:
        New Object instance pointing to result table (scalar type)
    """
    from ..snowflake_id import get_snowflake_id

    # Get SQL function from aggregation mapping
    sql_func = AGGREGATION_FUNCTIONS[agg_func]

    # Get value column type from source table (use base table for metadata)
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{info.base_table}' AND name = 'value'
    """
    type_result = await ch_client.query(type_query)
    source_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"

    # Determine result type based on aggregation function
    # - min/max preserve the source type
    # - sum preserves integer types, promotes to Float64 for float types
    # - mean/std always return Float64
    if agg_func in ("min", "max"):
        value_type = source_type
    elif agg_func == "sum":
        int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
        value_type = source_type if source_type in int_types else "Float64"
    else:
        # mean and std always return Float64
        value_type = "Float64"

    # Build schema for result table (scalar type)
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Generate a snowflake ID for the scalar result
    aai_id = get_snowflake_id()

    # Insert aggregated data
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT {aai_id} AS aai_id, {sql_func}(value) AS value
    FROM {info.source}
    """
    await ch_client.command(insert_query)

    return result


# Aggregation Operators

async def min_agg(info: QueryInfo, ch_client):
    """
    Calculate minimum value at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar minimum value
    """
    return await _apply_aggregation_db(info, "min", ch_client)


async def max_agg(info: QueryInfo, ch_client):
    """
    Calculate maximum value at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar maximum value
    """
    return await _apply_aggregation_db(info, "max", ch_client)


async def sum_agg(info: QueryInfo, ch_client):
    """
    Calculate sum at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar sum value
    """
    return await _apply_aggregation_db(info, "sum", ch_client)


async def mean_agg(info: QueryInfo, ch_client):
    """
    Calculate mean (average) at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar mean value
    """
    return await _apply_aggregation_db(info, "mean", ch_client)


async def std_agg(info: QueryInfo, ch_client):
    """
    Calculate standard deviation (population) at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar standard deviation value
    """
    return await _apply_aggregation_db(info, "std", ch_client)
