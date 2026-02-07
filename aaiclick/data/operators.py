"""
aaiclick.data.operators - Operator implementations for Object class.

This module contains database-level functions that implement all operators.
Each operator function takes table names and ch_client instead of Object instances.

ClickHouse Reference Documentation
==================================

Operator to ClickHouse function/operator mapping with official documentation links:

+----------+------------------------+---------------------------------------------------------------+
| Python   | ClickHouse             | Reference                                                     |
+----------+------------------------+---------------------------------------------------------------+
| +        | +                      | https://clickhouse.com/docs/sql-reference/operators#plus     |
| -        | -                      | https://clickhouse.com/docs/sql-reference/operators#minus    |
| *        | *                      | https://clickhouse.com/docs/sql-reference/operators#multiply |
| /        | /                      | https://clickhouse.com/docs/sql-reference/operators#divide   |
| //       | intDiv(a, b)           | https://clickhouse.com/docs/sql-reference/functions/arithmetic-functions#intdiva-b |
| %        | %                      | https://clickhouse.com/docs/sql-reference/operators#modulo   |
| **       | power(a, b)            | https://clickhouse.com/docs/sql-reference/functions/math-functions#pow |
| ==       | =                      | https://clickhouse.com/docs/sql-reference/operators#equals   |
| !=       | !=                     | https://clickhouse.com/docs/sql-reference/operators#not-equals |
| <        | <                      | https://clickhouse.com/docs/sql-reference/operators#less     |
| <=       | <=                     | https://clickhouse.com/docs/sql-reference/operators#less-or-equals |
| >        | >                      | https://clickhouse.com/docs/sql-reference/operators#greater  |
| >=       | >=                     | https://clickhouse.com/docs/sql-reference/operators#greater-or-equals |
| &        | bitAnd(a, b)           | https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitanda-b |
| |        | bitOr(a, b)            | https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitora-b |
| ^        | bitXor(a, b)           | https://clickhouse.com/docs/sql-reference/functions/bit-functions#bitxora-b |
+----------+------------------------+---------------------------------------------------------------+

Aggregation Functions (reduce to scalar):
+------------+------------------------+---------------------------------------------------------------+
| Python     | ClickHouse             | Reference                                                     |
+------------+------------------------+---------------------------------------------------------------+
| min()      | min()                  | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/min |
| max()      | max()                  | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/max |
| sum()      | sum()                  | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/sum |
| mean()     | avg()                  | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/avg |
| std()      | stddevPop()            | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/stddevpop |
| var()      | varPop()               | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/varpop |
| count()    | count()                | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/count |
| quantile(q)| quantile(q)()          | https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantile |
+------------+------------------------+---------------------------------------------------------------+

Window Functions (used for element-wise array operations):
- row_number(): https://clickhouse.com/docs/sql-reference/window-functions#row_number

Set Operations (for unique values):
- GROUP BY: https://clickhouse.com/docs/sql-reference/statements/select/group-by

Memory/Disk Management (for large datasets):
- max_bytes_before_external_sort: https://clickhouse.com/docs/operations/settings/query-complexity#max_bytes_before_external_sort
- max_bytes_in_join: https://clickhouse.com/docs/operations/settings/query-complexity#max_bytes_in_join
- join_algorithm: https://clickhouse.com/docs/operations/settings/settings#join_algorithm
"""

from __future__ import annotations

from .data_context import create_object
from .models import Schema, QueryInfo, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY


# Operator to SQL expression mapping
OPERATOR_EXPRESSIONS = {
    # Arithmetic operators
    # Docs: https://clickhouse.com/docs/sql-reference/operators#arithmetic
    "+": "a.value + b.value",
    "-": "a.value - b.value",
    "*": "a.value * b.value",
    "/": "a.value / b.value",
    # intDiv: https://clickhouse.com/docs/sql-reference/functions/arithmetic-functions#intdiva-b
    "//": "intDiv(a.value, b.value)",
    "%": "a.value % b.value",
    # power: https://clickhouse.com/docs/sql-reference/functions/math-functions#pow
    "**": "power(a.value, b.value)",
    # Comparison operators
    # Docs: https://clickhouse.com/docs/sql-reference/operators#comparison
    "==": "a.value = b.value",
    "!=": "a.value != b.value",
    "<": "a.value < b.value",
    "<=": "a.value <= b.value",
    ">": "a.value > b.value",
    ">=": "a.value >= b.value",
    # Bitwise functions
    # Docs: https://clickhouse.com/docs/sql-reference/functions/bit-functions
    "&": "bitAnd(a.value, b.value)",
    "|": "bitOr(a.value, b.value)",
    "^": "bitXor(a.value, b.value)",
}


async def _apply_operator_db(info_a: QueryInfo, info_b: QueryInfo, operator: str, ch_client):
    """
    Apply an operator on two tables at the database level.

    Args:
        info_a: QueryInfo for first operand (contains source, fieldtype, value_type)
        info_b: QueryInfo for second operand (contains source, fieldtype, value_type)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance

    Returns:
        New Object instance pointing to result table
    """
    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    # Use fieldtype from QueryInfo (already computed in _get_query_info)
    fieldtype = info_a.fieldtype

    # Determine result type from QueryInfo value_types
    type_a = info_a.value_type
    type_b = info_b.value_type

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
# Docs: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference
AGGREGATION_FUNCTIONS = {
    "min": "min",
    "max": "max",
    "sum": "sum",
    "mean": "avg",
    "std": "stddevPop",
    "var": "varPop",
    "count": "count",
}


async def _apply_aggregation(info: QueryInfo, agg_func: str, ch_client):
    """
    Apply an aggregation function on a table at the database level.

    Creates a new Object with a scalar result containing the aggregated value.
    All computation happens within ClickHouse - no data round-trips to Python.

    Args:
        info: QueryInfo for the source (contains source, base_table, value_column)
        agg_func: Aggregation function key (e.g., 'min', 'max', 'sum', 'mean', 'std')
        ch_client: ClickHouse client instance

    Returns:
        New Object instance pointing to result table (scalar type)
    """
    from ..snowflake_id import get_snowflake_id

    # Get SQL function from aggregation mapping
    sql_func = AGGREGATION_FUNCTIONS[agg_func]

    # Get value column type from source table (use base table for metadata)
    # Use value_column to query the correct column for single-field selection
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{info.base_table}' AND name = '{info.value_column}'
    """
    type_result = await ch_client.query(type_query)
    source_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"

    # Determine result type based on aggregation function
    # - min/max preserve the source type
    # - sum preserves integer types, promotes to Float64 for float types
    # - count always returns UInt64
    # - mean/std/var always return Float64
    if agg_func in ("min", "max"):
        value_type = source_type
    elif agg_func == "sum":
        int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
        value_type = source_type if source_type in int_types else "Float64"
    elif agg_func == "count":
        value_type = "UInt64"
    else:
        # mean, std, and var always return Float64
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
    # count() uses count() without column, others use func(value)
    if agg_func == "count":
        agg_expr = f"{sql_func}()"
    else:
        agg_expr = f"{sql_func}(value)"
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT {aai_id} AS aai_id, {agg_expr} AS value
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
    return await _apply_aggregation(info, "min", ch_client)


async def max_agg(info: QueryInfo, ch_client):
    """
    Calculate maximum value at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar maximum value
    """
    return await _apply_aggregation(info, "max", ch_client)


async def sum_agg(info: QueryInfo, ch_client):
    """
    Calculate sum at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar sum value
    """
    return await _apply_aggregation(info, "sum", ch_client)


async def mean_agg(info: QueryInfo, ch_client):
    """
    Calculate mean (average) at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar mean value
    """
    return await _apply_aggregation(info, "mean", ch_client)


async def std_agg(info: QueryInfo, ch_client):
    """
    Calculate standard deviation (population) at database level.

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar standard deviation value
    """
    return await _apply_aggregation(info, "std", ch_client)


async def var_agg(info: QueryInfo, ch_client):
    """
    Calculate variance (population) at database level.

    Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/varpop

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar variance value
    """
    return await _apply_aggregation(info, "var", ch_client)


async def count_agg(info: QueryInfo, ch_client):
    """
    Count the number of rows at database level.

    Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/count

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar count value (UInt64)
    """
    return await _apply_aggregation(info, "count", ch_client)


async def quantile_agg(info: QueryInfo, q: float, ch_client):
    """
    Calculate quantile at database level.

    Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantile

    Args:
        info: QueryInfo for source (contains source and base_table)
        q: Quantile level between 0 and 1 (e.g., 0.5 for median)
        ch_client: ClickHouse client instance

    Returns:
        New Object instance with scalar quantile value
    """
    from ..snowflake_id import get_snowflake_id

    if not 0 <= q <= 1:
        raise ValueError(f"Quantile level must be between 0 and 1, got {q}")

    # Quantile always returns Float64
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

    # Insert quantile result
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT {aai_id} AS aai_id, quantile({q})(value) AS value
    FROM {info.source}
    """
    await ch_client.command(insert_query)

    return result


async def unique_group(info: QueryInfo, ch_client):
    """
    Get unique values at database level using GROUP BY.

    Uses GROUP BY instead of DISTINCT for better performance on large datasets.
    ClickHouse's GROUP BY is optimized for aggregation and can leverage
    distributed processing more effectively.

    Reference: https://clickhouse.com/docs/sql-reference/statements/select/group-by

    Args:
        info: QueryInfo for source (contains source, base_table, value_column)
        ch_client: ClickHouse client instance

    Returns:
        New Object with array of unique values
    """
    from ..snowflake_id import get_snowflake_ids

    # Get value column type from source table (use base table for metadata)
    # Use value_column to query the correct column for single-field selection
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{info.base_table}' AND name = '{info.value_column}'
    """
    type_result = await ch_client.query(type_query)
    source_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"

    # Build schema for result table (array type - multiple unique values)
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": "UInt64", "value": source_type}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Query unique values using GROUP BY (not DISTINCT)
    # GROUP BY is preferred over DISTINCT as it's more efficient in ClickHouse
    # for large datasets and enables better distributed processing
    unique_query = f"""
    SELECT value FROM {info.source} GROUP BY value
    """
    unique_result = await ch_client.query(unique_query)

    # Generate snowflake IDs for each unique value
    unique_values = [row[0] for row in unique_result.result_rows]
    if unique_values:
        aai_ids = get_snowflake_ids(len(unique_values))
        data = [[aai_id, value] for aai_id, value in zip(aai_ids, unique_values)]
        await ch_client.insert(result.table, data)

    return result
