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

from typing import Union

from ..lineage.collector import get_lineage_collector
from ..snowflake_id import get_snowflake_id
from .data_context import create_object
from .models import ColumnInfo, Schema, QueryInfo, GroupByInfo, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY, FIELDTYPE_DICT, parse_ch_type, INT_TYPES, FLOAT_TYPES
from .sql_utils import quote_identifier


# Operator to arrayMap lambda expression mapping (uses x, y variables)
ARRAYMAP_EXPRESSIONS = {
    # Arithmetic operators
    "+": "x + y",
    "-": "x - y",
    "*": "x * y",
    "/": "x / y",
    "//": "intDiv(x, y)",
    "%": "x % y",
    "**": "power(x, y)",
    # Comparison operators
    "==": "toUInt8(x = y)",
    "!=": "toUInt8(x != y)",
    "<": "toUInt8(x < y)",
    "<=": "toUInt8(x <= y)",
    ">": "toUInt8(x > y)",
    ">=": "toUInt8(x >= y)",
    # Bitwise operators
    "&": "bitAnd(x, y)",
    "|": "bitOr(x, y)",
    "^": "bitXor(x, y)",
}

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


async def _validate_array_lengths(source_a, source_b, ch_client):
    """Validate two plain-table sources have equal row counts."""
    result = await ch_client.query(
        f"SELECT (SELECT count() FROM {source_a}), (SELECT count() FROM {source_b})"
    )
    cnt_a, cnt_b = result.result_rows[0]
    if cnt_a != cnt_b:
        raise ValueError(
            f"Operand length mismatch: left has {cnt_a} elements, right has {cnt_b} elements"
        )


async def _materialize_array_join(source_a, type_a, source_b, type_b, ch_client):
    """Materialize a FULL OUTER JOIN into a temp table and validate lengths match.

    Creates a temporary Memory table containing both operand values joined by
    row number, with window-function counts embedded in every row.  After
    insertion, a single-row read validates that both sides have the same
    number of elements.

    Args:
        source_a: SQL source for left operand (table name or subquery)
        type_a: ClickHouse value type of left operand (e.g. 'Float64')
        source_b: SQL source for right operand
        type_b: ClickHouse value type of right operand
        ch_client: ClickHouse async client

    Returns:
        Name of the temp table.  Caller is responsible for DROP.

    Raises:
        ValueError: If the two sources have different row counts.
    """
    temp_table = f"tmp_{get_snowflake_id()}"

    await ch_client.command(f"""
        CREATE TABLE {temp_table} (
            a_value Nullable({type_a}),
            b_value Nullable({type_b}),
            a_present Nullable(UInt8),
            b_present Nullable(UInt8)
        ) ENGINE = Memory
    """)

    # Cast rn to Nullable so FULL OUTER JOIN produces NULLs for non-matched rows.
    # Add explicit presence markers (also Nullable) to distinguish join-NULLs
    # from data-NULLs in nullable source columns.
    await ch_client.command(f"""
        INSERT INTO {temp_table}
        SELECT a.value AS a_value, b.value AS b_value,
               a.present AS a_present, b.present AS b_present
        FROM (
            SELECT CAST(row_number() OVER (ORDER BY aai_id) AS Nullable(UInt64)) AS rn,
                   CAST(value AS Nullable({type_a})) AS value,
                   CAST(1 AS Nullable(UInt8)) AS present
            FROM {source_a}
        ) AS a
        FULL OUTER JOIN (
            SELECT CAST(row_number() OVER (ORDER BY aai_id) AS Nullable(UInt64)) AS rn,
                   CAST(value AS Nullable({type_b})) AS value,
                   CAST(1 AS Nullable(UInt8)) AS present
            FROM {source_b}
        ) AS b
        ON a.rn = b.rn
    """)

    result = await ch_client.query(
        f"SELECT countIf(a_present IS NOT NULL), countIf(b_present IS NOT NULL) FROM {temp_table}"
    )
    if result.result_rows:
        cnt_a, cnt_b = result.result_rows[0]
        if cnt_a != cnt_b:
            await ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
            raise ValueError(
                f"Operand length mismatch: left has {cnt_a} elements, right has {cnt_b} elements"
            )

    return temp_table


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

    # Determine result fieldtype: array if either operand is array
    a_is_array = info_a.fieldtype == FIELDTYPE_ARRAY
    b_is_array = info_b.fieldtype == FIELDTYPE_ARRAY
    fieldtype = FIELDTYPE_ARRAY if (a_is_array or b_is_array) else FIELDTYPE_SCALAR

    # Determine result type from QueryInfo value_types
    type_a = info_a.value_type
    type_b = info_b.value_type

    if (type_a in INT_TYPES and type_b in FLOAT_TYPES) or (type_a in FLOAT_TYPES and type_b in INT_TYPES):
        value_type = "Float64"
    elif type_a in FLOAT_TYPES or type_b in FLOAT_TYPES:
        value_type = "Float64"
    else:
        value_type = type_a

    # Build schema for result table
    result_nullable = info_a.nullable or info_b.nullable
    schema = Schema(
        fieldtype=fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type, nullable=result_nullable)}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Insert data based on fieldtype combinations
    # aai_id uses DEFAULT generateSnowflakeID() for array-array and scalar-scalar.
    # For mixed cases, preserve source aai_id to maintain ordering.
    if a_is_array and b_is_array:
        either_is_view = info_a.source.startswith("(") or info_b.source.startswith("(")

        if either_is_view:
            temp_table = await _materialize_array_join(
                info_a.source, info_a.value_type,
                info_b.source, info_b.value_type,
                ch_client,
            )
            temp_expr = expression.replace("a.value", "a_value").replace("b.value", "b_value")
            try:
                await ch_client.command(f"""
                    INSERT INTO {result.table} (value)
                    SELECT {temp_expr} AS value FROM {temp_table}
                """)
            finally:
                await ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
        else:
            await _validate_array_lengths(info_a.source, info_b.source, ch_client)
            await ch_client.command(f"""
                INSERT INTO {result.table} (value)
                SELECT {expression} AS value
                FROM (SELECT row_number() OVER (ORDER BY aai_id) AS rn, value FROM {info_a.source}) AS a
                INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) AS rn, value FROM {info_b.source}) AS b
                ON a.rn = b.rn
            """)

        collector = get_lineage_collector()
        if collector:
            collector.record(result.table, operator, [info_a.base_table, info_b.base_table])
        return result

    # Scalar broadcasting (array⊗scalar, scalar⊗array, scalar⊗scalar):
    # Cross-join works for all cases; only the aai_id source differs.
    # Scalar-scalar omits aai_id (DEFAULT generateSnowflakeID() fills it).
    if a_is_array:
        insert_target = result.table
        select_cols = f"a.aai_id, {expression} AS value"
    elif b_is_array:
        insert_target = result.table
        select_cols = f"b.aai_id, {expression} AS value"
    else:
        insert_target = f"{result.table} (value)"
        select_cols = f"{expression} AS value"

    await ch_client.command(f"""
        INSERT INTO {insert_target}
        SELECT {select_cols}
        FROM {info_a.source} AS a, {info_b.source} AS b
    """)

    collector = get_lineage_collector()
    if collector:
        collector.record(result.table, operator, [info_a.base_table, info_b.base_table])
    return result


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
    "any": "any",
}


INT_TYPES = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}


def _determine_agg_result_type(agg_func: str, source_type: Union[str, ColumnInfo]) -> str:
    """
    Determine the ClickHouse result type for an aggregation function.

    Aggregation results are always non-nullable (ClickHouse aggregations
    skip NULLs and always produce a value).

    Rules:
    - min/max preserve the source base type
    - sum preserves integer types, promotes to Float64 for float types
    - count always returns UInt64
    - mean/std/var always return Float64

    Args:
        agg_func: Aggregation function key (e.g., 'min', 'sum', 'mean')
        source_type: ClickHouse type string or ColumnInfo

    Returns:
        ClickHouse base type string for the result (never Nullable)
    """
    base_type = source_type.type if isinstance(source_type, ColumnInfo) else parse_ch_type(source_type).type
    if agg_func in ("min", "max", "any"):
        return base_type
    elif agg_func == "sum":
        return base_type if base_type in INT_TYPES else "Float64"
    elif agg_func == "count":
        return "UInt64"
    else:
        return "Float64"


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
    # Get SQL function from aggregation mapping
    sql_func = AGGREGATION_FUNCTIONS[agg_func]

    # Get value column type from source table (use base table for metadata)
    # Use value_column to query the correct column for single-field selection
    safe_value_column = info.value_column.replace("'", "\\'")
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{info.base_table}' AND name = '{safe_value_column}'
    """
    type_result = await ch_client.query(type_query)
    source_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"

    value_type = _determine_agg_result_type(agg_func, source_type)

    # Build schema for result table (scalar type, never nullable)
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type)}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Insert aggregated data (aai_id uses DEFAULT generateSnowflakeID())
    # count() uses count() without column, others use func(value)
    if agg_func == "count":
        agg_expr = f"{sql_func}()"
    else:
        agg_expr = f"{sql_func}(value)"
    insert_query = f"""
    INSERT INTO {result.table} (value)
    SELECT {agg_expr} AS value
    FROM {info.source}
    """
    await ch_client.command(insert_query)

    collector = get_lineage_collector()
    if collector:
        collector.record(result.table, agg_func, [info.base_table])
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


async def count_if_agg(info: QueryInfo, condition: Union[str, dict[str, str]], ch_client):
    """
    Count rows matching condition(s) at database level using countIf().

    Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators#-if

    When condition is a str, returns a scalar Object (single countIf).
    When condition is a dict {name: condition}, returns a dict Object with
    one UInt64 column per entry, computed in a single table scan.

    Args:
        info: QueryInfo for source
        condition: Either a single SQL condition string, or a dict mapping
                   result column names to SQL condition strings
        ch_client: ClickHouse client instance

    Returns:
        Scalar Object (str condition) or dict Object (dict condition)
    """
    if isinstance(condition, str):
        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo("UInt64")},
        )
        result = await create_object(schema)
        query = (
            f"INSERT INTO {result.table} (value) "
            f"SELECT countIf({condition}) AS value FROM {info.source}"
        )
        await ch_client.command(query)
        return result

    columns = {"aai_id": ColumnInfo("UInt64")}
    select_exprs = []
    for name, cond in condition.items():
        columns[name] = ColumnInfo("UInt64")
        select_exprs.append(f"countIf({cond}) AS {name}")

    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns=columns,
        col_fieldtype=FIELDTYPE_SCALAR,
    )
    result = await create_object(schema)
    insert_cols = ", ".join(condition.keys())
    select_str = ", ".join(select_exprs)
    query = (
        f"INSERT INTO {result.table} ({insert_cols}) "
        f"SELECT {select_str} FROM {info.source}"
    )
    await ch_client.command(query)
    return result


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
    if not 0 <= q <= 1:
        raise ValueError(f"Quantile level must be between 0 and 1, got {q}")

    # Quantile always returns Float64
    value_type = "Float64"

    # Build schema for result table (scalar type)
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type)}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Insert quantile result (aai_id uses DEFAULT generateSnowflakeID())
    insert_query = f"""
    INSERT INTO {result.table} (value)
    SELECT quantile({q})(value) AS value
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
    # Get value column type from source table (use base table for metadata)
    # Use value_column to query the correct column for single-field selection
    safe_value_column = info.value_column.replace("'", "\\'")
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{info.base_table}' AND name = '{safe_value_column}'
    """
    type_result = await ch_client.query(type_query)
    source_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"
    source_col_def = parse_ch_type(source_type)

    # Build schema for result table (array type - multiple unique values)
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": ColumnInfo("UInt64"), "value": source_col_def}
    )

    # Create result object with schema
    result = await create_object(schema)

    # Insert unique values using GROUP BY (not DISTINCT) entirely in ClickHouse
    # aai_id uses DEFAULT generateSnowflakeID()
    insert_query = f"""
    INSERT INTO {result.table} (value)
    SELECT value FROM {info.source} GROUP BY value
    """
    await ch_client.command(insert_query)

    return result


# arrayMap Operators
# Docs: https://clickhouse.com/docs/sql-reference/functions/array-functions#arraymapfunc-arr1-


async def array_map_db(info_a: QueryInfo, info_b: QueryInfo, operator: str, ch_client):
    """
    Apply an element-wise operation using ClickHouse's arrayMap function.

    Collects both sources into arrays (preserving Snowflake ID order),
    applies arrayMap with a lambda, then expands back into rows via arrayJoin.

    Like _apply_operator_db, this raises an error when array sizes don't match.

    Args:
        info_a: QueryInfo for first operand (must be FIELDTYPE_ARRAY)
        info_b: QueryInfo for second operand (FIELDTYPE_ARRAY or FIELDTYPE_SCALAR)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance

    Returns:
        New Object instance pointing to result table (FIELDTYPE_ARRAY)

    Raises:
        ValueError: If operator is not supported
        DB::Exception: If array sizes don't match (from ClickHouse)
    """
    if operator not in ARRAYMAP_EXPRESSIONS:
        raise ValueError(f"Unsupported operator for array_map: {operator!r}")

    expression = ARRAYMAP_EXPRESSIONS[operator]

    # Determine result type
    type_a = info_a.value_type
    type_b = info_b.value_type

    comparison_ops = {"==", "!=", "<", "<=", ">", ">="}

    if operator in comparison_ops:
        value_type = "UInt8"
    elif (type_a in INT_TYPES and type_b in FLOAT_TYPES) or (type_a in FLOAT_TYPES and type_b in INT_TYPES):
        value_type = "Float64"
    elif type_a in FLOAT_TYPES or type_b in FLOAT_TYPES:
        value_type = "Float64"
    else:
        value_type = type_a

    result_nullable = info_a.nullable or info_b.nullable
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type, nullable=result_nullable)},
    )
    result = await create_object(schema)

    b_is_scalar = info_b.fieldtype == FIELDTYPE_SCALAR

    if b_is_scalar:
        # arrayMap with single array + scalar as subquery in lambda body
        scalar_expr = expression.replace("y", f"(SELECT value FROM {info_b.source})")
        insert_query = f"""
        INSERT INTO {result.table} (value)
        SELECT arrayJoin(
            arrayMap(
                x -> {scalar_expr},
                (SELECT groupArray(value) FROM (SELECT value FROM {info_a.source} ORDER BY aai_id))
            )
        ) AS value
        """
    else:
        # arrayMap with two arrays — ClickHouse enforces equal sizes
        insert_query = f"""
        INSERT INTO {result.table} (value)
        SELECT arrayJoin(
            arrayMap(
                (x, y) -> {expression},
                (SELECT groupArray(value) FROM (SELECT value FROM {info_a.source} ORDER BY aai_id)),
                (SELECT groupArray(value) FROM (SELECT value FROM {info_b.source} ORDER BY aai_id))
            )
        ) AS value
        """

    await ch_client.command(insert_query)
    return result


# Group By Operators

async def group_by_agg(info: GroupByInfo, aggregations: dict, ch_client):
    """
    Apply aggregations with GROUP BY at database level.

    Groups data by the specified keys and applies aggregation functions.
    Each entry maps a result column name to an aggregation function.
    For 'count', uses count() without arguments.

    Args:
        info: GroupByInfo with source, group keys, and column metadata
        aggregations: Dict mapping column_name -> agg_func
                      (e.g., {'amount': 'sum', '_count': 'count'})
        ch_client: ClickHouse client instance

    Returns:
        New dict Object with group keys + all aggregated columns
    """
    keys_str = ", ".join(info.group_keys)

    # Build aggregation expressions and result schema
    agg_exprs = []
    result_columns = {"aai_id": ColumnInfo("UInt64")}

    for key in info.group_keys:
        result_columns[key] = ColumnInfo(info.columns[key])

    for column, agg_func in aggregations.items():
        sql_func = AGGREGATION_FUNCTIONS[agg_func]
        if agg_func == "count":
            agg_exprs.append(f"{sql_func}() AS {column}")
            result_columns[column] = ColumnInfo("UInt64")
        else:
            agg_exprs.append(f"{sql_func}({column}) AS {column}")
            source_type = info.columns[column]
            result_columns[column] = ColumnInfo(_determine_agg_result_type(agg_func, source_type))

    agg_str = ", ".join(agg_exprs)

    # Build non-aai_id column names for INSERT
    insert_cols = [k for k in result_columns if k != "aai_id"]
    insert_cols_str = ", ".join(insert_cols)

    if info.having:
        # Use temporary aliases to avoid ClickHouse resolving HAVING column
        # references to SELECT aliases (which causes ILLEGAL_AGGREGATION error
        # when alias name matches source column name).
        tmp_agg_exprs = []
        rename_exprs = []
        for column, agg_func in aggregations.items():
            sql_func = AGGREGATION_FUNCTIONS[agg_func]
            tmp_alias = f"__agg_{column}"
            if agg_func == "count":
                tmp_agg_exprs.append(f"{sql_func}() AS {tmp_alias}")
            else:
                tmp_agg_exprs.append(f"{sql_func}({column}) AS {tmp_alias}")
            rename_exprs.append(f"{tmp_alias} AS {column}")
        tmp_agg_str = ", ".join(tmp_agg_exprs)
        rename_str = ", ".join(rename_exprs)
        inner = (
            f"SELECT {keys_str}, {tmp_agg_str} "
            f"FROM {info.source} GROUP BY {keys_str} "
            f"HAVING {info.having}"
        )
        query = f"SELECT {keys_str}, {rename_str} FROM ({inner})"
    else:
        query = f"SELECT {keys_str}, {agg_str} FROM {info.source} GROUP BY {keys_str}"

    schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=result_columns)
    result = await create_object(schema)

    # Insert directly from query (aai_id uses DEFAULT generateSnowflakeID())
    insert_query = f"INSERT INTO {result.table} ({insert_cols_str}) {query}"
    await ch_client.command(insert_query)

    return result


# String/Regex Operators
# Docs: https://clickhouse.com/docs/sql-reference/functions/string-search-functions

# SQL expression templates for string operations
# {pattern} and {replacement} are SQL-escaped string literals (with quotes)
STRING_OP_EXPRESSIONS = {
    "match": "match(a.value, {pattern})",
    "like": "a.value LIKE {pattern}",
    "ilike": "a.value ILIKE {pattern}",
    "extract": "extract(a.value, {pattern})",
    "replace": "replaceRegexpAll(a.value, {pattern}, {replacement})",
}

# Fixed result types for string operations
STRING_OP_RESULT_TYPES = {
    "match": "UInt8",
    "like": "UInt8",
    "ilike": "UInt8",
    "extract": "String",
    "replace": "String",
}


def _escape_sql_string(value: str) -> str:
    """Escape a Python string for use as a SQL string literal (with quotes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


async def _apply_string_op_db(
    info: QueryInfo,
    op_name: str,
    pattern: str,
    ch_client,
    replacement: str = None,
):
    """
    Apply a string/regex operation at the database level.

    Args:
        info: QueryInfo for source (string column)
        op_name: Operation name key in STRING_OP_EXPRESSIONS
        pattern: Regex or LIKE pattern string
        ch_client: ClickHouse client instance
        replacement: Replacement string (only for 'replace' operation)

    Returns:
        New Object instance pointing to result table
    """
    escaped_pattern = _escape_sql_string(pattern)
    format_args = {"pattern": escaped_pattern}
    if replacement is not None:
        format_args["replacement"] = _escape_sql_string(replacement)

    expression = STRING_OP_EXPRESSIONS[op_name].format(**format_args)
    value_type = STRING_OP_RESULT_TYPES[op_name]
    fieldtype = info.fieldtype

    schema = Schema(
        fieldtype=fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type)},
    )

    result = await create_object(schema)

    await ch_client.command(f"""
        INSERT INTO {result.table}
        SELECT a.aai_id, {expression} AS value
        FROM {info.source} AS a
    """)
    return result


async def match_op(info: QueryInfo, pattern: str, ch_client):
    """RE2 regex match. Returns UInt8 (1 if match, 0 otherwise)."""
    return await _apply_string_op_db(info, "match", pattern, ch_client)


async def like_op(info: QueryInfo, pattern: str, ch_client):
    """SQL LIKE pattern match. Returns UInt8 (1 if match, 0 otherwise)."""
    return await _apply_string_op_db(info, "like", pattern, ch_client)


async def ilike_op(info: QueryInfo, pattern: str, ch_client):
    """Case-insensitive SQL LIKE pattern match. Returns UInt8."""
    return await _apply_string_op_db(info, "ilike", pattern, ch_client)


async def extract_op(info: QueryInfo, pattern: str, ch_client):
    """Extract first regex capture group. Returns String."""
    return await _apply_string_op_db(info, "extract", pattern, ch_client)


async def replace_op(info: QueryInfo, pattern: str, replacement: str, ch_client):
    """Replace all regex matches. Returns String."""
    return await _apply_string_op_db(info, "replace", pattern, ch_client, replacement=replacement)


# Unary Transform Operations
# Apply a ClickHouse function to the value column, returning a new Object.
# Docs:
# - Date/time: https://clickhouse.com/docs/sql-reference/functions/date-time-functions
# - String:    https://clickhouse.com/docs/sql-reference/functions/string-functions
# - Math:      https://clickhouse.com/docs/sql-reference/functions/math-functions

# Mapping: Python method name -> (ClickHouse function, result type)
UNARY_TRANSFORMS = {
    # Date/time extractions
    "year": ("toYear", "UInt16"),
    "month": ("toMonth", "UInt8"),
    "day_of_week": ("toDayOfWeek", "UInt8"),
    # String transforms
    "lower": ("lower", "String"),
    "upper": ("upper", "String"),
    "length": ("length", "UInt64"),
    "trim": ("trimBoth", "String"),
    # Math transforms
    "abs": ("abs", "Float64"),
    "log2": ("log2", "Float64"),
    "sqrt": ("sqrt", "Float64"),
}


async def unary_transform(info: QueryInfo, transform: str, ch_client):
    """Apply a unary ClickHouse function to the value column.

    Args:
        info: QueryInfo for source
        transform: Transform key from UNARY_TRANSFORMS
        ch_client: ClickHouse client instance

    Returns:
        New Object with transformed values (preserves fieldtype)
    """
    ch_func, result_type = UNARY_TRANSFORMS[transform]

    schema = Schema(
        fieldtype=info.fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(result_type)},
    )
    result = await create_object(schema)

    await ch_client.command(f"""
        INSERT INTO {result.table}
        SELECT aai_id, {ch_func}(value) AS value FROM {info.source}
    """)
    return result


# Null Operations
# Docs: https://clickhouse.com/docs/sql-reference/functions/functions-for-nulls


async def is_null_op(info: QueryInfo, ch_client):
    """Apply isNull() — returns UInt8 Object (1 for NULL, 0 otherwise)."""
    schema = Schema(
        fieldtype=info.fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo("UInt8")},
    )
    result = await create_object(schema)
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, isNull(value) AS value FROM {info.source}
    """
    await ch_client.command(insert_query)
    return result


async def is_not_null_op(info: QueryInfo, ch_client):
    """Apply isNotNull() — returns UInt8 Object (1 for non-NULL, 0 otherwise)."""
    schema = Schema(
        fieldtype=info.fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo("UInt8")},
    )
    result = await create_object(schema)
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, isNotNull(value) AS value FROM {info.source}
    """
    await ch_client.command(insert_query)
    return result


async def coalesce_op(info_a: QueryInfo, info_b: QueryInfo, ch_client):
    """Apply coalesce(a, b) — returns first non-NULL value.

    Result is non-nullable if the fallback (info_b) is non-nullable.
    """
    a_is_array = info_a.fieldtype == FIELDTYPE_ARRAY
    b_is_array = info_b.fieldtype == FIELDTYPE_ARRAY
    fieldtype = FIELDTYPE_ARRAY if (a_is_array or b_is_array) else FIELDTYPE_SCALAR

    # Result type follows the first operand's base type
    value_type = info_a.value_type
    # Result is nullable only if both operands are nullable
    result_nullable = info_a.nullable and info_b.nullable

    schema = Schema(
        fieldtype=fieldtype,
        columns={"aai_id": ColumnInfo("UInt64"), "value": ColumnInfo(value_type, nullable=result_nullable)},
    )
    result = await create_object(schema)

    if a_is_array and b_is_array:
        either_is_view = info_a.source.startswith("(") or info_b.source.startswith("(")

        if either_is_view:
            temp_table = await _materialize_array_join(
                info_a.source, info_a.value_type,
                info_b.source, info_b.value_type,
                ch_client,
            )
            try:
                await ch_client.command(f"""
                    INSERT INTO {result.table} (value)
                    SELECT coalesce(a_value, b_value) AS value FROM {temp_table}
                """)
            finally:
                await ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
        else:
            await _validate_array_lengths(info_a.source, info_b.source, ch_client)
            await ch_client.command(f"""
                INSERT INTO {result.table} (value)
                SELECT coalesce(a.value, b.value) AS value
                FROM (SELECT row_number() OVER (ORDER BY aai_id) AS rn, value FROM {info_a.source}) AS a
                INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) AS rn, value FROM {info_b.source}) AS b
                ON a.rn = b.rn
            """)

        return result

    # Scalar broadcasting (array⊗scalar, scalar⊗array, scalar⊗scalar):
    if a_is_array:
        insert_target = result.table
        select_cols = f"a.aai_id, coalesce(a.value, b.value) AS value"
    elif b_is_array:
        insert_target = result.table
        select_cols = f"b.aai_id, coalesce(a.value, b.value) AS value"
    else:
        insert_target = f"{result.table} (value)"
        select_cols = f"coalesce(a.value, b.value) AS value"

    await ch_client.command(f"""
        INSERT INTO {insert_target}
        SELECT {select_cols}
        FROM {info_a.source} AS a, {info_b.source} AS b
    """)
    return result
