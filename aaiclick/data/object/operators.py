"""
aaiclick.data.operators - Operator implementations for Object class.

This module contains database-level functions that implement all operators.
Each operator function takes table names and ch_client instead of Object instances.

Design — two-stage planner / materializer
=========================================

Every value-column operator follows the same two-stage pattern:

- **Plan (sync, in ``object.py``):** Each binary dunder (``__add__``,
  ``__sub__``, etc.) calls ``Object._plan_operator(other, op_symbol)`` and
  returns a ``LazyOperator``. No DB call. Reverse operators (``__radd__``,
  ``__rsub__``, etc.) use ``_plan_operator_reverse`` which swaps operand
  order for ``scalar op object`` syntax. Aggregations, unary transforms,
  the string/regex matchers, the null checks, ``isin``, ``coalesce`` and
  ``array_map`` plan the same way through their own ``_plan_*`` helpers.
- **Materialize (async, here):** Awaiting the ``LazyOperator`` dispatches to
  the operator function for its family — ``_apply_operator_db()`` for the
  binary dunders, ``_apply_string_op_db()`` for the string/regex matchers,
  and so on. Each builds ``QueryInfo`` for its operands and emits the
  ``CREATE TABLE`` + ``INSERT INTO ... SELECT``, forwarding the ``name`` /
  ``scope`` that ``.as_()`` recorded. Python scalars are inlined as
  ``(SELECT literal AS value)`` — no extra ClickHouse table.

Shared schema-computation helpers (``_compute_operator_schema``,
``_preview_operator_schema``, ``_promote_arithmetic_type``,
``_scalar_to_schema``) live in the neutral ``schema_compute.py`` module so
both ``_plan_operator`` (preview) and ``_apply_operator_db`` (materialize)
hit the same code — no drift between preview and result schemas.

See ``docs/user_guide/object.md`` ("Lazy Operator Results") for the user-facing
LazyOperator design, ``.as_(name, scope=...)`` naming API, and the
materialize-on-await contract.

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

from typing import NamedTuple, cast

from aaiclick.oplog.oplog_api import oplog_record_sample
from aaiclick.snowflake import get_snowflake_id

from ..data_context import create_object
from ..data_context.ch_client import execute_for_stats
from ..models import (
    AAI_ID_COLUMN,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    Agg,
    ColumnInfo,
    GroupByInfo,
    GroupByOpType,
    QueryInfo,
    Schema,
    parse_ch_type,
)
from ..scope import NamedScope
from ..sql_utils import escape_sql_string, quote_identifier, quote_sql_literal
from .emit import emit_result
from .schema_compute import (
    AGGREGATION_FUNCTIONS,
    ARRAYMAP_EXPRESSIONS,
    STRING_OP_EXPRESSIONS,
    STRING_OP_RESULT_TYPES,
    UNARY_TRANSFORMS,
    _compute_array_map_schema,
    _compute_coalesce_schema,
    _compute_operator_schema,
    _determine_agg_result_type,
    _preview_mask_schema,
)

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
    result = await ch_client.query(f"SELECT (SELECT count() FROM {source_a}), (SELECT count() FROM {source_b})")
    cnt_a, cnt_b = result.result_rows[0]
    if cnt_a != cnt_b:
        raise ValueError(f"Operand length mismatch: left has {cnt_a} elements, right has {cnt_b} elements")


async def _materialize_array_join(
    source_a,
    type_a,
    source_b,
    type_b,
    ch_client,
    *,
    order_a: str,
    order_b: str,
    propagate_aai_id_from: str | None = None,
):
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
        order_a: ORDER BY expression for left source — from ``View._order_by``.
        order_b: ORDER BY expression for right source.
        propagate_aai_id_from: ``"a"`` or ``"b"`` to copy that side's
            ``aai_id`` column into the temp table as ``aai_id Nullable(UInt64)``;
            ``None`` (default) skips propagation.

    Returns:
        Name of the temp table.  Caller is responsible for DROP.

    Raises:
        ValueError: If the two sources have different row counts.
    """
    temp_table = f"tmp_{get_snowflake_id()}"

    if propagate_aai_id_from is None:
        aai_id_col_def = ""
        outer_select = ""
        inner_a = ""
        inner_b = ""
    else:
        aai_id_col_def = f", {AAI_ID_COLUMN} Nullable(UInt64)"
        outer_select = f", {propagate_aai_id_from}.{AAI_ID_COLUMN} AS {AAI_ID_COLUMN}"
        side_inner = f", CAST({AAI_ID_COLUMN} AS Nullable(UInt64)) AS {AAI_ID_COLUMN}"
        inner_a = side_inner if propagate_aai_id_from == "a" else ""
        inner_b = side_inner if propagate_aai_id_from == "b" else ""

    await ch_client.command(f"""
        CREATE TABLE {temp_table} (
            a_value Nullable({type_a}),
            b_value Nullable({type_b}),
            a_present Nullable(UInt8),
            b_present Nullable(UInt8){aai_id_col_def}
        ) ENGINE = Memory
    """)

    # Cast rn to Nullable so FULL OUTER JOIN produces NULLs for non-matched rows.
    # Add explicit presence markers (also Nullable) to distinguish join-NULLs
    # from data-NULLs in nullable source columns.
    await ch_client.command(f"""
        INSERT INTO {temp_table}
        SELECT a.value AS a_value, b.value AS b_value,
               a.present AS a_present, b.present AS b_present{outer_select}
        FROM (
            SELECT CAST(row_number() OVER (ORDER BY {order_a}) AS Nullable(UInt64)) AS rn,
                   CAST(value AS Nullable({type_a})) AS value,
                   CAST(1 AS Nullable(UInt8)) AS present{inner_a}
            FROM {source_a}
        ) AS a
        FULL OUTER JOIN (
            SELECT CAST(row_number() OVER (ORDER BY {order_b}) AS Nullable(UInt64)) AS rn,
                   CAST(value AS Nullable({type_b})) AS value,
                   CAST(1 AS Nullable(UInt8)) AS present{inner_b}
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
            raise ValueError(f"Operand length mismatch: left has {cnt_a} elements, right has {cnt_b} elements")

    return temp_table


class _AaiIdProj(NamedTuple):
    """SQL fragments emitted when ``aai_id`` propagates to the result.

    All fields are empty strings when no propagation occurs so call sites can
    interpolate them unconditionally without inline ternaries. ``alias``
    selects the side ``aliased`` references — "a" for LHS-driven paths,
    "b" for scalar broadcast with array RHS or array×array RHS-only aai_id.
    """

    insert_cols: str  # "(value, aai_id)" or "(value)"
    inner: str  # ", aai_id" — bare column reference (unaliased subquery / temp table)
    aliased: str  # ", <alias>.aai_id AS aai_id" — table-qualified projection


def _aai_id_proj(propagate: bool, alias: str = "a") -> _AaiIdProj:
    if not propagate:
        return _AaiIdProj("(value)", "", "")
    return _AaiIdProj(
        f"(value, {AAI_ID_COLUMN})",
        f", {AAI_ID_COLUMN}",
        f", {alias}.{AAI_ID_COLUMN} AS {AAI_ID_COLUMN}",
    )


async def _apply_operator_db(
    info_a: QueryInfo,
    info_b: QueryInfo,
    operator: str,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """
    Apply an operator on two tables at the database level.

    Args:
        info_a: QueryInfo for first operand (contains source, fieldtype, value_type)
        info_b: QueryInfo for second operand (contains source, fieldtype, value_type)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance
        name: Optional result table name (forwarded to ``create_object``).
        scope: Optional result table scope (forwarded to ``create_object``).
              Defaults to ``"temp_named"`` when ``name`` is set.

    Returns:
        New Object instance pointing to result table
    """
    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    schema, aai_id_side = _compute_operator_schema(
        fieldtype_a=info_a.fieldtype,
        fieldtype_b=info_b.fieldtype,
        type_a=info_a.value_type,
        type_b=info_b.value_type,
        nullable_a=info_a.nullable,
        nullable_b=info_b.nullable,
        aai_id_a=info_a.aai_id_info,
        aai_id_b=info_b.aai_id_info,
        operator=operator,
    )
    a_is_array = info_a.fieldtype == FIELDTYPE_ARRAY
    b_is_array = info_b.fieldtype == FIELDTYPE_ARRAY
    aai_id_alias = aai_id_side or "a"  # "a" default is ignored when propagate=False
    proj = _aai_id_proj(aai_id_side is not None, alias=aai_id_alias)
    result = await create_object(schema, name=name, scope=scope)

    # Insert data based on fieldtype combinations
    if a_is_array and b_is_array:
        # Same-table optimization: when both operands are field selections from
        # the same table with identical constraints, emit a single SELECT instead
        # of the expensive INNER JOIN on row_number().
        same_table = (
            info_a.base_table == info_b.base_table
            and info_a.value_column != "value"
            and info_b.value_column != "value"
            and info_a.constraint_sql == info_b.constraint_sql
        )

        if same_table:
            col_a = quote_identifier(info_a.value_column)
            col_b = quote_identifier(info_b.value_column)
            expr = expression.replace("a.value", col_a).replace("b.value", col_b)
            suffix = info_a.constraint_sql
            if suffix:
                source_sql = f"(SELECT {col_a}, {col_b}{proj.inner} FROM {info_a.base_table} {suffix})"
            else:
                source_sql = info_a.base_table
            result._stats = await execute_for_stats(
                f"""
                INSERT INTO {result.table} {proj.insert_cols}
                SELECT {expr} AS value{proj.inner} FROM {source_sql}
            """,
                client=ch_client,
            )
        else:
            # Cross-table contract (Object._apply_operator) guarantees order_by on both sides.
            assert info_a.order_by and info_b.order_by, (
                "cross-table elementwise op reached operator SQL without order_by "
                "on both sides — the contract check in _apply_operator should have rejected it"
            )
            either_is_view = info_a.source.startswith("(") or info_b.source.startswith("(")

            if either_is_view:
                temp_table = await _materialize_array_join(
                    info_a.source,
                    info_a.value_type,
                    info_b.source,
                    info_b.value_type,
                    ch_client,
                    order_a=info_a.order_by,
                    order_b=info_b.order_by,
                    propagate_aai_id_from=aai_id_side,
                )
                temp_expr = expression.replace("a.value", "a_value").replace("b.value", "b_value")
                try:
                    result._stats = await execute_for_stats(
                        f"""
                        INSERT INTO {result.table} {proj.insert_cols}
                        SELECT {temp_expr} AS value{proj.inner} FROM {temp_table}
                    """,
                        client=ch_client,
                    )
                finally:
                    await ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
            else:
                await _validate_array_lengths(info_a.source, info_b.source, ch_client)
                inner_a = proj.inner if aai_id_alias == "a" else ""
                inner_b = proj.inner if aai_id_alias == "b" else ""
                result._stats = await execute_for_stats(
                    f"""
                    INSERT INTO {result.table} {proj.insert_cols}
                    SELECT {expression} AS value{proj.aliased}
                    FROM (SELECT row_number() OVER (ORDER BY {info_a.order_by}) AS rn, value{inner_a} FROM {info_a.source}) AS a
                    INNER JOIN (SELECT row_number() OVER (ORDER BY {info_b.order_by}) AS rn, value{inner_b} FROM {info_b.source}) AS b
                    ON a.rn = b.rn
                """,
                    client=ch_client,
                )

        oplog_record_sample(result.table, operator, kwargs={"left": info_a.base_table, "right": info_b.base_table})
        return result

    # Scalar broadcasting (array⊗scalar, scalar⊗array, scalar⊗scalar):
    # Cross-join works for all cases. Row order follows ClickHouse's natural
    # source order; callers wanting determinism apply .view(order_by=...).
    result._stats = await execute_for_stats(
        f"""
        INSERT INTO {result.table} {proj.insert_cols}
        SELECT {expression} AS value{proj.aliased}
        FROM {info_a.source} AS a, {info_b.source} AS b
    """,
        client=ch_client,
    )

    oplog_record_sample(result.table, operator, kwargs={"left": info_a.base_table, "right": info_b.base_table})
    return result


# ``AGGREGATION_FUNCTIONS``, ``UNARY_TRANSFORMS``, and
# ``_determine_agg_result_type`` now live in ``schema_compute`` so the
# LazyOperator preview path and the materialize path share one source of truth.


async def _apply_aggregation(
    info: QueryInfo,
    agg_func: str,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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
    safe_value_column = escape_sql_string(info.value_column)
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
        columns={"value": ColumnInfo(value_type)},
    )

    # count() uses count() without column, others use func(value)
    agg_expr = f"{sql_func}()" if agg_func == "count" else f"{sql_func}(value)"
    return await emit_result(
        schema,
        f"SELECT {agg_expr} AS value FROM {info.source}",
        ch_client,
        name=name,
        scope=scope,
        oplog_op=agg_func,
        oplog_kwargs={"source": info.base_table},
    )


# Aggregation Operators


async def count_if_agg(
    info: QueryInfo,
    condition: str | dict[str, str],
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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
            columns={"value": ColumnInfo("UInt64")},
        )
        return await emit_result(
            schema,
            f"SELECT countIf({condition}) AS value FROM {info.source}",
            ch_client,
            name=name,
            scope=scope,
        )

    columns = {}
    select_exprs = []
    for col_name, cond in condition.items():
        columns[col_name] = ColumnInfo("UInt64")
        select_exprs.append(f"countIf({cond}) AS {col_name}")

    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns=columns,
    )
    select_str = ", ".join(select_exprs)
    return await emit_result(
        schema,
        f"SELECT {select_str} FROM {info.source}",
        ch_client,
        insert_cols=", ".join(condition.keys()),
        name=name,
        scope=scope,
    )


async def quantile_agg(
    info: QueryInfo,
    q: float,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"value": ColumnInfo("Float64")},
    )
    return await emit_result(
        schema,
        f"SELECT quantile({q})(value) AS value FROM {info.source}",
        ch_client,
        name=name,
        scope=scope,
    )


async def unique_group(
    info: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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
    # Use effective type from QueryInfo — info.value_type already reflects
    # post-explode scalar types for ARRAY JOIN views, avoiding a system.columns
    # query that would return the original (pre-explode) Array type.
    source_col_def = ColumnInfo(info.value_type, nullable=info.nullable)

    # Build schema for result table (array type - multiple unique values)
    schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns={"value": source_col_def})

    # Unique values via GROUP BY (not DISTINCT) entirely in ClickHouse
    return await emit_result(
        schema,
        f"SELECT value FROM {info.source} GROUP BY value",
        ch_client,
        name=name,
        scope=scope,
    )


async def nunique_agg(
    info: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """
    Count distinct values at database level using GROUP BY subquery.

    Fused operation equivalent to ``unique().count()`` but in a single query:
    ``SELECT count() FROM (SELECT value FROM source GROUP BY value)``

    Uses GROUP BY (not ``uniq()``) for exact results and compatibility with
    MergeTree sorted data (``optimize_aggregation_in_order``).

    Reference: https://clickhouse.com/docs/sql-reference/statements/select/group-by

    Args:
        info: QueryInfo for source
        ch_client: ClickHouse client instance

    Returns:
        New Object with scalar count of distinct values (UInt64)
    """
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"value": ColumnInfo("UInt64")},
    )
    return await emit_result(
        schema,
        f"SELECT count() AS value FROM (SELECT value FROM {info.source} GROUP BY value)",
        ch_client,
        name=name,
        scope=scope,
        oplog_op="nunique",
        oplog_kwargs={"source": info.base_table},
    )


# arrayMap Operators
# Docs: https://clickhouse.com/docs/sql-reference/functions/array-functions#arraymapfunc-arr1-


async def array_map_db(
    info_a: QueryInfo,
    info_b: QueryInfo,
    operator: str,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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

    schema = _compute_array_map_schema(
        operator=operator,
        type_a=info_a.value_type,
        type_b=info_b.value_type,
        nullable_a=info_a.nullable,
        nullable_b=info_b.nullable,
    )

    b_is_scalar = info_b.fieldtype == FIELDTYPE_SCALAR

    if b_is_scalar:
        # arrayMap with single array + scalar as subquery in lambda body
        scalar_expr = expression.replace("y", f"(SELECT value FROM {info_b.source})")
        select_query = f"""
        SELECT arrayJoin(
            arrayMap(
                x -> {scalar_expr},
                (SELECT groupArray(value) FROM (SELECT value FROM {info_a.source}))
            )
        ) AS value
        """
    else:
        # arrayMap with two arrays — ClickHouse enforces equal sizes
        select_query = f"""
        SELECT arrayJoin(
            arrayMap(
                (x, y) -> {expression},
                (SELECT groupArray(value) FROM (SELECT value FROM {info_a.source})),
                (SELECT groupArray(value) FROM (SELECT value FROM {info_b.source}))
            )
        ) AS value
        """

    return await emit_result(schema, select_query, ch_client, name=name, scope=scope)


# Group By Operators


def _normalize_aggregations(aggregations: dict) -> list[tuple[str, Agg]]:
    """Normalize aggregation spec into flat list of (source_col, Agg).

    Accepts:
        - ``{col: op}``                     → alias = col
        - ``{col: Agg(op, alias)}``         → explicit alias
        - ``{col: [Agg(op, alias), ...]}``  → multiple aggs on same column

    Plain tuples are converted to Agg, validating the input format.
    """
    result: list[tuple[str, Agg]] = []
    for column, spec in aggregations.items():
        if isinstance(spec, str):
            result.append((column, Agg(op=cast(GroupByOpType, spec), alias=column)))
        elif isinstance(spec, tuple):
            result.append((column, Agg._make(spec)))
        elif isinstance(spec, list):
            for entry in spec:
                result.append((column, Agg._make(entry) if not isinstance(entry, Agg) else entry))
    return result


def _agg_expr(op: str, source_col: str, alias: str) -> str:
    """One aggregation SELECT expression; count() takes no argument."""
    arg = "" if op == "count" else quote_identifier(source_col)
    return f"{AGGREGATION_FUNCTIONS[op]}({arg}) AS {quote_identifier(alias)}"


async def group_by_agg(
    info: GroupByInfo,
    aggregations: dict,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """
    Apply aggregations with GROUP BY at database level.

    Groups data by the specified keys and applies aggregation functions.
    For 'count', uses count() without arguments.

    Aggregation spec per column:
        - ``str``:  plain operator, alias = column name
        - ``(op, alias)``:  single operator with explicit alias
        - ``[(op, alias), ...]``:  multiple operators on the same column

    Args:
        info: GroupByInfo with source, group keys, and column metadata
        aggregations: Dict mapping source_column -> AggSpec
        ch_client: ClickHouse client instance
        name: Optional result table name (forwarded to ``create_object``).
        scope: Optional result table scope (forwarded to ``create_object``).

    Returns:
        New dict Object with group keys + all aggregated columns
    """
    entries = _normalize_aggregations(aggregations)
    keys_str = ", ".join(quote_identifier(k) for k in info.group_keys)

    # Build aggregation expressions and result schema. Every result column
    # carries multiple rows (one per group), so fieldtype=ARRAY.
    agg_exprs = []
    result_columns = {}

    for key in info.group_keys:
        result_columns[key] = ColumnInfo(info.columns[key], fieldtype=FIELDTYPE_ARRAY)

    for source_col, agg in entries:
        agg_exprs.append(_agg_expr(agg.op, source_col, agg.alias))
        if agg.op == "count":
            result_columns[agg.alias] = ColumnInfo("UInt64", fieldtype=FIELDTYPE_ARRAY)
        elif agg.op == "group_array_distinct":
            source_type = info.columns[source_col]
            base_type = parse_ch_type(source_type).type if isinstance(source_type, str) else source_type.type
            result_columns[agg.alias] = ColumnInfo(base_type, array=True, fieldtype=FIELDTYPE_ARRAY)
        else:
            source_type = info.columns[source_col]
            result_columns[agg.alias] = ColumnInfo(
                _determine_agg_result_type(agg.op, source_type), fieldtype=FIELDTYPE_ARRAY
            )

    if info.having:
        # Use temporary aliases to avoid ClickHouse resolving HAVING column
        # references to SELECT aliases (which causes ILLEGAL_AGGREGATION error
        # when alias name matches source column name).
        tmp_agg_exprs = []
        rename_exprs = []
        for source_col, agg in entries:
            tmp_alias = f"__agg_{agg.alias}"
            tmp_agg_exprs.append(_agg_expr(agg.op, source_col, tmp_alias))
            rename_exprs.append(f"{quote_identifier(tmp_alias)} AS {quote_identifier(agg.alias)}")
        tmp_agg_str = ", ".join(tmp_agg_exprs)
        rename_str = ", ".join(rename_exprs)
        inner = f"SELECT {keys_str}, {tmp_agg_str} FROM {info.source} GROUP BY {keys_str} HAVING {info.having}"
        query = f"SELECT {keys_str}, {rename_str} FROM ({inner})"
    else:
        query = f"SELECT {keys_str}, {', '.join(agg_exprs)} FROM {info.source} GROUP BY {keys_str}"

    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=result_columns)
    return await emit_result(schema, query, ch_client, name=name, scope=scope)


# String/Regex Operators
# Docs: https://clickhouse.com/docs/sql-reference/functions/string-search-functions

# ``STRING_OP_EXPRESSIONS`` / ``STRING_OP_RESULT_TYPES`` live in
# ``schema_compute`` so the preview path and the materialize path share one
# source of truth. ``{pattern}`` and ``{replacement}`` in the expression
# templates are SQL-escaped string literals (with quotes).


async def _apply_string_op_db(
    info: QueryInfo,
    op_name: str,
    pattern: str,
    ch_client,
    replacement: str | None = None,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
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
    escaped_pattern = quote_sql_literal(pattern)
    format_args = {"pattern": escaped_pattern}
    if replacement is not None:
        format_args["replacement"] = quote_sql_literal(replacement)

    expression = STRING_OP_EXPRESSIONS[op_name].format(**format_args)

    schema = Schema(
        fieldtype=info.fieldtype,
        columns={"value": ColumnInfo(STRING_OP_RESULT_TYPES[op_name])},
    )
    return await emit_result(
        schema,
        f"SELECT {expression} AS value FROM {info.source} AS a",
        ch_client,
        name=name,
        scope=scope,
    )


# IN (isin) Operations
# Docs: https://clickhouse.com/docs/sql-reference/operators#in


async def isin_op(
    info: QueryInfo,
    other_info: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """Test if values are in another Object's value set. Returns UInt8 (1 if in, 0 otherwise).

    Generates: value IN (SELECT value FROM other_table)
    """
    subquery = f"SELECT value FROM {other_info.source}"
    return await emit_result(
        _preview_mask_schema(info.fieldtype),
        f"SELECT toUInt8(a.value IN ({subquery})) AS value FROM {info.source} AS a",
        ch_client,
        name=name,
        scope=scope,
        oplog_op="isin",
        oplog_kwargs={"source": info.base_table, "other": other_info.base_table},
    )


# Unary Transform Operations
# Apply a ClickHouse function to the value column, returning a new Object.
# Docs:
# - Date/time: https://clickhouse.com/docs/sql-reference/functions/date-time-functions
# - String:    https://clickhouse.com/docs/sql-reference/functions/string-functions
# - Math:      https://clickhouse.com/docs/sql-reference/functions/math-functions
#
# ``UNARY_TRANSFORMS`` lives in ``schema_compute`` so the preview path and the
# materialize path share one source of truth.


async def unary_transform(
    info: QueryInfo,
    transform: str,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
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
        columns={"value": ColumnInfo(result_type)},
    )
    return await emit_result(
        schema,
        f"SELECT {ch_func}(value) AS value FROM {info.source}",
        ch_client,
        name=name,
        scope=scope,
    )


# Null Operations
# Docs: https://clickhouse.com/docs/sql-reference/functions/functions-for-nulls


async def is_null_op(
    info: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """Apply isNull() — returns UInt8 Object (1 for NULL, 0 otherwise)."""
    return await emit_result(
        _preview_mask_schema(info.fieldtype),
        f"SELECT isNull(value) AS value FROM {info.source}",
        ch_client,
        name=name,
        scope=scope,
    )


async def is_not_null_op(
    info: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """Apply isNotNull() — returns UInt8 Object (1 for non-NULL, 0 otherwise)."""
    return await emit_result(
        _preview_mask_schema(info.fieldtype),
        f"SELECT isNotNull(value) AS value FROM {info.source}",
        ch_client,
        name=name,
        scope=scope,
    )


async def coalesce_op(
    info_a: QueryInfo,
    info_b: QueryInfo,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """Apply coalesce(a, b) — returns first non-NULL value.

    Result is non-nullable if the fallback (info_b) is non-nullable.
    """
    a_is_array = info_a.fieldtype == FIELDTYPE_ARRAY
    b_is_array = info_b.fieldtype == FIELDTYPE_ARRAY

    schema = _compute_coalesce_schema(
        fieldtype_a=info_a.fieldtype,
        fieldtype_b=info_b.fieldtype,
        type_a=info_a.value_type,
        nullable_a=info_a.nullable,
        nullable_b=info_b.nullable,
    )
    result = await create_object(schema, name=name, scope=scope)

    if a_is_array and b_is_array:
        # Cross-table contract enforced by Object.coalesce — both operands must
        # be Views with explicit order_by. Same-base-table coalesce is legal
        # without views (matching source rows 1:1).
        same_table = info_a.base_table == info_b.base_table
        assert same_table or (info_a.order_by and info_b.order_by), (
            "cross-table coalesce reached operator SQL without order_by — the "
            "contract check in Object.coalesce should have rejected it"
        )
        either_is_view = info_a.source.startswith("(") or info_b.source.startswith("(")

        if either_is_view:
            temp_table = await _materialize_array_join(
                info_a.source,
                info_a.value_type,
                info_b.source,
                info_b.value_type,
                ch_client,
                order_a=info_a.order_by or "tuple()",
                order_b=info_b.order_by or "tuple()",
            )
            try:
                result._stats = await execute_for_stats(
                    f"""
                    INSERT INTO {result.table} (value)
                    SELECT coalesce(a_value, b_value) AS value FROM {temp_table}
                """,
                    client=ch_client,
                )
            finally:
                await ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
        else:
            await _validate_array_lengths(info_a.source, info_b.source, ch_client)
            order_a = info_a.order_by or "tuple()"
            order_b = info_b.order_by or "tuple()"
            result._stats = await execute_for_stats(
                f"""
                INSERT INTO {result.table} (value)
                SELECT coalesce(a.value, b.value) AS value
                FROM (SELECT row_number() OVER (ORDER BY {order_a}) AS rn, value FROM {info_a.source}) AS a
                INNER JOIN (SELECT row_number() OVER (ORDER BY {order_b}) AS rn, value FROM {info_b.source}) AS b
                ON a.rn = b.rn
            """,
                client=ch_client,
            )

        return result

    # Scalar broadcasting (array⊗scalar, scalar⊗array, scalar⊗scalar):
    insert_target = result.table if (a_is_array or b_is_array) else f"{result.table} (value)"

    result._stats = await execute_for_stats(
        f"""
        INSERT INTO {insert_target}
        SELECT coalesce(a.value, b.value) AS value
        FROM {info_a.source} AS a, {info_b.source} AS b
    """,
        client=ch_client,
    )
    return result
