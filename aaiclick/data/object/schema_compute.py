"""Sync schema-computation helpers for binary operator results.

Neutral module: imports only from ``..models`` and
``..data_context.data_context``. Both ``object.py`` (for ``LazyOperator``
planning) and ``operators.py`` (for materialize-time) depend on these
helpers — keeping them here avoids a circular import between the two.
"""

from __future__ import annotations

from typing import NamedTuple

from ..data_context.data_context import _infer_clickhouse_type
from ..models import (
    AAI_ID_COLUMN,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    FLOAT_TYPES,
    INT_TYPES,
    ColumnInfo,
    Schema,
    ValueScalarType,
    parse_ch_type,
)

# Division and power always return Float64 in ClickHouse.
_FLOAT_RESULT_OPS = frozenset({"/", "**"})

# Comparison operators always yield UInt8 rather than a promoted numeric type.
_COMPARISON_OPS = frozenset({"==", "!=", "<", "<=", ">", ">="})

# Small unsigned types (Bool, UInt8) promote to wider types in ClickHouse.
# Subtraction always produces signed result.
_SMALL_UNSIGNED = frozenset({"Bool", "UInt8"})


def _promote_arithmetic_type(operator: str, type_a: str, type_b: str) -> str:
    """Determine the ClickHouse result type for an arithmetic operation.

    Matches ClickHouse type promotion rules:
    - ``/`` and ``**`` always return Float64
    - int + float → Float64
    - Bool/UInt8 same-type ``+``/``*`` → UInt16, ``-`` → Int16
    - Subtraction always promotes to signed (Int64)
    - Mixed int widths promote to the wider type

    Validated by ``test_type_promotion.py::test_arithmetic_type_promotion``
    which compares against ``SELECT toTypeName(CAST(0, 'T1') op CAST(0, 'T2'))``.
    """
    if operator in _FLOAT_RESULT_OPS:
        return "Float64"
    if type_a in FLOAT_TYPES or type_b in FLOAT_TYPES:
        return "Float64"
    if type_a in _SMALL_UNSIGNED and type_b in _SMALL_UNSIGNED:
        return "Int16" if operator == "-" else "UInt16"
    if type_a in _SMALL_UNSIGNED or type_b in _SMALL_UNSIGNED:
        wider = type_b if type_a in _SMALL_UNSIGNED else type_a
        return wider
    if operator == "-":
        return "Int64"
    return type_a


def _scalar_to_schema(value: ValueScalarType) -> Schema:
    """Build a one-column scalar Schema from a Python scalar.

    Defers to ``_infer_clickhouse_type`` — the same routine
    ``create_object_from_value`` uses — so the preview schema for a scalar
    operand can't drift from the schema of the table that would be
    materialized at await time.
    """
    return Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"value": _infer_clickhouse_type(value)},
    )


def _compute_operator_schema(
    *,
    fieldtype_a: str,
    fieldtype_b: str,
    type_a: str,
    type_b: str,
    nullable_a: bool,
    nullable_b: bool,
    aai_id_a: ColumnInfo | None,
    aai_id_b: ColumnInfo | None,
    operator: str,
) -> tuple[Schema, str | None]:
    """Compute the result Schema for a binary operator and the side ("a" or
    "b") whose ``aai_id`` column propagates — or ``None`` when neither does.

    Single source of truth for fieldtype/type/nullable promotion and aai_id
    routing, shared between ``_preview_operator_schema`` (sync
    pre-materialize preview) and ``_apply_operator_db`` (materialize-time).
    """
    a_is_array = fieldtype_a == FIELDTYPE_ARRAY
    b_is_array = fieldtype_b == FIELDTYPE_ARRAY
    fieldtype = FIELDTYPE_ARRAY if (a_is_array or b_is_array) else FIELDTYPE_SCALAR
    value_type = "UInt8" if operator in _COMPARISON_OPS else _promote_arithmetic_type(operator, type_a, type_b)
    result_nullable = nullable_a or nullable_b

    result_columns: dict[str, ColumnInfo] = {
        "value": ColumnInfo(value_type, nullable=result_nullable),
    }

    aai_id_side: str | None = None
    aai_id_source: ColumnInfo | None = None
    if a_is_array and aai_id_a is not None:
        aai_id_source = aai_id_a
        aai_id_side = "a"
    elif b_is_array and aai_id_b is not None:
        aai_id_source = aai_id_b
        aai_id_side = "b"

    if aai_id_source is not None:
        # Mirror the source column shape, but drop DEFAULT — values are copied
        # via INSERT, not generated per-row by ClickHouse.
        result_columns[AAI_ID_COLUMN] = aai_id_source.model_copy(update={"default": None})

    return Schema(fieldtype=fieldtype, columns=result_columns), aai_id_side


# Unary transform key → (ClickHouse function, result type) — single source of
# truth shared between ``_preview_unary_schema`` (preview) and
# ``operators.unary_transform`` (materialize). Mirrored to ``operators.UNARY_TRANSFORMS``
# via re-export so existing imports keep working.
UNARY_TRANSFORMS: dict[str, tuple[str, str]] = {
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
    # Null checks
    "is_null": ("isNull", "UInt8"),
    "is_not_null": ("isNotNull", "UInt8"),
}


# Aggregation key → ClickHouse function name. Mirrored to
# ``operators.AGGREGATION_FUNCTIONS``.
AGGREGATION_FUNCTIONS: dict[str, str] = {
    "min": "min",
    "max": "max",
    "sum": "sum",
    "mean": "avg",
    "std": "stddevPop",
    "var": "varPop",
    "count": "count",
    "any": "any",
    "group_array_distinct": "groupArrayDistinct",
}


def _determine_agg_result_type(agg_func: str, source_type: str | ColumnInfo) -> str:
    """Determine the ClickHouse result type for an aggregation function.

    Aggregation results are always non-nullable (ClickHouse aggregations
    skip NULLs and always produce a value).

    Rules:
        - min/max/any preserve the source base type
        - sum preserves integer types, promotes to Float64 for float types
        - count always returns UInt64
        - mean/std/var always return Float64
    """
    base_type = source_type.type if isinstance(source_type, ColumnInfo) else parse_ch_type(source_type).type
    if agg_func in ("min", "max", "any"):
        return base_type
    if agg_func == "sum":
        if base_type == "Bool":
            return "UInt64"
        return base_type if base_type in INT_TYPES else "Float64"
    if agg_func == "count":
        return "UInt64"
    return "Float64"


def _preview_fixed_type_schema(input_fieldtype: str, result_type: str) -> Schema:
    """Sync preview for every value-wise operator with a fixed result type —
    unary transforms, string/regex operators and ``isin``.

    Each preserves the input fieldtype (scalar→scalar, array→array) and
    replaces the value type with the operator's fixed result type.
    """
    return Schema(fieldtype=input_fieldtype, columns={"value": ColumnInfo(result_type)})


def _preview_agg_schema(input_value_type: str, agg_func: str) -> Schema:
    """Sync preview of the scalar Schema ``operators._apply_aggregation`` will produce.

    All seven simple aggregations (min/max/sum/mean/std/var/count) produce a
    scalar, non-nullable result. The value type follows ClickHouse promotion
    rules encoded in ``_determine_agg_result_type``.
    """
    value_type = _determine_agg_result_type(agg_func, input_value_type)
    return Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo(value_type)})


def _preview_count_if_schema(condition: str | dict[str, str]) -> Schema:
    """Sync preview of ``operators.count_if_agg``'s result Schema.

    A ``str`` condition produces a scalar UInt64. A ``dict`` produces a dict
    Object with one UInt64 column per entry, names taken from the dict keys.
    """
    if isinstance(condition, str):
        return Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo("UInt64")})
    columns = {name: ColumnInfo("UInt64") for name in condition}
    return Schema(fieldtype=FIELDTYPE_DICT, columns=columns)


def _preview_quantile_schema() -> Schema:
    """Sync preview of ``operators.quantile_agg`` — always scalar Float64."""
    return Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo("Float64")})


def _preview_unique_schema(input_value_type: str, input_nullable: bool) -> Schema:
    """Sync preview of ``operators.unique_group`` — array of source value type."""
    return Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"value": ColumnInfo(input_value_type, nullable=input_nullable)},
    )


def _preview_nunique_schema() -> Schema:
    """Sync preview of ``operators.nunique_agg`` — scalar UInt64 count."""
    return Schema(fieldtype=FIELDTYPE_SCALAR, columns={"value": ColumnInfo("UInt64")})


def _preview_operator_schema(schema_a: Schema, schema_b: Schema, operator: str) -> Schema:
    """Sync preview of the Schema that ``_apply_operator_db`` will produce.

    Thin wrapper that adapts ``Schema`` operands to ``_compute_operator_schema``
    — the actual promotion/aai_id logic lives there so preview can't drift
    from materialize.
    """
    col_a = schema_a.columns.get("value")
    col_b = schema_b.columns.get("value")
    schema, _ = _compute_operator_schema(
        fieldtype_a=schema_a.fieldtype,
        fieldtype_b=schema_b.fieldtype,
        type_a=col_a.type if col_a is not None else "Float64",
        type_b=col_b.type if col_b is not None else "Float64",
        nullable_a=col_a.nullable if col_a is not None else False,
        nullable_b=col_b.nullable if col_b is not None else False,
        aai_id_a=schema_a.columns.get(AAI_ID_COLUMN),
        aai_id_b=schema_b.columns.get(AAI_ID_COLUMN),
        operator=operator,
    )
    return schema


# String/regex operator key → (SQL expression template, result type) — single
# source of truth shared between ``_preview_fixed_type_schema`` (preview) and
# ``operators._apply_string_op_db`` (materialize). ``{pattern}`` and
# ``{replacement}`` are substituted with SQL-escaped string literals at
# materialize time.
class StringOp(NamedTuple):
    expression: str
    result_type: str


STRING_OPS: dict[str, StringOp] = {
    "match": StringOp("match(a.value, {pattern})", "UInt8"),
    "like": StringOp("a.value LIKE {pattern}", "UInt8"),
    "ilike": StringOp("a.value ILIKE {pattern}", "UInt8"),
    "extract": StringOp("extract(a.value, {pattern})", "String"),
    "replace": StringOp("replaceRegexpAll(a.value, {pattern}, {replacement})", "String"),
}


def _compute_coalesce_schema(
    *,
    fieldtype_a: str,
    fieldtype_b: str,
    type_a: str,
    nullable_a: bool,
    nullable_b: bool,
) -> Schema:
    """Result Schema for ``coalesce(a, b)``.

    The value type follows the first operand; the result is nullable only when
    both operands are (a non-nullable fallback always supplies a value). Shared
    between ``Object.coalesce`` (preview) and ``operators.coalesce_op``
    (materialize) so the two cannot drift.
    """
    either_is_array = FIELDTYPE_ARRAY in (fieldtype_a, fieldtype_b)
    return Schema(
        fieldtype=FIELDTYPE_ARRAY if either_is_array else FIELDTYPE_SCALAR,
        columns={"value": ColumnInfo(type_a, nullable=nullable_a and nullable_b)},
    )


def _compute_array_map_schema(
    *,
    operator: str,
    type_a: str,
    type_b: str,
    nullable_a: bool,
    nullable_b: bool,
) -> Schema:
    """Result Schema for ``array_map(a, b, operator)`` — always an array.

    Comparison operators yield UInt8; the rest follow the arithmetic promotion
    rules. Shared between ``Object.array_map`` (preview) and
    ``operators.array_map_db`` (materialize).
    """
    value_type = "UInt8" if operator in _COMPARISON_OPS else _promote_arithmetic_type(operator, type_a, type_b)
    return Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"value": ColumnInfo(value_type, nullable=nullable_a or nullable_b)},
    )
