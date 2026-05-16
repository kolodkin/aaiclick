"""Sync schema-computation helpers for binary operator results.

Neutral module: imports only from ``..models`` and
``..data_context.data_context``. Both ``object.py`` (for ``LazyOperator``
planning) and ``operators.py`` (for materialize-time) depend on these
helpers — keeping them here avoids a circular import between the two.
"""

from __future__ import annotations

from ..data_context.data_context import _infer_clickhouse_type
from ..models import (
    AAI_ID_COLUMN,
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
    FLOAT_TYPES,
    ColumnInfo,
    Schema,
    ValueScalarType,
)


# Division and power always return Float64 in ClickHouse.
_FLOAT_RESULT_OPS = frozenset({"/", "**"})

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
    value_type = _promote_arithmetic_type(operator, type_a, type_b)
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
