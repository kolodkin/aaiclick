"""Inline SQL literals for the Python operands of a ``LazyOperator``.

``obj + 3`` and ``obj.isin([1, 2])`` carry a Python value as an operand. Rather
than loading it into a one-row / N-row ClickHouse table, the operand becomes a
constant subquery — ``(SELECT CAST(3 AS Int64) AS value)`` — inside the
operator SQL, saving a CREATE TABLE + INSERT round-trip. The declared type is
the one ``create_object_from_value`` would infer, and the sync preview reads
the same ``operand_type``, so result schemas match exactly.
"""

from ..data_context.data_context import _infer_clickhouse_type
from ..models import FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, QueryInfo, ValueScalarType
from ..sql_utils import sql_literal

# Lists longer than this materialize as a table instead: the inlined SQL text
# grows with the list, and ClickHouse caps query size (``max_query_size``).
LITERAL_LIST_MAX = 1000


def operand_type(value: ValueScalarType | list) -> tuple[str, str]:
    """``(fieldtype, value_type)`` a Python operand contributes to an operator.

    A list is an array operand, a scalar a scalar one, typed as
    ``create_object_from_value`` would infer.
    """
    fieldtype = FIELDTYPE_ARRAY if isinstance(value, list) else FIELDTYPE_SCALAR
    return fieldtype, _infer_clickhouse_type(value).type


def literal_query_info(value: ValueScalarType | list) -> QueryInfo | None:
    """Build the ``QueryInfo`` for a Python operand.

    Returns ``None`` when the value has to go through a table instead — a list
    longer than ``LITERAL_LIST_MAX`` or one containing ``None`` — and the
    caller falls back to ``create_object_from_value``.
    """
    if isinstance(value, list) and (len(value) > LITERAL_LIST_MAX or any(v is None for v in value)):
        return None
    fieldtype, value_type = operand_type(value)
    if isinstance(value, list):
        items = ", ".join(sql_literal(v) for v in value)
        source = f"(SELECT arrayJoin(CAST([{items}] AS Array({value_type}))) AS value)"
    else:
        source = f"(SELECT CAST({sql_literal(value)} AS {value_type}) AS value)"
    # A literal has no table: ``base_table`` stays empty, so ``same_table_as``
    # never matches it and oplog lineage leaves it out.
    return QueryInfo(source=source, base_table="", value_column="value", fieldtype=fieldtype, value_type=value_type)
