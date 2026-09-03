"""Inline SQL literals for the Python operands of a ``LazyOperator``.

``obj + 3`` and ``obj.isin([1, 2])`` carry a Python value as an operand. Rather
than loading it into a one-row / N-row ClickHouse table, the operand becomes a
constant subquery — ``(SELECT CAST(3 AS Int64) AS value)`` — inside the
operator SQL, saving a CREATE TABLE + INSERT round-trip. The declared type is
the one ``create_object_from_value`` would infer, so result schemas match the
sync preview exactly.
"""

from datetime import datetime, timezone

from ..data_context.data_context import _infer_clickhouse_type
from ..models import FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, QueryInfo, ValueScalarType
from ..sql_utils import quote_sql_literal

# Lists longer than this materialize as a table instead: the inlined SQL text
# grows with the list, and ClickHouse caps query size (``max_query_size``).
LITERAL_LIST_MAX = 1000

# ``QueryInfo.base_table`` of a literal operand — it has no table, and oplog
# lineage leaves it out.
LITERAL_BASE_TABLE = ""


def scalar_literal_sql(value: ValueScalarType) -> str:
    """Render a Python scalar as an untyped ClickHouse literal.

    Datetimes follow the storage convention — naive UTC — so a tz-aware value
    is converted before formatting.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        utc = value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
        return quote_sql_literal(utc.strftime("%Y-%m-%d %H:%M:%S.%f"))
    if isinstance(value, str):
        return quote_sql_literal(value)
    return repr(value)


def literal_query_info(value: ValueScalarType | list) -> QueryInfo | None:
    """Build the ``QueryInfo`` for a Python operand.

    Returns ``None`` when the value has to go through a table instead: a list
    longer than ``LITERAL_LIST_MAX`` or one containing ``None``.
    """
    if isinstance(value, list):
        if len(value) > LITERAL_LIST_MAX or any(v is None for v in value):
            return None
        value_type = _infer_clickhouse_type(value).type
        items = ", ".join(scalar_literal_sql(v) for v in value)
        source = f"(SELECT arrayJoin(CAST([{items}] AS Array({value_type}))) AS value)"
        fieldtype = FIELDTYPE_ARRAY
    else:
        value_type = _infer_clickhouse_type(value).type
        source = f"(SELECT CAST({scalar_literal_sql(value)} AS {value_type}) AS value)"
        fieldtype = FIELDTYPE_SCALAR
    return QueryInfo(
        source=source,
        base_table=LITERAL_BASE_TABLE,
        value_column="value",
        fieldtype=fieldtype,
        value_type=value_type,
    )
