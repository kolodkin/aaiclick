"""Computed column helper functions for common ClickHouse transformations."""


from ..models import Computed


def _escape_sql_string(value: str) -> str:
    """Escape a Python string for use as a single-quoted SQL literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def literal(value: str | int | float | bool, ch_type: str) -> Computed:
    """Create a Computed column with a constant SQL literal.

    Args:
        value: Python value to embed as a SQL literal.
        ch_type: ClickHouse type for the column, e.g. "String", "UInt8", "Float64".

    Examples:
        literal("dataset_a", "String")    # Computed("String", "'dataset_a'")
        literal(42, "UInt32")              # Computed("UInt32", "42")
        literal(3.14, "Float64")           # Computed("Float64", "3.14")
        literal(True, "UInt8")             # Computed("UInt8", "true")
    """
    if isinstance(value, bool):
        expr = "true" if value else "false"
    elif isinstance(value, str):
        expr = _escape_sql_string(value)
    elif isinstance(value, (int, float)):
        expr = str(value)
    else:
        raise TypeError(f"Unsupported literal type: {type(value).__name__}")
    return Computed(ch_type, expr)


def cast(col: str, to_type: str, nullable: bool = True) -> Computed:
    """Create a Computed column casting col to a ClickHouse type.

    Uses to{Type}OrNull (Nullable result) when nullable=True (default),
    or to{Type} (raises on invalid input) when nullable=False.

    Args:
        col: Source column name.
        to_type: Target ClickHouse base type, e.g. "UInt32", "Float64", "Date".
        nullable: Wrap result in Nullable and use OrNull variant (default True).

    Examples:
        cast("start_year", "UInt32")              # Nullable(UInt32) via toUInt32OrNull
        cast("price", "Float64")                   # Nullable(Float64) via toFloat64OrNull
        cast("age", "UInt8", nullable=False)       # UInt8 via toUInt8
    """
    if nullable:
        return Computed(f"Nullable({to_type})", f"to{to_type}OrNull({col})")
    return Computed(to_type, f"to{to_type}({col})")


def split_by_char(col: str, separator: str, element_type: str = "String") -> Computed:
    """Create a Computed Array column by splitting col on a character separator.

    Uses ClickHouse's splitByChar(sep, col) function.

    Args:
        col: Source string column name.
        separator: Single character to split on.
        element_type: ClickHouse type for array elements (default "String").

    Examples:
        split_by_char("genres", ",")
        split_by_char("tags", ",", element_type="LowCardinality(String)")
    """
    sep_escaped = _escape_sql_string(separator)
    return Computed(f"Array({element_type})", f"splitByChar({sep_escaped}, {col})")
