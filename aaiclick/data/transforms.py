"""Computed column helper functions for common ClickHouse transformations."""

from .models import Computed


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
    escaped = separator.replace("'", "\\'")
    return Computed(f"Array({element_type})", f"splitByChar('{escaped}', {col})")
