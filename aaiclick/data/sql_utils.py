"""
aaiclick.data.sql_utils - SQL utility functions for safe identifier handling.
"""


def quote_identifier(name: str) -> str:
    """Backtick-quote a ClickHouse identifier, escaping internal backticks."""
    return f"`{name.replace('`', '``')}`"


def escape_sql_string(value: str) -> str:
    """Escape a string literal for inlining into a single-quoted context.

    Replaces the ``s.replace("'", "\\'")`` pattern hand-rolled at many
    call sites. Does NOT add the surrounding quotes — callers wrap the
    result themselves so the helper stays usable for both
    ``'...'`` literals and backtick-wrapped identifiers.

    Only handles single quotes. For values that may contain backslashes
    (chdb settings, arbitrary user input), prefer parameter binding or
    wrap with an additional ``s.replace("\\", "\\\\")`` call.
    """
    return value.replace("'", "\\'")


def quote_sql_literal(value: str) -> str:
    """Escape a Python string and wrap it as a single-quoted SQL literal.

    Handles both backslashes and single quotes, so it is safe for
    arbitrary user input inlined into SQL text.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
