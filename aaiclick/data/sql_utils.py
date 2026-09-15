"""
aaiclick.data.sql_utils - SQL utility functions for safe identifier and literal handling.
"""

import re
from datetime import datetime, timezone

# --- SQL text guards (shared by the lineage agent tools and the viewer) ---

COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
# Single-quoted SQL string literal with '' or \' escape handling.
STRING_LITERAL_RE = re.compile(r"'(?:\\.|''|[^'\\])*'", re.DOTALL)
FORBIDDEN_KEYWORDS_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|RENAME|ATTACH|"
    r"DETACH|OPTIMIZE|GRANT|REVOKE|USE|SET|SYSTEM|KILL|REPLACE|EXCHANGE)\b",
    re.IGNORECASE,
)
# A WHERE expression has no table position unless it opens a subquery; refusing
# these keywords is what keeps an object-scoped query inside its object.
SUBQUERY_KEYWORDS_RE = re.compile(r"\b(SELECT|FROM|JOIN|UNION|WITH)\b", re.IGNORECASE)


def normalize_sql_for_scan(sql: str) -> str:
    """Strip comments and string literals so keyword / scope regex passes don't
    trip on them — ``WHERE event = 'INSERT'`` must not look like DML."""
    return STRING_LITERAL_RE.sub("''", COMMENT_RE.sub(" ", sql))


def validate_where_expression(expr: str) -> str | None:
    """The reason ``expr`` is not a single boolean expression, or ``None``.

    Rejects statement separators, DDL/DML keywords, and any subquery keyword.
    """
    scan = normalize_sql_for_scan(expr)
    if ";" in scan:
        return "Only a single expression is allowed."
    if FORBIDDEN_KEYWORDS_RE.search(scan):
        return "DDL/DML keywords are rejected in a where expression."
    if SUBQUERY_KEYWORDS_RE.search(scan):
        return "Subqueries are not allowed in a where expression."
    return None


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


def naive_utc(dt: datetime) -> datetime:
    """Coerce a datetime to the storage convention — naive UTC."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def sql_literal(value: bool | int | float | str | datetime) -> str:
    """Render a Python scalar as an untyped ClickHouse literal.

    Datetimes become a naive-UTC ``'YYYY-MM-DD HH:MM:SS.ffffff'`` string;
    wrap the result in a CAST when the column type matters.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return quote_sql_literal(naive_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f"))
    if isinstance(value, str):
        return quote_sql_literal(value)
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")
