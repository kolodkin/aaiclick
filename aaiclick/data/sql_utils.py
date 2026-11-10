"""
aaiclick.data.sql_utils - SQL utility functions for safe identifier and literal handling.
"""

import re
from datetime import UTC, datetime
from urllib.parse import urlparse

# --- SQL text guards (shared by the lineage query sandbox and the viewer) ---

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
# Any quoted token — string literal or backtick/double-quoted identifier —
# matched in one left-to-right pass so a quote inside one cannot open another.
QUOTED_TOKEN_RE = re.compile(STRING_LITERAL_RE.pattern + r"|`(?:\\.|``|[^`\\])*`|\"(?:\\.|\"\"|[^\"\\])*\"", re.DOTALL)
# ClickHouse comment openers: ``--``, ``/*`` and ``#``.
COMMENT_OPENER_RE = re.compile(r"--|/\*|#")


def normalize_sql_for_scan(sql: str) -> str:
    """Strip comments and string literals so keyword / scope regex passes don't
    trip on them — ``WHERE event = 'INSERT'`` must not look like DML."""
    return STRING_LITERAL_RE.sub("''", COMMENT_RE.sub(" ", sql))


def validate_where_expression(expr: str) -> str | None:
    """The reason ``expr`` is not a single boolean expression, or ``None``.

    The expression is spliced in as ``WHERE (<expr>)``, so it must not close
    that parenthesis or comment out what follows it. Rejects comments,
    unbalanced parentheses, statement separators, DDL/DML keywords, and any
    subquery keyword.
    """
    bare = QUOTED_TOKEN_RE.sub(" 0 ", expr)
    if any(quote in bare for quote in "'`\""):
        return "Unterminated quote in a where expression."
    if COMMENT_OPENER_RE.search(bare):
        return "Comments are not allowed in a where expression."
    depth = 0
    for char in bare:
        depth += {"(": 1, ")": -1}.get(char, 0)
        if depth < 0:
            break
    if depth != 0:
        return "Unbalanced parentheses in a where expression."
    if ";" in bare:
        return "Only a single expression is allowed."
    if FORBIDDEN_KEYWORDS_RE.search(bare):
        return "DDL/DML keywords are rejected in a where expression."
    if SUBQUERY_KEYWORDS_RE.search(bare):
        return "Subqueries are not allowed in a where expression."
    return None


BLOB_URL_SCHEMES = ("http", "https", "s3")


def is_url(path: str) -> bool:
    """True when *path* names a URL rather than a local file."""
    return "://" in path


def validate_blob_url(url: str) -> None:
    """Reject a URL the ClickHouse ``s3`` function / ``S3`` engine cannot address."""
    parsed = urlparse(url)
    if parsed.scheme not in BLOB_URL_SCHEMES:
        raise ValueError(f"Blob URL must use http, https or s3 scheme, got '{parsed.scheme}'")
    if not parsed.netloc:
        raise ValueError("Blob URL must have a valid host or bucket")
    # The URL is rendered into SQL and stored in the registry, so it must not
    # be able to carry a secret: no userinfo, no (possibly signed) query string.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Blob URL must not carry credentials; ClickHouse resolves them itself")
    if parsed.query:
        raise ValueError("Blob URL must not carry a query string")


def quote_identifier(name: str) -> str:
    """Backtick-quote a ClickHouse identifier, escaping internal backticks."""
    return f"`{name.replace('`', '``')}`"


def quote_sql_literal(value: str) -> str:
    """Escape a Python string and wrap it as a single-quoted SQL literal.

    Handles both backslashes and single quotes, so it is safe for arbitrary
    user input inlined into SQL text. The only string-literal helper: a
    quote-only escape leaves a trailing backslash to neutralise the closing
    quote.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def naive_utc(dt: datetime) -> datetime:
    """Coerce a datetime to the storage convention — naive UTC."""
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


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
