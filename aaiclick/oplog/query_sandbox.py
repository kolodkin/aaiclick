"""
aaiclick.oplog.query_sandbox - Read-only, graph-scoped SQL for lineage triage.

The MCP ``query_table`` tool runs every ``SELECT`` through this sandbox.
A query is accepted only when it is a single read-only statement and every
table it reads belongs to the caller's scope — ClickHouse parses the SQL
and the scope check reads table references off the parse tree. Results are
row-capped and execution-time-capped. A refused query raises
``SandboxError``; ``internal_api.lineage`` maps it to ``Invalid``.

See ``docs/designs/lineage.md`` for the design.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, Field

from aaiclick.data.data_context import get_ch_client
from aaiclick.data.data_context.ch_client import DEFAULT_MAX_EXECUTION_TIME, query_text
from aaiclick.data.sql_utils import (
    FORBIDDEN_KEYWORDS_RE,
    normalize_sql_for_scan,
    quote_identifier,
    quote_sql_literal,
)
from aaiclick.data.view_models import ColumnSchema

logger = logging.getLogger(__name__)

SandboxErrorKind = Literal["not_select", "out_of_scope", "invalid_argument"]


class SandboxError(Exception):
    """A query the sandbox refuses; ``kind`` names the rule it broke."""

    def __init__(self, kind: SandboxErrorKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class TableSchema(BaseModel):
    """Table schema returned by ``describe_table`` / ``get_table_schema``."""

    table: str
    columns: list[ColumnSchema] = Field(default_factory=list)


class QueryResult(BaseModel):
    """Result of a sandboxed read-only SELECT.

    ``rows`` is ``list[list[Any]]`` rather than ``list[tuple]`` so the type
    serializes cleanly through the MCP/REST surfaces (JSON arrays of arrays).
    """

    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    truncated: bool = False


DEFAULT_ROW_LIMIT = 100
ROW_LIMIT_CEILING = 1000

_AST_TABLE_EXPRESSION = "TableExpression"
_AST_TABLE_IDENTIFIER = "TableIdentifier "
_AST_IDENTIFIER = "Identifier "
_AST_FUNCTION = "Function "
_AST_SUBQUERY = "Subquery"
# A SETTINGS clause, at any depth, parses to this node.
_AST_SETTINGS = "Set"
# Every IN-family function ClickHouse exposes — in, notIn, globalIn,
# nullIn, globalNotNullIn, inIgnoreSet, … — reads a table named on its right.
_IN_FUNCTION_RE = re.compile(r"^(global)?(not)?(null)?in(ignoreset)?$", re.IGNORECASE)
_STATEMENT_START_RE = re.compile(r"^\s*(?:WITH\b|SELECT\b)", re.IGNORECASE)


def validate_select_safety(sql: str) -> None:
    """Raise unless ``sql`` is a single read-only ``SELECT`` (or ``WITH … SELECT``)."""
    scan = normalize_sql_for_scan(sql)
    # Any terminator: a second statement, or a trailing ``;`` that chdb's
    # appended SETTINGS clause would turn into a syntax error.
    if ";" in scan:
        raise SandboxError("not_select", "Only a single SELECT statement, without a trailing semicolon, is allowed.")
    if not _STATEMENT_START_RE.match(scan):
        raise SandboxError("not_select", "Only SELECT (or WITH … SELECT) is permitted.")
    if FORBIDDEN_KEYWORDS_RE.search(scan):
        raise SandboxError("not_select", "DDL/DML keywords are rejected; only SELECT is permitted.")


async def _explain_ast(sql: str) -> list[str]:
    """Lines of ``sql``'s ``EXPLAIN AST`` dump — a parse, never an evaluation.

    Fetched with no format so the driver appends none: a ``FORMAT`` after an
    ``EXPLAIN`` binds to the explained query and shows up in its AST. A
    ``SELECT * FROM (EXPLAIN AST …)`` wrapper would keep it outside, but lets
    ``sql`` close the parenthesis and run its own statement.
    """
    return (await query_text(f"EXPLAIN AST {sql}")).splitlines()


class _AstRow(NamedTuple):
    indent: int
    text: str


def _ast_rows(ast_lines: Iterable[str]) -> list[_AstRow]:
    """``EXPLAIN AST`` is an indented tree, one node per row, one space per level."""
    return [_AstRow(len(line) - len(line.lstrip(" ")), line.strip()) for line in ast_lines]


def _node_name(text: str) -> str:
    """Drop the printer's trailing ``(alias a)`` / ``(children N)`` annotations."""
    return text.split(" (")[0]


def _direct_children(rows: list[_AstRow], index: int) -> list[int]:
    """Indexes of the rows one level below ``rows[index]``."""
    indent = rows[index].indent
    children = []
    for child_index in range(index + 1, len(rows)):
        child_indent = rows[child_index].indent
        if child_indent <= indent:
            break
        if child_indent == indent + 1:
            children.append(child_index)
    return children


@dataclass
class _TableReads:
    """Everything in a parse tree that reads a table, plus the one clause that lifts the caps."""

    tables: set[str] = field(default_factory=set)
    """``TableIdentifier`` names in table position."""
    functions: set[str] = field(default_factory=set)
    """Table functions in table position — ``merge``, ``remote``, ``url``, …"""
    opaque: set[str] = field(default_factory=set)
    """Table expressions of a kind this walk does not recognize."""
    in_tables: set[str] = field(default_factory=set)
    """Bare identifiers on the right of an IN-family function.

    ``expr IN name`` reads a table without a table position: the name is an
    ``Identifier`` operand, never a ``TableExpression``. ClickHouse reads it
    as a table when one exists by that name and as an array column otherwise;
    the parse tree cannot tell the two apart, so each is treated as a table.
    """
    has_settings: bool = False


def _table_reads(rows: list[_AstRow]) -> _TableReads:
    """One pass over an ``EXPLAIN AST`` dump.

    A ``TableExpression`` is what sits in table position, and its single child
    (the next row) says which kind it is: ``TableIdentifier <name>``,
    ``Subquery``, or ``Function <name>`` for a table function.
    """
    reads = _TableReads()
    for index, row in enumerate(rows):
        name = _node_name(row.text)
        if name == _AST_SETTINGS:
            reads.has_settings = True
        elif name == _AST_TABLE_EXPRESSION:
            if index + 1 == len(rows) or rows[index + 1].indent != row.indent + 1:
                continue
            child = _node_name(rows[index + 1].text)
            if child.startswith(_AST_TABLE_IDENTIFIER):
                reads.tables.add(child.removeprefix(_AST_TABLE_IDENTIFIER))
            elif child.startswith(_AST_FUNCTION):
                reads.functions.add(child.removeprefix(_AST_FUNCTION))
            elif not child.startswith(_AST_SUBQUERY):
                reads.opaque.add(child)
        elif name.startswith(_AST_FUNCTION) and _IN_FUNCTION_RE.match(name.removeprefix(_AST_FUNCTION)):
            for operands in _direct_children(rows, index):
                args = _direct_children(rows, operands)
                if len(args) == 2 and rows[args[1]].text.startswith(_AST_IDENTIFIER):
                    reads.in_tables.add(_node_name(rows[args[1]].text).removeprefix(_AST_IDENTIFIER))
    return reads


async def validate_scope(sql: str, scope_tables: set[str]) -> None:
    """Raise unless every table ``sql`` reads is in ``scope_tables``.

    ClickHouse parses the SQL (``EXPLAIN AST``) and this reads the table
    positions off the tree, so the check is positive: anything in table
    position that is not a known in-scope table is rejected, including table
    functions such as ``merge`` / ``remote`` / ``url`` / ``file``, which name
    their targets in string literals rather than as identifiers.

    Parsing with the engine that will run the query is deliberate — a
    second-guessing parser that disagreed with ClickHouse would be a bypass.

    The right-hand side of ``IN`` is held to the same check — see
    ``_TableReads.in_tables`` for why an array column is rejected there too.

    A CTE name is a table identifier no graph contains, so ``WITH`` queries are
    rejected; the ``query_table`` tool description tells the caller to write
    the CTE as a subquery in ``FROM``, which parses to a ``Subquery`` and is
    allowed.

    A ``SETTINGS`` clause is rejected at any depth: ``readonly=2`` permits
    settings changes, and a clause in the text outranks the caps
    ``run_select`` sends beside the query.
    """
    try:
        reads = _table_reads(_ast_rows(await _explain_ast(sql)))
    except Exception as exc:
        logger.debug("EXPLAIN AST failed for sandboxed SQL", exc_info=True)
        raise SandboxError("invalid_argument", f"Could not parse SQL: {exc}") from exc

    if reads.has_settings:
        raise SandboxError("invalid_argument", "A SETTINGS clause is not permitted; the tool sets the execution caps.")
    if reads.functions:
        listed = ", ".join(sorted(reads.functions))
        raise SandboxError("out_of_scope", f"Table functions are not permitted: {listed}.")

    # Fail closed: an opaque table expression is not provably in scope.
    in_unknown = reads.in_tables - scope_tables
    unknown = (reads.tables - scope_tables) | reads.opaque | in_unknown
    if unknown:
        listed = ", ".join(sorted(unknown)[:3])
        hint = " IN <identifier> reads a table; for an array column use has(column, value)." if in_unknown else ""
        raise SandboxError("out_of_scope", f"Tables not in scope: {listed}.{hint}")


async def run_select(sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult:
    """Execute a (pre-validated) ``SELECT`` with row + execution-time caps.

    The row cap is the ``limit`` setting rather than a ``LIMIT`` spliced into
    the SQL: it caps the outermost query and yields to a smaller ``LIMIT`` of
    the query's own. clickhouse-connect sends settings as parameters; chdb
    appends them as a ``SETTINGS`` clause on a new line, so a trailing
    ``--`` comment cannot swallow them (a trailing ``;`` would break them,
    which is why ``validate_select_safety`` rejects it).
    """
    row_limit = max(1, min(row_limit, ROW_LIMIT_CEILING))

    ch_client = get_ch_client()
    result = await ch_client.query(
        sql,
        settings={
            "max_execution_time": DEFAULT_MAX_EXECUTION_TIME,
            # One past the limit so truncation is detectable below.
            "limit": row_limit + 1,
            # ``limit`` applies per branch of a UNION; the ceiling still bounds
            # the whole result, and ``break`` truncates instead of failing.
            "max_result_rows": ROW_LIMIT_CEILING + 1,
            "result_overflow_mode": "break",
            # Enforce read-only in the engine, so validate_select_safety's
            # keyword regex is a first line rather than the only one. Level 2
            # rather than 1 because 1 also forbids the settings set alongside it.
            "readonly": 2,
            "allow_ddl": 0,
        },
    )

    rows = [list(r) for r in result.result_rows]
    truncated = len(rows) > row_limit
    if truncated:
        rows = rows[:row_limit]
    return QueryResult(columns=list(result.column_names), rows=rows, truncated=truncated)


async def sandboxed_select(sql: str, scope_tables: set[str], row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult:
    """``validate_select_safety`` → ``validate_scope`` → ``run_select``; raises ``SandboxError``."""
    validate_select_safety(sql)
    await validate_scope(sql, scope_tables)
    return await run_select(sql, row_limit)


async def describe_table(table: str) -> TableSchema:
    """Run ``DESCRIBE TABLE`` and return a ``TableSchema``.

    Raises whatever the ClickHouse client raises if the table is missing —
    ``internal_api.lineage`` translates that to ``NotFound``.
    """
    ch_client = get_ch_client()
    result = await ch_client.query(f"DESCRIBE TABLE {quote_identifier(table)}")
    columns = [ColumnSchema(name=row[0], type=row[1]) for row in result.result_rows]
    return TableSchema(table=table, columns=columns)


async def liveness(tables: set[str]) -> dict[str, bool]:
    """One round-trip to ClickHouse: which of these tables currently exist?

    Persistent (``p_*``) tables are treated as always live at the spec
    level but we still verify to catch schema drift.
    """
    if not tables:
        return {}
    ch_client = get_ch_client()
    quoted = ", ".join(quote_sql_literal(t) for t in tables)
    result = await ch_client.query(
        f"SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ({quoted})"
    )
    alive = {row[0] for row in result.result_rows}
    return {t: t in alive for t in tables}
