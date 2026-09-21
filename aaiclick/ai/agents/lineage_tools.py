"""
aaiclick.ai.agents.lineage_tools - Tier 1 agent tools scoped to a lineage graph.

All tools operate on a single ``OplogGraph`` — the backward lineage of the
target table being debugged. ``query_table`` is read-only and row-limited,
and every table it reads must be one the graph contains — ClickHouse parses
the SQL and the scope check reads table references off the parse tree.

See ``docs/designs/lineage.md`` for the design.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
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
from aaiclick.oplog.lineage import OplogGraph

logger = logging.getLogger(__name__)

NodeKind = Literal["input", "intermediate", "target"]

ToolErrorKind = Literal[
    "not_select",
    "out_of_scope",
    "not_found",
    "not_live",
    "invalid_argument",
]


class ToolError(NamedTuple):
    kind: ToolErrorKind
    message: str


class GraphNode(BaseModel):
    """Single node in the lineage graph with kind + liveness."""

    table: str
    kind: NodeKind
    operation: str
    live: bool
    task_id: int | None = None
    job_id: int | None = None


class TableSchema(BaseModel):
    """Table schema returned by ``get_schema`` / ``get_table_schema``."""

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

_AST_TABLE_IDENTIFIER = "TableIdentifier "
_AST_IDENTIFIER = "Identifier "
_AST_FUNCTION = "Function "
_AST_SUBQUERY = "Subquery"
# A SETTINGS clause, at any depth, parses to this node.
_AST_SETTINGS = "Set"
_AST_IN_FUNCTIONS = {f"{_AST_FUNCTION}{name}" for name in ("in", "notIn", "globalIn", "globalNotIn")}
_STATEMENT_START_RE = re.compile(r"^\s*(?:WITH\b|SELECT\b)", re.IGNORECASE)
_SEMICOLON_RE = re.compile(r";\s*\S")


def _target_tables(graph: OplogGraph) -> set[str]:
    """Nodes that no other node in the graph consumes."""
    consumed = {src for n in graph.nodes for src in n.kwargs.values() if src}
    return {n.table for n in graph.nodes if n.table not in consumed}


def _input_tables(graph: OplogGraph) -> set[str]:
    """Tables referenced as sources but never produced — plus any ``p_*`` node."""
    produced = {n.table for n in graph.nodes}
    referenced = {src for n in graph.nodes for src in n.kwargs.values() if src}
    inputs = referenced - produced
    inputs |= {n.table for n in graph.nodes if n.table.startswith("p_")}
    return inputs


def _classify_nodes(graph: OplogGraph) -> dict[str, NodeKind]:
    """Label every table in the graph as input / intermediate / target."""
    targets = _target_tables(graph)
    inputs = _input_tables(graph)
    kinds: dict[str, NodeKind] = {}
    for table in graph.tables:
        if table in inputs:
            kinds[table] = "input"
        elif table in targets:
            kinds[table] = "target"
        else:
            kinds[table] = "intermediate"
    return kinds


def validate_select_safety(sql: str, *, scan: str | None = None) -> ToolError | None:
    """Reject anything that isn't a single read-only ``SELECT`` (or ``WITH … SELECT``).

    Stateless. Pass ``scan`` to skip the comment + literal strip when the caller
    has already normalized the SQL.
    """
    if scan is None:
        scan = normalize_sql_for_scan(sql)
    if _SEMICOLON_RE.search(scan):
        return ToolError("not_select", "Only a single SELECT statement is allowed.")
    if not _STATEMENT_START_RE.match(scan):
        return ToolError("not_select", "Only SELECT (or WITH … SELECT) is permitted.")
    if FORBIDDEN_KEYWORDS_RE.search(scan):
        return ToolError("not_select", "DDL/DML keywords are rejected; only SELECT is permitted.")
    return None


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


def _table_expressions(rows: list[_AstRow]) -> list[str]:
    """The single child of every ``TableExpression`` node.

    A ``TableExpression`` is what sits in table position, and its child says
    which kind it is: ``TableIdentifier <name>``, ``Subquery``, or
    ``Function <name>`` for a table function.
    """
    return [
        rows[child].text
        for index, row in enumerate(rows)
        if row.text.startswith("TableExpression")
        for child in _direct_children(rows, index)[:1]
    ]


def _in_identifiers(rows: list[_AstRow]) -> list[str]:
    """Bare identifiers on the right-hand side of ``IN`` / ``NOT IN`` / ``GLOBAL IN``.

    ``expr IN name`` reads a table without a table position: the name is an
    ``Identifier`` operand, never a ``TableExpression``. ClickHouse reads it
    as a table when one exists by that name and as an array column otherwise;
    the parse tree cannot tell the two apart, so the caller treats each as a
    table reference.
    """
    names = []
    for index, row in enumerate(rows):
        if _node_name(row.text) not in _AST_IN_FUNCTIONS:
            continue
        for operands in _direct_children(rows, index):
            args = _direct_children(rows, operands)
            if len(args) == 2 and rows[args[1]].text.startswith(_AST_IDENTIFIER):
                names.append(_node_name(rows[args[1]].text).removeprefix(_AST_IDENTIFIER))
    return names


async def validate_scope(sql: str, scope_tables: set[str]) -> ToolError | None:
    """Reject unless every table ``sql`` reads is in ``scope_tables``.

    ClickHouse parses the SQL (``EXPLAIN AST``) and this reads the table
    positions off the tree, so the check is positive: anything in table
    position that is not a known in-scope table is rejected, including table
    functions such as ``merge`` / ``remote`` / ``url`` / ``file``, which name
    their targets in string literals rather than as identifiers.

    Parsing with the engine that will run the query is deliberate — a
    second-guessing parser that disagreed with ClickHouse would be a bypass.

    The right-hand side of ``IN`` is held to the same check — see
    ``_in_identifiers`` for why an array column is rejected there too.

    A CTE name is a table identifier no graph contains, so ``WITH`` queries are
    rejected. The tool description tells the model to write the CTE as a
    subquery in ``FROM``, which parses to a ``Subquery`` and is allowed.

    A ``SETTINGS`` clause is rejected at any depth: ``readonly=2`` permits
    settings changes, and a clause in the text outranks the caps
    ``run_select`` sends beside the query.
    """
    try:
        rows = _ast_rows(await _explain_ast(sql))
    except Exception as exc:
        logger.debug("EXPLAIN AST failed for agent SQL", exc_info=True)
        return ToolError("invalid_argument", f"Could not parse SQL: {exc}")

    if any(_node_name(row.text) == _AST_SETTINGS for row in rows):
        return ToolError("invalid_argument", "A SETTINGS clause is not permitted; the tool sets the execution caps.")

    unknown: set[str] = set()
    functions: set[str] = set()
    for child in _table_expressions(rows):
        if child.startswith(_AST_TABLE_IDENTIFIER):
            name = _node_name(child).removeprefix(_AST_TABLE_IDENTIFIER)
            if name not in scope_tables:
                unknown.add(name)
        elif child.startswith(_AST_FUNCTION):
            functions.add(_node_name(child).removeprefix(_AST_FUNCTION))
        elif not child.startswith(_AST_SUBQUERY):
            # Fail closed: an unrecognized table expression is not provably in scope.
            unknown.add(_node_name(child))
    if functions:
        listed = ", ".join(sorted(functions))
        return ToolError("out_of_scope", f"Table functions are not permitted: {listed}.")

    in_unknown = {name for name in _in_identifiers(rows) if name not in scope_tables}
    unknown |= in_unknown
    if unknown:
        listed = ", ".join(sorted(unknown)[:3])
        hint = " IN <identifier> reads a table; for an array column use has(column, value)." if in_unknown else ""
        return ToolError("out_of_scope", f"Tables not in scope: {listed}.{hint}")
    return None


async def run_select(sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult:
    """Execute a (pre-validated) ``SELECT`` with row + execution-time caps.

    The row cap is the ``limit`` setting rather than text appended to the
    SQL: it caps the outermost query, yields to a smaller ``LIMIT`` of the
    query's own, and nothing appended can land inside a trailing comment.
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


async def describe_table(table: str) -> TableSchema:
    """Run ``DESCRIBE TABLE`` and return a ``TableSchema``.

    Raises whatever the ClickHouse client raises if the table is missing —
    callers translate to ``NotFound`` / ``ToolError`` per their layer.
    """
    ch_client = get_ch_client()
    result = await ch_client.query(f"DESCRIBE TABLE {quote_identifier(table)}")
    columns = [ColumnSchema(name=row[0], type=row[1]) for row in result.result_rows]
    return TableSchema(table=table, columns=columns)


async def _liveness(tables: set[str]) -> dict[str, bool]:
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


class LineageToolbox:
    """Scoped Tier 1 tool surface.

    Instantiate once per debug session with the backward lineage graph of
    the target table. A query is rejected unless every table it reads is in
    ``graph.tables``; table functions are rejected outright.
    """

    def __init__(self, graph: OplogGraph):
        self.graph = graph
        self._tables = graph.tables
        self._kinds = _classify_nodes(graph)
        self._node_by_table = {n.table: n for n in graph.nodes}
        self._liveness_cache: dict[str, bool] | None = None

    async def query_table(self, sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult | ToolError:
        """Execute a read-only SELECT against tables in the current graph.

        Composes the stateless ``validate_select_safety`` / ``validate_scope``
        / ``run_select`` helpers. The out-of-scope error gets a graph-flavored
        suffix so the LLM knows which tool to call to inspect the scope.
        """
        if not isinstance(row_limit, int) or isinstance(row_limit, bool):
            try:
                row_limit = int(row_limit)
            except (TypeError, ValueError):
                return ToolError("invalid_argument", f"row_limit must be an integer, got {row_limit!r}.")
        if err := validate_select_safety(sql):
            return err
        if err := await validate_scope(sql, self._tables):
            if err.kind == "out_of_scope":
                err = err._replace(message=err.message + " Use list_graph_nodes() to see what's in scope.")
            return err
        return await run_select(sql, row_limit)

    async def get_op_sql(self, table: str) -> str | ToolError:
        """Rendered SQL template for the operation that produced ``table``."""
        node = self._node_by_table.get(table)
        if node is None:
            return ToolError("not_found", f"No operation in the graph produced {table}.")
        return node.sql_template or ""

    async def list_graph_nodes(self) -> list[GraphNode]:
        """Every table in the graph with kind + liveness.

        The liveness lookup is cached for the lifetime of the toolbox so the
        agent can re-call the tool (or both seed-context and tool call) without
        a second round-trip to ``system.tables``.
        """
        if self._liveness_cache is None:
            self._liveness_cache = await _liveness(self._tables)
        liveness = self._liveness_cache
        nodes: list[GraphNode] = []
        for table in sorted(self._tables):
            node = self._node_by_table.get(table)
            operation = node.operation if node else "(input)"
            task_id = node.task_id if node else None
            job_id = node.job_id if node else None
            nodes.append(
                GraphNode(
                    table=table,
                    kind=self._kinds[table],
                    operation=operation,
                    live=liveness.get(table, False),
                    task_id=task_id,
                    job_id=job_id,
                )
            )
        return nodes

    async def get_schema(self, table: str) -> TableSchema | ToolError:
        """Columns and types for a table in the graph."""
        if table not in self._tables:
            return ToolError("out_of_scope", f"{table} is not in the lineage graph.")
        try:
            return await describe_table(table)
        except Exception as exc:
            logger.exception("DESCRIBE TABLE %s failed", table)
            return ToolError("not_live", f"Could not describe {table}: {exc}")

    async def dispatch_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """Invoke a tool by name and format the result as LLM-readable text.

        Missing arguments, unknown tools, and in-tool ``ToolError`` results are
        all returned as strings so one bad call doesn't abort the loop — the
        model can read the error and retry.
        """
        handler = _TOOL_HANDLERS.get(name)
        if handler is None:
            return f"(unknown tool: {name})"
        try:
            result = await handler(self, arguments)
        except KeyError as exc:
            return f"(error calling {name}: missing required argument {exc})"
        except Exception as exc:
            logger.exception("tool %s raised an unexpected exception", name)
            return f"(error calling {name}: {exc})"
        return _format_tool_result(result)


_TOOL_HANDLERS: dict[str, Any] = {
    "query_table": lambda tb, a: tb.query_table(a["sql"], row_limit=a.get("row_limit", DEFAULT_ROW_LIMIT)),
    "get_op_sql": lambda tb, a: tb.get_op_sql(a["table"]),
    "list_graph_nodes": lambda tb, _a: tb.list_graph_nodes(),
    "get_schema": lambda tb, a: tb.get_schema(a["table"]),
}


def _format_tool_result(result: Any) -> str:
    """Serialize a tool's typed result as compact text for the LLM."""
    if isinstance(result, ToolError):
        return json.dumps({"error": {"kind": result.kind, "message": result.message}})
    if isinstance(result, QueryResult):
        if not result.rows and not result.columns:
            return "(empty result)"
        header = " | ".join(result.columns)
        body = "\n".join(" | ".join(str(v) for v in row) for row in result.rows)
        suffix = f"\n(truncated to {len(result.rows)} rows)" if result.truncated else ""
        return f"{header}\n{body}{suffix}" if body else f"{header}\n(no rows){suffix}"
    if isinstance(result, TableSchema):
        lines = [f"{c.name}: {c.type}" for c in result.columns]
        return f"`{result.table}`:\n" + "\n".join(lines) if lines else f"`{result.table}`: (no columns)"
    if isinstance(result, list):
        lines = []
        for n in result:
            lines.append(
                f"- {n.table} [{n.kind}] operation={n.operation} live={n.live} task_id={n.task_id} job_id={n.job_id}"
            )
        return "\n".join(lines) if lines else "(no nodes)"
    if isinstance(result, str):
        return result or "(empty)"
    return str(result)


LINEAGE_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_graph_nodes",
            "description": (
                "List every table in the current lineage graph with its kind "
                "(input / intermediate / target), the operation that produced it, "
                "and whether it currently exists in ClickHouse."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_op_sql",
            "description": (
                "Return the rendered SQL template for the operation that produced "
                "`table`. Use this first to form a hypothesis before running queries."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table in the graph"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": (
                "Return column names and types for a table in the graph. Rejects tables outside the graph."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table in the graph"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_table",
            "description": (
                "Execute a read-only SELECT against tables in the current lineage "
                "graph. Rejects non-SELECT, out-of-scope tables, table functions, and "
                "SETTINGS clauses; write a CTE as a subquery in FROM instead, and use "
                "has(column, value) rather than IN for an array column. Results are "
                "capped at row_limit."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SELECT statement"},
                    "row_limit": {
                        "type": "integer",
                        "description": f"Max rows returned (default {DEFAULT_ROW_LIMIT}, ceiling {ROW_LIMIT_CEILING})",
                    },
                },
                "required": ["sql"],
            },
        },
    },
]
