"""
aaiclick.ai.agents.lineage_tools - Tier 1 agent tools scoped to a lineage graph.

All tools operate on a single ``OplogGraph`` — the backward lineage of the
target table being debugged. Scope enforcement prevents accidental
cross-job queries; ``query_table`` is read-only and row-limited.

See ``docs/lineage.md`` for the design and ``docs/lineage_implementation_plan.md``
for the rollout plan (Phase 1).
"""

from __future__ import annotations

import logging
import re
from typing import Literal, NamedTuple

from aaiclick.data.data_context import get_ch_client
from aaiclick.data.sql_utils import escape_sql_string, quote_identifier
from aaiclick.oplog.lineage import OplogGraph

logger = logging.getLogger(__name__)

NodeKind = Literal["input", "intermediate", "target"]

ToolErrorKind = Literal[
    "not_select",
    "out_of_scope",
    "not_found",
    "not_live",
]


class ToolError(NamedTuple):
    kind: ToolErrorKind
    message: str


class GraphNode(NamedTuple):
    table: str
    kind: NodeKind
    operation: str
    live: bool
    task_id: int | None
    job_id: int | None


class ColumnSchema(NamedTuple):
    name: str
    type: str


class TableSchema(NamedTuple):
    table: str
    columns: list[ColumnSchema]


class QueryResult(NamedTuple):
    columns: list[str]
    rows: list[tuple]
    truncated: bool


DEFAULT_ROW_LIMIT = 100
ROW_LIMIT_CEILING = 1000
DEFAULT_MAX_EXECUTION_TIME = 30

_TABLE_REF_RE = re.compile(r"\bt_\d{16,20}\b|\bp_[A-Za-z_][A-Za-z0-9_]*\b")
_STATEMENT_START_RE = re.compile(r"^\s*(?:WITH\b|SELECT\b)", re.IGNORECASE)
_FORBIDDEN_KEYWORDS_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|RENAME|ATTACH|"
    r"DETACH|OPTIMIZE|GRANT|REVOKE|USE|SET|SYSTEM|KILL|REPLACE|EXCHANGE)\b",
    re.IGNORECASE,
)
_LIMIT_RE = re.compile(r"\bLIMIT\b\s+\d+", re.IGNORECASE)
_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_SEMICOLON_RE = re.compile(r";\s*\S")


def _strip_comments(sql: str) -> str:
    return _COMMENT_RE.sub(" ", sql)


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


async def _liveness(tables: set[str]) -> dict[str, bool]:
    """One round-trip to ClickHouse: which of these tables currently exist?

    Persistent (``p_*``) tables are treated as always live at the spec
    level but we still verify to catch schema drift.
    """
    if not tables:
        return {}
    ch_client = get_ch_client()
    quoted = ", ".join(f"'{escape_sql_string(t)}'" for t in tables)
    result = await ch_client.query(
        f"SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ({quoted})"
    )
    alive = {row[0] for row in result.result_rows}
    return {t: t in alive for t in tables}


class LineageToolbox:
    """Scoped Tier 1 tool surface.

    Instantiate once per debug session with the backward lineage graph of
    the target table. All queries are rejected if they reference tables
    outside ``graph.tables``.
    """

    def __init__(self, graph: OplogGraph):
        self.graph = graph
        self._tables = graph.tables
        self._kinds = _classify_nodes(graph)
        self._node_by_table = {n.table: n for n in graph.nodes}

    async def query_table(
        self, sql: str, row_limit: int = DEFAULT_ROW_LIMIT
    ) -> QueryResult | ToolError:
        """Execute a read-only SELECT against tables in the current graph.

        - Rejects anything that isn't a ``SELECT`` / ``WITH ... SELECT``
        - Rejects if any ``t_*`` / ``p_*`` token references a table outside
          the graph
        - Wraps in ``LIMIT row_limit + 1`` when no ``LIMIT`` is present so
          truncation can be reported
        - Pins ``max_execution_time`` and ``max_result_rows`` so an
          accidental scan can't tie up the cluster
        """
        stripped = _strip_comments(sql).strip()
        if _SEMICOLON_RE.search(stripped):
            return ToolError("not_select", "Only a single SELECT statement is allowed.")
        if not _STATEMENT_START_RE.match(stripped):
            return ToolError(
                "not_select",
                "query_table accepts only SELECT (or WITH ... SELECT) statements.",
            )
        if _FORBIDDEN_KEYWORDS_RE.search(stripped):
            return ToolError(
                "not_select",
                "query_table rejects DDL/DML keywords; only SELECT is permitted.",
            )

        referenced = set(_TABLE_REF_RE.findall(stripped))
        unknown = referenced - self._tables
        if unknown:
            sample = ", ".join(sorted(unknown)[:3])
            return ToolError(
                "out_of_scope",
                f"Tables not in the lineage graph: {sample}. "
                f"Use list_graph_nodes() to see what's in scope.",
            )

        row_limit = max(1, min(row_limit, ROW_LIMIT_CEILING))
        effective_sql = sql if _LIMIT_RE.search(stripped) else f"{sql.rstrip().rstrip(';')} LIMIT {row_limit + 1}"

        ch_client = get_ch_client()
        result = await ch_client.query(
            effective_sql,
            settings={
                "max_execution_time": DEFAULT_MAX_EXECUTION_TIME,
                "max_result_rows": ROW_LIMIT_CEILING + 1,
            },
        )

        rows = [tuple(r) for r in result.result_rows]
        truncated = len(rows) > row_limit
        if truncated:
            rows = rows[:row_limit]
        return QueryResult(columns=list(result.column_names), rows=rows, truncated=truncated)

    async def get_op_sql(self, table: str) -> str | ToolError:
        """Rendered SQL template for the operation that produced ``table``."""
        node = self._node_by_table.get(table)
        if node is None:
            return ToolError("not_found", f"No operation in the graph produced {table}.")
        return node.sql_template or ""

    async def list_graph_nodes(self) -> list[GraphNode]:
        """Every table in the graph with kind + liveness."""
        liveness = await _liveness(self._tables)
        nodes: list[GraphNode] = []
        for table in sorted(self._tables):
            node = self._node_by_table.get(table)
            operation = node.operation if node else "(input)"
            task_id = node.task_id if node else None
            job_id = node.job_id if node else None
            nodes.append(
                GraphNode(
                    table=table,
                    kind=self._kinds.get(table, "intermediate"),
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
        ch_client = get_ch_client()
        try:
            result = await ch_client.query(f"DESCRIBE TABLE {quote_identifier(table)}")
        except Exception as exc:
            logger.exception("DESCRIBE TABLE %s failed", table)
            return ToolError("not_live", f"Could not describe {table}: {exc}")
        columns = [ColumnSchema(name=row[0], type=row[1]) for row in result.result_rows]
        return TableSchema(table=table, columns=columns)
