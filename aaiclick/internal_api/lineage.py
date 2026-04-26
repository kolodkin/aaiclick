"""Internal API for lineage queries and AI agents.

Each function runs inside an active ``orch_context(with_ch=True)`` and
reads the ClickHouse client via ``get_ch_client``. ``oplog_subgraph``
returns the ``OplogGraph`` directly; the AI agents return
``LineageAnswer`` wrapping their plain-text response.
"""

from __future__ import annotations

from aaiclick.ai.agents.debug_agent import debug_result as _debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage as _explain_lineage
from aaiclick.oplog.lineage import LineageDirection, OplogGraph
from aaiclick.oplog.lineage import oplog_subgraph as _oplog_subgraph
from aaiclick.oplog.view_models import LineageAnswer


async def oplog_subgraph(
    target_table: str,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> OplogGraph:
    """Return the lineage graph for ``target_table`` in the given direction."""
    return await _oplog_subgraph(target_table, direction=direction, max_depth=max_depth)


async def explain_lineage(target_table: str, question: str | None = None) -> LineageAnswer:
    """Trace and explain how ``target_table`` was produced.

    Uses the lineage AI agent (no live-query tools — purely structural
    explanation from the operation graph + table schemas).
    """
    text = await _explain_lineage(target_table, question=question)
    return LineageAnswer(text=text)


async def debug_result(
    target_table: str,
    question: str,
    max_iterations: int = 10,
) -> LineageAnswer:
    """Run the lineage debug agent's tool loop on ``target_table``.

    The agent has access to the backward lineage graph and live-query
    tools; it can iterate up to ``max_iterations`` rounds before producing
    a final answer.
    """
    text = await _debug_result(target_table, question=question, max_iterations=max_iterations)
    return LineageAnswer(text=text)
