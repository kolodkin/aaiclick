"""Internal API for turnkey LLM lineage agents (requires the ``ai`` extra).

Kept separate from ``aaiclick.internal_api.lineage`` so callers without the
``ai`` extra installed (``server.mcp``, orchestration) can import the
primitives without pulling in litellm. For MCP / programmatic use, prefer
the primitives in ``internal_api.lineage``.
"""

from __future__ import annotations

from aaiclick.ai.agents.debug_agent import debug_result as _debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage as _explain_lineage
from aaiclick.oplog.view_models import LineageAnswer


async def explain_lineage(target_table: str, question: str | None = None) -> LineageAnswer:
    """Trace and explain how ``target_table`` was produced."""
    text = await _explain_lineage(target_table, question=question)
    return LineageAnswer(text=text)


async def debug_result(
    target_table: str,
    question: str,
    max_iterations: int = 10,
) -> LineageAnswer:
    """Run the lineage debug agent's tool loop on ``target_table``."""
    text = await _debug_result(target_table, question=question, max_iterations=max_iterations)
    return LineageAnswer(text=text)
