"""Internal API for the turnkey LLM lineage agents (requires the ``ai`` extra).

Kept out of ``aaiclick.internal_api.__init__`` and separate from
``internal_api.lineage`` so callers without the ``ai`` extra (``server.mcp``,
orchestration) can import the AI-independent primitives without pulling in
litellm. Import this module explicitly::

    from aaiclick.internal_api import lineage_ai

Both wrappers run inside an active ``orch_context(with_ch=True)``: the
oplog graph and the debug agent's live queries read ClickHouse.
"""

from __future__ import annotations

from aaiclick.ai.agents.debug_agent import debug_result as _debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage as _explain_lineage
from aaiclick.view_models import LineageAnswer

DEFAULT_MAX_ITERATIONS = 10


async def explain_lineage(target_table: str, question: str | None = None) -> LineageAnswer:
    """Explain how ``target_table`` was produced (single-shot, structural).

    ``question`` replaces the default "how was this produced?" prompt; for
    value-level "why" questions use ``debug_result``.
    """
    answer = await _explain_lineage(target_table, question=question)
    return LineageAnswer(target_table=target_table, question=question, answer=answer)


async def debug_result(
    target_table: str,
    question: str,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
) -> LineageAnswer:
    """Run the Tier 1 debug agent's tool loop on ``target_table``."""
    answer = await _debug_result(target_table, question=question, max_iterations=max_iterations)
    return LineageAnswer(target_table=target_table, question=question, answer=answer)
