"""Internal API for turnkey LLM lineage agents.

Wraps ``aaiclick.ai.agents.lineage_agent.explain_lineage`` and
``debug_agent.debug_result`` so the CLI surface can return a typed
``LineageAnswer`` view model. Pulls in ``litellm`` (via
``aaiclick.ai.agents.*``) so this module requires the ``ai`` extra.

Kept separate from ``aaiclick.internal_api.lineage`` so that callers
(``server.mcp``, ``orchestration``) without the ``ai`` extra installed can
still import the primitives.
"""

from __future__ import annotations

from aaiclick.ai.agents.debug_agent import debug_result as _debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage as _explain_lineage
from aaiclick.oplog.view_models import LineageAnswer


async def explain_lineage(target_table: str, question: str | None = None) -> LineageAnswer:
    """Trace and explain how ``target_table`` was produced (turnkey LLM agent).

    Calls Ollama / configured LLM internally — prefer the primitives in
    ``internal_api.lineage`` for MCP / programmatic use; this is a CLI /
    convenience surface only.
    """
    text = await _explain_lineage(target_table, question=question)
    return LineageAnswer(text=text)


async def debug_result(
    target_table: str,
    question: str,
    max_iterations: int = 10,
) -> LineageAnswer:
    """Run the lineage debug agent's tool loop on ``target_table`` (turnkey LLM agent).

    Calls the configured LLM internally and runs its own tool loop. Prefer the
    primitives in ``internal_api.lineage`` for MCP / programmatic use.
    """
    text = await _debug_result(target_table, question=question, max_iterations=max_iterations)
    return LineageAnswer(text=text)
