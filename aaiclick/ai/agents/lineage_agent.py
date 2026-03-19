"""
aaiclick.ai.agents.lineage_agent - LLM-powered lineage explanation.
"""

from __future__ import annotations

from aaiclick.oplog.lineage import OplogEdge, OplogGraph, backward_oplog
from aaiclick.ai.agents.tools import sample_table
from aaiclick.ai.config import get_ai_provider

_SYSTEM_PROMPT = """\
You are a data lineage expert analyzing a data pipeline built on ClickHouse.
Explain clearly and concisely how the target table was produced, including
the sequence of operations and the role of each input table."""


async def explain_lineage(target_table: str, question: str | None = None) -> str:
    """Trace and explain how target_table was produced.

    Calls backward_oplog(), samples each node, formats context for LLM.
    Can be called standalone or as a @task in a job.
    """
    nodes = await backward_oplog(target_table)

    edges: list[OplogEdge] = []
    for node in nodes:
        for src in node.args:
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))
        for src in node.kwargs.values():
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))

    graph = OplogGraph(nodes=nodes, edges=edges)
    context = graph.to_prompt_context()

    for node in nodes:
        try:
            sample = await sample_table(node.table, limit=3)
            context += f"\n\nSample rows from `{node.table}`:\n{sample}"
        except Exception:
            pass

    prompt = question or f"Explain how the table `{target_table}` was produced."
    provider = get_ai_provider()
    return await provider.query(prompt, context=context, system=_SYSTEM_PROMPT)
