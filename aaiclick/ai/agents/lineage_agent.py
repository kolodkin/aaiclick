"""
aaiclick.ai.agents.lineage_agent - LLM-powered lineage explanation.
"""

from __future__ import annotations

from aaiclick.oplog.lineage import oplog_subgraph
from aaiclick.ai.agents.tools import get_schemas_for_nodes, sample_table
from aaiclick.ai.agents.prompts import AAI_ID_WARNING
from aaiclick.ai.config import get_ai_provider

_SYSTEM_PROMPT = f"""\
You are a data lineage expert analyzing a data pipeline built on ClickHouse.
Explain clearly and concisely how the target table was produced, including
the sequence of operations and the role of each input table.

{AAI_ID_WARNING}"""


async def explain_lineage(target_table: str, question: str | None = None) -> str:
    """Trace and explain how target_table was produced.

    Calls backward_oplog(), samples each node, formats context for LLM.
    Can be called standalone or as a @task in a job.
    """
    graph = await oplog_subgraph(target_table, direction="backward")
    context = graph.to_prompt_context()

    schemas = await get_schemas_for_nodes(graph.nodes)
    if schemas:
        context += "\n\n" + schemas

    for node in graph.nodes:
        try:
            sample = await sample_table(node.table, limit=3)
            context += f"\n\nSample rows from `{node.table}`:\n{sample}"
        except Exception:
            pass

    prompt = question or f"Explain how the table `{target_table}` was produced."
    provider = get_ai_provider()
    return await provider.query(prompt, context=context, system=_SYSTEM_PROMPT)
