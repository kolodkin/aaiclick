"""
aaiclick.ai.agents.lineage_agent - LLM-powered lineage explanation.
"""

from __future__ import annotations

from aaiclick.ai.agents.prompts import OUTPUT_FORMAT
from aaiclick.ai.agents.tools import get_schemas_for_nodes
from aaiclick.ai.config import get_ai_provider
from aaiclick.oplog.lineage import OplogGraph, oplog_subgraph

_SYSTEM_PROMPT = f"""\
You are a data lineage expert analyzing a ClickHouse data pipeline.
Explain how the target table was produced as a short numbered list of steps.
Each step: operation, input tables, output table.

{OUTPUT_FORMAT}"""


async def explain_lineage(
    target_table: str,
    question: str | None = None,
    graph: OplogGraph | None = None,
) -> str:
    """Trace and explain how target_table was produced.

    Context given to the LLM is purely structural: the operation graph, the
    rendered SQL templates, and the table schemas. No row samples — they
    invited the model to invent narratives about partial data.

    For value-level questions use ``debug_result()``, which hands the agent
    live-query tools.
    """
    if graph is None:
        graph = await oplog_subgraph(target_table, direction="backward")
    labels = graph.build_labels()
    context = graph.to_prompt_context()

    schemas = await get_schemas_for_nodes(graph.nodes)
    if schemas:
        context += "\n\n" + schemas

    prompt = question or f"Explain how the table `{target_table}` was produced."
    provider = get_ai_provider()
    response = await provider.query(prompt, context=context, system=_SYSTEM_PROMPT)
    return OplogGraph.replace_labels(response, labels)
