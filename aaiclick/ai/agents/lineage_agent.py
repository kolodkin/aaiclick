"""
aaiclick.ai.agents.lineage_agent - LLM-powered lineage explanation.
"""

from __future__ import annotations

import asyncio

from aaiclick.oplog.lineage import OplogGraph, oplog_subgraph
from aaiclick.ai.agents.tools import get_schemas_for_nodes, sample_table
from aaiclick.ai.agents.prompts import AAI_ID_WARNING, OUTPUT_FORMAT
from aaiclick.ai.config import get_ai_provider

_SYSTEM_PROMPT = f"""\
You are a data lineage expert analyzing a ClickHouse data pipeline.
Explain how the target table was produced as a short numbered list of steps.
Each step: operation, input tables, output table.

{AAI_ID_WARNING}

{OUTPUT_FORMAT}"""


async def explain_lineage(
    target_table: str,
    question: str | None = None,
    graph: OplogGraph | None = None,
) -> str:
    """Trace and explain how target_table was produced.

    Calls backward_oplog(), samples each node, formats context for LLM.
    Post-processes the response to replace raw table IDs with labels.
    Pass `graph` to reuse a pre-built lineage graph and skip the traversal.
    """
    if graph is None:
        graph = await oplog_subgraph(target_table, direction="backward")
    labels = graph.build_labels()
    context = graph.to_prompt_context()

    schemas = await get_schemas_for_nodes(graph.nodes)
    if schemas:
        context += "\n\n" + schemas

    samples = await asyncio.gather(
        *(sample_table(node.table, limit=3) for node in graph.nodes),
        return_exceptions=True,
    )
    parts = [
        f"\n\nSample rows from `{node.table}`:\n{sample}"
        for node, sample in zip(graph.nodes, samples)
        if not isinstance(sample, Exception)
    ]
    if parts:
        context += "".join(parts)

    prompt = question or f"Explain how the table `{target_table}` was produced."
    provider = get_ai_provider()
    response = await provider.query(prompt, context=context, system=_SYSTEM_PROMPT)
    return OplogGraph.replace_labels(response, labels)
