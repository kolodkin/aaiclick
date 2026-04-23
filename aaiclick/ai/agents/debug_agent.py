"""
aaiclick.ai.agents.debug_agent - Tier 1 lineage debugger.

`debug_result()` runs an LLM tool loop scoped to the backward lineage graph of
the target table. The agent reads rendered SQL templates, queries live tables
via ``LineageToolbox``, and produces an explanation citing concrete evidence.

See ``docs/lineage.md``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from aaiclick.ai.agents.lineage_tools import LINEAGE_TOOL_DEFINITIONS, LineageToolbox
from aaiclick.ai.agents.prompts import LINEAGE_TIER1_SYSTEM_PROMPT
from aaiclick.ai.config import get_ai_provider
from aaiclick.oplog.lineage import OplogGraph, oplog_subgraph

logger = logging.getLogger(__name__)


async def debug_result(
    target_table: str,
    question: str,
    graph: OplogGraph | None = None,
    max_iterations: int = 10,
) -> str:
    """Tier 1 lineage debug loop.

    The agent starts with the question, the backward oplog graph of
    `target_table`, and the four graph-scoped tools from ``LineageToolbox``.
    It iterates up to `max_iterations` rounds of tool calls before producing
    a final explanation.

    Pass `graph` to reuse a pre-built backward lineage graph and skip
    traversal.
    """
    if graph is None:
        graph = await oplog_subgraph(target_table, direction="backward")
    labels = graph.build_labels()
    toolbox = LineageToolbox(graph)

    context = graph.to_prompt_context()
    nodes = await toolbox.list_graph_nodes()
    liveness_lines = [f"- {n.table} [{n.kind}] live={n.live}" for n in nodes]
    context += "\n\n# Graph Node Liveness\n" + "\n".join(liveness_lines)

    user_content = f"Context:\n{context}\n\nTarget table: `{target_table}`\n\nQuestion: {question}"

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": LINEAGE_TIER1_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    provider = get_ai_provider()
    try:
        for _ in range(max_iterations):
            response = await provider.complete(messages, tools=LINEAGE_TOOL_DEFINITIONS)
            choice = response.choices[0]
            message = choice.message

            if choice.finish_reason != "tool_calls" or not message.tool_calls:
                return OplogGraph.replace_labels(message.content or "", labels)

            messages.append(
                {
                    "role": "assistant",
                    "content": message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in message.tool_calls
                    ],
                }
            )

            tool_results = await asyncio.gather(
                *(
                    toolbox.dispatch_tool(tc.function.name, _parse_arguments(tc.function.arguments))
                    for tc in message.tool_calls
                )
            )
            for tc, result in zip(message.tool_calls, tool_results, strict=False):
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    }
                )

        messages.append({"role": "user", "content": "Please provide your final answer."})
        response = await provider.complete(messages)
        return OplogGraph.replace_labels(response.choices[0].message.content or "", labels)
    except Exception as exc:
        logger.warning("debug_result provider call failed: %s", exc)
        return f"(debug agent did not converge: {exc})"


def _parse_arguments(raw: str | None) -> dict[str, Any]:
    """Parse a tool call's JSON arguments, returning {} on malformed input.

    A model occasionally emits empty-string arguments for zero-arg tools;
    tolerate that instead of aborting the loop.
    """
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("tool arguments not valid JSON: %r", raw)
        return {}
    return parsed if isinstance(parsed, dict) else {}
