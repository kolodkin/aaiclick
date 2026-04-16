"""
aaiclick.ai.agents.debug_agent - LLM-powered result debugging with tool calling.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from aaiclick.ai.agents.prompts import AAI_ID_WARNING, OUTPUT_FORMAT
from aaiclick.ai.agents.tools import (
    TOOL_DEFINITIONS,
    dispatch_tool,
    get_schemas_for_nodes,
)
from aaiclick.ai.config import get_ai_provider
from aaiclick.oplog.lineage import OplogGraph, oplog_subgraph

logger = logging.getLogger(__name__)


def _system_prompt(body: str) -> str:
    return (
        f"You are a data debugging expert analyzing a ClickHouse data pipeline.\n"
        f"{body}\n\n{AAI_ID_WARNING}\n\n{OUTPUT_FORMAT}"
    )


_SYSTEM_PROMPT = _system_prompt(
    "Use the available tools to investigate data and answer the question.\n"
    "Be specific: cite actual values and trace the root cause."
)

_MAX_TOOL_ROUNDS = 5


async def debug_result(
    target_table: str,
    question: str,
    graph: OplogGraph | None = None,
) -> str:
    """Answer 'why' questions about a result by tracing lineage
    and inspecting intermediate data.

    Examples:
        "Why is this value negative?"
        "Why are there only 3 rows instead of 10?"
        "Which input caused the NaN values?"

    Pass `graph` to reuse a pre-built backward lineage graph and skip the traversal.
    """
    if graph is None:
        graph = await oplog_subgraph(target_table, direction="backward")
    labels = graph.build_labels()
    context = graph.to_prompt_context()

    schemas = await get_schemas_for_nodes(graph.nodes)
    if schemas:
        context += "\n\n" + schemas

    provider = get_ai_provider()
    prompt = f"Target table: `{target_table}`\n\nQuestion: {question}"
    user_content = f"Context:\n{context}\n\n{prompt}"

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    for _ in range(_MAX_TOOL_ROUNDS):
        response = await provider.complete(messages, tools=TOOL_DEFINITIONS)
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
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in message.tool_calls
                ],
            }
        )

        tool_results = await asyncio.gather(
            *(dispatch_tool(tc.function.name, json.loads(tc.function.arguments)) for tc in message.tool_calls)
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
