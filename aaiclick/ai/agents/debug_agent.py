"""
aaiclick.ai.agents.debug_agent - LLM-powered result debugging with tool calling.
"""

from __future__ import annotations

import json
from typing import Any

from litellm import acompletion

from aaiclick.oplog.lineage import OplogEdge, OplogGraph, backward_oplog
from aaiclick.ai.agents.tools import TOOL_DEFINITIONS, dispatch_tool
from aaiclick.ai.config import get_ai_provider

_SYSTEM_PROMPT = """\
You are a data debugging expert analyzing a ClickHouse data pipeline.
Use the available tools to investigate the data and answer the user's question.
Be specific, cite actual values, and trace root causes."""

_MAX_TOOL_ROUNDS = 10


async def debug_result(target_table: str, question: str) -> str:
    """Answer 'why' questions about a result by tracing lineage
    and inspecting intermediate data.

    Examples:
        "Why is this value negative?"
        "Why are there only 3 rows instead of 10?"
        "Which input caused the NaN values?"
    """
    nodes = await backward_oplog(target_table)

    edges: list[OplogEdge] = []
    for node in nodes:
        for src in node.kwargs.values():
            edges.append(OplogEdge(source=src, target=node.table, operation=node.operation))

    graph = OplogGraph(nodes=nodes, edges=edges)
    context = graph.to_prompt_context()

    provider = get_ai_provider()
    prompt = f"Target table: `{target_table}`\n\nQuestion: {question}"

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": f"Context:\n{context}\n\n{prompt}"},
    ]

    # Agentic loop: model may call tools repeatedly to inspect tables before answering.
    # Each iteration appends tool results to the conversation and re-queries the model.
    # Loop exits early when the model stops requesting tools (finish_reason != "tool_calls").
    for _ in range(_MAX_TOOL_ROUNDS):
        kwargs: dict[str, Any] = {
            "model": provider.model,
            "messages": messages,
            "tools": TOOL_DEFINITIONS,
        }
        if provider._api_key:
            kwargs["api_key"] = provider._api_key
        response = await acompletion(**kwargs)
        choice = response.choices[0]
        message = choice.message

        if choice.finish_reason != "tool_calls" or not message.tool_calls:
            return message.content or ""

        messages.append({
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
        })

        for tc in message.tool_calls:
            args = json.loads(tc.function.arguments)
            result = await dispatch_tool(tc.function.name, args)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # Max rounds reached — ask for final answer without tools
    messages.append({"role": "user", "content": "Please provide your final answer."})
    final_kwargs: dict[str, Any] = {"model": provider.model, "messages": messages}
    if provider._api_key:
        final_kwargs["api_key"] = provider._api_key
    response = await acompletion(**final_kwargs)
    return response.choices[0].message.content or ""
