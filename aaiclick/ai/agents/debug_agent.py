"""
aaiclick.ai.agents.debug_agent - LLM-powered result debugging with tool calling.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqlmodel import select

from aaiclick.data.data_context.ch_client import get_ch_client
from aaiclick.oplog.lineage import OplogGraph, oplog_subgraph
from aaiclick.oplog.sampling import SamplingStrategy
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.replay import replay_job
from aaiclick.ai.agents.strategy_agent import format_strategy, produce_strategy
from aaiclick.ai.agents.tools import (
    TOOL_DEFINITIONS,
    dispatch_tool,
    get_schemas_for_nodes,
    trace_row,
)
from aaiclick.ai.agents.prompts import AAI_ID_WARNING, OUTPUT_FORMAT
from aaiclick.ai.config import get_ai_provider

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = f"""\
You are a data debugging expert analyzing a ClickHouse data pipeline.
Use the available tools to investigate data and answer the question.
Be specific: cite actual values and trace the root cause.

{AAI_ID_WARNING}

{OUTPUT_FORMAT}"""

_MAX_TOOL_ROUNDS = 10


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

    # Failure is non-fatal — we degrade to graph-only context.
    try:
        strategy = await produce_strategy(question, graph, schemas=schemas)
    except ValueError as exc:
        logger.warning("strategy agent skipped: %s", exc)
        strategy = {}
    context += format_strategy(strategy)

    if strategy:
        row_trace = await _replay_and_trace(target_table, graph, strategy)
        if row_trace:
            context += f"\n\nRow-level lineage (from strategy replay):\n{row_trace}"

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
        response = await provider.complete(messages, tools=TOOL_DEFINITIONS)
        choice = response.choices[0]
        message = choice.message

        if choice.finish_reason != "tool_calls" or not message.tool_calls:
            return OplogGraph.replace_labels(message.content or "", labels)

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

        tool_results = await asyncio.gather(
            *(dispatch_tool(tc.function.name, json.loads(tc.function.arguments))
              for tc in message.tool_calls)
        )
        for tc, result in zip(message.tool_calls, tool_results):
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # Max rounds reached — ask for final answer without tools
    messages.append({"role": "user", "content": "Please provide your final answer."})
    response = await provider.complete(messages)
    return OplogGraph.replace_labels(response.choices[0].message.content or "", labels)


async def _replay_and_trace(
    target_table: str,
    graph: OplogGraph,
    strategy: SamplingStrategy,
) -> str:
    """Replay the job that produced ``target_table`` under ``strategy`` and
    trace one matched row backward through the replayed oplog.

    Returns an empty string on any failure — the rest of ``debug_result``
    keeps working with just the graph + strategy context, matching the
    same degrade-gracefully posture used for ``produce_strategy``.
    """
    target_node = next((n for n in graph.nodes if n.table == target_table), None)
    if target_node is None or target_node.job_id is None:
        return ""
    try:
        replayed = await replay_job(target_node.job_id, sampling_strategy=strategy)
        await run_job_tasks(replayed)
    except Exception as exc:
        logger.warning("replay skipped: %s", exc)
        return ""

    new_target = await _find_replay_target_table(target_node.task_id, replayed.id)
    if new_target is None:
        return ""

    ch_client = get_ch_client()
    rows = await ch_client.query(f"SELECT aai_id FROM {new_target} LIMIT 1")
    if not rows.result_rows:
        return ""
    return await trace_row(new_target, rows.result_rows[0][0])


async def _find_replay_target_table(
    original_task_id: int | None,
    replayed_job_id: int,
) -> str | None:
    """Return the replayed clone's output table for ``original_task_id``.

    Matching is by entrypoint: ``replay_job`` preserves each cloned task's
    entrypoint even though it reallocates the snowflake id, so pairing
    original → clone is a single equality lookup.
    """
    if original_task_id is None:
        return None
    async with get_sql_session() as session:
        entrypoint = (
            await session.execute(
                select(Task.entrypoint).where(Task.id == original_task_id)
            )
        ).scalar_one_or_none()
        if entrypoint is None:
            return None
        result = (
            await session.execute(
                select(Task.result)
                .where(Task.job_id == replayed_job_id, Task.entrypoint == entrypoint)
                .limit(1)
            )
        ).scalar_one_or_none()
    if isinstance(result, dict):
        return result.get("table")
    return None
