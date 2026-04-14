"""
aaiclick.ai.agents.debug_agent - LLM-powered result debugging with tool calling.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqlmodel import select

from aaiclick.oplog.lineage import OplogGraph, OplogNode, oplog_subgraph
from aaiclick.oplog.lineage_forest import build_and_render
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
)
from aaiclick.ai.agents.prompts import AAI_ID_WARNING, OUTPUT_FORMAT
from aaiclick.ai.config import get_ai_provider

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

_SYSTEM_PROMPT_WITH_FOREST = _system_prompt(
    "The context below already includes a row-level lineage forest for every\n"
    "strategy-matched row: every hop, every aai_id, and every column value the\n"
    "strategy selected. Answer the user's question directly by citing those\n"
    "values. Do not ask for more data — it is already in the prompt."
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

    try:
        strategy = await produce_strategy(question, graph, schemas=schemas)
    except ValueError as exc:
        logger.warning("strategy agent skipped: %s", exc)
        strategy = {}
    context += format_strategy(strategy)

    forest_text = await _try_build_forest(target_table, graph)
    if not forest_text and strategy:
        forest_text = await _replay_and_build_forest(target_table, graph, strategy)
    if forest_text:
        context += "\n\n" + forest_text

    provider = get_ai_provider()
    prompt = f"Target table: `{target_table}`\n\nQuestion: {question}"
    user_content = f"Context:\n{context}\n\n{prompt}"

    # Forest present → every row-level question is answerable from static
    # context, so skip the tool-call loop entirely. Weak models otherwise
    # waste rounds on redundant tool calls against a large context.
    if forest_text:
        response = await provider.complete([
            {"role": "system", "content": _SYSTEM_PROMPT_WITH_FOREST},
            {"role": "user", "content": user_content},
        ])
        return OplogGraph.replace_labels(
            response.choices[0].message.content or "", labels
        )

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

    messages.append({"role": "user", "content": "Please provide your final answer."})
    response = await provider.complete(messages)
    return OplogGraph.replace_labels(response.choices[0].message.content or "", labels)


def _find_target_node(graph: OplogGraph, target_table: str) -> OplogNode | None:
    return next((n for n in graph.nodes if n.table == target_table), None)


async def _safe_build(label: str, table: str, job_id: int | None) -> str:
    """Call ``build_and_render`` with a shared warn-and-skip envelope."""
    try:
        return await build_and_render(table, job_id=job_id)
    except Exception as exc:
        logger.warning("%s skipped: %s", label, exc)
        return ""


async def _try_build_forest(target_table: str, graph: OplogGraph) -> str:
    """Render a forest from the target's existing oplog.

    Returns an empty string when the target's job ran with empty lineage
    arrays (``NONE`` / ``FULL`` mode); the caller can then fall through
    to a STRATEGY replay.
    """
    target_node = _find_target_node(graph, target_table)
    job_id = target_node.job_id if target_node is not None else None
    return await _safe_build("forest build", target_table, job_id)


async def _replay_and_build_forest(
    target_table: str,
    graph: OplogGraph,
    strategy: SamplingStrategy,
) -> str:
    """Replay the target's job under ``strategy`` and render a forest
    from the replayed oplog."""
    target_node = _find_target_node(graph, target_table)
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
    return await _safe_build("forest build from replay", new_target, replayed.id)


async def _find_replay_target_table(
    original_task_id: int | None,
    replayed_job_id: int,
) -> str | None:
    """Return the replayed clone's output table for ``original_task_id``.

    Matched by entrypoint — ``replay_job`` preserves each cloned task's
    entrypoint but reallocates its snowflake id.
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
