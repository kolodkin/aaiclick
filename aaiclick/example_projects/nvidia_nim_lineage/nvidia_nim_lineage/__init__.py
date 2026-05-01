"""
AI-powered lineage explanation for a revenue pipeline, backed by NVIDIA NIM.

Pipeline: prices * quantities + bonus = total_revenue

Runs the pipeline under PRESERVATION_FULL so all intermediate tables are
preserved for debugging. The lineage agent and the debug agent both call
through ``aaiclick.ai`` -> LiteLLM -> the NIM endpoint configured by
``AAICLICK_AI_MODEL`` (default: ``nvidia_nim/meta/llama-3.1-8b-instruct``).

Required env:
    NVIDIA_NIM_API_KEY  — NVIDIA API key (also used as AAICLICK_AI_API_KEY).

Optional env:
    AAICLICK_AI_MODEL    — any LiteLLM ``nvidia_nim/...`` model string.
    NVIDIA_NIM_API_BASE  — override the default NIM endpoint.
"""

import asyncio
import os

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration import (
    JOB_COMPLETED,
    PRESERVATION_FULL,
    ajob_test,
    get_tasks_for_job,
    job,
    task,
    tasks_list,
)
from aaiclick.orchestration.orch_context import orch_context

from .report import print_report


@task
async def create_prices() -> Object:
    return await create_object_from_value(
        [10.0, 20.0, 30.0, 40.0, 50.0],
        name="nvidia_nim_lineage_prices",
        aai_id=True,
    )


@task
async def create_quantities() -> Object:
    return await create_object_from_value(
        [2.0, 3.0, 1.0, 5.0, 4.0],
        name="nvidia_nim_lineage_quantities",
        aai_id=True,
    )


@task
async def compute_revenue(prices: Object, quantities: Object) -> Object:
    return await (prices * quantities)


@task
async def add_bonus(revenue: Object) -> Object:
    bonus = await create_object_from_value([5.0, 5.0, 5.0, 5.0, 5.0], aai_id=True)
    return await (revenue + bonus)


@job("revenue_pipeline_nvidia_nim")
def revenue_pipeline():
    prices = create_prices()
    quantities = create_quantities()
    revenue = compute_revenue(prices=prices, quantities=quantities)
    total = add_bonus(revenue=revenue)
    return tasks_list(prices, quantities, revenue, total)


def _check_nim_credentials() -> None:
    """Fail fast with a clear message if NIM credentials are missing.

    LiteLLM would otherwise raise a generic auth error mid-pipeline; this
    surfaces the misconfiguration before we run the whole job.
    """
    if not (os.environ.get("NVIDIA_NIM_API_KEY") or os.environ.get("AAICLICK_AI_API_KEY")):
        raise RuntimeError(
            "NVIDIA_NIM_API_KEY is not set. Export your NVIDIA API key "
            "(https://build.nvidia.com) before running this example."
        )


async def main():
    _check_nim_credentials()

    model = os.environ.get("AAICLICK_AI_MODEL", "nvidia_nim/meta/llama-3.1-8b-instruct")
    print(f"Using NVIDIA NIM model: {model}\n")

    async with orch_context():
        pipeline = await revenue_pipeline(preservation_mode=PRESERVATION_FULL)
        await ajob_test(pipeline)
        assert pipeline.status == JOB_COMPLETED, f"Job failed: {pipeline.error}"

        tasks = await get_tasks_for_job(pipeline.id)
        target_table = next(t for t in tasks if t.name == "add_bonus").result["table"]
        source_table = next(t for t in tasks if t.name == "create_prices").result["table"]

        async with lineage_context():
            backward_graph, forward_graph = await asyncio.gather(
                oplog_subgraph(target_table, direction="backward"),
                oplog_subgraph(source_table, direction="forward"),
            )
            explanation = await explain_lineage(
                target_table,
                question="How was this table produced? What arithmetic was applied?",
                graph=backward_graph,
            )
            debug_answer = await debug_result(
                target_table,
                question=(
                    "Which output row has the highest value, and which input "
                    "rows drove it? Use the tools to inspect the tables."
                ),
                graph=backward_graph,
            )

        print_report(
            tasks=tasks,
            target_table=target_table,
            backward_graph=backward_graph,
            forward_graph=forward_graph,
            source_table=source_table,
            explanation=explanation,
            debug_answer=debug_answer,
            model=model,
        )
