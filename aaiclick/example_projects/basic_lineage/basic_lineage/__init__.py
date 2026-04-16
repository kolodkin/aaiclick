"""
AI-powered lineage explanation for a revenue pipeline.

Pipeline: prices * quantities + bonus = total_revenue

Runs the pipeline under PreservationMode.FULL so all intermediate tables
are preserved for debugging. The debug agent uses its tool loop to inspect
tables and trace the computation graph.
"""

import asyncio

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration import (
    JobStatus,
    PreservationMode,
    ajob_test,
    get_tasks_for_job,
    job,
    task,
    tasks_list,
)
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.snowflake_id import get_snowflake_id

from .report import print_report


@task
async def create_prices(suffix: str) -> Object:
    return await create_object_from_value(
        [10.0, 20.0, 30.0, 40.0, 50.0],
        name=f"basic_lineage_prices_{suffix}",
    )


@task
async def create_quantities(suffix: str) -> Object:
    return await create_object_from_value(
        [2.0, 3.0, 1.0, 5.0, 4.0],
        name=f"basic_lineage_quantities_{suffix}",
    )


@task
async def compute_revenue(prices: Object, quantities: Object) -> Object:
    return await (prices * quantities)


@task
async def add_bonus(revenue: Object) -> Object:
    bonus = await create_object_from_value([5.0, 5.0, 5.0, 5.0, 5.0])
    return await (revenue + bonus)


@job("revenue_pipeline")
def revenue_pipeline(suffix: str):
    prices = create_prices(suffix=suffix)
    quantities = create_quantities(suffix=suffix)
    revenue = compute_revenue(prices=prices, quantities=quantities)
    total = add_bonus(revenue=revenue)
    return tasks_list(prices, quantities, revenue, total)


async def main():
    suffix = str(get_snowflake_id())

    async with orch_context():
        pipeline = await revenue_pipeline(
            suffix=suffix,
            preservation_mode=PreservationMode.FULL,
        )
        await ajob_test(pipeline)
        assert pipeline.status == JobStatus.COMPLETED, f"Job failed: {pipeline.error}"

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
        )
