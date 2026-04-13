"""
AI-powered lineage explanation for a revenue pipeline.

Pipeline: prices * quantities + bonus = total_revenue

Runs the pipeline, traces backward + forward lineage, and exercises both
the explain and debug agents.
"""

import asyncio

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration import JobStatus, ajob_test, get_tasks_for_job, job, task, tasks_list
from aaiclick.orchestration.orch_context import orch_context

from .report import print_report


@task
async def create_prices() -> Object:
    return await create_object_from_value([10.0, 20.0, 30.0, 40.0, 50.0])


@task
async def create_quantities() -> Object:
    return await create_object_from_value([2.0, 3.0, 1.0, 5.0, 4.0])


@task
async def compute_revenue(prices: Object, quantities: Object) -> Object:
    return await (prices * quantities)


@task
async def add_bonus(revenue: Object) -> Object:
    bonus = await create_object_from_value([5.0, 5.0, 5.0, 5.0, 5.0])
    return await (revenue + bonus)


@job("revenue_pipeline")
def revenue_pipeline():
    prices = create_prices()
    quantities = create_quantities()
    revenue = compute_revenue(prices=prices, quantities=quantities)
    total = add_bonus(revenue=revenue)
    return tasks_list(prices, quantities, revenue, total)


async def main():
    async with orch_context():
        pipeline = await revenue_pipeline()
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
                question="Which row has the highest value and which inputs drove it?",
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
