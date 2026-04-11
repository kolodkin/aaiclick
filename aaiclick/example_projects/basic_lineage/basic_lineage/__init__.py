"""
AI-powered lineage explanation for a revenue pipeline.

Pipeline: prices * quantities + bonus = total_revenue

Steps:
1. Run the revenue pipeline (4 tasks)
2. Trace backward lineage from the final result
3. AI explains how the result was produced
"""

from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration import JobStatus, ajob_test, get_tasks_for_job, job, task, tasks_list
from aaiclick.orchestration.orch_context import orch_context

from .report import print_report


@task
async def create_prices() -> Object:
    """Source: daily product prices."""
    return await create_object_from_value([10.0, 20.0, 30.0, 40.0, 50.0])


@task
async def create_quantities() -> Object:
    """Source: daily quantities sold."""
    return await create_object_from_value([2.0, 3.0, 1.0, 5.0, 4.0])


@task
async def compute_revenue(prices: Object, quantities: Object) -> Object:
    """Revenue = prices * quantities."""
    return await (prices * quantities)


@task
async def add_bonus(revenue: Object) -> Object:
    """Total = revenue + flat bonus per item."""
    bonus = await create_object_from_value([5.0, 5.0, 5.0, 5.0, 5.0])
    return await (revenue + bonus)


@job("revenue_pipeline")
def revenue_pipeline():
    """Compute total revenue with bonus from prices and quantities."""
    prices = create_prices()
    quantities = create_quantities()
    revenue = compute_revenue(prices=prices, quantities=quantities)
    total = add_bonus(revenue=revenue)
    return tasks_list(prices, quantities, revenue, total)


async def main():
    """Run pipeline, trace lineage, and explain with AI."""
    async with orch_context():
        pipeline = await revenue_pipeline()
        await ajob_test(pipeline)
        assert pipeline.status == JobStatus.COMPLETED, f"Job failed: {pipeline.error}"

        tasks = await get_tasks_for_job(pipeline.id)
        target_table = tasks[-1].result["table"]

        async with lineage_context():
            graph = await oplog_subgraph(target_table, direction="backward")
            labels = graph.build_labels()
            prompt_context = graph.to_prompt_context()

            explanation = await explain_lineage(
                target_table, question="How was this table produced? What arithmetic was applied?",
            )

        print_report(
            tasks=tasks,
            target_table=target_table,
            graph=graph,
            labels=labels,
            prompt_context=prompt_context,
            explanation=explanation,
        )
