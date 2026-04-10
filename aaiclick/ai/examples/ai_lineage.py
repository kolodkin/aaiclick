"""
AI lineage example for aaiclick.

This example demonstrates how to use AI-powered lineage explanation
and debugging agents. It shows:
1. Defining a data pipeline as @task/@job
2. Running the job with ajob_test()
3. Querying backward lineage on the result table
4. Printing the prompt context and AI agent response

Requires:
  - pip install aaiclick[ai]
  - A reachable LLM (set AAICLICK_AI_MODEL, default: ollama/llama3.1:8b)
"""

import asyncio
import os

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration import (
    JobStatus,
    ajob_test,
    get_tasks_for_job,
    job,
    task,
    tasks_list,
)
from aaiclick.orchestration.orch_context import orch_context


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


async def example():
    """Run the pipeline, then query lineage and print AI response."""
    print("Step 1: Running the revenue pipeline")
    print("-" * 50)

    pipeline = await revenue_pipeline()
    await ajob_test(pipeline)

    assert pipeline.status == JobStatus.COMPLETED, f"Job failed: {pipeline.error}"
    print(f"Job '{pipeline.name}' completed (ID: {pipeline.id})")

    tasks = await get_tasks_for_job(pipeline.id)
    for t in tasks:
        print(f"  Task '{t.name}': status={t.status.value}, result={t.result}")

    last_task = tasks[-1]
    target_table = last_task.result["table"]
    print(f"\nTarget table for lineage: {target_table}")

    print("\n" + "=" * 50)
    print("Step 2: Backward lineage graph")
    print("-" * 50)

    async with lineage_context():
        graph = await oplog_subgraph(target_table, direction="backward")

        print(f"\n{len(graph.nodes)} operations, {len(graph.edges)} edges\n")  # → 5 operations, 4 edges
        for edge in graph.edges:
            print(f"  {edge.source} -> {edge.target}  (via {edge.operation})")

        prompt_context = graph.to_prompt_context()
        print("\n" + "=" * 50)
        print("Step 3: Prompt context sent to AI")
        print("-" * 50)
        print(prompt_context)

        if os.environ.get("AAICLICK_AI_MODEL"):
            print("\n" + "=" * 50)
            print("Step 4: AI lineage explanation")
            print("-" * 50)

            question = "How was this table produced? What arithmetic was applied?"
            print(f"\nQuestion: {question}")

            explanation = await explain_lineage(target_table, question=question)
            print(f"\nAI response:\n{explanation}")

            print("\n" + "=" * 50)
            print("Step 5: AI debug agent")
            print("-" * 50)

            debug_question = "Why is the largest value 205 instead of 250?"
            print(f"\nQuestion: {debug_question}")

            answer = await debug_result(target_table, debug_question)
            print(f"\nAI response:\n{answer}")
        else:
            print("\n(Set AAICLICK_AI_MODEL to enable AI agent responses)")


async def amain():
    """Main entry point that creates orch_context() and calls example."""
    async with orch_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick AI Lineage Example")
    print("=" * 50)
    print()
    asyncio.run(amain())
