"""
AI lineage example for aaiclick.

This example demonstrates how to use AI-powered lineage explanation
and debugging agents. It shows:
1. Building a data pipeline inside a task_scope (which records oplog entries)
2. Using explain_lineage() to get an AI explanation of how a table was produced
3. Using debug_result() to ask questions about intermediate data

Requires:
  - pip install aaiclick[ai]
  - A reachable LLM (set AAICLICK_AI_MODEL, default: ollama/llama3.1:8b)
"""

import asyncio

from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import lineage_context, oplog_subgraph
from aaiclick.orchestration.orch_context import task_scope


async def example(run_ai: bool = True):
    """Build a pipeline and demonstrate lineage queries.

    Args:
        run_ai: When True, call AI agents for natural-language explanations.
                Set to False to show only the raw lineage graph (no LLM needed).
    """
    # --- Step 1: Build a data pipeline inside task_scope ---
    # task_scope records every operation in the operation log (oplog),
    # which the AI agents later query to reconstruct lineage.
    print("Step 1: Building a data pipeline")
    print("-" * 50)

    async with task_scope(task_id=1, job_id=1, run_id=100):
        prices = await create_object_from_value([10.0, 20.0, 30.0, 40.0, 50.0])
        print(f"Created prices: {await prices.data()}")  # -> [10.0, 20.0, 30.0, 40.0, 50.0]

        quantities = await create_object_from_value([2.0, 3.0, 1.0, 5.0, 4.0])
        print(f"Created quantities: {await quantities.data()}")  # -> [2.0, 3.0, 1.0, 5.0, 4.0]

        revenue = await (prices * quantities)
        print(f"Computed revenue (prices * quantities): {await revenue.data()}")  # -> [20.0, 60.0, 30.0, 200.0, 200.0]

        bonus = await create_object_from_value([5.0, 5.0, 5.0, 5.0, 5.0])
        total = await (revenue + bonus)
        print(f"Computed total (revenue + bonus): {await total.data()}")  # -> [25.0, 65.0, 35.0, 205.0, 205.0]

        target_table = total.table

    # --- Step 2: Query the lineage graph ---
    # After data_context exits, use lineage_context to query the oplog.
    print("\n" + "=" * 50)
    print("Step 2: Querying the lineage graph")
    print("-" * 50)

    async with lineage_context():
        graph = await oplog_subgraph(target_table, direction="backward")

        print(f"\nLineage graph: {len(graph.nodes)} operations, {len(graph.edges)} edges\n")

        for edge in graph.edges:
            print(f"  {edge.source} -> {edge.target}  (via {edge.operation})")

        # --- Step 3: AI-powered explanation ---
        if run_ai:
            print("\n" + "=" * 50)
            print("Step 3: AI lineage explanation")
            print("-" * 50)

            from aaiclick.ai.agents.lineage_agent import explain_lineage

            explanation = await explain_lineage(
                target_table,
                question="How was this table produced? What arithmetic was applied?",
            )
            print(f"\nAI explanation:\n{explanation}")

            # --- Step 4: AI-powered debugging ---
            print("\n" + "=" * 50)
            print("Step 4: AI debugging agent")
            print("-" * 50)

            from aaiclick.ai.agents.debug_agent import debug_result

            answer = await debug_result(
                target_table,
                "Why is the largest value 205 instead of 250?",
            )
            print(f"\nAI debug answer:\n{answer}")
        else:
            print("\n(Skipping AI agents -- set run_ai=True and configure AAICLICK_AI_MODEL)")

        # --- Print raw lineage context ---
        print("\n" + "=" * 50)
        print("Raw lineage context (sent to AI)")
        print("-" * 50)
        print(graph.to_prompt_context())


async def amain():
    """Main entry point that creates orch_context() and calls example."""
    from aaiclick.orchestration.orch_context import orch_context

    async with orch_context():
        await example(run_ai=False)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick AI Lineage Example")
    print("=" * 50)
    print()
    asyncio.run(amain())
