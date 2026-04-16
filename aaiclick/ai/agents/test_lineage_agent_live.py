"""
Live LLM tests for explain_lineage and debug_result.

Run with:
    AAICLICK_AI_LIVE_TESTS=1 pytest aaiclick/ai/agents/test_lineage_agent_live.py -v

Requires a running ClickHouse (or chdb) and a reachable LLM.
Set AAICLICK_AI_MODEL to choose the model (default: ollama/llama3.1:8b).
"""

import pytest

from aaiclick.ai.agents.debug_agent import debug_result
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.data.data_context import create_object_from_value
from aaiclick.oplog.lineage import lineage_context
from aaiclick.orchestration.orch_context import task_scope


@pytest.mark.live_llm
async def test_lineage_and_debug(orch_ctx):
    """explain_lineage and debug_result both complete on a single concat pipeline."""
    async with task_scope(task_id=1, job_id=1, run_id=100):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([-1, -2, -3])
        result = await a.concat(b)
        result_table = result.table

    async with lineage_context():
        explanation = await explain_lineage(
            result_table,
            question="How many source tables were combined and what operation was used?",
        )
        answer = await debug_result(result_table, "Why does the result contain negative values?")

    assert isinstance(explanation, str)
    assert isinstance(answer, str)
