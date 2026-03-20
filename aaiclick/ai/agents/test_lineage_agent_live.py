"""
Live LLM tests for explain_lineage and debug_result.

Run with:
    AAICLICK_AI_LIVE_TESTS=1 pytest aaiclick/ai/agents/test_lineage_agent_live.py -v

Requires a running ClickHouse (or chdb) and a reachable LLM.
Set AAICLICK_AI_MODEL to choose the model (default: ollama/llama3.1:8b).
"""

import pytest

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.oplog.lineage import lineage_context
from aaiclick.ai.agents.lineage_agent import explain_lineage
from aaiclick.ai.agents.debug_agent import debug_result


@pytest.mark.live_llm
async def test_explain_lineage_real_pipeline():
    """explain_lineage completes without error for a real concat pipeline."""
    async with data_context(oplog=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        result = await a.concat(b)
        result_table = result.table

    async with lineage_context():
        explanation = await explain_lineage(result_table)

    assert isinstance(explanation, str)


@pytest.mark.live_llm
async def test_explain_lineage_with_custom_question():
    """explain_lineage accepts a custom question and returns a string."""
    async with data_context(oplog=True):
        a = await create_object_from_value([10, 20, 30])
        b = await create_object_from_value([40, 50, 60])
        result = await a.concat(b)
        result_table = result.table

    async with lineage_context():
        explanation = await explain_lineage(
            result_table,
            question="How many source tables were combined and what operation was used?",
        )

    assert isinstance(explanation, str)


@pytest.mark.live_llm
async def test_debug_result_real_pipeline():
    """debug_result completes without error and returns a string."""
    async with data_context(oplog=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([-1, -2, -3])
        result = await a.concat(b)
        result_table = result.table

    async with lineage_context():
        answer = await debug_result(result_table, "Why does the result contain negative values?")

    assert isinstance(answer, str)
