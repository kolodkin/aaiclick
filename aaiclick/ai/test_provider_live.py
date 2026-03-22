"""
Live LLM tests for AIProvider.

Run with:
    AAICLICK_AI_LIVE_TESTS=1 pytest aaiclick/ai/test_provider_live.py -v

Set AAICLICK_AI_MODEL to choose the model (default: ollama/llama3.1:8b).
"""

import pytest

from aaiclick.ai.config import get_ai_provider
from aaiclick.ai.agents.tools import TOOL_DEFINITIONS


@pytest.mark.live_llm
async def test_query_with_tools():
    """Provider returns valid tool-call shape when given all available tools."""
    provider = get_ai_provider()
    result = await provider.query_with_tools(
        prompt="Sample 5 rows from the table called my_table, then get its schema and stats for the value column.",
        tools=TOOL_DEFINITIONS,
    )
    assert isinstance(result, dict)
    assert "finish_reason" in result
    assert "tool_calls" in result
    assert "content" in result
    assert isinstance(result["tool_calls"], list)
