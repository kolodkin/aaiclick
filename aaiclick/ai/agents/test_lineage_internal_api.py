"""
Tests for the AI-backed ``internal_api.lineage_ai`` wrappers.

They live under ``aaiclick/ai/`` so they only run in matrices that install
the ``ai`` extra; the agents themselves are mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import aaiclick.internal_api as internal_api_package
from aaiclick.internal_api import lineage_ai
from aaiclick.view_models import LineageAnswer


async def test_explain_lineage_wraps_agent_answer():
    mock_explain = AsyncMock(return_value="steps")

    with patch("aaiclick.internal_api.lineage_ai._explain_lineage", new=mock_explain):
        result = await lineage_ai.explain_lineage("p_revenue")

    assert result == LineageAnswer(target_table="p_revenue", question=None, answer="steps")
    mock_explain.assert_awaited_once_with("p_revenue", question=None)


async def test_explain_lineage_passes_custom_question():
    mock_explain = AsyncMock(return_value="steps")

    with patch("aaiclick.internal_api.lineage_ai._explain_lineage", new=mock_explain):
        result = await lineage_ai.explain_lineage("p_revenue", question="Which join fed this?")

    assert result.question == "Which join fed this?"
    mock_explain.assert_awaited_once_with("p_revenue", question="Which join fed this?")


async def test_debug_result_wraps_agent_answer_and_forwards_max_iterations():
    mock_debug = AsyncMock(return_value="because")

    with patch("aaiclick.internal_api.lineage_ai._debug_result", new=mock_debug):
        result = await lineage_ai.debug_result("p_revenue", question="Why?", max_iterations=3)

    assert result == LineageAnswer(target_table="p_revenue", question="Why?", answer="because")
    mock_debug.assert_awaited_once_with("p_revenue", question="Why?", max_iterations=3)


def test_lineage_ai_is_not_exported_from_internal_api_package():
    """Importing ``aaiclick.internal_api`` must stay litellm-free."""
    assert not hasattr(internal_api_package, "explain_lineage")
    assert not hasattr(internal_api_package, "debug_result")
