"""
Tests for the AI-backed ``internal_api.lineage_ai`` wrappers.

Lives under ``aaiclick/ai/`` (rather than ``aaiclick/internal_api/``) so
collection only runs in matrices with the ``ai`` extra installed -
otherwise the imports below fail with ``ModuleNotFoundError: litellm``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from aaiclick.internal_api import lineage_ai
from aaiclick.oplog.view_models import LineageAnswer


async def test_explain_lineage_wraps_string_in_answer():
    mock_explain = AsyncMock(return_value="Explanation text.")

    with patch("aaiclick.internal_api.lineage_ai._explain_lineage", new=mock_explain):
        result = await lineage_ai.explain_lineage("result", question="Why?")

    assert isinstance(result, LineageAnswer)
    assert result.text == "Explanation text."
    mock_explain.assert_awaited_once_with("result", question="Why?")


async def test_debug_result_wraps_string_in_answer():
    mock_debug = AsyncMock(return_value="Debug answer.")

    with patch("aaiclick.internal_api.lineage_ai._debug_result", new=mock_debug):
        result = await lineage_ai.debug_result("result", question="Why?", max_iterations=3)

    assert isinstance(result, LineageAnswer)
    assert result.text == "Debug answer."
    mock_debug.assert_awaited_once_with("result", question="Why?", max_iterations=3)
