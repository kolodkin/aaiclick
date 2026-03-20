"""
Pytest configuration for aaiclick.ai tests.

Live LLM tests are skipped by default. To run them:

    AAICLICK_AI_LIVE_TESTS=1 pytest aaiclick/ai/ -m live_llm -v

Set AAICLICK_AI_MODEL to choose the model (default: ollama/llama3.1:8b).
Standard LiteLLM env vars (ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.) also apply.
"""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    """Auto-skip live_llm tests unless AAICLICK_AI_LIVE_TESTS=1.

    Also attach LiteLLM-specific warning filters to live_llm tests.
    LiteLLM creates async_success_handler coroutines that are never awaited
    (a known upstream bug). Per-test filterwarnings marks take precedence over
    -W error so this suppresses only this specific LiteLLM issue.
    """
    litellm_warnings = [
        pytest.mark.filterwarnings(
            "ignore:coroutine 'Logging.async_success_handler' was never awaited:RuntimeWarning"
        ),
        pytest.mark.filterwarnings(
            "ignore:.*async_success_handler.*:pytest.PytestUnraisableExceptionWarning"
        ),
    ]
    if os.environ.get("AAICLICK_AI_LIVE_TESTS") == "1":
        for item in items:
            if item.get_closest_marker("live_llm"):
                for mark in litellm_warnings:
                    item.add_marker(mark)
        return
    skip = pytest.mark.skip(reason="Set AAICLICK_AI_LIVE_TESTS=1 to run live LLM tests")
    for item in items:
        if item.get_closest_marker("live_llm"):
            item.add_marker(skip)
