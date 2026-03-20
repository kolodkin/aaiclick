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

    Also attaches LiteLLM-specific warning suppressions to live_llm tests.
    LiteLLM 1.82.4 still leaves async_success_handler coroutines unawaited on
    the Ollama code path despite the upstream fix in v1.76.1 (PR #14050).
    Per-test filterwarnings marks override -W error so this is scoped only to
    live LLM tests and doesn't mask warnings in other tests.
    TODO: recheck when LiteLLM fixes the Ollama-specific code path.
    """
    # Technical debt: LiteLLM async_success_handler still unawaited on Ollama path
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
