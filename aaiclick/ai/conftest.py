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
    """Auto-skip live_llm tests unless AAICLICK_AI_LIVE_TESTS=1."""
    if os.environ.get("AAICLICK_AI_LIVE_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="Set AAICLICK_AI_LIVE_TESTS=1 to run live LLM tests")
    for item in items:
        if item.get_closest_marker("live_llm"):
            item.add_marker(skip)
