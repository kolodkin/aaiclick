"""
Pytest configuration for aaiclick.ai tests.

Live LLM tests are skipped by default. To run them:

    AAICLICK_AI_LIVE_TESTS=1 pytest aaiclick/ai/ -m live_llm -v

Set AAICLICK_AI_MODEL to choose the model (default: ollama/llama3.1:8b).
Standard LiteLLM env vars (ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.) also apply.
"""

import os
import shutil
import tempfile

import pytest


@pytest.fixture
async def orch_ctx():
    """Function-scoped orch context for AI tests using a temporary SQLite database."""
    tmpdir = tempfile.mkdtemp(prefix="aaiclick_ai_test_")
    db_path = os.path.join(tmpdir, "test.db")
    old_url = os.environ.get("AAICLICK_SQL_URL")
    os.environ["AAICLICK_SQL_URL"] = f"sqlite+aiosqlite:///{db_path}"

    from sqlalchemy import create_engine

    from aaiclick.orchestration.models import SQLModel
    from aaiclick.orchestration.context import orch_context

    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    engine.dispose()

    try:
        async with orch_context():
            yield
    finally:
        if old_url is not None:
            os.environ["AAICLICK_SQL_URL"] = old_url
        elif "AAICLICK_SQL_URL" in os.environ:
            del os.environ["AAICLICK_SQL_URL"]
        shutil.rmtree(tmpdir, ignore_errors=True)


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
