"""
Tests for examples to ensure they run without errors.
"""

import tempfile


async def test_basic_operators_example(ctx):
    """Test that the basic_operators example runs successfully."""
    from aaiclick.examples.basic_operators import main

    # Run the example - should complete without errors
    await main()


async def test_orchestration_basic_example(orch_ctx, monkeypatch):
    """Test that the orchestration_basic example runs successfully."""
    from aaiclick.examples.orchestration_basic import async_main

    # Use a temp directory for logs
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # Run the async version (main() uses job.test() which calls asyncio.run()
        # and cannot be used inside an already-running event loop)
        await async_main()


async def test_statistics_example(ctx):
    """Test that the statistics example runs successfully."""
    from aaiclick.examples.statistics import main

    # Run the example - should complete without errors
    await main()


async def test_views_example(ctx):
    """Test that the views example runs successfully."""
    from aaiclick.examples.views import main

    # Run the example - should complete without errors
    await main()
