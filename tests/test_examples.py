"""
Tests for examples to ensure they run without errors.
"""

import tempfile


async def test_basic_operators_example(ctx):
    """Test that the basic_operators example runs successfully."""
    from aaiclick.data.examples.basic_operators import main

    # Run the example - should complete without errors
    await main()


async def test_orchestration_basic_example(orch_ctx, monkeypatch):
    """Test that the orchestration_basic example runs successfully."""
    from aaiclick.data.examples.orchestration_basic import main

    # Use a temp directory for logs
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # Run the example - should complete without errors
        await main()


async def test_statistics_example(ctx):
    """Test that the statistics example runs successfully."""
    from aaiclick.data.examples.statistics import main

    # Run the example - should complete without errors
    await main()


async def test_views_example(ctx):
    """Test that the views example runs successfully."""
    from aaiclick.data.examples.views import main

    # Run the example - should complete without errors
    await main()
