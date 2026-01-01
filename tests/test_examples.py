"""
Tests for examples to ensure they run without errors.
"""


async def test_basic_operators_example():
    """Test that the basic_operators example runs successfully."""
    from aaiclick.examples.basic_operators import main

    # Run the example - should complete without errors
    await main()


async def test_statistics_example():
    """Test that the statistics example runs successfully."""
    from aaiclick.examples.statistics import main

    # Run the example - should complete without errors
    await main()
