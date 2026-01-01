"""
Tests for examples to ensure they run without errors.
"""

import pytest


@pytest.mark.asyncio
async def test_basic_operators_example():
    """Test that the basic_operators example runs successfully."""
    from aaiclick.examples.basic_operators import main

    # Run the example - should complete without errors
    await main()
