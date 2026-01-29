"""
Run all aaiclick examples.

This script runs all example modules in sequence within a single DataContext.
Orchestration examples run with their own context management.
"""

import asyncio

from aaiclick import DataContext

from .basic_operators import example as basic_operators_example
from .data_manipulation import example as data_manipulation_example
from .orchestration_basic import async_main as orchestration_basic_example
from .statistics import example as statistics_example
from .views import example as views_example


async def main():
    """Run all examples."""
    async with DataContext() as context:
        print("=" * 60)
        print("RUNNING: Basic Operators Example")
        print("=" * 60)
        await basic_operators_example(context)

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Data Manipulation Example")
        print("=" * 60)
        await data_manipulation_example(context)

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Statistics Example")
        print("=" * 60)
        await statistics_example(context)

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Views Example")
        print("=" * 60)
        await views_example(context)

    # Orchestration example manages its own contexts (OrchContext + DataContext)
    print("\n" * 2)
    await orchestration_basic_example()

    print("\n" * 2)
    print("=" * 60)
    print("ALL EXAMPLES COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
