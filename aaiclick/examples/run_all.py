"""
Run all aaiclick examples.

This script runs all example modules in sequence within a single data_context().
Orchestration examples run with their own context management.
"""

import asyncio

from aaiclick.data.data_context import data_context

from .basic_operators import example as basic_operators_example
from .data_manipulation import example as data_manipulation_example
from .group_by import example as group_by_example
from .orchestration_basic import amain as orchestration_basic_example
from .orchestration_map import amain as orchestration_map_example
from .selectors import example as selectors_example
from .statistics import example as statistics_example
from .views import example as views_example


async def main():
    """Run all examples."""
    async with data_context():
        print("=" * 60)
        print("RUNNING: Basic Operators Example")
        print("=" * 60)
        await basic_operators_example()

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Data Manipulation Example")
        print("=" * 60)
        await data_manipulation_example()

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Statistics Example")
        print("=" * 60)
        await statistics_example()

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Views Example")
        print("=" * 60)
        await views_example()

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Group By Example")
        print("=" * 60)
        await group_by_example()

        print("\n" * 2)
        print("=" * 60)
        print("RUNNING: Dict Selectors Example")
        print("=" * 60)
        await selectors_example()

    # Orchestration examples manage their own contexts (OrchContext + data_context())
    print("\n" * 2)
    await orchestration_basic_example()

    print("\n" * 2)
    await orchestration_map_example()

    print("\n" * 2)
    print("=" * 60)
    print("ALL EXAMPLES COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
