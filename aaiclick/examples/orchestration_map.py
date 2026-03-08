"""
Map operator example for aaiclick orchestration.

Demonstrates using map() to partition an Object and apply a callback
to each partition in parallel. Shows:
1. Defining a callback with @task
2. Using map() to create parallel partition tasks
3. Executing the pipeline with job_test()

Note: This requires running PostgreSQL and ClickHouse servers.
"""

import asyncio

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.orchestration import JobStatus, ajob_test, job, map, task


@task
async def load_numbers() -> list:
    """Load sample data into an Object."""
    return list(range(1, 101))


async def double(row):
    """Callback applied to each row by map_part."""
    print(f"  Processing row: {row}")


@job("map_example")
def map_pipeline():
    """Pipeline that loads data and maps a callback over partitions."""
    data = load_numbers()
    mapped = map(cbk=double, obj=data, partition=25)
    return [data, mapped]


async def amain():
    """Run the map example."""
    print("=" * 50)
    print("aaiclick Map Operator Example")
    print("=" * 50)

    async with data_context():
        job_instance = await map_pipeline()
        print(f"Created job: {job_instance.name} (ID: {job_instance.id})")

        await ajob_test(job_instance)
        print(f"Job status: {job_instance.status}")
        assert job_instance.status == JobStatus.COMPLETED


if __name__ == "__main__":
    asyncio.run(amain())
