"""
NYC Taxi-style pipeline example using @task and @job decorators.

Demonstrates:
- @task decorator for creating task functions
- @job decorator for defining workflows
- Automatic dependency detection when passing Task as argument
- Upstream result injection at runtime

Usage:
    # Register job (requires PostgreSQL)
    python -m aaiclick.example_projects.taxi_pipeline

    # Then run worker to execute
    python -m aaiclick.orchestration.worker
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import OrchContext, job, task


@task
async def create_dataset(values: list) -> Object:
    """Create initial dataset from values."""
    return await create_object_from_value(values)


@task
async def transform_data(data: Object, multiplier: int) -> Object:
    """Transform data by multiplying values."""
    return await (data * multiplier)


@task
async def compute_stats(data: Object) -> dict:
    """Compute statistics on the data."""
    total = await (await data.sum()).data()
    mean = await (await data.mean()).data()
    count = await data.count()
    return {
        "sum": total,
        "mean": mean,
        "count": count,
    }


@task
async def combine_results(stats1: dict, stats2: dict) -> dict:
    """Combine statistics from multiple sources."""
    return {
        "combined_sum": stats1["sum"] + stats2["sum"],
        "combined_mean": (stats1["mean"] + stats2["mean"]) / 2,
        "total_count": stats1["count"] + stats2["count"],
    }


@job("taxi_pipeline")
def taxi_pipeline(fares: list, distances: list, multiplier: int = 2):
    """
    Example pipeline similar to NYC Taxi data processing.

    Creates two datasets (fares and distances), transforms them,
    computes stats, and combines results.

    DAG structure:
        create_fares -----> transform_fares -----> stats_fares ----+
                                                                    +--> combine
        create_distances -> transform_distances -> stats_distances -+
    """
    # Create datasets
    fares_obj = create_dataset(values=fares)
    distances_obj = create_dataset(values=distances)

    # Transform (auto-dependency: create >> transform)
    fares_transformed = transform_data(data=fares_obj, multiplier=multiplier)
    distances_transformed = transform_data(data=distances_obj, multiplier=multiplier)

    # Compute stats (auto-dependency: transform >> stats)
    fares_stats = compute_stats(data=fares_transformed)
    distances_stats = compute_stats(data=distances_transformed)

    # Combine results (auto-dependency: stats >> combine)
    combined = combine_results(stats1=fares_stats, stats2=distances_stats)

    return [
        fares_obj,
        distances_obj,
        fares_transformed,
        distances_transformed,
        fares_stats,
        distances_stats,
        combined,
    ]


async def main():
    """Register the taxi pipeline job."""
    async with OrchContext():
        # Job registration only needs OrchContext (PostgreSQL)
        # DataContext (ClickHouse) is used during task execution by workers
        created_job = await taxi_pipeline(
            fares=[10.5, 15.0, 8.75, 22.0, 12.5],
            distances=[2.5, 4.0, 1.8, 6.2, 3.1],
            multiplier=2,
        )
        print(f"Registered job: {created_job.name} (ID: {created_job.id})")
        print("Run worker to execute: python -m aaiclick.orchestration.worker")


if __name__ == "__main__":
    asyncio.run(main())
