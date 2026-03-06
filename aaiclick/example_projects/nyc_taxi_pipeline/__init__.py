"""
NYC Taxi Pipeline - Distributed Computing Example

Demonstrates aaiclick's distributed computing capabilities:
- Dict Objects (columnar data)
- Basic Aggregations (count, sum, mean, min, max)
- Statistical Operators (std, var, quantile)
- Group By Operations (by zone, payment type, hour)
- Distributed Computation Patterns

Usage:
    # Register job (requires PostgreSQL)
    python -m aaiclick.example_projects.nyc_taxi_pipeline

    # Then run worker to execute
    python -m aaiclick.orchestration.worker
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import job, task


# =============================================================================
# Task Definitions - Each runs as a distributed task
# =============================================================================


@task
async def load_taxi_data() -> Object:
    """Load simulated NYC taxi trip data as a Dict Object."""
    return await create_object_from_value({
        "pickup_hour": [8, 9, 12, 18, 19, 22, 8, 9, 17, 18, 7, 23, 14, 15, 20],
        "pickup_zone": [
            "Manhattan", "Manhattan", "Brooklyn", "Manhattan", "Queens",
            "Brooklyn", "Manhattan", "Brooklyn", "Manhattan", "Queens",
            "Bronx", "Manhattan", "Brooklyn", "Manhattan", "Queens"
        ],
        "payment_type": [
            "card", "card", "cash", "card", "card",
            "cash", "card", "cash", "card", "card",
            "cash", "card", "card", "cash", "card"
        ],
        "fare_amount": [
            15.50, 22.00, 8.75, 35.00, 28.50,
            12.00, 18.25, 9.50, 42.00, 31.75,
            7.25, 55.00, 14.50, 19.00, 26.00
        ],
        "tip_amount": [
            3.10, 4.40, 0.00, 7.00, 5.70,
            0.00, 3.65, 0.00, 8.40, 6.35,
            0.00, 11.00, 2.90, 0.00, 5.20
        ],
        "trip_distance": [
            2.5, 4.1, 1.8, 8.2, 6.5,
            2.2, 3.3, 1.5, 9.8, 7.1,
            1.2, 12.5, 2.8, 3.5, 5.9
        ],
    })


@task
async def compute_basic_stats(trips: Object) -> dict:
    """
    Compute basic aggregations - directly combinable across partitions.

    Distributed pattern:
        count()  -> sum of partial counts
        sum()    -> sum of partial sums
        min()    -> min of partial mins
        max()    -> max of partial maxes
        mean()   -> sum(x) / count(x)
    """
    fares = trips["fare_amount"]
    tips = trips["tip_amount"]
    distances = trips["trip_distance"]

    return {
        "total_trips": len(await fares.data()),
        "total_fares": await (await fares.sum()).data(),
        "total_tips": await (await tips.sum()).data(),
        "avg_fare": await (await fares.mean()).data(),
        "avg_distance": await (await distances.mean()).data(),
        "min_fare": await (await fares.min()).data(),
        "max_fare": await (await fares.max()).data(),
    }


@task
async def compute_statistical_metrics(trips: Object) -> dict:
    """
    Compute statistical aggregations - algebraic combination.

    Distributed pattern:
        var()    -> (sum_x2 - sum_x^2/n) / (n-1)
        std()    -> sqrt(variance)
        quantile() -> t-digest approximation
    """
    fares = trips["fare_amount"]
    tips = trips["tip_amount"]

    return {
        "fare_std": await (await fares.std()).data(),
        "fare_var": await (await fares.var()).data(),
        "fare_median": await (await fares.quantile(0.5)).data(),
        "fare_p90": await (await fares.quantile(0.9)).data(),
        "tip_std": await (await tips.std()).data(),
    }


@task
async def analyze_by_zone(trips: Object) -> Object:
    """
    Group by pickup zone - aggregations per partition, then merge.

    Returns Dict Object with zone-level statistics.
    """
    return await trips.group_by("pickup_zone").agg({
        "fare_amount": "sum",
        "tip_amount": "sum",
        "trip_distance": "mean",
    })


@task
async def analyze_by_payment(trips: Object) -> Object:
    """
    Group by payment type - compare card vs cash behavior.

    Demonstrates multi-aggregation with different functions.
    """
    return await trips.group_by("payment_type").agg({
        "fare_amount": "mean",
        "tip_amount": "mean",
    })


@task
async def analyze_by_hour(trips: Object) -> Object:
    """
    Group by pickup hour - time-based demand patterns.
    """
    return await trips.group_by("pickup_hour").count()


@task
async def find_high_revenue_zones(trips: Object) -> Object:
    """
    Filter groups using HAVING - zones with total fares > $50.

    Demonstrates post-aggregation filtering.
    """
    return await (
        trips.group_by("pickup_zone")
        .having("sum(fare_amount) > 50")
        .sum("fare_amount")
    )


@task
async def compute_tip_percentages(trips: Object) -> dict:
    """
    Compute derived metrics - tip as percentage of fare.

    All computation happens in ClickHouse.
    """
    tips = trips["tip_amount"]
    fares = trips["fare_amount"]

    tip_pct = await ((tips / fares) * 100)

    return {
        "avg_tip_pct": await (await tip_pct.mean()).data(),
        "max_tip_pct": await (await tip_pct.max()).data(),
        "median_tip_pct": await (await tip_pct.quantile(0.5)).data(),
    }


@task
async def generate_summary_report(
    basic_stats: dict,
    statistical_metrics: dict,
    tip_analysis: dict,
    by_zone: Object,
    by_payment: Object,
) -> dict:
    """
    Combine all analysis results into final report.
    """
    zone_data = await by_zone.data()
    payment_data = await by_payment.data()

    return {
        "overview": {
            "total_trips": basic_stats["total_trips"],
            "total_revenue": basic_stats["total_fares"] + basic_stats["total_tips"],
            "avg_fare": basic_stats["avg_fare"],
            "avg_distance": basic_stats["avg_distance"],
        },
        "statistics": {
            "fare_std": statistical_metrics["fare_std"],
            "fare_median": statistical_metrics["fare_median"],
            "fare_p90": statistical_metrics["fare_p90"],
        },
        "tip_analysis": tip_analysis,
        "zones": {
            zone: {
                "total_fares": zone_data["fare_amount"][i],
                "total_tips": zone_data["tip_amount"][i],
            }
            for i, zone in enumerate(zone_data["pickup_zone"])
        },
        "payment_comparison": {
            ptype: {
                "avg_fare": payment_data["fare_amount"][i],
                "avg_tip": payment_data["tip_amount"][i],
            }
            for i, ptype in enumerate(payment_data["payment_type"])
        },
    }


# =============================================================================
# Job Definition - Workflow DAG
# =============================================================================


@job("nyc_taxi_analysis")
def nyc_taxi_pipeline():
    """
    NYC Taxi Data Analysis Pipeline.

    Demonstrates distributed computing patterns:
    1. Load columnar data as Dict Object
    2. Parallel aggregation tasks (basic stats, statistical metrics)
    3. Group by operations (zone, payment, hour)
    4. HAVING filters (high revenue zones)
    5. Derived computations (tip percentages)
    6. Final report combining all results

    DAG Structure:
                      +-> basic_stats --------+
                      |                       |
        load_data ----+-> statistical_metrics-+-> summary_report
                      |                       |
                      +-> by_zone ------------+
                      |                       |
                      +-> by_payment ---------+
                      |
                      +-> by_hour
                      |
                      +-> high_revenue_zones
                      |
                      +-> tip_percentages ----+
    """
    # Load taxi data
    trips = load_taxi_data()

    # Parallel analysis branches
    basic_stats = compute_basic_stats(trips=trips)
    statistical_metrics = compute_statistical_metrics(trips=trips)

    # Group by analyses
    by_zone = analyze_by_zone(trips=trips)
    by_payment = analyze_by_payment(trips=trips)
    by_hour = analyze_by_hour(trips=trips)

    # HAVING filter
    high_revenue = find_high_revenue_zones(trips=trips)

    # Derived metrics
    tip_analysis = compute_tip_percentages(trips=trips)

    # Final report (depends on multiple upstream tasks)
    report = generate_summary_report(
        basic_stats=basic_stats,
        statistical_metrics=statistical_metrics,
        tip_analysis=tip_analysis,
        by_zone=by_zone,
        by_payment=by_payment,
    )

    return [
        trips,
        basic_stats,
        statistical_metrics,
        by_zone,
        by_payment,
        by_hour,
        high_revenue,
        tip_analysis,
        report,
    ]


async def main():
    """Register the NYC taxi analysis pipeline job."""
    created_job = await nyc_taxi_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    print(f"Tasks: {len(created_job.tasks) if hasattr(created_job, 'tasks') else 'N/A'}")
    print("\nRun worker to execute: python -m aaiclick.orchestration.worker")


if __name__ == "__main__":
    asyncio.run(main())
