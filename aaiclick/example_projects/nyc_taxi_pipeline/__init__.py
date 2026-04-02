"""
NYC Taxi Pipeline - Distributed Computing Example

Demonstrates aaiclick's distributed computing capabilities using real
NYC TLC Yellow Taxi trip data loaded directly from Parquet URLs:

- URL Data Loading (create_object_from_url, insert_from_url)
- Dict Objects (columnar data)
- Basic Aggregations (count, sum, mean, min, max)
- Statistical Operators (std, var, quantile)
- Group By Operations (by zone, payment type, hour)
- Distributed Computation Patterns

Data source: NYC TLC Trip Record Data
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Usage:
    # Register job (requires PostgreSQL)
    python -m aaiclick.example_projects.nyc_taxi_pipeline

    # Then run worker to execute
    python -m aaiclick worker start
"""

import asyncio

from aaiclick import create_object_from_url
from aaiclick.data.object import Object
from aaiclick.orchestration import job, task

from .report import generate_summary_report

# NYC TLC Yellow Taxi data URLs (Parquet format)
NYC_TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_TAXI_2023_01 = f"{NYC_TAXI_BASE_URL}/yellow_tripdata_2023-01.parquet"
NYC_TAXI_2023_02 = f"{NYC_TAXI_BASE_URL}/yellow_tripdata_2023-02.parquet"

# Columns to load from NYC taxi data
# Full schema: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
TAXI_COLUMNS = [
    "tpep_pickup_datetime",   # Pickup timestamp
    "tpep_dropoff_datetime",  # Dropoff timestamp
    "passenger_count",        # Number of passengers
    "trip_distance",          # Trip distance in miles
    "PULocationID",           # Pickup location zone ID
    "DOLocationID",           # Dropoff location zone ID
    "payment_type",           # 1=Credit, 2=Cash, 3=No charge, 4=Dispute
    "fare_amount",            # Base fare
    "tip_amount",             # Tip (credit card only)
    "total_amount",           # Total charged
]


# =============================================================================
# Task Definitions - Each runs as a distributed task
# =============================================================================


@task
async def load_taxi_data(url: str, columns: list[str], limit: int | None = None) -> Object:
    """
    Load NYC taxi trip data from Parquet URL.

    Data flows directly into ClickHouse - zero Python memory footprint.
    Uses ClickHouse's native url() table function.
    """
    return await create_object_from_url(
        url=url,
        columns=columns,
        format="Parquet",
        limit=limit,
    )


@task
async def insert_more_data(target: Object, url: str, columns: list[str]) -> Object:
    """
    Insert additional data from another URL into existing Object.

    Enables incremental loading of multiple months/files.
    """
    await target.insert_from_url(url=url, columns=columns, format="Parquet")
    return target


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
    totals = trips["total_amount"]
    distances = trips["trip_distance"]
    passengers = trips["passenger_count"]

    total_trips = await (await fares.count()).data()
    total_fares = await (await fares.sum()).data()
    total_tips = await (await tips.sum()).data()
    total_revenue = await (await totals.sum()).data()
    avg_fare = await (await fares.mean()).data()
    avg_tip = await (await tips.mean()).data()
    avg_distance = await (await distances.mean()).data()
    avg_passengers = await (await passengers.mean()).data()
    min_fare = await (await fares.min()).data()
    max_fare = await (await fares.max()).data()
    min_distance = await (await distances.min()).data()
    max_distance = await (await distances.max()).data()

    return {
        "total_trips": total_trips,
        "total_fares": total_fares,
        "total_tips": total_tips,
        "total_revenue": total_revenue,
        "avg_fare": avg_fare,
        "avg_tip": avg_tip,
        "avg_distance": avg_distance,
        "avg_passengers": avg_passengers,
        "min_fare": min_fare,
        "max_fare": max_fare,
        "min_distance": min_distance,
        "max_distance": max_distance,
    }


@task
async def compute_statistical_metrics(trips: Object) -> dict:
    """
    Compute statistical aggregations - algebraic combination.

    Distributed pattern:
        var()      -> (sum_x2 - sum_x^2/n) / (n-1)
        std()      -> sqrt(variance)
        quantile() -> t-digest approximation
    """
    fares = trips["fare_amount"]
    tips = trips["tip_amount"]
    distances = trips["trip_distance"]

    # Fare statistics
    fare_std = await (await fares.std()).data()
    fare_var = await (await fares.var()).data()
    fare_p25 = await (await fares.quantile(0.25)).data()
    fare_median = await (await fares.quantile(0.5)).data()
    fare_p75 = await (await fares.quantile(0.75)).data()
    fare_p90 = await (await fares.quantile(0.9)).data()
    fare_p99 = await (await fares.quantile(0.99)).data()

    # Tip statistics
    tip_std = await (await tips.std()).data()
    tip_median = await (await tips.quantile(0.5)).data()
    tip_p90 = await (await tips.quantile(0.9)).data()

    # Distance statistics
    distance_std = await (await distances.std()).data()
    distance_median = await (await distances.quantile(0.5)).data()
    distance_p90 = await (await distances.quantile(0.9)).data()

    return {
        "fare_std": fare_std,
        "fare_var": fare_var,
        "fare_p25": fare_p25,
        "fare_median": fare_median,
        "fare_p75": fare_p75,
        "fare_p90": fare_p90,
        "fare_p99": fare_p99,
        "tip_std": tip_std,
        "tip_median": tip_median,
        "tip_p90": tip_p90,
        "distance_std": distance_std,
        "distance_median": distance_median,
        "distance_p90": distance_p90,
    }


@task
async def analyze_by_pickup_zone(trips: Object) -> Object:
    """
    Group by pickup location zone - aggregations per partition, then merge.

    Returns Dict Object with zone-level statistics.
    """
    result = await trips.group_by("PULocationID").agg({
        "fare_amount": "sum",
        "tip_amount": "sum",
        "trip_distance": "mean",
        "total_amount": "sum",
    })
    return result


@task
async def analyze_by_payment_type(trips: Object) -> Object:
    """
    Group by payment type - compare card vs cash behavior.

    Payment types: 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
    """
    result = await trips.group_by("payment_type").agg({
        "fare_amount": "mean",
        "tip_amount": "mean",
        "trip_distance": "mean",
        "total_amount": "sum",
    })
    return result


@task
async def analyze_by_passenger_count(trips: Object) -> Object:
    """
    Group by passenger count - analyze trip patterns by group size.
    """
    result = await trips.group_by("passenger_count").agg({
        "fare_amount": "mean",
        "trip_distance": "mean",
        "total_amount": "sum",
    })
    return result


@task
async def find_top_revenue_zones(trips: Object, min_revenue: float = 100000) -> Object:
    """
    Filter groups using HAVING - zones with total revenue above threshold.

    Demonstrates post-aggregation filtering.
    """
    result = await (
        trips.group_by("PULocationID")
        .having(f"sum(total_amount) > {min_revenue}")
        .agg({
            "total_amount": "sum",
            "fare_amount": "sum",
            "tip_amount": "sum",
        })
    )
    return result


@task
async def compute_tip_analysis(trips: Object) -> dict:
    """
    Compute tip-related metrics.

    Tip analysis is interesting because tips are only recorded for credit cards.
    """
    tips = trips["tip_amount"]
    fares = trips["fare_amount"]
    totals = trips["total_amount"]

    # Tip as percentage of fare (avoid division by zero via WHERE)
    tip_ratio = await (tips / fares)
    tip_pct = await (tip_ratio * 100)
    tip_share_ratio = await (tips / totals)
    tip_share = await (tip_share_ratio * 100)

    avg_tip = await (await tips.mean()).data()
    median_tip = await (await tips.quantile(0.5)).data()
    avg_tip_pct = await (await tip_pct.mean()).data()
    median_tip_pct = await (await tip_pct.quantile(0.5)).data()
    max_tip = await (await tips.max()).data()
    tip_share_of_total = await (await tip_share.mean()).data()

    return {
        "avg_tip": avg_tip,
        "median_tip": median_tip,
        "avg_tip_pct": avg_tip_pct,
        "median_tip_pct": median_tip_pct,
        "max_tip": max_tip,
        "tip_share_of_total": tip_share_of_total,
    }


@task
async def compute_distance_analysis(trips: Object) -> dict:
    """
    Analyze trip distances - useful for understanding trip patterns.
    """
    distances = trips["trip_distance"]
    fares = trips["fare_amount"]

    # Fare per mile
    fare_per_mile = await (fares / distances)

    # Compute metrics
    avg_distance = await (await distances.mean()).data()
    median_distance = await (await distances.quantile(0.5)).data()
    short_trips = await (distances < 1)
    short_trips_pct = (await (await short_trips.mean()).data()) * 100
    long_trips = await (distances > 10)
    long_trips_pct = (await (await long_trips.mean()).data()) * 100
    avg_fare_per_mile = await (await fare_per_mile.mean()).data()
    median_fare_per_mile = await (await fare_per_mile.quantile(0.5)).data()

    return {
        "avg_distance": avg_distance,
        "median_distance": median_distance,
        "short_trips_pct": short_trips_pct,
        "long_trips_pct": long_trips_pct,
        "avg_fare_per_mile": avg_fare_per_mile,
        "median_fare_per_mile": median_fare_per_mile,
    }


# =============================================================================
# Job Definition - Workflow DAG
# =============================================================================


@job("nyc_taxi_analysis")
def nyc_taxi_pipeline(
    url: str = NYC_TAXI_2023_01,
    columns: list[str] = TAXI_COLUMNS,
    limit: int | None = 100000,  # Limit for demo; remove for full dataset
):
    """
    NYC Taxi Data Analysis Pipeline.

    Loads real NYC TLC Yellow Taxi data from Parquet URLs and performs
    comprehensive analysis - all computation happens in ClickHouse.

    Demonstrates distributed computing patterns:
    1. Load data from URL directly into ClickHouse (zero Python memory)
    2. Parallel aggregation tasks (basic stats, statistical metrics)
    3. Group by operations (zone, payment type, passenger count)
    4. HAVING filters (top revenue zones)
    5. Derived computations (tip %, fare per mile)
    6. Final report combining all results

    DAG Structure:
                          +-> basic_stats -----------+
                          |                          |
                          +-> statistical_metrics ---+
                          |                          |
        load_data --------+-> by_pickup_zone         +-> summary_report
                          |                          |
                          +-> by_payment ------------+
                          |                          |
                          +-> by_passenger_count     |
                          |                          |
                          +-> top_revenue_zones      |
                          |                          |
                          +-> tip_analysis ----------+
                          |                          |
                          +-> distance_analysis -----+
    """
    # Load taxi data from URL
    trips = load_taxi_data(url=url, columns=columns, limit=limit)

    # Parallel analysis branches - all run independently
    basic_stats = compute_basic_stats(trips=trips)
    statistical_metrics = compute_statistical_metrics(trips=trips)

    # Group by analyses
    by_pickup_zone = analyze_by_pickup_zone(trips=trips)
    by_payment = analyze_by_payment_type(trips=trips)
    by_passenger = analyze_by_passenger_count(trips=trips)

    # HAVING filter - top revenue zones
    top_zones = find_top_revenue_zones(trips=trips, min_revenue=10000)

    # Derived metrics
    tip_analysis = compute_tip_analysis(trips=trips)
    distance_analysis = compute_distance_analysis(trips=trips)

    # Final report (depends on multiple upstream tasks)
    report = generate_summary_report(
        trips=trips,
        basic_stats=basic_stats,
        statistical_metrics=statistical_metrics,
        tip_analysis=tip_analysis,
        distance_analysis=distance_analysis,
        by_payment=by_payment,
        by_pickup_zone=by_pickup_zone,
        by_passenger=by_passenger,
        top_zones=top_zones,
    )

    return report


async def main():
    """Register the NYC taxi analysis pipeline job."""
    created_job = await nyc_taxi_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
