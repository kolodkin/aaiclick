"""Tests for the NYC taxi pipeline example project.

Tests the core data loading and analysis functions without full orchestration.
Requires network access to fetch real NYC TLC taxi data from public URLs.

Set AAICLICK_URL_TEST_ENABLE=1 to run these tests.
"""

import os

import pytest

from aaiclick import create_object_from_url

# Test with small limit for speed
_TEST_LIMIT = int(os.getenv("AAICLICK_NYC_TEST_LIMIT", "100"))

# NYC TLC data URL
NYC_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

# Minimal columns for testing
TEST_COLUMNS = [
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
]


@pytest.mark.url
async def test_load_taxi_data(ctx):
    """Test loading NYC taxi data from Parquet URL."""
    trips = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=TEST_COLUMNS,
        format="Parquet",
        limit=_TEST_LIMIT,
    )
    data = await trips.data()

    assert isinstance(data, dict)
    assert len(data["fare_amount"]) == _TEST_LIMIT
    assert all(col in data for col in TEST_COLUMNS)


@pytest.mark.url
async def test_basic_aggregations(ctx):
    """Test basic aggregation operators on taxi data."""
    trips = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=["fare_amount", "tip_amount"],
        format="Parquet",
        limit=_TEST_LIMIT,
    )

    # Copy to materialized array Object for aggregation
    fares = await trips["fare_amount"].copy()

    # Test aggregations
    total = await (await fares.sum()).data()
    avg = await (await fares.mean()).data()
    minimum = await (await fares.min()).data()
    maximum = await (await fares.max()).data()
    count_obj = await fares.count()
    count = await count_obj.data()

    assert isinstance(total, (int, float))
    assert isinstance(avg, (int, float))
    assert isinstance(minimum, (int, float))
    assert isinstance(maximum, (int, float))
    assert count == _TEST_LIMIT

    # Sanity checks
    assert minimum <= avg <= maximum
    assert total >= 0


@pytest.mark.url
async def test_statistical_operators(ctx):
    """Test statistical operators (std, var, quantile) on taxi data."""
    # Single column creates array Object with column named "value"
    fares = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=["fare_amount"],
        format="Parquet",
        limit=_TEST_LIMIT,
    )

    # Test statistical operators
    std = await (await fares.std()).data()
    var = await (await fares.var()).data()
    median = await (await fares.quantile(0.5)).data()
    p25 = await (await fares.quantile(0.25)).data()
    p75 = await (await fares.quantile(0.75)).data()

    assert isinstance(std, (int, float))
    assert isinstance(var, (int, float))
    assert isinstance(median, (int, float))

    # Sanity checks
    assert std >= 0
    assert var >= 0
    assert p25 <= median <= p75


@pytest.mark.url
async def test_group_by_operations(ctx):
    """Test group by operations on taxi data."""
    trips = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=["payment_type", "fare_amount", "tip_amount"],
        format="Parquet",
        limit=_TEST_LIMIT,
    )

    # Group by payment type
    result = await trips.group_by("payment_type").agg({
        "fare_amount": "mean",
        "tip_amount": "sum",
    })

    data = await result.data()
    assert isinstance(data, dict)
    assert "payment_type" in data
    assert "fare_amount" in data
    assert "tip_amount" in data
    # Should have at least 1 payment type
    assert len(data["payment_type"]) >= 1


@pytest.mark.url
async def test_insert_from_url_with_limit(ctx):
    """Test insert_from_url with limit parameter."""
    # Create initial object with 50 rows
    trips = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=["fare_amount", "tip_amount"],
        format="Parquet",
        limit=50,
    )
    initial_count = len((await trips.data())["fare_amount"])
    assert initial_count == 50

    # Insert 25 more rows
    await trips.insert_from_url(
        url=NYC_TAXI_URL,
        columns=["fare_amount", "tip_amount"],
        format="Parquet",
        limit=25,
    )

    final_count = len((await trips.data())["fare_amount"])
    assert final_count == 75


@pytest.mark.url
async def test_column_operations(ctx):
    """Test column indexing and arithmetic operations."""
    trips = await create_object_from_url(
        url=NYC_TAXI_URL,
        columns=["fare_amount", "tip_amount", "trip_distance"],
        format="Parquet",
        limit=_TEST_LIMIT,
    )

    # Column indexing - copy to materialized Objects
    fares = await trips["fare_amount"].copy()
    tips = await trips["tip_amount"].copy()
    distances = await trips["trip_distance"].copy()

    # Arithmetic operations between arrays (same aai_ids, so element-wise works)
    tip_ratio = await (tips / fares)
    fare_per_mile = await (fares / distances)

    # Verify results
    tip_ratio_data = await tip_ratio.data()
    fare_per_mile_data = await fare_per_mile.data()

    assert len(tip_ratio_data) == _TEST_LIMIT
    assert len(fare_per_mile_data) == _TEST_LIMIT
