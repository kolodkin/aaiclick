# Public Datasets for Distributed Processing Research

This document explores publicly available datasets suitable for demonstrating distributed data processing and statistics calculation with aaiclick.

## Dataset Candidates

### 1. NYC Taxi Trip Data (Recommended)

**Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**AWS Registry**: [NYC TLC Trip Records](https://registry.opendata.aws/nyc-tlc-trip-records-pds/)

| Property | Value |
|----------|-------|
| Size | 3+ billion trips since 2009 |
| Format | Parquet (since May 2022) |
| S3 Bucket | `s3://nyc-tlc/` |
| Region | `us-east-1` |
| Update Frequency | Monthly (2-month delay) |
| Access | Free, AWS account for direct S3 |

**Schema (Yellow Taxi)**:
```
- VendorID: Integer
- tpep_pickup_datetime: Timestamp
- tpep_dropoff_datetime: Timestamp
- passenger_count: Integer
- trip_distance: Float
- PULocationID: Integer (pickup zone)
- DOLocationID: Integer (dropoff zone)
- payment_type: Integer
- fare_amount: Float
- tip_amount: Float
- total_amount: Float
```

**Why It's Great for Distributed Processing**:
- Naturally partitioned by year/month
- Rich for statistics (temporal patterns, geographic analysis, pricing)
- Well-documented and widely used in benchmarks
- Large enough to benefit from distribution

**Example Statistics to Calculate**:
- Average trip distance by hour of day
- Tip percentage distribution by payment type
- Busiest pickup/dropoff zones
- Revenue trends over time

---

### 2. UK House Prices

**Source**: [ClickHouse Datasets Documentation](https://clickhouse.com/docs/getting-started/example-datasets/uk-price-paid)

| Property | Value |
|----------|-------|
| Size | ~30 million transactions |
| Format | Parquet |
| S3 Bucket | `s3://datasets-documentation/uk-house-prices/parquet/` |
| Region | `eu-west-3` |
| Time Range | 1995 - present |
| Access | Public, no auth required |

**Schema**:
```
- price: UInt32 (transaction price in GBP)
- date: Date
- postcode: String
- type: Enum (detached, semi-detached, terraced, flat)
- is_new: Bool
- duration: Enum (freehold, leasehold)
- addr1, addr2: String
- street, locality, town, district, county: String
```

**Direct Query Example**:
```sql
SELECT
    toYear(date) AS year,
    round(avg(price)) AS avg_price
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/uk-house-prices/parquet/house_prices_all.parquet'
)
WHERE town = 'LONDON'
GROUP BY year
ORDER BY year ASC
```

**Example Statistics**:
- Price trends by region/year
- Property type distribution
- New vs existing property price premium
- Geographic price heatmaps

---

### 3. ClickBench Hits Dataset

**Source**: [ClickBench GitHub](https://github.com/ClickHouse/ClickBench)

| Property | Value |
|----------|-------|
| Size | ~100 million rows |
| Format | CSV, TSV, JSON, Parquet |
| Source | Yandex.Metrica web analytics |
| Access | Public download |

**URLs**:
- CSV: `https://datasets.clickhouse.com/hits_compatible/hits.csv.gz`
- Parquet: `https://datasets.clickhouse.com/hits_compatible/hits.parquet`
- Partitioned (100 files): Available for distributed testing

**Schema** (105 columns including):
```
- WatchID: UInt64
- JavaEnable: UInt8
- Title: String
- GoodEvent: Int16
- EventTime: DateTime
- EventDate: Date
- CounterID: UInt32
- ClientIP: UInt32
- Region: UInt32
- URL: String
- Referer: String
- IsRefresh: UInt8
- RefererCategoryID: UInt16
- ... (many more web analytics fields)
```

**Example Statistics**:
- Page views by region
- Browser/OS distribution
- Referrer analysis
- Time-based traffic patterns

---

### 4. Forex Historical Data

**Source**: [ClickHouse Datasets Documentation](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)

| Property | Value |
|----------|-------|
| Size | 269 files (monthly partitions) |
| Format | Parquet, CSV (zst compressed) |
| S3 Path | `s3://datasets-documentation/forex/parquet/year_month/` |
| Time Range | ~22 years |

**Use Cases**:
- Time series analysis
- Currency pair correlations
- Volatility calculations
- Moving averages and technical indicators

---

## Recommended Dataset: NYC Taxi

For aaiclick distributed processing demonstration, **NYC Taxi** is the best choice because:

1. **Natural Partitioning**: Files organized by `year/month` - perfect for distributed reads
2. **Rich Statistics**: Many dimensions for aggregation (time, location, payment, distance)
3. **Real-World Scale**: Billions of rows requiring actual distributed processing
4. **Well-Known**: Familiar to data engineering community, easy to validate results
5. **Active Updates**: New data added monthly

---

## Distributed Processing Architecture

### Pattern: Partition-Process-Aggregate

```
                    ┌─────────────────────────────────────────┐
                    │           S3: NYC Taxi Data             │
                    │  /2023/01/*.parquet                     │
                    │  /2023/02/*.parquet                     │
                    │  /2023/03/*.parquet                     │
                    │  ...                                    │
                    └─────────────────┬───────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ↓                       ↓                       ↓
    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │ Worker 1        │     │ Worker 2        │     │ Worker 3        │
    │ Read: 2023/01   │     │ Read: 2023/02   │     │ Read: 2023/03   │
    │ Calc: local stats│    │ Calc: local stats│    │ Calc: local stats│
    └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
             │                       │                       │
             └───────────────────────┼───────────────────────┘
                                     ↓
                         ┌─────────────────────┐
                         │ Aggregator          │
                         │ Combine partial     │
                         │ statistics          │
                         └─────────────────────┘
                                     ↓
                         ┌─────────────────────┐
                         │ Final Statistics    │
                         │ - Avg trip distance │
                         │ - Revenue by zone   │
                         │ - Temporal patterns │
                         └─────────────────────┘
```

### Implementation with aaiclick

**Step 1: Define Partition Reader Task**

```python
async def read_partition_task(ctx: OrchContext, year: int, month: int) -> dict:
    """Read one month of taxi data and compute local statistics."""

    # Read from S3 via ClickHouse
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    query = f"""
        SELECT
            count() as trip_count,
            sum(trip_distance) as total_distance,
            sum(total_amount) as total_revenue,
            sum(tip_amount) as total_tips,
            avg(trip_distance) as avg_distance,
            avg(total_amount) as avg_fare
        FROM url('{url}', Parquet)
        WHERE trip_distance > 0 AND total_amount > 0
    """

    result = await ctx.data_context.ch_client.query(query)
    row = result.result_rows[0]

    return {
        "year": year,
        "month": month,
        "trip_count": row[0],
        "total_distance": row[1],
        "total_revenue": row[2],
        "total_tips": row[3],
        "avg_distance": row[4],
        "avg_fare": row[5],
    }
```

**Step 2: Define Aggregator Task**

```python
async def aggregate_stats_task(ctx: OrchContext, partial_stats: list[dict]) -> dict:
    """Combine partial statistics from all partitions."""

    total_trips = sum(s["trip_count"] for s in partial_stats)
    total_distance = sum(s["total_distance"] for s in partial_stats)
    total_revenue = sum(s["total_revenue"] for s in partial_stats)
    total_tips = sum(s["total_tips"] for s in partial_stats)

    return {
        "total_trips": total_trips,
        "total_distance": total_distance,
        "total_revenue": total_revenue,
        "total_tips": total_tips,
        "avg_distance": total_distance / total_trips,
        "avg_fare": total_revenue / total_trips,
        "avg_tip_pct": (total_tips / total_revenue) * 100,
    }
```

**Step 3: Create Distributed Job**

```python
from aaiclick.orchestration import Job, Task

def create_taxi_stats_job(year: int, months: list[int]) -> Job:
    """Create a job to compute taxi statistics across multiple months."""

    # Partition tasks - one per month
    partition_tasks = [
        Task(
            name=f"read_{year}_{month:02d}",
            fn="taxi_stats:read_partition_task",
            params={"year": year, "month": month},
        )
        for month in months
    ]

    # Aggregation task - depends on all partitions
    agg_task = Task(
        name="aggregate",
        fn="taxi_stats:aggregate_stats_task",
        depends_on=[t.name for t in partition_tasks],
    )

    return Job(
        name=f"taxi_stats_{year}",
        tasks=partition_tasks + [agg_task],
    )

# Usage
job = create_taxi_stats_job(2023, [1, 2, 3, 4, 5, 6])
result = await job.run()
```

---

## Statistics to Compute

### Basic Aggregations

| Statistic | SQL | Distributed Strategy |
|-----------|-----|---------------------|
| Total trips | `count()` | Sum of partition counts |
| Avg distance | `avg(trip_distance)` | Weighted average |
| Total revenue | `sum(total_amount)` | Sum of partition sums |
| Max fare | `max(total_amount)` | Max of partition maxes |

### Grouped Statistics

| Statistic | Complexity | Notes |
|-----------|------------|-------|
| By hour of day | Low | 24 buckets, combine counts |
| By day of week | Low | 7 buckets |
| By pickup zone | Medium | ~260 zones |
| By zone pair | High | 260×260 combinations |

### Advanced Analytics

1. **Percentiles**: Requires t-digest or exact sorting
2. **Moving averages**: Needs temporal ordering
3. **Correlations**: Needs paired data points
4. **Anomaly detection**: Needs full distribution

---

## Data Access Methods

### Method 1: ClickHouse URL Function (Simplest)

```sql
SELECT * FROM url(
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet',
    Parquet
) LIMIT 10
```

### Method 2: ClickHouse S3 Function (AWS)

```sql
SELECT * FROM s3(
    'https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2023-01.parquet',
    Parquet
) LIMIT 10
```

### Method 3: s3Cluster for Distributed Reads

```sql
SELECT * FROM s3Cluster(
    'default',
    'https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2023-*.parquet',
    Parquet
)
```

---

## Current Object/View Capabilities

### Available Operators

The current `Object` class provides these operators that work on single-column (`value`) data:

#### Element-wise Binary Operators (14)

| Category | Operators | Notes |
|----------|-----------|-------|
| Arithmetic | `+`, `-`, `*`, `/`, `//`, `%`, `**` | Element-wise on arrays |
| Comparison | `==`, `!=`, `<`, `<=`, `>`, `>=` | Returns boolean Object |
| Bitwise | `&`, `\|`, `^` | Integer types only |

#### Aggregation Operators (6)

| Method | ClickHouse Function | Distributed Strategy |
|--------|--------------------|--------------------|
| `min()` | `min(value)` | Min of partition mins |
| `max()` | `max(value)` | Max of partition maxes |
| `sum()` | `sum(value)` | Sum of partition sums |
| `mean()` | `avg(value)` | Weighted: `Σsum / Σcount` |
| `std()` | `stddevPop(value)` | Requires `Σx`, `Σx²`, `n` |
| `unique()` | `GROUP BY value` | Union of partition uniques |

#### Data Operations

| Method | Description |
|--------|-------------|
| `copy()` | Create copy of Object |
| `concat(*args)` | Concatenate multiple Objects (new Object) |
| `insert(*args)` | Append to existing Object (in-place) |

#### View Constraints

| Constraint | SQL Clause | Example |
|------------|-----------|---------|
| `where` | `WHERE` | `view(where="value > 10")` |
| `limit` | `LIMIT` | `view(limit=100)` |
| `offset` | `OFFSET` | `view(offset=50)` |
| `order_by` | `ORDER BY` | `view(order_by="value DESC")` |

### Mapping to NYC Taxi Statistics

| Statistic | Current Support | How |
|-----------|-----------------|-----|
| Trip count | Via `sum()` on count column | Need to create count Object first |
| Total revenue | `sum()` | Load `total_amount` as Object |
| Avg distance | `mean()` | Load `trip_distance` as Object |
| Min/max fare | `min()` / `max()` | Load `fare_amount` as Object |
| Std deviation | `std()` | Load column as Object |
| Unique zones | `unique()` | Load zone column as Object |

### Current Limitations

| Gap | Impact | Potential Solution |
|-----|--------|-------------------|
| Single `value` column only | Can't do multi-column stats | Support dict Objects in aggregations |
| No `count()` aggregation | Must create intermediary | Add `count()` method |
| No `variance()` | Can derive from `std()` | Add `var()` method |
| No grouped aggregation | Can't do "avg by zone" | Add `group_by()` support |
| No percentile/median | Need approximate or exact | Add `quantile()` method |
| No correlation | Need pairs of columns | Add `corr()` for dict Objects |

---

## Low-Hanging Fruit Improvements

### 1. Add `count()` Method (Easy)

```python
async def count(self) -> int:
    """Return count of rows."""
    query = f"SELECT count() FROM {self._build_select()}"
    result = await self.ch_client.query(query)
    return result.result_rows[0][0]
```

**Why**: Essential for weighted averages in distributed aggregation.

### 2. Add `var()` Method (Easy)

```python
# In AGGREGATION_FUNCTIONS
"var": "varPop",
```

**Why**: Common statistical operation, trivial to add alongside `std()`.

### 3. Add `quantile()` Method (Medium)

```python
async def quantile(self, q: float = 0.5) -> Self:
    """Calculate quantile (default median)."""
    # Uses ClickHouse's quantile() function
```

**Why**: Median/percentiles are common statistics. ClickHouse has efficient approximate quantiles.

### 4. Dict Object Aggregations (Medium)

Currently aggregations only work on `value` column. Extend to support:

```python
# Current: only works on single 'value' column
obj = await create_object_from_value([1, 2, 3, 4, 5])
result = await obj.sum()  # Works

# Desired: work on dict objects with column selection
obj = await create_object_from_value({
    "distance": [1.5, 2.3, 3.1],
    "fare": [10, 15, 20]
})
result = await obj.sum(column="fare")  # Return sum of fares
```

### 5. Grouped Aggregations (Larger)

For "average fare by zone" type queries:

```python
# Desired API
result = await obj.group_by("zone").mean("fare")
```

This would require:
- New `GroupBy` class or method
- SQL generation for `GROUP BY` clause
- Result as dict Object with group keys + aggregated values

---

## Example: NYC Taxi with Current API

Given current limitations, here's how to compute basic stats:

```python
async with DataContext() as ctx:
    # Load a single column from parquet
    # (would need new function to load from URL)
    distances = await create_object_from_value([...])  # trip_distance values
    fares = await create_object_from_value([...])      # fare_amount values

    # Compute statistics
    avg_distance = await distances.mean()
    total_fare = await fares.sum()
    max_fare = await fares.max()
    std_fare = await fares.std()

    # Get results
    print(await avg_distance.data())  # Single value
    print(await total_fare.data())
```

### What's Missing for Full Taxi Analysis

1. **Data Loading**: No `create_object_from_url()` or S3 loader
2. **Multi-column**: Can't load full taxi schema as dict Object and aggregate
3. **Grouping**: Can't do "by hour" or "by zone" aggregations
4. **Count**: Need explicit count for weighted averages

---

## Recommended Implementation Order

1. **`count()` method** - Trivial, immediately useful
2. **`var()` method** - Trivial, completes statistical suite
3. **Dict column aggregation** - Medium, unlocks multi-column data
4. **`quantile()` method** - Medium, adds percentile support
5. **Data loader from URL** - Medium, enables external data
6. **`group_by()` support** - Larger, enables dimensional analysis

---

## Sources

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [AWS Registry - NYC TLC](https://registry.opendata.aws/nyc-tlc-trip-records-pds/)
- [ClickHouse S3 Integration](https://clickhouse.com/docs/knowledgebase/ingest-parquet-files-in-s3)
- [ClickBench Dataset](https://github.com/ClickHouse/ClickBench)
- [UK House Prices Dataset](https://clickhouse.com/docs/getting-started/example-datasets/uk-price-paid)
- [Getting Data Into ClickHouse - S3](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)
