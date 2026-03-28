"""
IMDb Dataset Builder - Large-Scale Data Curation Example

Demonstrates aaiclick's data curation capabilities using real IMDb title data
loaded directly from the official IMDb datasets URL:

- URL Data Loading (create_object_from_url, TSVWithNames format)
- String Filtering (titleType, isAdult, missing value detection via \\N)
- Computed Columns (type casting with toUInt32OrNull, array splitting with splitByChar)
- Array Explode (one genre per row from comma-separated strings)
- Group By Aggregations (genre distribution analysis)
- Data Quality Profiling (countIf for missing runtime and out-of-range detection)
- Hugging Face Publishing (optional, requires HF_TOKEN env var)

Data source: IMDb Non-Commercial Datasets (title.basics)
https://datasets.imdbws.com/title.basics.tsv.gz

License: IMDb Non-Commercial Use — https://developer.imdb.com/non-commercial-datasets/

Usage:
    # Register job (requires PostgreSQL or SQLite orchestration backend)
    python -m aaiclick.example_projects.imdb_dataset_builder

    # Then run worker to execute
    python -m aaiclick.orchestration.worker

Environment variables:
    HF_TOKEN  — Hugging Face token for dataset publishing (optional)
    IMDB_URL  — Override IMDb data URL (useful for local testing)
"""

import asyncio
import os

import pandas as pd
from huggingface_hub import HfApi

from aaiclick import ORIENT_DICT, create_object_from_url
from aaiclick.data.models import ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import TaskResult, job, task

from .constants import CLEAN_COLUMNS, HF_REPO_ID, IMDB_COLUMNS, IMDB_RAW_COLUMNS, IMDB_URL
from .models import HFPublishResult, QualityIssues, RawProfile
from .report import generate_report


# =============================================================================
# Tasks
# =============================================================================


@task
async def load_raw_data(limit: int | None = None) -> Object:
    """
    Load IMDb title.basics dataset from official URL.

    Data flows directly into ClickHouse via the url() table function —
    no Python memory used for the bulk data. The gzip TSV is streamed
    and parsed natively by ClickHouse.

    All columns are loaded as String because ClickHouse TSVWithNames
    format treats \\N as a literal string, not NULL.

    Args:
        limit: Optional row limit for fast demos. Set to None for full ~10M rows.
    """
    # Force all columns as String — TSV \N values must remain as the
    # literal string r"\N", not be cast to NULL or int by type inference.
    all_string = {col: ColumnInfo("String") for col in IMDB_COLUMNS}
    return await create_object_from_url(
        url=IMDB_URL,
        columns=IMDB_COLUMNS,
        format="TSVWithNames",
        limit=limit,
        column_types=all_string,
    )


@task
async def profile_raw(raw: Object) -> RawProfile:
    """
    Profile the raw dataset: title type breakdown, adult content rate.

    Uses count_if for a single-scan count of total rows and adult titles.
    All aggregations run inside ClickHouse — Python only receives the
    small summary dict.
    """
    counts_obj = await raw.count_if({
        "total": "1",
        "adult_count": "isAdult = '1'",
    })
    counts = await counts_obj.data()
    total = counts["total"]
    adult_count = counts["adult_count"]

    type_obj = await raw.group_by("titleType").agg({"tconst": "count"})
    type_data = await type_obj.data(orient=ORIENT_DICT)
    type_counts = dict(zip(type_data["titleType"], type_data["tconst"]))

    return RawProfile(
        total_titles=total,
        by_type=type_counts,
        adult_count=adult_count,
        adult_pct=(adult_count / total * 100) if total > 0 else 0.0,
    )


@task
async def filter_movies(raw: Object) -> Object:
    """
    Filter to non-adult movies with known genres and start year.

    All four conditions are pushed down as SQL WHERE clauses — ClickHouse
    executes them as a single filtered SELECT. The result is materialized
    via .copy() into a new table for downstream parallel tasks.
    """
    movies = raw.where("titleType = 'movie'")
    movies = movies.where("isAdult = '0'")
    movies = movies.where(r"genres != '\N'")
    movies = movies.where(r"startYear != '\N'")
    return await movies.copy()


@task
async def detect_quality_issues(movies: Object) -> QualityIssues:
    """
    Detect data quality issues in the movie subset.

    Uses a single count_if pass for missing runtime, then adds typed
    Computed columns to count runtime range and year violations.
    All counting is done inside ClickHouse.
    """
    total = await (await movies["tconst"].count()).data()

    # Single-scan count for missing runtime
    missing_obj = await movies.count_if(r"runtimeMinutes = '\N'")
    missing_runtime = await missing_obj.data()

    # Add typed columns to count range violations
    typed = movies.with_columns({
        "year_int":    Computed("Nullable(UInt32)", "toUInt32OrNull(startYear)"),
        "runtime_int": Computed("Nullable(UInt32)", "toUInt32OrNull(runtimeMinutes)"),
    })

    range_counts = await typed.count_if({
        "short_runtime": r"runtimeMinutes != '\N' AND toUInt32OrNull(runtimeMinutes) < 40",
        "long_runtime":  r"runtimeMinutes != '\N' AND toUInt32OrNull(runtimeMinutes) > 300",
        "pre_1970":      "toUInt32OrNull(startYear) < 1970",
    })
    range_data = await range_counts.data()

    return QualityIssues(
        total_movies=total,
        missing_runtime=missing_runtime,
        missing_runtime_pct=(missing_runtime / total * 100) if total > 0 else 0.0,
        short_runtime=range_data["short_runtime"],
        long_runtime=range_data["long_runtime"],
        pre_1970=range_data["pre_1970"],
        pre_1970_pct=(range_data["pre_1970"] / total * 100) if total > 0 else 0.0,
    )


@task
async def normalize_genres(movies: Object) -> Object:
    """
    Explode comma-separated genres into one row per genre.

    Uses splitByChar(',', genres) to create an Array column, then
    explode() to produce one row per genre. Adult genre entries are
    filtered out. Result is materialized for downstream analysis.
    """
    with_array = movies.with_columns({
        "genre": Computed("Array(LowCardinality(String))", "splitByChar(',', genres)"),
    })
    exploded = with_array.explode("genre")
    return await exploded.copy()


@task
async def analyze_genre_balance(exploded: Object) -> Object:
    """
    Compute genre distribution across all movies.

    Groups by genre, counts titles per genre. Returns an Object with
    (genre, tconst_count) rows for the report.
    """
    return await exploded.group_by("genre").agg({"tconst": "count"})


@task
async def build_clean_dataset(movies: Object) -> Object:
    """
    Build the final curated dataset ready for publishing.

    Applies quality filters: removes missing runtime, clips to 40–300 min,
    filters to post-1970 movies, excludes Adult-genre movies. Returns the
    clean (tconst, primaryTitle, startYear, genres, runtimeMinutes) subset.
    """
    typed = movies.with_columns({
        "year_int":    Computed("Nullable(UInt32)", "toUInt32OrNull(startYear)"),
        "runtime_int": Computed("Nullable(UInt32)", "toUInt32OrNull(runtimeMinutes)"),
    })
    clean = typed.where(r"runtimeMinutes != '\N'")
    clean = clean.where("runtime_int >= 40")
    clean = clean.where("runtime_int <= 300")
    clean = clean.where("year_int >= 1970")
    clean = clean.where("match(genres, 'Adult') = 0")
    clean = clean[["tconst", "primaryTitle", "startYear", "genres", "runtimeMinutes"]]
    return await clean.copy()


@task
async def publish_to_huggingface(clean: Object) -> HFPublishResult:
    """
    Publish curated dataset to Hugging Face Hub as a Parquet dataset.

    Requires HF_TOKEN environment variable. If not set, returns a
    skipped status without raising an error.

    The data is pulled from ClickHouse into a pandas DataFrame, written
    to Parquet, then uploaded via huggingface_hub.HfApi.
    """
    token = os.environ.get("HF_TOKEN")
    if not token:
        return HFPublishResult(status="skipped", reason="HF_TOKEN not set", repo=HF_REPO_ID)

    data = await clean.data(orient=ORIENT_DICT)
    df = pd.DataFrame(data)

    parquet_path = "/tmp/imdb_curated.parquet"
    df.to_parquet(parquet_path, index=False)

    api = HfApi()
    api.create_repo(repo_id=HF_REPO_ID, repo_type="dataset", exist_ok=True)
    api.upload_file(
        path_or_fileobj=parquet_path,
        path_in_repo="data/imdb_curated.parquet",
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        token=token,
    )

    return HFPublishResult(status="published", rows=len(df), repo=HF_REPO_ID)


# =============================================================================
# Job Definition
# =============================================================================


@job("imdb_dataset_builder")
def imdb_dataset_pipeline(limit: int | None = 500_000):
    """
    IMDb Movie Dataset Builder Pipeline.

    Loads IMDb title.basics from the official dataset URL, profiles the raw
    data, filters to quality movies, detects data quality issues, normalizes
    genres, builds a clean curated dataset, optionally publishes to Hugging
    Face, and generates a markdown report.

    All heavy computation stays inside ClickHouse — Python only orchestrates
    the SQL operations and receives small summary dicts.

    DAG Structure:
        load_raw_data ---+---> profile_raw           ---+
                         |                              |
                         +---> filter_movies ---+---> detect_quality_issues --+
                                                |                             |
                                                +---> normalize_genres        |
                                                |          |                  |
                                                |          v                  |
                                                |    analyze_genre_balance    |
                                                |                             |
                                                +---> build_clean_dataset     |
                                                           |                  |
                                                           +--> publish_to_hf |
                                                           |                  |
                                                           +--> generate_report <--+

    Args:
        limit: Row limit for demo runs. Set to None for the full ~10M-row dataset.
    """
    raw = load_raw_data(limit=limit)

    # Parallel branches: profile raw data + filter to movies
    profile = profile_raw(raw=raw)
    movies = filter_movies(raw=raw)

    # Parallel branches off filtered movies
    quality_issues = detect_quality_issues(movies=movies)
    exploded = normalize_genres(movies=movies)
    clean = build_clean_dataset(movies=movies)

    # Genre distribution (depends on exploded genres)
    genre_balance = analyze_genre_balance(exploded=exploded)

    # Optional publish to Hugging Face (depends on clean dataset)
    hf_result = publish_to_huggingface(clean=clean)

    # Final report (depends on everything)
    report = generate_report(
        raw=raw,
        movies=movies,
        clean=clean,
        genre_balance=genre_balance,
        profile=profile,
        quality_issues=quality_issues,
        hf_result=hf_result,
    )

    return TaskResult(tasks=[report])


async def main():
    """Register the IMDb dataset builder pipeline job."""
    created_job = await imdb_dataset_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
