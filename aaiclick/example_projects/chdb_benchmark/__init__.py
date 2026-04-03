"""
chdb Benchmark — aaiclick vs native chdb SQL performance comparison.

Compares aaiclick's Object API against hand-written chdb SQL queries
on identical data (1M rows, 10 runs averaged) to identify and measure
abstraction overhead.

Operations tested:
- Ingest: Python dict → ClickHouse table
- Column sum, multiply, filter, sort
- Count distinct, group-by (single/multi-key, multi-agg, high-cardinality)

Usage:
    python -m aaiclick.example_projects.chdb_benchmark
    python -m aaiclick.example_projects.chdb_benchmark --rows 100000 --runs 5
"""
