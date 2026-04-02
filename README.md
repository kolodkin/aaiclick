# aaiclick

A Python framework that translates Python code into ClickHouse operations for big data computing.

## Overview

aaiclick provides a Python API for big data computing backed by ClickHouse. Data lives in ClickHouse as columnar tables; Python code orchestrates operations — arithmetic, filtering, aggregation, joins — that execute as ClickHouse queries. An orchestration layer (Airflow-inspired) manages task dependencies and scheduling, while the Object API (Pandas/SQLAlchemy-inspired) keeps the interface familiar. Runs locally with embedded chdb + SQLite, or scales out with remote ClickHouse + PostgreSQL.

## Installation

The base install includes embedded [chdb](https://clickhouse.com/docs/chdb) and SQLite — no external servers needed:

```bash
pip install aaiclick
python -m aaiclick setup
```

For a distributed deployment (remote ClickHouse server + PostgreSQL):

```bash
pip install "aaiclick[distributed]"
```

For AI features (lineage tracing, debug agents):

```bash
pip install "aaiclick[ai]"
# or everything:
pip install "aaiclick[all]"
```

## License

MIT License - see [LICENSE](LICENSE) for details.
