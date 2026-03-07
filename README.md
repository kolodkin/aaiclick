# aaiclick

A Python framework that translates Python code into ClickHouse operations for big data computing.

## Overview

aaiclick converts Python computational logic into a flow of ClickHouse database operations, enabling execution of Python-equivalent computations at scale. The framework analyzes Python code and generates optimized ClickHouse queries that produce results equivalent to native Python execution, while leveraging ClickHouse's columnar storage and distributed processing capabilities for big data workloads.

## Inspiration

- **ClickHouse** — data warehouse for storage and built-in operators; all computation runs as ClickHouse queries whenever possible
- **Apache Spark** — inspiration for custom operators when the data warehouse's built-in operators are not sufficient
- **Airflow** — enterprise-grade orchestration
- **Pandas** — API design inspiration
- **SQLAlchemy** — API design inspiration

## License

MIT License - see [LICENSE](LICENSE) for details.
