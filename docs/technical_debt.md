# Technical Debt

## Warning Suppressions

- **clickhouse-connect FutureWarning** (`aaiclick/data/data_context.py`)
  - **Issue**: clickhouse-connect 0.6.x–0.8.x emits FutureWarnings from numpy datetime internals during queries.
  - **Debt**: Blocked on clickhouse-connect fix, expected in 1.0.0; remove global `warnings.filterwarnings` once upgraded.
