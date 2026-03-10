# Technical Debt

## Warning Suppressions

- **clickhouse-connect FutureWarning** (`aaiclick/data/data_context.py`): Global `warnings.filterwarnings` suppresses FutureWarnings from numpy datetime internals in clickhouse-connect 0.6.x–0.8.x. Remove once clickhouse-connect no longer emits these — verify by dropping the filter and running tests with `-W error`.
