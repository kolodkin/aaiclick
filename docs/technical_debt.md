# Technical Debt

## Warning Suppressions

- **clickhouse-connect FutureWarning** (`aaiclick/data/data_context.py`)
  - **Issue**: clickhouse-connect 0.6.x–0.8.x emits FutureWarnings from numpy datetime internals during queries.
  - **Debt**: Blocked on clickhouse-connect fix, expected in 1.0.0; remove global `warnings.filterwarnings` once upgraded.

## GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.
