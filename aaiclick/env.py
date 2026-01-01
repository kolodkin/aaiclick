"""
aaiclick.env - Environment variable configuration.

This module centralizes all environment variable reading with sensible defaults.
"""

import os

# ClickHouse connection parameters
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

# Snowflake ID parameters
# Machine ID for distributed ID generation (0-1023, 10 bits)
SNOWFLAKE_MACHINE_ID = int(os.getenv("SNOWFLAKE_MACHINE_ID", "0"))

# Validate machine ID is within valid range
if not 0 <= SNOWFLAKE_MACHINE_ID <= 1023:
    raise ValueError(
        f"SNOWFLAKE_MACHINE_ID must be between 0 and 1023, got {SNOWFLAKE_MACHINE_ID}"
    )
