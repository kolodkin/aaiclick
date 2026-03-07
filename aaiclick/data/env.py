"""
aaiclick.data.env - Environment variable configuration.

This module centralizes all environment variable reading with sensible defaults.
"""

import os

from dotenv import load_dotenv

from .models import ClickHouseCreds

load_dotenv()

# ClickHouse connection parameters
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")


def get_ch_creds() -> ClickHouseCreds:
    """
    Create ClickHouseCreds from environment variables.

    Returns:
        ClickHouseCreds with values from environment or defaults
    """
    return ClickHouseCreds(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
