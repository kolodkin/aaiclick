"""aaiclick.snowflake — Snowflake ID generation backed by ClickHouse.

See ``snowflake_id.py`` for the implementation.
"""

from .snowflake_id import (
    MAX_SEQUENCE,
    SnowflakeGenerator,
    decode_snowflake_id,
    get_snowflake_id,
    get_snowflake_ids,
)
