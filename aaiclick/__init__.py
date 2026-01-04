"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import context manager (primary API) and client helper
from .context import Context, get_ch_client

# Import core objects
from .object import (
    Object,
    DataResult,
    Schema,
    ColumnMeta,
    ColumnType,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
)

# Import Snowflake ID generation
from .snowflake import get_snowflake_id, get_snowflake_ids

# Import ingest functions
from .ingest import concat

# Note: Factory functions (create_object, create_object_from_value) are internal.
# Use Context.create_object() and Context.create_object_from_value() instead.

