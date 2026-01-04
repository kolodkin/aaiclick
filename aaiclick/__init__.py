"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import client management
from .ch_client import get_ch_client, is_connected

# Import core objects
from .object import (
    Object,
    DataResult,
    ColumnMeta,
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

# Import context manager (primary API)
from .context import Context

# Note: Factory functions (create_object, create_object_from_value) are internal.
# Use Context.create_object() and Context.create_object_from_value() instead.

