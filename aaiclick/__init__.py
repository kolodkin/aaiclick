"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import context manager (primary API) and factory functions
from .data import (
    data_context,
    get_ch_client,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    create_object,
    create_object_from_url,
    create_object_from_value,
    list_persistent_objects,
    open_object,
    LifecycleHandler,
    LocalLifecycleHandler,
)

# Import core objects
from .data import Object, View, DataResult
from .data import (
    Schema,
    ColumnInfo,
    ColumnMeta,
    ColumnType,
    ViewSchema,
    QueryInfo,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ValueScalarType,
    ValueListType,
    ValueType,
)

# Import Snowflake ID generation
from .snowflake_id import get_snowflake_id, get_snowflake_ids

# Note: Ingest functions (copy_db, concat_objects_db, insert_objects_db) are internal.
# Use Object.copy(), Object.concat(), Object.insert() methods instead.

