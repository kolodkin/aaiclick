"""
aaiclick.data - Data context and object management for ClickHouse operations.

This module provides the core data management capabilities for aaiclick,
including context management, object creation, and database operations.
"""

from .data_context import (
    DataContext,
    create_object,
    create_object_from_value,
    get_ch_client,
    get_data_context,
    get_pool,
)
from .lifecycle import LifecycleHandler, LocalLifecycleHandler
from .models import (
    ENGINE_DEFAULT,
    ENGINE_MEMORY,
    ENGINE_MERGE_TREE,
    ENGINES,
    EngineType,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnMeta,
    ColumnType,
    ObjectMetadata,
    ViewMetadata,
    QueryInfo,
    Schema,
    ValueListType,
    ValueScalarType,
    ValueType,
)
from .object import DataResult, Object, View
