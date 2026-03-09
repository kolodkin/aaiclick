"""
aaiclick.data - Data context and object management for ClickHouse operations.

This module provides the core data management capabilities for aaiclick,
including context management, object creation, and database operations.
"""

from .data_context import (
    DataCtxState,
    _get_data_state,
    create_object,
    create_object_from_value,
    data_context,
    decref,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    get_engine,
    get_pool,
    incref,
    list_persistent_objects,
    open_object,
    register_object,
)
from .url import create_object_from_url
from .lifecycle import LifecycleHandler, LocalLifecycleHandler
from .models import (
    ColumnDef,
    ENGINE_DEFAULT,
    ENGINE_MEMORY,
    ENGINE_MERGE_TREE,
    ENGINES,
    EngineType,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    GB_COUNT,
    GB_MAX,
    GB_MEAN,
    GB_MIN,
    GB_STD,
    GB_SUM,
    GB_VAR,
    GroupByOpType,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnMeta,
    ColumnType,
    GroupByInfo,
    ObjectMetadata,
    ViewMetadata,
    QueryInfo,
    Schema,
    parse_ch_type,
    ValueListType,
    ValueScalarType,
    ValueType,
)
from .object import DataResult, GroupByQuery, Object, View
