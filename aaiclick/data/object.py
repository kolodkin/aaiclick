"""
aaiclick.data.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from __future__ import annotations

import sys
import weakref
from typing import Optional, Dict, List, Tuple, Any, Union
from dataclasses import dataclass
from typing_extensions import Self

from . import operators, ingest
from ..snowflake_id import get_snowflake_id

from .models import (
    Schema,
    CopyInfo,
    ColumnMeta,
    ColumnInfo,
    ColumnType,
    GroupByInfo,
    GroupByOpType,
    GB_COUNT,
    GB_MAX,
    GB_MEAN,
    GB_MIN,
    GB_STD,
    GB_SUM,
    GB_VAR,
    ObjectMetadata,
    ViewMetadata,
    QueryInfo,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
)
from .data_context import get_data_context, DataContext, create_object_from_value
from .sql_utils import quote_identifier


@dataclass
class DataResult:
    """
    Result container for Object.data() that includes both rows and column metadata.

    Attributes:
        rows: List of tuples containing row data
        columns: Dict mapping column name to ColumnMeta with datatype/fieldtype info
    """

    rows: List[Tuple[Any, ...]]
    columns: Dict[str, ColumnMeta]


class Object:
    """
    Represents a data object stored in a ClickHouse table.

    Each Object instance corresponds to a ClickHouse table and supports
    operations through operator overloading that create new tables with results.

    Supports 14 operators: arithmetic (+, -, *, /, //, %, **), comparison
    (==, !=, <, <=, >, >=), and bitwise (&, |, ^).

    All operators work element-wise on both scalar and array data types.

    For detailed operator documentation, examples, and Python-to-ClickHouse
    operator mapping, see docs/object.md.
    """

    def __init__(
        self,
        table: Optional[str] = None,
        schema: Optional[Schema] = None,
    ):
        """
        Initialize an Object.

        Args:
            table: Optional table name. If not provided, generates unique table name
                  using Snowflake ID prefixed with 't' for ClickHouse compatibility
            schema: Optional Schema with column types (cached for internal use)
        """
        self._table_name = table if table is not None else f"t{get_snowflake_id()}"
        self._stale = False
        self._schema = schema
        self._data_ctx_ref: Optional[weakref.ref[DataContext]] = None
        self._where_clauses: List[Tuple[str, str]] = []

    def _register(self, context: DataContext) -> None:
        """Register this object with context for lifecycle tracking."""
        self._data_ctx_ref = weakref.ref(context)
        context.incref(self._table_name)

    def __del__(self):
        """Decrement refcount on deletion."""
        # Guard 1: Interpreter shutdown
        if sys.is_finalizing():
            return

        # Guard 2: Never registered
        if self._data_ctx_ref is None:
            return

        # Guard 3: Context gone
        context = self._data_ctx_ref()
        if context is None:
            return

        # Decref (handles worker=None internally)
        context.decref(self._table_name)

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    @property
    def schema(self) -> Optional[Schema]:
        """Get the cached schema for this object (read-only)."""
        return self._schema

    @property
    def ctx(self) -> Context:
        """Get the context managing this object."""
        self.checkstale()
        return get_data_context()

    @property
    def limit(self) -> Optional[int]:
        """Get LIMIT (None for base Object)."""
        return None

    @property
    def offset(self) -> Optional[int]:
        """Get OFFSET (None for base Object)."""
        return None

    @property
    def order_by(self) -> Optional[str]:
        """Get ORDER BY clause (None for base Object)."""
        return None

    @property
    def selected_fields(self) -> Optional[List[str]]:
        """Get selected field names (None for base Object)."""
        return None

    def _serialize_ref(self) -> dict:
        """Serialize this Object to a reference dict for task kwargs/results."""
        return {"object_type": "object", "table": self.table}

    @property
    def is_single_field(self) -> bool:
        """Check if this is a single-field selection (False for base Object)."""
        return False

    @property
    def ch_client(self):
        """Get the ClickHouse client from the context."""
        self.checkstale()
        return self.ctx.ch_client

    @property
    def stale(self) -> bool:
        """Check if this object has been deleted."""
        return self._stale

    @property
    def has_constraints(self) -> bool:
        """Check if this object has any view constraints."""
        return bool(
            self._where_clauses
            or self.limit is not None
            or self.offset is not None
            or self.order_by
            or self.selected_fields
        )

    def checkstale(self):
        """
        Check if object is stale and raise error if so.

        Raises:
            RuntimeError: If object has been deleted (stale)
        """
        if self._stale:
            raise RuntimeError(
                f"Cannot use stale Object. Table '{self._table_name}' has been deleted."
            )

    def _build_where(self) -> Optional[str]:
        """Build the combined WHERE clause from stored conditions."""
        if not self._where_clauses:
            return None
        parts = []
        for i, (condition, connector) in enumerate(self._where_clauses):
            if i == 0:
                parts.append(f"({condition})")
            else:
                parts.append(f"{connector} ({condition})")
        return " ".join(parts)

    def _build_select(self, columns: str = "*", default_order_by: Optional[str] = None) -> str:
        """
        Build a SELECT query with view constraints applied.

        Args:
            columns: Column specification (default "*")
            default_order_by: Default ORDER BY clause if view doesn't have custom order_by

        Returns:
            str: SELECT query string with WHERE/LIMIT/OFFSET/ORDER BY applied
        """
        query = f"SELECT {columns} FROM {self.table}"
        where = self._build_where()
        if where:
            query += f" WHERE {where}"
        # Use custom order_by if set, otherwise use default
        order_clause = self.order_by or default_order_by
        if order_clause:
            query += f" ORDER BY {order_clause}"
        if self.limit is not None:
            query += f" LIMIT {self.limit}"
        if self.offset is not None:
            query += f" OFFSET {self.offset}"
        return query

    def _get_query_info(self) -> QueryInfo:
        """
        Get query information for database operations.

        Encapsulates the data source, base table name, and schema metadata
        to avoid needing to query system tables in operators.

        Returns:
            QueryInfo: Dataclass with source, base_table, value_column, fieldtype, and value_type
        """
        source = f"({self._build_select()})" if self.has_constraints else self.table
        # Get fieldtype and value_type from cached schema
        fieldtype = self._schema.fieldtype if self._schema else FIELDTYPE_ARRAY
        value_type = "Float64"
        if self._schema and "value" in self._schema.columns:
            value_type = str(self._schema.columns["value"])
        return QueryInfo(
            source=source,
            base_table=self.table,
            value_column="value",
            fieldtype=fieldtype,
            value_type=value_type,
        )

    def _get_copy_info(self) -> CopyInfo:
        """
        Get copy info for database-level copy operations.

        Returns:
            CopyInfo with source query and schema metadata from cached _schema
        """
        source_query = f"({self._build_select()})" if self.has_constraints else self.table
        return CopyInfo(
            source_query=source_query,
            fieldtype=self._schema.fieldtype,
            columns=self._schema.columns,
        )

    async def result(self):
        """
        Query and return all data from the object's table.

        Returns:
            Query result with all rows from the table
        """
        self.checkstale()
        return await self.ch_client.query(f"SELECT * FROM {self.table}")

    async def data(self, orient: str = ORIENT_DICT):
        """
        Get the data from the object's table.

        Args:
            orient: Output format for dict data. Options:
                - ORIENT_DICT ('dict'): returns dict with column names as keys (default)
                - ORIENT_RECORDS ('records'): returns list of dicts (one per row)

        Returns:
            - For scalar: returns the value directly
            - For array: returns list of values
            - For dict: returns dict or list of dicts based on orient
        """
        self.checkstale()
        from . import data_extraction

        # Query column names and comments
        columns_query = f"""
        SELECT name, comment
        FROM system.columns
        WHERE table = '{self.table}'
        ORDER BY position
        """
        columns_result = await self.ch_client.query(columns_query)

        # Parse YAML from comments and get column names
        columns: Dict[str, ColumnMeta] = {}
        column_names: List[str] = []
        for name, comment in columns_result.result_rows:
            columns[name] = ColumnMeta.from_yaml(comment)
            column_names.append(name)

        # Determine data type based on columns
        is_simple_structure = set(column_names) <= {"aai_id", "value"}

        if not is_simple_structure:
            # Dict type (scalar or arrays)
            return await data_extraction.extract_dict_data(self, column_names, columns, orient)

        # Simple structure: aai_id and value columns
        value_meta = columns.get("value")
        if value_meta and value_meta.fieldtype == FIELDTYPE_SCALAR:
            # Scalar: return single value
            return await data_extraction.extract_scalar_data(self)
        else:
            # Array: return list of values
            return await data_extraction.extract_array_data(self)

    async def _get_fieldtype(self) -> Optional[str]:
        """Get the fieldtype of the value column."""
        self.checkstale()
        columns_query = f"""
        SELECT comment FROM system.columns
        WHERE table = '{self.table}' AND name = 'value'
        """
        result = await self.ch_client.query(columns_query)
        if result.result_rows:
            meta = ColumnMeta.from_yaml(result.result_rows[0][0])
            return meta.fieldtype
        return None

    async def metadata(self) -> ObjectMetadata:
        """
        Get metadata for this object including table name, fieldtype, and column info.

        Builds metadata from cached schema if available, otherwise
        queries the ClickHouse system.columns table.

        Returns:
            ObjectMetadata: Dataclass with table, fieldtype, and columns info

        Examples:
            >>> obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})
            >>> meta = await obj.metadata()
            >>> print(meta)
            ObjectMetadata(table='t...', fieldtype='d', columns={
                'aai_id': ColumnInfo(name='aai_id', type='UInt64', fieldtype='s'),
                'param1': ColumnInfo(name='param1', type='Int64', fieldtype='a'),
                'param2': ColumnInfo(name='param2', type='Int64', fieldtype='a')
            })
            >>> view = obj['param1']
            >>> view_meta = await view.metadata()  # Returns ViewMetadata
        """
        self.checkstale()

        # Build from cached schema if available
        if self._schema is not None:
            column_infos: Dict[str, ColumnInfo] = {}
            for name, col_type in self._schema.columns.items():
                col_fieldtype = FIELDTYPE_SCALAR if name == "aai_id" else self._schema.fieldtype
                column_infos[name] = ColumnInfo(
                    name=name,
                    type=str(col_type),
                    fieldtype=col_fieldtype,
                )

            column_names = set(self._schema.columns.keys())
            is_dict_type = not (column_names <= {"aai_id", "value"})
            overall_fieldtype = FIELDTYPE_DICT if is_dict_type else self._schema.fieldtype

            return ObjectMetadata(
                table=self.table,
                fieldtype=overall_fieldtype,
                columns=column_infos,
            )

        # Fallback: query database for metadata (for objects not created via create_object)
        columns_query = f"""
        SELECT name, type, comment
        FROM system.columns
        WHERE table = '{self.table}'
        ORDER BY position
        """
        columns_result = await self.ch_client.query(columns_query)

        # Parse columns and determine overall fieldtype
        columns: Dict[str, ColumnInfo] = {}
        overall_fieldtype = FIELDTYPE_SCALAR
        column_names = []

        for name, col_type, comment in columns_result.result_rows:
            meta = ColumnMeta.from_yaml(comment)
            columns[name] = ColumnInfo(
                name=name,
                type=col_type,
                fieldtype=meta.fieldtype
            )
            column_names.append(name)

            # Determine overall fieldtype from value column or detect dict type
            if name == "value" and meta.fieldtype:
                overall_fieldtype = meta.fieldtype

        # If we have columns beyond aai_id and value, it's a dict type
        is_dict_type = not (set(column_names) <= {"aai_id", "value"})
        if is_dict_type:
            overall_fieldtype = FIELDTYPE_DICT

        return ObjectMetadata(
            table=self.table,
            fieldtype=overall_fieldtype,
            columns=columns
        )

    async def _apply_operator(self, obj_b: Object, operator: str) -> Object:
        """
        Apply an operator on two objects using SQL templates.

        Args:
            obj_b: Another Object to operate with
            operator: Operator symbol (e.g., '+', '-', '**', '==', '&')

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        obj_b.checkstale()
        info_a = self._get_query_info()
        info_b = obj_b._get_query_info()
        return await operators._apply_operator_db(
            info_a, info_b, operator, self.ch_client, self.ctx
        )

    async def __add__(self, other: Object) -> Object:
        """
        Add two objects together.

        Creates a new Object with a table containing the result of element-wise addition.

        Args:
            other: Another Object to add

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.add(info_a, info_b, self.ch_client)

    async def __sub__(self, other: Object) -> Object:
        """
        Subtract one object from another.

        Creates a new Object with a table containing the result of element-wise subtraction.

        Args:
            other: Another Object to subtract

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.sub(info_a, info_b, self.ch_client)

    async def __mul__(self, other: Object) -> Object:
        """
        Multiply two objects together.

        Creates a new Object with a table containing the result of element-wise multiplication.

        Args:
            other: Another Object to multiply

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.mul(info_a, info_b, self.ch_client)

    async def __truediv__(self, other: Object) -> Object:
        """
        Divide one object by another.

        Creates a new Object with a table containing the result of element-wise division.

        Args:
            other: Another Object to divide by

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.truediv(info_a, info_b, self.ch_client)

    async def __floordiv__(self, other: Object) -> Object:
        """
        Floor divide one object by another.

        Creates a new Object with a table containing the result of element-wise floor division.

        Args:
            other: Another Object to floor divide by

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.floordiv(info_a, info_b, self.ch_client)

    async def __mod__(self, other: Object) -> Object:
        """
        Modulo operation between two objects.

        Creates a new Object with a table containing the result of element-wise modulo.

        Args:
            other: Another Object to modulo with

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.mod(info_a, info_b, self.ch_client)

    async def __pow__(self, other: Object) -> Object:
        """
        Raise one object to the power of another.

        Creates a new Object with a table containing the result of element-wise power operation.

        Args:
            other: Another Object representing the exponent

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.pow(info_a, info_b, self.ch_client)

    async def __eq__(self, other: Object) -> Object:
        """
        Check equality between two objects.

        Creates a new Object with a table containing the result of element-wise equality comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.eq(info_a, info_b, self.ch_client)

    async def __ne__(self, other: Object) -> Object:
        """
        Check inequality between two objects.

        Creates a new Object with a table containing the result of element-wise inequality comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.ne(info_a, info_b, self.ch_client)

    async def __lt__(self, other: Object) -> Object:
        """
        Check if one object is less than another.

        Creates a new Object with a table containing the result of element-wise less than comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.lt(info_a, info_b, self.ch_client)

    async def __le__(self, other: Object) -> Object:
        """
        Check if one object is less than or equal to another.

        Creates a new Object with a table containing the result of element-wise less than or equal comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.le(info_a, info_b, self.ch_client)

    async def __gt__(self, other: Object) -> Object:
        """
        Check if one object is greater than another.

        Creates a new Object with a table containing the result of element-wise greater than comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.gt(info_a, info_b, self.ch_client)

    async def __ge__(self, other: Object) -> Object:
        """
        Check if one object is greater than or equal to another.

        Creates a new Object with a table containing the result of element-wise greater than or equal comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.ge(info_a, info_b, self.ch_client)

    async def __and__(self, other: Object) -> Object:
        """
        Bitwise AND operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise AND.

        Args:
            other: Another Object to AND with

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.and_(info_a, info_b, self.ch_client)

    async def __or__(self, other: Object) -> Object:
        """
        Bitwise OR operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise OR.

        Args:
            other: Another Object to OR with

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.or_(info_a, info_b, self.ch_client)

    async def __xor__(self, other: Object) -> Object:
        """
        Bitwise XOR operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise XOR.

        Args:
            other: Another Object to XOR with

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.xor(info_a, info_b, self.ch_client)

    async def copy(self) -> "Object":
        """
        Copy this object to a new object and table.

        Creates a new Object with a copy of all data from this object.
        Preserves all column metadata including fieldtype.
        Also works for Views, handling field selection and constraints.

        Returns:
            Object: New Object instance with copied data

        Examples:
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_copy = await obj_a.copy()
            >>> await obj_copy.data()  # Returns [1, 2, 3]
            >>>
            >>> # Also works for views with field selection
            >>> obj = await create_object_from_value({'x': [1, 2], 'y': [3, 4]})
            >>> arr = await obj['x'].copy()  # Creates new array Object
            >>> await arr.data()  # Returns [1, 2]
        """
        self.checkstale()
        copy_info = self._get_copy_info()
        if copy_info.selected_fields:
            return await ingest.copy_db_selected_fields(copy_info, self.ch_client)
        return await ingest.copy_db(copy_info, self.ch_client)

    async def concat(self, *args: Union["Object", "ValueType"]) -> "Object":
        """
        Concatenate multiple objects or values to this object.

        Creates a new Object with rows from self followed by all args.
        Self must have array fieldtype. Each arg can be:
        - An Object (array or scalar)
        - A ValueType (scalar or list)

        Args:
            *args: Variable number of Objects or ValueTypes to concatenate

        Returns:
            Object: New Object instance with concatenated data

        Raises:
            ValueError: If no arguments provided
            ValueError: If self does not have array fieldtype

        Examples:
            >>> # Concatenate with another Object
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_b = await ctx.create_object_from_value([4, 5, 6])
            >>> result = await obj_a.concat(obj_b)
            >>> await result.data()  # Returns [1, 2, 3, 4, 5, 6]
            >>>
            >>> # Concatenate with multiple objects
            >>> result = await obj_a.concat(obj_b, obj_c, obj_d)
            >>> await result.data()  # Returns concatenated data
            >>>
            >>> # Concatenate with mixed types
            >>> result = await obj_a.concat(42, [7, 8], obj_b)
            >>> await result.data()  # Returns [1, 2, 3, 42, 7, 8, ...]
        """
        if not args:
            raise ValueError("concat requires at least one argument")

        self.checkstale()
        from . import ingest

        # Convert all arguments to QueryInfo
        query_infos = [self._get_query_info()]
        temp_objects = []

        for arg in args:
            if isinstance(arg, Object):
                arg.checkstale()
                query_infos.append(arg._get_query_info())
            else:
                # Skip empty lists to avoid type conflicts
                if isinstance(arg, list) and len(arg) == 0:
                    continue
                # Convert ValueType to temporary Object
                temp = await create_object_from_value(arg)
                temp_objects.append(temp)
                query_infos.append(temp._get_query_info())

        # If all args were empty lists, just copy self
        if len(query_infos) == 1:
            result = await self.copy()
        else:
            # Single database operation for all sources
            result = await ingest.concat_objects_db(
                query_infos, self.ch_client
            )

        # Cleanup temporary objects
        for temp in temp_objects:
            await self.ch_client.command(f"DROP TABLE IF EXISTS {temp.table}")

        return result

    async def insert(self, *args: Union[Self, "ValueType"]) -> None:
        """
        Insert multiple objects or values into this object in place.

        Modifies self's table directly by appending data from all args.
        Self must have array fieldtype. Each arg can be:
        - An Object (array or scalar)
        - A ValueType (scalar or list)

        Unlike concat, this method modifies the table in place without creating a new object.

        Args:
            *args: Variable number of Objects or ValueTypes to insert

        Raises:
            ValueError: If no arguments provided
            ValueError: If self does not have array fieldtype
            ValueError: If any arg type is incompatible

        Examples:
            >>> # Insert another Object
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_b = await ctx.create_object_from_value([4, 5, 6])
            >>> await obj_a.insert(obj_b)
            >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5, 6]
            >>>
            >>> # Insert multiple objects and values
            >>> await obj_a.insert(obj_b, 42, [7, 8])
            >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5, 6, 42, 7, 8]
        """
        if not args:
            raise ValueError("insert requires at least one argument")

        self.checkstale()
        from . import ingest

        # Convert all arguments to table names
        source_tables = []
        temp_objects = []

        for arg in args:
            if isinstance(arg, Object):
                arg.checkstale()
                source_tables.append(arg.table)
            else:
                # Skip empty lists
                if isinstance(arg, list) and len(arg) == 0:
                    continue
                # Convert ValueType to temporary Object
                temp = await create_object_from_value(arg)
                temp_objects.append(temp)
                source_tables.append(temp.table)

        # Single database operation for all sources
        if source_tables:
            await ingest.insert_objects_db(
                self.table, source_tables, self.ch_client
            )

        # Cleanup temporary objects
        for temp in temp_objects:
            await self.ch_client.command(f"DROP TABLE IF EXISTS {temp.table}")

    async def insert_from_url(
        self,
        url: str,
        columns: list[str] | None = None,
        format: str = "Parquet",
        where: str | None = None,
        limit: int | None = None,
    ) -> None:
        """
        Insert data from an external URL into this object in place.

        Uses ClickHouse's native url() table function - zero Python memory footprint.
        Data flows directly from URL into this object's table.

        Args:
            url: HTTP(S) URL to load data from (e.g., Parquet file on S3)
            columns: Column names to select. If None, uses this object's columns
                (excluding aai_id). Column names must match the URL source.
            format: ClickHouse format name. Default "Parquet".
                Supported: Parquet, CSV, CSVWithNames, TSV, JSON, JSONEachRow, etc.
            where: Optional SQL WHERE clause for filtering rows at load time
            limit: Optional row limit applied at load time

        Raises:
            ValueError: If URL, columns, format, or limit are invalid
            RuntimeError: If object has incompatible schema

        Examples:
            >>> # Create schema from first month, then insert more months
            >>> trips = await create_object_from_url(
            ...     "https://example.com/jan.parquet",
            ...     columns=["fare", "tip", "distance"],
            ... )
            >>> await trips.insert_from_url("https://example.com/feb.parquet")
            >>> await trips.insert_from_url("https://example.com/mar.parquet")
        """
        from .url import (
            _validate_url,
            _validate_url_format,
            _validate_url_columns,
            SUPPORTED_URL_FORMATS,
        )

        self.checkstale()

        _validate_url(url)
        _validate_url_format(format)

        # If columns not specified, use this object's columns (excluding aai_id)
        if columns is None:
            columns = [c for c in self.schema.columns.keys() if c != "aai_id"]

        _validate_url_columns(columns)

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError(f"limit must be a positive integer, got {limit}")
        if where is not None and ";" in where:
            raise ValueError("WHERE clause must not contain ';'")

        # Escape single quotes in URL for safe SQL embedding
        safe_url = url.replace("'", "\\'")

        # Build column selection
        quoted_columns = [quote_identifier(c) for c in columns]
        columns_str = ", ".join(quoted_columns)

        # Handle single-column case (mapped to "value")
        if len(columns) == 1 and "value" in self.schema.columns:
            select_cols = f"{quoted_columns[0]} AS value"
        else:
            select_cols = columns_str

        # Build INSERT query with Snowflake ID generation
        base_id = get_snowflake_id()
        where_clause = f" WHERE {where}" if where else ""
        limit_clause = f" LIMIT {limit}" if limit is not None else ""

        insert_query = (
            f"INSERT INTO {self.table} "
            f"SELECT toUInt64({base_id} + row_number() OVER ()) AS aai_id, {select_cols} "
            f"FROM url('{safe_url}', '{format}')"
            f"{where_clause}"
            f"{limit_clause}"
        )
        await self.ch_client.command(insert_query)

    async def min(self) -> Self:
        """
        Calculate the minimum value from the object's table.

        Creates a new Object with a scalar result containing the minimum value.
        All computation happens within ClickHouse - no data round-trips to Python.

        Returns:
            Self: New scalar Object containing the minimum value

        Examples:
            >>> obj = await create_object_from_value([5, 2, 8, 1, 9])
            >>> result = await obj.min()
            >>> await result.data()  # Returns 1
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.min_agg(info, self.ch_client)

    async def max(self) -> Self:
        """
        Calculate the maximum value from the object's table.

        Creates a new Object with a scalar result containing the maximum value.
        All computation happens within ClickHouse - no data round-trips to Python.

        Returns:
            Self: New scalar Object containing the maximum value

        Examples:
            >>> obj = await create_object_from_value([5, 2, 8, 1, 9])
            >>> result = await obj.max()
            >>> await result.data()  # Returns 9
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.max_agg(info, self.ch_client)

    async def sum(self) -> Self:
        """
        Calculate the sum of values from the object's table.

        Creates a new Object with a scalar result containing the sum.
        All computation happens within ClickHouse - no data round-trips to Python.

        Returns:
            Self: New scalar Object containing the sum value

        Examples:
            >>> obj = await create_object_from_value([1, 2, 3, 4, 5])
            >>> result = await obj.sum()
            >>> await result.data()  # Returns 15
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.sum_agg(info, self.ch_client)

    async def mean(self) -> Self:
        """
        Calculate the mean (average) value from the object's table.

        Creates a new Object with a scalar result containing the mean.
        All computation happens within ClickHouse - no data round-trips to Python.

        Returns:
            Self: New scalar Object containing the mean value

        Examples:
            >>> obj = await create_object_from_value([10, 20, 30, 40])
            >>> result = await obj.mean()
            >>> await result.data()  # Returns 25.0
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.mean_agg(info, self.ch_client)

    async def std(self) -> Self:
        """
        Calculate the standard deviation of values from the object's table.

        Creates a new Object with a scalar result containing the standard deviation (population).
        All computation happens within ClickHouse - no data round-trips to Python.

        Returns:
            Self: New scalar Object containing the standard deviation value

        Examples:
            >>> obj = await create_object_from_value([2, 4, 6, 8])
            >>> result = await obj.std()
            >>> await result.data()  # Returns 2.2360679774997898
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.std_agg(info, self.ch_client)

    async def var(self) -> Self:
        """
        Calculate the variance of values from the object's table.

        Creates a new Object with a scalar result containing the variance (population).
        All computation happens within ClickHouse - no data round-trips to Python.

        Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/varpop

        Returns:
            Self: New scalar Object containing the variance value

        Examples:
            >>> obj = await create_object_from_value([2, 4, 6, 8])
            >>> result = await obj.var()
            >>> await result.data()  # Returns 5.0 (std^2 = 2.236^2)
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.var_agg(info, self.ch_client)

    async def count(self) -> Self:
        """
        Count the number of values in the object's table.

        Creates a new Object with a scalar result containing the count.
        All computation happens within ClickHouse - no data round-trips to Python.

        Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/count

        Returns:
            Self: New scalar Object containing the count value (UInt64)

        Examples:
            >>> obj = await create_object_from_value([1, 2, 3, 4, 5])
            >>> result = await obj.count()
            >>> await result.data()  # Returns 5
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.count_agg(info, self.ch_client)

    async def quantile(self, q: float) -> Self:
        """
        Calculate the quantile of values from the object's table.

        Creates a new Object with a scalar result containing the quantile value.
        All computation happens within ClickHouse - no data round-trips to Python.

        Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantile

        Args:
            q: Quantile level between 0 and 1 (e.g., 0.5 for median, 0.25 for Q1)

        Returns:
            Self: New scalar Object containing the quantile value

        Raises:
            ValueError: If q is not between 0 and 1

        Examples:
            >>> obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            >>> median = await obj.quantile(0.5)
            >>> await median.data()  # Returns 5.5 (median)
            >>> q1 = await obj.quantile(0.25)
            >>> await q1.data()  # Returns first quartile
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.quantile_agg(info, q, self.ch_client)

    async def unique(self) -> Self:
        """
        Get unique values from the object's table.

        Creates a new Object with an array containing only unique values.
        Uses GROUP BY instead of DISTINCT for better performance on large datasets.
        All computation happens within ClickHouse - no data round-trips to Python.

        Note: The order of unique values is not guaranteed. Use GROUP BY internally
        as it's more efficient than DISTINCT in ClickHouse for large datasets.
        Reference: https://clickhouse.com/docs/sql-reference/statements/select/group-by

        Returns:
            Self: New array Object containing unique values

        Examples:
            >>> obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])
            >>> result = await obj.unique()
            >>> sorted(await result.data())  # Returns [1, 2, 3, 4]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.unique_group(info, self.ch_client)

    def view(
        self,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> "View":
        """
        Create a read-only view of this object with query constraints.

        Args:
            where: Optional WHERE clause condition
            limit: Optional LIMIT for number of rows
            offset: Optional OFFSET for skipping rows
            order_by: Optional ORDER BY clause

        Returns:
            View: A new View instance with the specified constraints

        Examples:
            >>> obj = await ctx.create_object_from_value([1, 2, 3, 4, 5])
            >>> view = obj.view(where="value > 2", limit=2)
            >>> await view.data()  # Returns [3, 4]
        """
        return View(self, where=where, limit=limit, offset=offset, order_by=order_by)

    def where(self, condition: str) -> "View":
        """
        Create a View with a WHERE condition.

        Shortcut for obj.view(where=condition). The returned View supports
        chaining with .where() (AND) and .or_where() (OR).

        Args:
            condition: Raw SQL WHERE expression (e.g., 'value > 100')

        Returns:
            View with the WHERE condition applied

        Examples:
            >>> obj = await create_object_from_value([1, 2, 3, 4, 5])
            >>> view = obj.where("value > 2").where("value < 5")
            >>> await view.data()  # Returns [3, 4]
        """
        return View(self, where=condition)

    def or_where(self, condition: str) -> "View":
        """
        Create a View with an OR WHERE condition.

        Note: or_where() on Object requires a prior where() call.
        Use where() first to start a chain.

        Raises:
            ValueError: Always, since there's no prior WHERE clause on a plain Object.
                       Use where() first: obj.where('x > 5').or_where('y < 3')
        """
        raise ValueError(
            "or_where() requires a prior where condition. "
            "Use where() first: obj.where('x > 5').or_where('y < 3')"
        )

    def group_by(self, *keys: str) -> GroupByQuery:
        """
        Group data by one or more columns, returning a GroupByQuery.

        The GroupByQuery is an intermediate object that stores grouping info.
        Call aggregation methods (sum, mean, count, etc.) on it to produce
        a dict Object with the grouped results.

        Works on both dict Objects (group by named columns) and array Objects
        (group by 'value' for value_counts-style operations).

        Args:
            *keys: Column name(s) to group by

        Returns:
            GroupByQuery: Intermediate object for applying aggregations

        Raises:
            ValueError: If no keys provided, key doesn't exist, or key is 'aai_id'

        Examples:
            >>> obj = await create_object_from_value({
            ...     'category': ['A', 'A', 'B', 'B'],
            ...     'amount': [10, 20, 30, 40],
            ... })
            >>> result = await obj.group_by('category').sum('amount')
            >>> await result.data()  # {'category': ['A', 'B'], 'amount': [30, 70]}
        """
        self.checkstale()
        return GroupByQuery(self, list(keys))

    def __getitem__(self, key: Union[str, List[str]]) -> "View":
        """
        Select field(s) from a dict Object, returning a View.

        Creates a View that selects only the specified column(s) from the dict Object.
        The View can be used in operations or materialized with copy().

        Args:
            key: Column name (str) or list of column names (list) to select

        Returns:
            View: A new View instance selecting the specified field(s)

        Examples:
            >>> obj = await create_object_from_value({'param1': [123, 234], 'param2': [456, 342]})
            >>>
            >>> # Single field selector - returns array-like view
            >>> view = obj['param1']
            >>> await view.data()  # Returns [123, 234]
            >>> arr = await view.copy()  # Returns new array Object
            >>>
            >>> # Multi-field selector - returns dict-like view
            >>> view = obj[['param1', 'param2']]
            >>> await view.data()  # Returns {'param1': [123, 234], 'param2': [456, 342]}
            >>> dict_obj = await view.copy()  # Returns new dict Object
        """
        self.checkstale()
        # Always store as list - single field is just a list of one
        fields = key if isinstance(key, list) else [key]
        return View(self, selected_fields=fields)

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"


class GroupByQuery:
    """
    Intermediate object representing a GROUP BY operation on an Object.

    GroupByQuery stores the source Object and grouping keys. It does NOT
    inherit from Object because it has no ClickHouse table, no data(),
    and no lifecycle management.

    Call aggregation methods (sum, mean, count, etc.) to execute the
    GROUP BY query and produce a dict Object with the results.

    Examples:
        >>> obj = await create_object_from_value({
        ...     'category': ['A', 'A', 'B', 'B'],
        ...     'amount': [10, 20, 30, 40],
        ... })
        >>> result = await obj.group_by('category').sum('amount')
        >>> await result.data()  # {'category': ['A', 'B'], 'amount': [30, 70]}
    """

    def __init__(self, source: Object, keys: List[str]):
        """
        Initialize a GroupByQuery.

        Args:
            source: Source Object to group
            keys: List of column names to group by

        Raises:
            ValueError: If no keys, key is 'aai_id', key doesn't exist in schema
        """
        if not keys:
            raise ValueError("group_by requires at least one key")

        if "aai_id" in keys:
            raise ValueError("Cannot group by 'aai_id'")

        schema = source._schema
        if schema is None:
            raise ValueError("Source object has no cached schema")

        # Determine available columns based on source type
        if isinstance(source, View) and source.is_single_field:
            # Single-field View projects to {aai_id, value}
            available = {"value"}
        elif isinstance(source, View) and source._selected_fields:
            # Multi-field View projects to selected fields only
            available = set(source._selected_fields)
        else:
            available = set(schema.columns.keys()) - {"aai_id"}

        for key in keys:
            if key not in available:
                raise ValueError(
                    f"Key '{key}' not found in source columns. "
                    f"Available: {sorted(available)}"
                )

        self._source = source
        self._keys = keys
        self._having_clauses: List[Tuple[str, str]] = []

    @property
    def ch_client(self):
        """Get the ClickHouse client from the source object."""
        return self._source.ch_client

    def _clone_with_having(self, condition: str, connector: str) -> GroupByQuery:
        """Create a new GroupByQuery with all current state plus an additional HAVING clause."""
        new_gbq = GroupByQuery.__new__(GroupByQuery)
        new_gbq._source = self._source
        new_gbq._keys = self._keys
        new_gbq._having_clauses = list(self._having_clauses)
        new_gbq._having_clauses.append((condition, connector))
        return new_gbq

    def having(self, condition: str) -> GroupByQuery:
        """
        Return a new GroupByQuery with an AND-chained HAVING condition.

        Multiple calls chain with AND:
            .having('sum(x) > 10').having('count() >= 2')
            → HAVING (sum(x) > 10) AND (count() >= 2)

        The original GroupByQuery is not modified.

        Args:
            condition: Raw SQL HAVING expression (e.g., 'sum(amount) > 100')

        Returns:
            New GroupByQuery with the condition added

        Raises:
            ValueError: If condition is empty
        """
        if not condition or not condition.strip():
            raise ValueError("HAVING condition must be a non-empty string")
        return self._clone_with_having(condition.strip(), "AND")

    def or_having(self, condition: str) -> GroupByQuery:
        """
        Return a new GroupByQuery with an OR-chained HAVING condition.

        Use after .having() to add an alternative condition:
            .having('sum(x) > 100').or_having('count() >= 5')
            → HAVING (sum(x) > 100) OR (count() >= 5)

        The original GroupByQuery is not modified.

        Args:
            condition: Raw SQL HAVING expression (e.g., 'max(amount) > 50')

        Returns:
            New GroupByQuery with the condition added

        Raises:
            ValueError: If condition is empty or no prior having clause exists
        """
        if not condition or not condition.strip():
            raise ValueError("HAVING condition must be a non-empty string")
        if not self._having_clauses:
            raise ValueError("or_having() requires a prior having() call")
        return self._clone_with_having(condition.strip(), "OR")

    def _build_having(self) -> Optional[str]:
        """Build the combined HAVING clause from stored conditions."""
        if not self._having_clauses:
            return None
        parts = []
        for i, (condition, connector) in enumerate(self._having_clauses):
            if i == 0:
                parts.append(f"({condition})")
            else:
                parts.append(f"{connector} ({condition})")
        return " ".join(parts)

    def _get_group_by_info(self) -> GroupByInfo:
        """
        Build GroupByInfo from the source Object.

        Handles plain Objects, Views with WHERE/LIMIT constraints,
        multi-field Views, and single-field Views.

        Returns:
            GroupByInfo with source, group keys, and column metadata
        """
        source = self._source
        schema = source._schema

        # Determine source query and columns based on source type
        if isinstance(source, View):
            if source.is_single_field:
                # Single-field View: columns are {aai_id, value}
                field = source._selected_fields[0]
                field_type = str(schema.columns.get(field, "Float64"))
                columns = {"aai_id": "UInt64", "value": field_type}
                source_query = f"({source._build_select()})"
            elif source._selected_fields:
                # Multi-field View: only selected columns available
                columns = {"aai_id": "UInt64"}
                for field in source._selected_fields:
                    columns[field] = str(schema.columns.get(field, "Float64"))
                source_query = f"({source._build_select()})"
            elif source.has_constraints:
                # WHERE/LIMIT View: full columns, wrapped in subquery
                columns = {k: str(v) for k, v in schema.columns.items()}
                source_query = f"({source._build_select()})"
            else:
                # Base View (no constraints): same as plain Object
                columns = {k: str(v) for k, v in schema.columns.items()}
                source_query = source.table
        else:
            # Plain Object
            columns = {k: str(v) for k, v in schema.columns.items()}
            source_query = (
                f"({source._build_select()})"
                if hasattr(source, "has_constraints") and source.has_constraints
                else source.table
            )

        return GroupByInfo(
            source=source_query,
            base_table=source.table,
            group_keys=self._keys,
            columns=columns,
            fieldtype=schema.fieldtype,
            having=self._build_having(),
        )

    async def agg(self, aggregations: Dict[str, GroupByOpType]) -> Object:
        """
        Apply aggregations per group. Core method — all convenience methods delegate here.

        Each entry maps a column name to an aggregation function.
        Result columns keep the same name as the source columns.
        For 'count', the column key becomes the result column name
        and count() is called without arguments.

        Args:
            aggregations: Dict mapping column_name -> GroupByOpType
                         (GB_SUM, GB_MEAN, GB_MIN, GB_MAX, GB_COUNT, GB_STD, GB_VAR)

        Returns:
            Dict Object with group keys + all aggregated columns

        Examples:
            >>> result = await obj.group_by('category').agg({
            ...     'amount': GB_SUM,
            ...     'price': GB_MEAN,
            ... })
        """
        info = self._get_group_by_info()
        return await operators.group_by_agg(info, aggregations, self.ch_client)

    async def sum(self, column: str) -> Object:
        """Convenience: sum per group. Delegates to agg()."""
        return await self.agg({column: GB_SUM})

    async def mean(self, column: str) -> Object:
        """Convenience: mean per group. Delegates to agg()."""
        return await self.agg({column: GB_MEAN})

    async def min(self, column: str) -> Object:
        """Convenience: min per group. Delegates to agg()."""
        return await self.agg({column: GB_MIN})

    async def max(self, column: str) -> Object:
        """Convenience: max per group. Delegates to agg()."""
        return await self.agg({column: GB_MAX})

    async def count(self) -> Object:
        """Convenience: count per group. Delegates to agg()."""
        return await self.agg({"_count": GB_COUNT})

    async def std(self, column: str) -> Object:
        """Convenience: std per group. Delegates to agg()."""
        return await self.agg({column: GB_STD})

    async def var(self, column: str) -> Object:
        """Convenience: var per group. Delegates to agg()."""
        return await self.agg({column: GB_VAR})

    def __repr__(self) -> str:
        """String representation of the GroupByQuery."""
        keys_str = ", ".join(f"'{k}'" for k in self._keys)
        return f"GroupByQuery(keys=[{keys_str}])"


class View(Object):
    """
    A view of an Object with query constraints (WHERE, LIMIT, OFFSET, ORDER BY).

    Views are read-only and reference the same underlying table as their source Object.
    They cannot be modified with operations like insert().

    Views can also select fields from a dict Object using selected_fields.
    Single-field selection (len=1) returns array-like data, multi-field returns dict-like.
    """

    def __init__(
        self,
        source: Object,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        selected_fields: Optional[List[str]] = None,
    ):
        """
        Initialize a View.

        Args:
            source: Source Object to create view from
            where: Optional WHERE clause
            limit: Optional LIMIT
            offset: Optional OFFSET
            order_by: Optional ORDER BY clause
            selected_fields: Optional list of field names to select
                             Single field [name] returns array-like view
                             Multiple fields returns dict-like view
        """
        super().__init__(table=source.table, schema=source._schema)
        if where:
            self._where_clauses.append((where.strip(), "AND"))
        self._limit = limit
        self._offset = offset
        self._order_by = order_by
        self._selected_fields = selected_fields

        # Register with context for lifecycle tracking and stale marking
        if source._data_ctx_ref is not None:
            context = source._data_ctx_ref()
            if context is not None:
                self._register(context)
                context._register_object(self)

    @property
    def limit(self) -> Optional[int]:
        """Get LIMIT."""
        return self._limit

    @property
    def offset(self) -> Optional[int]:
        """Get OFFSET."""
        return self._offset

    @property
    def order_by(self) -> Optional[str]:
        """Get ORDER BY clause."""
        return self._order_by

    @property
    def selected_fields(self) -> Optional[List[str]]:
        """Get the selected field names for dict column selection."""
        return self._selected_fields

    @property
    def is_single_field(self) -> bool:
        """Check if this is a single-field selection (array-like output)."""
        return self._selected_fields is not None and len(self._selected_fields) == 1

    def _serialize_ref(self) -> dict:
        """Serialize this View to a reference dict for task kwargs/results."""
        return {
            "object_type": "view",
            "table": self.table,
            "where": self._build_where(),
            "limit": self.limit,
            "offset": self.offset,
            "order_by": self.order_by,
            "selected_fields": self.selected_fields,
        }

    def _clone_with_clause(self, condition: str, connector: str) -> View:
        """Create a new View with all current constraints plus an additional WHERE clause."""
        new_view = View.__new__(View)
        Object.__init__(new_view, table=self.table, schema=self._schema)
        new_view._where_clauses = list(self._where_clauses)
        new_view._where_clauses.append((condition, connector))
        new_view._limit = self._limit
        new_view._offset = self._offset
        new_view._order_by = self._order_by
        new_view._selected_fields = self._selected_fields
        if self._data_ctx_ref is not None:
            context = self._data_ctx_ref()
            if context is not None:
                new_view._register(context)
                context._register_object(new_view)
        return new_view

    def where(self, condition: str) -> View:
        """
        Return a new View with an AND-chained WHERE condition.

        Multiple calls chain with AND:
            .where('x > 10').where('y < 20')
            → WHERE (x > 10) AND (y < 20)

        The original View is not modified.

        Args:
            condition: Raw SQL WHERE expression (e.g., 'value > 100')

        Returns:
            New View with the condition added

        Raises:
            ValueError: If condition is empty
        """
        if not condition or not condition.strip():
            raise ValueError("WHERE condition must be a non-empty string")
        return self._clone_with_clause(condition.strip(), "AND")

    def or_where(self, condition: str) -> View:
        """
        Return a new View with an OR-chained WHERE condition.

        Use after .view(where=...) or .where() to add an alternative condition:
            .where('x > 100').or_where('y < 5')
            → WHERE (x > 100) OR (y < 5)

        The original View is not modified.

        Args:
            condition: Raw SQL WHERE expression (e.g., 'value < 10')

        Returns:
            New View with the condition added

        Raises:
            ValueError: If condition is empty or no prior where clause exists
        """
        if not condition or not condition.strip():
            raise ValueError("WHERE condition must be a non-empty string")
        if not self._where_clauses:
            raise ValueError("or_where() requires a prior where condition")
        return self._clone_with_clause(condition.strip(), "OR")

    def _build_select(self, columns: str = "*", default_order_by: Optional[str] = None) -> str:
        """
        Build a SELECT query with view constraints applied.

        For single-field selection, renames the field as 'value' for array compatibility.
        For multi-field selection, selects all specified fields.
        If columns="value" is requested, only the value column is selected.

        Args:
            columns: Column specification (default "*", respected for field selection views)
            default_order_by: Default ORDER BY clause if view doesn't have custom order_by

        Returns:
            str: SELECT query string with WHERE/LIMIT/OFFSET/ORDER BY applied
        """
        if self._selected_fields:
            if self.is_single_field:
                # Single field: rename as 'value' for array compatibility
                field = quote_identifier(self._selected_fields[0])
                if columns == "value":
                    select_cols = f"{field} AS value"
                else:
                    select_cols = f"aai_id, {field} AS value"
            else:
                # Multiple fields: select all specified fields
                fields_str = ", ".join(quote_identifier(f) for f in self._selected_fields)
                select_cols = f"aai_id, {fields_str}"
        else:
            select_cols = columns

        query = f"SELECT {select_cols} FROM {self.table}"
        where_clause = self._build_where()
        if where_clause:
            query += f" WHERE {where_clause}"
        # Use custom order_by if set, otherwise use default
        order_clause = self.order_by or default_order_by
        if order_clause:
            query += f" ORDER BY {order_clause}"
        if self.limit is not None:
            query += f" LIMIT {self.limit}"
        if self.offset is not None:
            query += f" OFFSET {self.offset}"
        return query

    def _get_query_info(self) -> QueryInfo:
        """
        Get query information for database operations.

        For Views with single-field selection, the source is always a subquery
        that renames the selected column to 'value'.

        Returns:
            QueryInfo: Dataclass with source, base_table, value_column, fieldtype, and value_type
        """
        # For single-field views, use the field name as value_column for metadata queries
        value_column = self._selected_fields[0] if self.is_single_field else "value"

        # Get fieldtype and value_type from schema
        source_schema = self._schema
        if self.is_single_field and source_schema:
            # Single-field selection yields array type
            fieldtype = FIELDTYPE_ARRAY
            value_type = str(source_schema.columns.get(value_column, "Float64"))
        elif source_schema:
            fieldtype = source_schema.fieldtype
            value_type = str(source_schema.columns.get("value", "Float64"))
        else:
            fieldtype = FIELDTYPE_ARRAY
            value_type = "Float64"

        # Always use subquery when has_constraints (includes selected_fields)
        if self.has_constraints:
            return QueryInfo(
                source=f"({self._build_select()})",
                base_table=self.table,
                value_column=value_column,
                fieldtype=fieldtype,
                value_type=value_type,
            )
        return QueryInfo(
            source=self.table,
            base_table=self.table,
            value_column=value_column,
            fieldtype=fieldtype,
            value_type=value_type,
        )

    def _get_copy_info(self) -> CopyInfo:
        """
        Get copy info for database-level copy operations.

        For Views, includes source schema columns and selected fields info.

        Returns:
            CopyInfo with source query, schema metadata, and field selection info
        """
        source_schema = self._schema
        source_query = f"({self._build_select()})" if self.has_constraints else self.table
        return CopyInfo(
            source_query=source_query,
            fieldtype=source_schema.fieldtype,
            columns=source_schema.columns,
            selected_fields=self._selected_fields,
            is_single_field=self.is_single_field,
        )

    async def data(self, orient: str = ORIENT_DICT):
        """
        Get the data from the view.

        For single-field selection, returns array data (the selected column).
        For multi-field selection, returns dict data (subset of columns).

        Args:
            orient: Output format for dict data

        Returns:
            - For single-field views: returns list of values (array)
            - For multi-field views: returns dict with selected columns
            - Otherwise: delegates to parent Object.data()
        """
        self.checkstale()

        if self._selected_fields:
            from . import data_extraction

            if self.is_single_field:
                # Single field: return as array
                return await data_extraction.extract_array_data(self)
            else:
                # Multiple fields: return as dict with only selected fields
                columns: Dict[str, ColumnMeta] = {}
                for field in self._selected_fields:
                    columns[field] = ColumnMeta(fieldtype=FIELDTYPE_ARRAY)

                column_names = ["aai_id"] + list(self._selected_fields)
                return await data_extraction.extract_dict_data(self, column_names, columns, orient)

        # Delegate to parent for normal views
        return await super().data(orient=orient)

    async def metadata(self) -> ViewMetadata:
        """
        Get metadata for this view including table info and view constraints.

        Builds ViewMetadata from source's metadata on demand.

        Returns:
            ViewMetadata: Dataclass with table, fieldtype, columns, and view constraints

        Examples:
            >>> obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})
            >>> view = obj['param1']
            >>> meta = await view.metadata()
            >>> print(meta.selected_fields)  # ['param1']
            >>> print(meta.fieldtype)  # 'd' (source table is dict type)
            >>>
            >>> filtered = obj.view(where="param1 > 1", limit=10)
            >>> meta2 = await filtered.metadata()
            >>> print(meta2.where)  # '(param1 > 1)'
            >>> print(meta2.limit)  # 10
        """
        # Reuse Object.metadata() to build column_infos and compute overall_fieldtype
        # from self._schema (handles scalar/array/dict detection, DB fallback, etc.)
        base_meta = await super().metadata()

        return ViewMetadata(
            table=base_meta.table,
            fieldtype=base_meta.fieldtype,
            columns=base_meta.columns,
            where=self._build_where(),
            limit=self._limit,
            offset=self._offset,
            order_by=self._order_by,
            selected_fields=self._selected_fields,
        )

    async def insert(self, *args) -> None:
        """Views are read-only and cannot be modified."""
        raise RuntimeError("Cannot insert into a view")

    def __repr__(self) -> str:
        """String representation of the View."""
        constraints = []
        if self.selected_fields:
            constraints.append(f"selected_fields={self.selected_fields}")
        where_clause = self._build_where()
        if where_clause:
            constraints.append(f"where='{where_clause}'")
        if self.limit:
            constraints.append(f"limit={self.limit}")
        if self.offset:
            constraints.append(f"offset={self.offset}")
        if self.order_by:
            constraints.append(f"order_by='{self.order_by}'")
        constraint_str = ", ".join(constraints) if constraints else "no constraints"
        return f"View(table='{self.table}', {constraint_str})"
