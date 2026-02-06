"""
aaiclick.data.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from __future__ import annotations

from typing import Optional, Dict, List, Tuple, Any, Union
from dataclasses import dataclass
from typing_extensions import Self

from . import operators
from ..snowflake_id import get_snowflake_id
from .models import (
    Schema,
    ColumnMeta,
    ColumnInfo,
    ColumnType,
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
    operator mapping, see object.md in this directory.
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
        self._schema = schema  # Cache schema for internal use (e.g., clone_view_db)

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    @property
    def ctx(self) -> Context:
        """Get the context managing this object."""
        self.checkstale()
        return get_data_context()

    @property
    def where(self) -> Optional[str]:
        """Get WHERE clause (None for base Object)."""
        return None

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
        return bool(self.where or self.limit is not None or self.offset is not None or self.order_by)

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
        if self.where:
            query += f" WHERE {self.where}"
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

        Encapsulates both the data source (which may be a subquery for Views)
        and the base table name (for metadata queries).

        Returns:
            QueryInfo: NamedTuple with source and base_table fields
        """
        source = f"({self._build_select()})" if self.has_constraints else self.table
        return QueryInfo(source=source, base_table=self.table)

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

        Returns:
            Object: New Object instance with copied data

        Examples:
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_copy = await obj_a.copy()
            >>> await obj_copy.data()  # Returns [1, 2, 3]
        """
        self.checkstale()
        from . import ingest
        return await ingest.copy_db(self.table, self.ch_client)

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

    def __getitem__(self, key: Union[str, List[str]]) -> "View":
        """
        Select field(s) from a dict Object, returning a View.

        Creates a View that selects only the specified column(s) from the dict Object.
        The View can be used in operations or materialized with clone().

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
            >>> arr = await view.clone()  # Returns new array Object
            >>>
            >>> # Multi-field selector - returns dict-like view
            >>> view = obj[['param1', 'param2']]
            >>> await view.data()  # Returns {'param1': [123, 234], 'param2': [456, 342]}
            >>> dict_obj = await view.clone()  # Returns new dict Object
        """
        self.checkstale()
        if isinstance(key, list):
            return View(self, selected_fields=key)
        return View(self, selected_field=key)

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"


class View(Object):
    """
    A view of an Object with query constraints (WHERE, LIMIT, OFFSET, ORDER BY).

    Views are read-only and reference the same underlying table as their source Object.
    They cannot be modified with operations like insert().

    Views can also select a specific field from a dict Object using selected_field.
    """

    def __init__(
        self,
        source: Object,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        selected_field: Optional[str] = None,
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
            selected_field: Optional single field name to select (returns array-like view)
            selected_fields: Optional list of field names to select (returns dict-like view)
        """
        self._source = source
        self._where = where
        self._limit = limit
        self._offset = offset
        self._order_by = order_by
        self._selected_field = selected_field
        self._selected_fields = selected_fields

    @property
    def ctx(self) -> Context:
        """Get the context from the source object."""
        return self._source.ctx

    @property
    def table(self) -> str:
        """Get the table name from the source object."""
        return self._source.table

    @property
    def where(self) -> Optional[str]:
        """Get WHERE clause."""
        return self._where

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
    def selected_field(self) -> Optional[str]:
        """Get the selected field name (for single-field dict column selection)."""
        return self._selected_field

    @property
    def selected_fields(self) -> Optional[List[str]]:
        """Get the selected field names (for multi-field dict column selection)."""
        return self._selected_fields

    @property
    def stale(self) -> bool:
        """Check if source object's context has been cleaned up."""
        return self._source.stale

    @property
    def has_constraints(self) -> bool:
        """Check if this view has any constraints including selected_field/fields."""
        return bool(
            self.where
            or self.limit is not None
            or self.offset is not None
            or self.order_by
            or self.selected_field
            or self.selected_fields
        )

    def checkstale(self):
        """Check if source object is stale and raise error if so."""
        self._source.checkstale()

    def _build_select(self, columns: str = "*", default_order_by: Optional[str] = None) -> str:
        """
        Build a SELECT query with view constraints applied.

        When selected_field is set, selects aai_id and the field as 'value'.
        When selected_fields is set, selects aai_id and all specified fields.
        If columns="value" is requested, only the value column is selected.

        Args:
            columns: Column specification (default "*", respected for selected_field views)
            default_order_by: Default ORDER BY clause if view doesn't have custom order_by

        Returns:
            str: SELECT query string with WHERE/LIMIT/OFFSET/ORDER BY applied
        """
        # When selected_field is set (single field), rename the field as 'value'
        if self._selected_field:
            if columns == "value":
                # Only select the value column (renamed from selected field)
                select_cols = f"{self._selected_field} AS value"
            else:
                # Select both aai_id and value
                select_cols = f"aai_id, {self._selected_field} AS value"
        # When selected_fields is set (multiple fields), select all specified fields
        elif self._selected_fields:
            fields_str = ", ".join(self._selected_fields)
            select_cols = f"aai_id, {fields_str}"
        else:
            select_cols = columns

        query = f"SELECT {select_cols} FROM {self.table}"
        if self.where:
            query += f" WHERE {self.where}"
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

        For Views with selected_field, the source is always a subquery
        that renames the selected column to 'value'.

        Returns:
            QueryInfo: NamedTuple with source, base_table, and value_column fields
        """
        # For selected_field views, use the selected field name as value_column
        # This tells operators which column to query for metadata
        value_column = self._selected_field if self._selected_field else "value"

        # Always use subquery for selected_field views or when has_constraints
        if self.has_constraints:
            return QueryInfo(
                source=f"({self._build_select()})",
                base_table=self.table,
                value_column=value_column,
            )
        return QueryInfo(source=self.table, base_table=self.table, value_column=value_column)

    async def data(self, orient: str = ORIENT_DICT):
        """
        Get the data from the view.

        For views with selected_field, returns array data (the selected column).
        For views with selected_fields, returns dict data (subset of columns).

        Args:
            orient: Output format for dict data

        Returns:
            - For selected_field views: returns list of values (array)
            - For selected_fields views: returns dict with selected columns
            - Otherwise: delegates to parent Object.data()
        """
        self.checkstale()

        if self._selected_field:
            # For single selected_field views, return as array
            from . import data_extraction
            return await data_extraction.extract_array_data(self)

        if self._selected_fields:
            # For multi-field views, return as dict with only selected fields
            from . import data_extraction

            # Build column metadata for selected fields only
            columns: Dict[str, ColumnMeta] = {}
            for field in self._selected_fields:
                columns[field] = ColumnMeta(fieldtype=FIELDTYPE_ARRAY)

            # Query with aai_id + selected fields
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
            >>> print(meta.selected_field)  # 'param1'
            >>> print(meta.fieldtype)  # 'd' (source table is dict type)
            >>>
            >>> filtered = obj.view(where="param1 > 1", limit=10)
            >>> meta2 = await filtered.metadata()
            >>> print(meta2.where)  # 'param1 > 1'
            >>> print(meta2.limit)  # 10
        """
        # Get base metadata from source (may query DB if no cached schema)
        base_meta = await self._source.metadata()

        return ViewMetadata(
            table=base_meta.table,
            fieldtype=base_meta.fieldtype,
            columns=base_meta.columns,
            where=self._where,
            limit=self._limit,
            offset=self._offset,
            order_by=self._order_by,
            selected_field=self._selected_field,
            selected_fields=self._selected_fields,
        )

    async def clone(self) -> "Object":
        """
        Materialize this view as a new array Object.

        Creates a new Object with a copy of the view's data.
        For views with selected_field, this creates an array Object.

        Returns:
            Object: New Object instance with the view's data materialized

        Examples:
            >>> obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})
            >>> view = obj['param1']  # View selecting param1
            >>> arr = await view.clone()  # New array Object
            >>> await arr.data()  # Returns [1, 2, 3]
        """
        self.checkstale()
        from . import ingest
        return await ingest.clone_view_db(self)

    async def insert(self, *args) -> None:
        """Views are read-only and cannot be modified."""
        raise RuntimeError("Cannot insert into a view")

    def __repr__(self) -> str:
        """String representation of the View."""
        constraints = []
        if self.selected_field:
            constraints.append(f"selected_field='{self.selected_field}'")
        if self.selected_fields:
            constraints.append(f"selected_fields={self.selected_fields}")
        if self.where:
            constraints.append(f"where='{self.where}'")
        if self.limit:
            constraints.append(f"limit={self.limit}")
        if self.offset:
            constraints.append(f"offset={self.offset}")
        if self.order_by:
            constraints.append(f"order_by='{self.order_by}'")
        constraint_str = ", ".join(constraints) if constraints else "no constraints"
        return f"View(table='{self.table}', {constraint_str})"
