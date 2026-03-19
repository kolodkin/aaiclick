"""
aaiclick.data.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from __future__ import annotations

import sys
from typing import Optional, Dict, List, Tuple, Any, Union
from dataclasses import dataclass, replace as dataclass_replace
from typing_extensions import Self

from . import operators, ingest, data_extraction
from ..oplog.collector import oplog_record
from ..snowflake_id import get_snowflake_id

from .models import (
    Schema,
    ColumnInfo,
    Computed,
    CopyInfo,
    ColumnMeta,
    ColumnType,
    parse_ch_type,
    GroupByInfo,
    GroupByOpType,
    GB_ANY,
    GB_COUNT,
    GB_GROUP_ARRAY_DISTINCT,
    GB_MAX,
    GB_MEAN,
    GB_MIN,
    GB_STD,
    GB_SUM,
    GB_VAR,
    ViewSchema,
    QueryInfo,
    IngestQueryInfo,
    ValueScalarType,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
)
from .data_context import (
    get_ch_client,
    incref,
    decref,
    register_object,
    create_object_from_value,
)
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
        table_name = table if table is not None else f"t_{get_snowflake_id()}"
        if schema is None:
            schema = Schema(fieldtype=FIELDTYPE_SCALAR, columns={})
        self._stale = False
        self._schema = dataclass_replace(schema, table=table_name)
        self._registered = False

    @property
    def persistent(self) -> bool:
        """Check if this is a persistent (named) object that survives context exit."""
        return self.table.startswith("p_")

    def _register(self) -> None:
        """Register this object with the active context for lifecycle tracking."""
        self._registered = True
        if not self.persistent:
            incref(self.table)

    def __del__(self):
        """Decrement refcount on deletion."""
        if sys.is_finalizing():
            return
        if not self._registered:
            return
        if self.table.startswith("p_"):
            return
        decref(self.table)

    @property
    def table(self) -> str:
        """Get the table name for this object (read-only)."""
        return self._schema.table

    @property
    def schema(self) -> Schema:
        """Get the cached schema for this object."""
        return self._schema

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

    @property
    def where_clauses(self) -> List[Tuple[str, str]]:
        """Get WHERE clauses (empty for base Object)."""
        return []

    @property
    def computed_columns(self) -> Optional[Dict[str, Computed]]:
        """Get computed columns (None for base Object)."""
        return None

    @property
    def renamed_columns(self) -> Optional[Dict[str, str]]:
        """Get renamed columns mapping old->new (None for base Object)."""
        return None

    def _serialize_ref(self) -> dict:
        """Serialize this Object to a reference dict for task kwargs/results."""
        ref = {"object_type": "object", "table": self.table}
        if self.persistent:
            ref["persistent"] = True
        return ref

    @property
    def is_single_field(self) -> bool:
        """Check if this is a single-field selection."""
        return self.selected_fields is not None and len(self.selected_fields) == 1

    @property
    def ch_client(self):
        """Get the ClickHouse client from the context."""
        self.checkstale()
        return get_ch_client()

    @property
    def stale(self) -> bool:
        """Check if this object has been deleted."""
        return self._stale

    @property
    def has_constraints(self) -> bool:
        """Check if this object has any view constraints."""
        return bool(
            self.where_clauses
            or self.limit is not None
            or self.offset is not None
            or self.order_by
            or self.selected_fields
            or self.computed_columns
            or self.renamed_columns
        )

    def checkstale(self):
        """
        Check if object is stale and raise error if so.

        Raises:
            RuntimeError: If object has been deleted (stale)
        """
        if self._stale:
            raise RuntimeError(
                f"Cannot use stale Object. Table '{self.table}' has been deleted."
            )

    def _build_where(self) -> Optional[str]:
        """Build the combined WHERE clause from stored conditions."""
        if not self.where_clauses:
            return None
        parts = []
        for i, (condition, connector) in enumerate(self.where_clauses):
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
        Get query information for operator operations.

        Encapsulates the data source, base table name, and schema metadata
        to avoid needing to query system tables in operators.
        Works for both Object (value column) and View (selected field).
        """
        source = f"({self._build_select()})" if self.has_constraints else self.table
        value_column = self.selected_fields[0] if self.is_single_field else "value"

        if self.is_single_field:
            fieldtype = FIELDTYPE_ARRAY
            col_def = self._schema.columns.get(value_column, ColumnInfo("Float64"))
        else:
            fieldtype = self._schema.fieldtype
            col_def = self._schema.columns.get("value", ColumnInfo("Float64"))

        return QueryInfo(
            source=source,
            base_table=self.table,
            value_column=value_column,
            fieldtype=fieldtype,
            value_type=col_def.type,
            nullable=col_def.nullable,
        )

    def _get_ingest_query_info(self) -> IngestQueryInfo:
        """
        Get query information for concat/insert operations.

        Extends QueryInfo with full column schema so ingest functions
        can validate without querying system.columns.
        """
        info = self._get_query_info()
        columns = self._schema.columns
        return IngestQueryInfo(**vars(info), columns=columns)

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

    async def markdown(self, truncate: Optional[Dict[str, int]] = None) -> str:
        """Return the object's data formatted as a markdown table.

        Fetches data via ``.data()`` and renders it as a plain-text markdown
        table with auto-sized column widths.  The internal ``aai_id`` column
        is omitted.

        Args:
            truncate: Optional mapping of column name to maximum character
                width.  Values longer than the limit are truncated with an
                ellipsis (``…``).  Columns not present in the mapping are
                never truncated.

        Returns:
            Multi-line string containing the markdown table.
        """
        raw = await self.data()
        trunc = truncate or {}

        # For scalar / array data wrap into a single-column dict
        if not isinstance(raw, dict):
            raw = {"value": raw if isinstance(raw, list) else [raw]}

        columns = [c for c in raw if c != "aai_id"]
        if not columns:
            return ""
        n_rows = len(raw[columns[0]]) if isinstance(raw[columns[0]], list) else 1

        def _cell(val: object, col: str) -> str:
            if val is None:
                return "N/A"
            if isinstance(val, float):
                return f"{val:.2f}"
            s = str(val)
            # Sanitize: collapse newlines/tabs and escape pipes
            s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
            s = s.replace("\t", " ").replace("|", "\\|")
            limit = trunc.get(col)
            if limit is not None and len(s) > limit:
                return s[: limit - 1] + "…"
            return s

        # Ensure we can iterate rows uniformly
        def _get(col: str, i: int) -> object:
            v = raw[col]
            return v[i] if isinstance(v, list) else v

        widths: Dict[str, int] = {}
        for col in columns:
            max_val = max((len(_cell(_get(col, i), col)) for i in range(n_rows)), default=0)
            cap = trunc.get(col)
            w = max(len(col), max_val)
            widths[col] = min(w, cap) if cap is not None else w

        lines: List[str] = []
        header = "| " + " | ".join(f"{col:<{widths[col]}s}" for col in columns) + " |"
        sep = "|" + "|".join("-" * (w + 2) for w in (widths[col] for col in columns)) + "|"
        lines.append(header)
        lines.append(sep)
        for i in range(n_rows):
            row = "| " + " | ".join(
                f"{_cell(_get(col, i), col):<{widths[col]}s}" for col in columns
            ) + " |"
            lines.append(row)
        return "\n".join(lines)

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

    @staticmethod
    async def _ensure_object(value: Union[Object, ValueScalarType]) -> Object:
        """
        Ensure value is an Object, converting Python scalars if needed.

        Python scalars are converted to Objects via create_object_from_value,
        so all data stays in ClickHouse with a unified code path.

        Args:
            value: An Object or a Python scalar (int, float, bool, str)

        Returns:
            Object instance (existing or newly created from scalar)
        """
        if isinstance(value, (int, float, bool, str)):
            return await create_object_from_value(value)
        return value

    async def _apply_operator(self, other: Union[Object, ValueScalarType], operator: str) -> Object:
        """
        Apply an operator on two objects using SQL templates.

        Supports scalar broadcast: if other is a Python scalar (int, float, bool, str),
        it is converted to a scalar Object via create_object_from_value.

        Args:
            other: Another Object or Python scalar to operate with
            operator: Operator symbol (e.g., '+', '-', '**', '==', '&')

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other = await self._ensure_object(other)
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators._apply_operator_db(
            info_a, info_b, operator, self.ch_client
        )

    async def _apply_operator_reverse(self, other: Union[Object, ValueScalarType], operator: str) -> Object:
        """
        Apply an operator with reversed operands (other op self).

        Used for __radd__, __rsub__, etc. when the left operand is a scalar.

        Args:
            other: A Python scalar or Object (left operand)
            operator: Operator symbol

        Returns:
            Object: New Object instance pointing to result table
        """
        self.checkstale()
        other = await self._ensure_object(other)
        other.checkstale()
        info_a = other._get_query_info()
        info_b = self._get_query_info()
        return await operators._apply_operator_db(
            info_a, info_b, operator, self.ch_client
        )

    async def __add__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Add: self + other. Supports scalar broadcast."""
        return await self._apply_operator(other, "+")

    async def __radd__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse add: other + self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "+")

    async def __sub__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Subtract: self - other. Supports scalar broadcast."""
        return await self._apply_operator(other, "-")

    async def __rsub__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse subtract: other - self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "-")

    async def __mul__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Multiply: self * other. Supports scalar broadcast."""
        return await self._apply_operator(other, "*")

    async def __rmul__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse multiply: other * self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "*")

    async def __truediv__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Divide: self / other. Supports scalar broadcast."""
        return await self._apply_operator(other, "/")

    async def __rtruediv__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse divide: other / self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "/")

    async def __floordiv__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Floor divide: self // other. Supports scalar broadcast."""
        return await self._apply_operator(other, "//")

    async def __rfloordiv__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse floor divide: other // self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "//")

    async def __mod__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Modulo: self % other. Supports scalar broadcast."""
        return await self._apply_operator(other, "%")

    async def __rmod__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse modulo: other % self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "%")

    async def __pow__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Power: self ** other. Supports scalar broadcast."""
        return await self._apply_operator(other, "**")

    async def __rpow__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse power: other ** self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "**")

    async def __eq__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Equality: self == other. Supports scalar broadcast."""
        return await self._apply_operator(other, "==")

    async def __ne__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Inequality: self != other. Supports scalar broadcast."""
        return await self._apply_operator(other, "!=")

    async def __lt__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Less than: self < other. Supports scalar broadcast."""
        return await self._apply_operator(other, "<")

    async def __le__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Less or equal: self <= other. Supports scalar broadcast."""
        return await self._apply_operator(other, "<=")

    async def __gt__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Greater than: self > other. Supports scalar broadcast."""
        return await self._apply_operator(other, ">")

    async def __ge__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Greater or equal: self >= other. Supports scalar broadcast."""
        return await self._apply_operator(other, ">=")

    async def __and__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Bitwise AND: self & other. Supports scalar broadcast."""
        return await self._apply_operator(other, "&")

    async def __rand__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse bitwise AND: other & self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "&")

    async def __or__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Bitwise OR: self | other. Supports scalar broadcast."""
        return await self._apply_operator(other, "|")

    async def __ror__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse bitwise OR: other | self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "|")

    async def __xor__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Bitwise XOR: self ^ other. Supports scalar broadcast."""
        return await self._apply_operator(other, "^")

    async def __rxor__(self, other: Union[Object, ValueScalarType]) -> Object:
        """Reverse bitwise XOR: other ^ self. Supports scalar broadcast."""
        return await self._apply_operator_reverse(other, "^")

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
        source_table = self.table
        copy_info = self._get_copy_info()
        if copy_info.selected_fields:
            result = await ingest.copy_db_selected_fields(copy_info, self.ch_client)
        else:
            result = await ingest.copy_db(copy_info, self.ch_client)
        oplog_record(result.table, "copy", kwargs={"source": source_table})
        return result

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
            >>>
            >>> # Nullable promotion: if any source has nullable columns,
            >>> # result is promoted to nullable
            >>> obj_nullable = await create_object(Schema(
            ...     fieldtype=FIELDTYPE_ARRAY,
            ...     columns={"aai_id": ColumnInfo("UInt64"),
            ...              "value": ColumnInfo("Int64", nullable=True)},
            ... ))
            >>> obj_non_null = await create_object_from_value([3, 4])
            >>> result = await obj_nullable.concat(obj_non_null)
            >>> result.schema.columns["value"].nullable  # True
        """
        if not args:
            raise ValueError("concat requires at least one argument")

        self.checkstale()

        # Convert all arguments to IngestQueryInfo
        query_infos = [self._get_ingest_query_info()]
        temp_objects = []

        for arg in args:
            if isinstance(arg, Object):
                arg.checkstale()
                query_infos.append(arg._get_ingest_query_info())
            else:
                # Skip empty lists to avoid type conflicts
                if isinstance(arg, list) and len(arg) == 0:
                    continue
                # Convert ValueType to temporary Object
                temp = await create_object_from_value(arg)
                temp_objects.append(temp)
                query_infos.append(temp._get_ingest_query_info())

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

        # Convert all arguments to IngestQueryInfo
        query_infos = []
        temp_objects = []

        for arg in args:
            if isinstance(arg, Object):
                arg.checkstale()
                query_infos.append(arg._get_ingest_query_info())
            else:
                # Skip empty lists
                if isinstance(arg, list) and len(arg) == 0:
                    continue
                # Convert ValueType to temporary Object
                temp = await create_object_from_value(arg)
                temp_objects.append(temp)
                query_infos.append(temp._get_ingest_query_info())

        # Single database operation for all sources
        if query_infos:
            await ingest.insert_objects_db(
                self._get_ingest_query_info(), query_infos, self.ch_client
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

        # Build INSERT query (aai_id uses DEFAULT generateSnowflakeID())
        insert_col_names = [k for k in self.schema.columns if k != "aai_id"]
        insert_cols_str = ", ".join(insert_col_names)
        where_clause = f" WHERE {where}" if where else ""
        limit_clause = f" LIMIT {limit}" if limit is not None else ""

        insert_query = (
            f"INSERT INTO {self.table} ({insert_cols_str}) "
            f"SELECT {select_cols} "
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

    async def count_if(self, condition: Union[str, Dict[str, str]]) -> Self:
        """
        Count rows matching condition(s) using countIf().

        When condition is a str, returns a scalar Object (single countIf).
        When condition is a dict {name: condition_str}, returns a dict Object
        with one UInt64 column per entry, computed in a single table scan.

        Reference: https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators#-if

        Args:
            condition: SQL condition string, or dict mapping result names to conditions

        Returns:
            Self: Scalar Object (str) or dict Object (dict)

        Examples:
            >>> obj = await create_object_from_value([1, 2, 3, 4, 5])
            >>> result = await obj.count_if("value > 3")
            >>> await result.data()  # Returns 2

            >>> stats = await obj.count_if({
            ...     "small": "value <= 2",
            ...     "large": "value >= 4",
            ... })
            >>> await stats.data()  # {"small": 2, "large": 2}
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.count_if_agg(info, condition, self.ch_client)

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

    # Unary Transform Operators

    async def _apply_unary_transform(self, transform: str) -> Self:
        """Apply a unary ClickHouse function to the value column.

        Args:
            transform: Transform key from operators.UNARY_TRANSFORMS

        Returns:
            Self: New Object with transformed values
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.unary_transform(info, transform, self.ch_client)

    async def year(self) -> Self:
        """Extract year from Date/DateTime values.

        Returns:
            Self: New Object with UInt16 year values
        """
        return await self._apply_unary_transform("year")

    async def month(self) -> Self:
        """Extract month (1-12) from Date/DateTime values.

        Returns:
            Self: New Object with UInt8 month values
        """
        return await self._apply_unary_transform("month")

    async def day_of_week(self) -> Self:
        """Extract day of week (1=Mon, 7=Sun) from Date/DateTime values.

        Returns:
            Self: New Object with UInt8 day-of-week values
        """
        return await self._apply_unary_transform("day_of_week")

    async def lower(self) -> Self:
        """Lowercase string values.

        Returns:
            Self: New Object with lowercased String values
        """
        return await self._apply_unary_transform("lower")

    async def upper(self) -> Self:
        """Uppercase string values.

        Returns:
            Self: New Object with uppercased String values
        """
        return await self._apply_unary_transform("upper")

    async def length(self) -> Self:
        """String length of values.

        Returns:
            Self: New Object with UInt64 length values
        """
        return await self._apply_unary_transform("length")

    async def trim(self) -> Self:
        """Trim whitespace from string values.

        Returns:
            Self: New Object with trimmed String values
        """
        return await self._apply_unary_transform("trim")

    async def abs(self) -> Self:
        """Absolute value of numeric values.

        Returns:
            Self: New Object with Float64 absolute values
        """
        return await self._apply_unary_transform("abs")

    async def log2(self) -> Self:
        """Log base 2 of numeric values.

        Returns:
            Self: New Object with Float64 log2 values
        """
        return await self._apply_unary_transform("log2")

    async def sqrt(self) -> Self:
        """Square root of numeric values.

        Returns:
            Self: New Object with Float64 sqrt values
        """
        return await self._apply_unary_transform("sqrt")

    # String/Regex Operators

    async def match(self, pattern: str) -> Self:
        """
        Test if string values match a RE2 regex pattern.

        Args:
            pattern: RE2 regex pattern string

        Returns:
            Self: New Object with UInt8 values (1 for match, 0 for no match)

        Examples:
            >>> obj = await create_object_from_value(["apple", "banana", "avocado"])
            >>> result = await obj.match("^a")
            >>> await result.data()  # [1, 0, 1]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.match_op(info, pattern, self.ch_client)

    async def like(self, pattern: str) -> Self:
        """
        Test if string values match a SQL LIKE pattern.

        Uses SQL LIKE syntax: % for any sequence, _ for single character.

        Args:
            pattern: SQL LIKE pattern string

        Returns:
            Self: New Object with UInt8 values (1 for match, 0 for no match)

        Examples:
            >>> obj = await create_object_from_value(["apple", "banana", "avocado"])
            >>> result = await obj.like("a%")
            >>> await result.data()  # [1, 0, 1]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.like_op(info, pattern, self.ch_client)

    async def ilike(self, pattern: str) -> Self:
        """
        Test if string values match a SQL LIKE pattern (case-insensitive).

        Args:
            pattern: SQL LIKE pattern string (case-insensitive)

        Returns:
            Self: New Object with UInt8 values (1 for match, 0 for no match)

        Examples:
            >>> obj = await create_object_from_value(["Apple", "BANANA", "avocado"])
            >>> result = await obj.ilike("a%")
            >>> await result.data()  # [1, 0, 1]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.ilike_op(info, pattern, self.ch_client)

    async def extract(self, pattern: str) -> Self:
        """
        Extract the first regex capture group match from string values.

        Args:
            pattern: RE2 regex pattern with a capture group

        Returns:
            Self: New Object with String values (extracted matches, empty string if no match)

        Examples:
            >>> obj = await create_object_from_value(["user_123", "user_456", "admin_789"])
            >>> result = await obj.extract("_(\\\\d+)")
            >>> await result.data()  # ["123", "456", "789"]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.extract_op(info, pattern, self.ch_client)

    async def replace(self, pattern: str, replacement: str) -> Self:
        """
        Replace all regex matches in string values.

        Uses replaceRegexpAll for replacing all occurrences (like Python re.sub).

        Args:
            pattern: RE2 regex pattern to match
            replacement: Replacement string (supports \\\\1, \\\\2 backreferences)

        Returns:
            Self: New Object with String values (after replacement)

        Examples:
            >>> obj = await create_object_from_value(["hello world", "foo bar"])
            >>> result = await obj.replace(" ", "_")
            >>> await result.data()  # ["hello_world", "foo_bar"]
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.replace_op(info, pattern, replacement, self.ch_client)

    async def is_null(self) -> Self:
        """Check which values are NULL.

        Returns:
            Self: New Object with UInt8 values (1 where NULL, 0 otherwise)
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.is_null_op(info, self.ch_client)

    async def is_not_null(self) -> Self:
        """Check which values are not NULL.

        Returns:
            Self: New Object with UInt8 values (1 where not NULL, 0 otherwise)
        """
        self.checkstale()
        info = self._get_query_info()
        return await operators.is_not_null_op(info, self.ch_client)

    async def coalesce(self, other) -> Self:
        """Return first non-NULL value from self or other.

        In ClickHouse, NULL = NULL returns NULL (standard SQL semantics).
        Use coalesce to replace NULLs with a fallback value.

        Args:
            other: Fallback value — Object, View, or Python scalar

        Returns:
            Self: New Object with coalesced values
        """
        self.checkstale()
        other = await self._ensure_object(other)
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.coalesce_op(info_a, info_b, self.ch_client)

    # arrayMap Operator

    async def array_map(self, other: Union["Object", ValueScalarType], operator: str) -> Self:
        """
        Apply an element-wise operation using ClickHouse's arrayMap function.

        Uses ClickHouse's arrayMap function for element-wise operations.
        Raises an error when array sizes don't match.

        Args:
            other: Another Object or Python scalar to operate with
            operator: Operator symbol (e.g., '+', '-', '**', '==', '&')

        Returns:
            Self: New array Object with element-wise results

        Raises:
            DB::Exception: If both operands are arrays with different sizes

        Examples:
            >>> a = await create_object_from_value([1, 2, 3])
            >>> b = await create_object_from_value([10, 20, 30])
            >>> result = await a.array_map(b, '+')
            >>> await result.data()  # [11, 22, 33]
            >>>
            >>> # With scalar
            >>> result = await a.array_map(5, '*')
            >>> await result.data()  # [5, 10, 15]
            >>>
            >>> # Size mismatch raises error
            >>> c = await create_object_from_value([10, 20])
            >>> await a.array_map(c, '+')  # Raises DB::Exception
        """
        self.checkstale()
        other = await self._ensure_object(other)
        other.checkstale()
        info_a = self._get_query_info()
        info_b = other._get_query_info()
        return await operators.array_map_db(info_a, info_b, operator, self.ch_client)

    @staticmethod
    def _validate_expression(expression: str) -> None:
        """Validate a SQL expression for safety."""
        if ";" in expression:
            raise ValueError("Expression must not contain ';'")
        upper = expression.upper()
        if "SELECT" in upper.split():
            raise ValueError("Expression must not contain subqueries")

    def with_columns(self, columns: Dict[str, Computed]) -> "View":
        """Add computed columns to this Object, returning a View.

        Synchronous — no database call. The computed columns exist only
        in the View's SELECT list as ``expr AS name`` aliases.

        Args:
            columns: Mapping of column name to Computed(type, expression).

        Returns:
            View with original columns + computed expression aliases.

        Raises:
            ValueError: If columns is empty, name collides with existing,
                or Object is scalar.
        """
        self.checkstale()
        if not columns:
            raise ValueError("columns must be a non-empty dict")
        if self._schema.fieldtype == FIELDTYPE_SCALAR:
            raise ValueError("with_columns() cannot be used on scalar Objects")
        existing = set(self._schema.columns.keys())
        for name, computed in columns.items():
            if name in existing:
                raise ValueError(
                    f"Computed column '{name}' collides with existing column"
                )
            self._validate_expression(computed.expression)
        return View(self, computed_columns=columns)

    def rename(self, columns: Dict[str, str]) -> "View":
        """Rename columns, returning a View with aliased column names.

        Synchronous — no database call. The renamed columns exist only
        in the View's SELECT list as ``old_name AS new_name`` aliases.
        The original Object column names are mapped to new names in the
        View's schema, so insert() and other operations see the new names.

        Args:
            columns: Mapping of old_name -> new_name.

        Returns:
            View with renamed columns.

        Raises:
            ValueError: If columns is empty, old_name doesn't exist,
                or new_name collides with an existing column.
        """
        self.checkstale()
        if not columns:
            raise ValueError("columns must be a non-empty dict")
        if self._schema.fieldtype == FIELDTYPE_SCALAR:
            raise ValueError("rename() cannot be used on scalar Objects")
        existing = set(self._schema.columns.keys())
        renamed_away = set(columns.keys())
        effective = (existing - renamed_away) | set(columns.values())
        for old_name, new_name in columns.items():
            if old_name not in existing:
                raise ValueError(
                    f"Column '{old_name}' does not exist in schema"
                )
            if old_name == "aai_id":
                raise ValueError("Cannot rename 'aai_id' column")
        # Check for duplicate new names
        new_names = list(columns.values())
        if len(new_names) != len(set(new_names)):
            raise ValueError("Duplicate new column names in rename mapping")
        # Check collision: new names must not collide with non-renamed columns
        kept = existing - renamed_away - {"aai_id"}
        for new_name in new_names:
            if new_name in kept:
                raise ValueError(
                    f"Renamed column '{new_name}' collides with existing column"
                )
        return View(self, renamed_columns=columns)

    # -----------------------------------------------------------------
    # Domain helpers — each delegates to with_columns()
    # -----------------------------------------------------------------

    def with_year(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Extract year from a Date/DateTime column."""
        name = alias or f"{column}_year"
        return self.with_columns({name: Computed("UInt16", f"toYear({column})")})

    def with_month(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Extract month (1-12) from a Date/DateTime column."""
        name = alias or f"{column}_month"
        return self.with_columns({name: Computed("UInt8", f"toMonth({column})")})

    def with_day_of_week(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Extract day of week (1=Mon, 7=Sun) from a Date/DateTime column."""
        name = alias or f"{column}_dow"
        return self.with_columns({name: Computed("UInt8", f"toDayOfWeek({column})")})

    def with_date_diff(
        self,
        unit: str,
        col_a: str,
        col_b: str,
        *,
        alias: Optional[str] = None,
    ) -> "View":
        """Compute date difference between two columns.

        Args:
            unit: Time unit ('day', 'hour', 'minute', 'second', 'month', 'year')
            col_a: Start date column
            col_b: End date column
            alias: Result column name (default: '{col_a}_{col_b}_diff')
        """
        name = alias or f"{col_a}_{col_b}_diff"
        return self.with_columns(
            {name: Computed("Int64", f"dateDiff('{unit}', {col_a}, {col_b})")}
        )

    def with_lower(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Lowercase a String column."""
        name = alias or f"{column}_lower"
        return self.with_columns({name: Computed("String", f"lower({column})")})

    def with_upper(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Uppercase a String column."""
        name = alias or f"{column}_upper"
        return self.with_columns({name: Computed("String", f"upper({column})")})

    def with_length(self, column: str, *, alias: Optional[str] = None) -> "View":
        """String length of a column."""
        name = alias or f"{column}_length"
        return self.with_columns({name: Computed("UInt64", f"length({column})")})

    def with_trim(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Trim whitespace from a String column."""
        name = alias or f"{column}_trimmed"
        return self.with_columns({name: Computed("String", f"trim({column})")})

    def with_abs(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Absolute value of a numeric column. Result type is Float64."""
        name = alias or f"{column}_abs"
        return self.with_columns({name: Computed("Float64", f"abs({column})")})

    def with_log2(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Log base 2 of a numeric column."""
        name = alias or f"{column}_log2"
        return self.with_columns({name: Computed("Float64", f"log2({column})")})

    def with_sqrt(self, column: str, *, alias: Optional[str] = None) -> "View":
        """Square root of a numeric column."""
        name = alias or f"{column}_sqrt"
        return self.with_columns({name: Computed("Float64", f"sqrt({column})")})

    def with_bucket(
        self, column: str, size: int, *, alias: Optional[str] = None
    ) -> "View":
        """Integer division bucketing: intDiv(column, size)."""
        name = alias or f"{column}_bucket"
        return self.with_columns(
            {name: Computed("Int64", f"intDiv({column}, {size})")}
        )

    def with_hash_bucket(
        self, column: str, n_buckets: int, *, alias: Optional[str] = None
    ) -> "View":
        """Hash bucketing: cityHash64(column) % n_buckets."""
        name = alias or f"{column}_hash"
        return self.with_columns(
            {name: Computed("UInt64", f"cityHash64({column}) % {n_buckets}")}
        )

    def with_if(
        self,
        condition: str,
        then_value: str,
        else_value: str,
        *,
        alias: str,
        type: str = "String",
    ) -> "View":
        """Conditional column: if(condition, then, else).

        Args:
            condition: SQL boolean expression
            then_value: Value when true (SQL literal or column)
            else_value: Value when false (SQL literal or column)
            alias: Required — result column name
            type: ClickHouse result type (default 'String')
        """
        return self.with_columns(
            {alias: Computed(type, f"if({condition}, {then_value}, {else_value})")}
        )

    def with_cast(
        self, column: str, ch_type: str, *, alias: Optional[str] = None
    ) -> "View":
        """Cast a column to a different ClickHouse type.

        Args:
            column: Source column name
            ch_type: Target ClickHouse type (e.g., 'Float64', 'String', 'Date')
            alias: Result column name (default: '{column}_{type_lower}')
        """
        name = alias or f"{column}_{ch_type.lower()}"
        func = f"to{ch_type}" if not ch_type.startswith("to") else ch_type
        return self.with_columns({name: Computed(ch_type, f"{func}({column})")})

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
        return f"Object(table='{self.table}')"


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
        elif isinstance(source, View) and source.selected_fields:
            # Multi-field View projects to selected fields only
            available = set(source.selected_fields)
        else:
            available = set(schema.columns.keys()) - {"aai_id"}
        # Include computed columns
        if source.computed_columns:
            available |= set(source.computed_columns.keys())

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
                field = source.selected_fields[0]
                col_def = schema.columns.get(field, ColumnInfo("Float64"))
                columns = {"aai_id": "UInt64", "value": col_def.type}
                source_query = f"({source._build_select()})"
            elif source.selected_fields:
                # Multi-field View: only selected columns available
                columns = {"aai_id": "UInt64"}
                for field in source.selected_fields:
                    col_def = schema.columns.get(field, ColumnInfo("Float64"))
                    columns[field] = col_def.type
                if source.computed_columns:
                    for col_name, comp in source.computed_columns.items():
                        columns[col_name] = comp.type
                source_query = f"({source._build_select()})"
            elif source.has_constraints:
                # WHERE/LIMIT View: full columns, wrapped in subquery
                columns = {k: cd.type for k, cd in schema.columns.items()}
                if source.computed_columns:
                    for col_name, comp in source.computed_columns.items():
                        columns[col_name] = comp.type
                source_query = f"({source._build_select()})"
            else:
                # Base View (no constraints): same as plain Object
                columns = {k: cd.type for k, cd in schema.columns.items()}
                source_query = source.table
        else:
            # Plain Object
            columns = {k: cd.type for k, cd in schema.columns.items()}
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

    async def any(self, column: str) -> Object:
        """Convenience: any (pick arbitrary non-NULL) per group. Delegates to agg()."""
        return await self.agg({column: GB_ANY})

    async def group_array_distinct(self, column: str) -> Object:
        """Convenience: collect distinct values into an array per group. Delegates to agg()."""
        return await self.agg({column: GB_GROUP_ARRAY_DISTINCT})

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
        computed_columns: Optional[Dict[str, Computed]] = None,
        renamed_columns: Optional[Dict[str, str]] = None,
        where_connector: str = "AND",
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
            computed_columns: Optional dict of computed column definitions
            renamed_columns: Optional dict mapping old_name -> new_name
            where_connector: Connector for the where clause ("AND" or "OR")
        """
        super().__init__(table=source.table, schema=source._schema)

        # Inherit existing constraints when source is already a View
        is_view = isinstance(source, View)
        self._where_clauses: List[Tuple[str, str]] = (
            list(source._where_clauses) if is_view else []
        )
        if where:
            self._where_clauses.append((where.strip(), where_connector))
        self._limit = limit if limit is not None else (source._limit if is_view else None)
        self._offset = offset if offset is not None else (source._offset if is_view else None)
        self._order_by = order_by if order_by is not None else (source._order_by if is_view else None)
        self._selected_fields = selected_fields if selected_fields is not None else (
            source._selected_fields if is_view else None
        )
        self._computed_columns: Optional[Dict[str, Computed]] = computed_columns if computed_columns is not None else (
            source._computed_columns if is_view else None
        )
        self._renamed_columns: Optional[Dict[str, str]] = renamed_columns if renamed_columns is not None else (
            source._renamed_columns if is_view else None
        )

        # Register with context for lifecycle tracking and stale marking
        if source._registered:
            self._register()
            register_object(self)

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
    def where_clauses(self) -> List[Tuple[str, str]]:
        """Get WHERE clauses."""
        return self._where_clauses

    @property
    def selected_fields(self) -> Optional[List[str]]:
        """Get selected field names."""
        return self._selected_fields

    @property
    def computed_columns(self) -> Optional[Dict[str, Computed]]:
        """Get computed columns."""
        return self._computed_columns

    @property
    def renamed_columns(self) -> Optional[Dict[str, str]]:
        """Get renamed columns mapping old->new."""
        return self._renamed_columns

    @property
    def _effective_columns(self) -> Dict[str, ColumnInfo]:
        """Column schema with renames, field selection, and computed columns applied.

        Returns the effective column names and types as seen by consumers
        (data(), insert(), concat). Accounts for:
        - Field selection (narrows to selected fields)
        - Renamed columns (old_name -> new_name)
        - Computed columns (added as new columns)

        Always includes aai_id.
        """
        orig = self._schema.columns
        renames = self._renamed_columns or {}

        if self._selected_fields and self.is_single_field:
            field = self._selected_fields[0]
            col_def = orig.get(field, ColumnInfo("Float64"))
            columns = {"aai_id": ColumnInfo("UInt64"), "value": col_def}
        elif self._selected_fields:
            columns = {"aai_id": ColumnInfo("UInt64")}
            for f in self._selected_fields:
                columns[f] = orig[f]
        else:
            columns = {
                renames.get(name, name): info
                for name, info in orig.items()
            }

        if self._computed_columns:
            for name, comp in self._computed_columns.items():
                columns[name] = parse_ch_type(comp.type)

        return columns

    def _serialize_ref(self) -> dict:
        """Serialize this View to a reference dict for task kwargs/results."""
        ref = {
            "object_type": "view",
            "table": self.table,
            "where": self._build_where(),
            "limit": self.limit,
            "offset": self.offset,
            "order_by": self.order_by,
            "selected_fields": self.selected_fields,
            "renamed_columns": self._renamed_columns,
        }
        if self.persistent:
            ref["persistent"] = True
        return ref

    def where(self, condition: str) -> "View":
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
        return View(self, where=condition.strip())

    def or_where(self, condition: str) -> "View":
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
        if not self.where_clauses:
            raise ValueError("or_where() requires a prior where condition")
        return View(self, where=condition.strip(), where_connector="OR")

    def with_columns(self, columns: Dict[str, Computed]) -> "View":
        """Add computed columns to this View, returning a new View.

        Additive — merges with any existing computed columns.
        Preserves all existing constraints (WHERE, LIMIT, etc.).
        """
        self.checkstale()
        if not columns:
            raise ValueError("columns must be a non-empty dict")

        # Check collision with effective column names (renames + computed applied)
        existing = set(self._effective_columns.keys())
        for name, computed in columns.items():
            if name in existing:
                raise ValueError(
                    f"Computed column '{name}' collides with existing column"
                )
            self._validate_expression(computed.expression)

        merged = dict(self.computed_columns) if self.computed_columns else {}
        merged.update(columns)
        return View(self, computed_columns=merged)

    def _build_select(self, columns: str = "*", default_order_by: Optional[str] = None) -> str:
        """
        Build a SELECT query with view constraints applied.

        For single-field selection, renames the field as 'value' for array compatibility.
        For multi-field selection, selects all specified fields.
        If columns="value" is requested, only the value column is selected.
        Computed columns are appended as ``expr AS name`` aliases.
        Renamed columns are emitted as ``old_name AS new_name`` aliases.

        Args:
            columns: Column specification (default "*", respected for field selection views)
            default_order_by: Default ORDER BY clause if view doesn't have custom order_by

        Returns:
            str: SELECT query string with WHERE/LIMIT/OFFSET/ORDER BY applied
        """
        if self.selected_fields:
            if self.is_single_field:
                # Single field: rename as 'value' for array compatibility
                field = quote_identifier(self.selected_fields[0])
                if columns == "value":
                    select_cols = f"{field} AS value"
                else:
                    select_cols = f"aai_id, {field} AS value"
            else:
                # Multiple fields: select all specified fields
                fields_str = ", ".join(quote_identifier(f) for f in self.selected_fields)
                select_cols = f"aai_id, {fields_str}"
        elif self._renamed_columns and columns == "*":
            # Expand * into explicit columns with renames applied
            renames = self._renamed_columns
            col_parts = []
            for col_name in self._schema.columns:
                qname = quote_identifier(col_name)
                if col_name in renames:
                    col_parts.append(f"{qname} AS {quote_identifier(renames[col_name])}")
                else:
                    col_parts.append(qname)
            select_cols = ", ".join(col_parts)
        else:
            select_cols = columns

        # Append computed column expressions
        if self.computed_columns:
            computed_parts = [
                f"{comp.expression} AS {quote_identifier(name)}"
                for name, comp in self.computed_columns.items()
            ]
            select_cols += ", " + ", ".join(computed_parts)

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
            selected_fields=self.selected_fields,
            is_single_field=self.is_single_field,
        )

    async def data(self, orient: str = ORIENT_DICT):
        """
        Get the data from the view.

        For single-field selection, returns array data (the selected column).
        For multi-field selection, returns dict data (subset of columns).
        For computed columns, returns dict data with computed values included.

        Args:
            orient: Output format for dict data

        Returns:
            - For single-field views: returns list of values (array)
            - For multi-field views: returns dict with selected columns
            - For computed column views: returns dict with real + computed columns
            - Otherwise: delegates to parent Object.data()
        """
        self.checkstale()

        if self.selected_fields:
            if self.is_single_field:
                # Single field: return as array
                return await data_extraction.extract_array_data(self)
            else:
                # Multiple fields: return as dict with only selected fields
                columns: Dict[str, ColumnMeta] = {}
                for field in self.selected_fields:
                    columns[field] = ColumnMeta(fieldtype=FIELDTYPE_ARRAY)

                column_names = ["aai_id"] + list(self.selected_fields)
                return await data_extraction.extract_dict_data(self, column_names, columns, orient)

        if self.computed_columns or self._renamed_columns:
            eff = self._effective_columns
            columns: Dict[str, ColumnMeta] = {
                name: ColumnMeta(fieldtype=FIELDTYPE_ARRAY)
                for name in eff if name != "aai_id"
            }
            column_names = list(eff.keys())
            return await data_extraction.extract_dict_data(self, column_names, columns, orient)

        # Delegate to parent for normal views
        return await super().data(orient=orient)

    @property
    def schema(self) -> ViewSchema:
        """Get schema for this view including view constraints."""
        return ViewSchema(
            fieldtype=self._schema.fieldtype,
            columns=self._schema.columns,
            table=self._schema.table,
            where=self._build_where(),
            limit=self._limit,
            offset=self._offset,
            order_by=self._order_by,
            selected_fields=self.selected_fields,
            computed_columns=self.computed_columns,
        )

    def _get_ingest_query_info(self) -> IngestQueryInfo:
        """Build effective column schema for insert/concat validation.

        Delegates to effective_columns property for column resolution.
        """
        info = self._get_query_info()
        return IngestQueryInfo(**vars(info), columns=self._effective_columns)

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
