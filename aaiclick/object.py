"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from __future__ import annotations

from typing import Optional, Dict, List, Tuple, Any, Union
from dataclasses import dataclass

from .snowflake import get_snowflake_id
from .sql_template_loader import load_sql_template
from .models import (
    Schema,
    ColumnMeta,
    ColumnType,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
)


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

    def __init__(self, ctx: Context, table: Optional[str] = None):
        """
        Initialize an Object.

        Args:
            ctx: Context instance managing this object
            table: Optional table name. If not provided, generates unique table name
                  using Snowflake ID prefixed with 't' for ClickHouse compatibility
        """
        self._ctx = ctx
        self._table_name = table if table is not None else f"t{get_snowflake_id()}"

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    @property
    def ctx(self) -> Context:
        """Get the context managing this object."""
        self.checkstale()
        return self._ctx

    @property
    def ch_client(self):
        """Get the ClickHouse client from the context."""
        self.checkstale()
        return self.ctx.ch_client

    @property
    def stale(self) -> bool:
        """Check if this object's context has been cleaned up."""
        return self._ctx is None

    def checkstale(self):
        """
        Check if object is stale and raise error if so.

        Raises:
            RuntimeError: If object context is None (stale)
        """
        if self._ctx is None:
            raise RuntimeError(
                f"Cannot use stale Object. Table '{self._table_name}' has been deleted."
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
        from . import operators

        return await operators._apply_operator_db(
            self.table, obj_b.table, operator, self.ch_client, self.ctx
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
        from . import operators

        self.checkstale()
        return await operators.add(self, other)

    async def __sub__(self, other: Object) -> Object:
        """
        Subtract one object from another.

        Creates a new Object with a table containing the result of element-wise subtraction.

        Args:
            other: Another Object to subtract

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.sub(self, other)

    async def __mul__(self, other: Object) -> Object:
        """
        Multiply two objects together.

        Creates a new Object with a table containing the result of element-wise multiplication.

        Args:
            other: Another Object to multiply

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.mul(self, other)

    async def __truediv__(self, other: Object) -> Object:
        """
        Divide one object by another.

        Creates a new Object with a table containing the result of element-wise division.

        Args:
            other: Another Object to divide by

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.truediv(self, other)

    async def __floordiv__(self, other: Object) -> Object:
        """
        Floor divide one object by another.

        Creates a new Object with a table containing the result of element-wise floor division.

        Args:
            other: Another Object to floor divide by

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.floordiv(self, other)

    async def __mod__(self, other: Object) -> Object:
        """
        Modulo operation between two objects.

        Creates a new Object with a table containing the result of element-wise modulo.

        Args:
            other: Another Object to modulo with

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.mod(self, other)

    async def __pow__(self, other: Object) -> Object:
        """
        Raise one object to the power of another.

        Creates a new Object with a table containing the result of element-wise power operation.

        Args:
            other: Another Object representing the exponent

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.pow(self, other)

    async def __eq__(self, other: Object) -> Object:
        """
        Check equality between two objects.

        Creates a new Object with a table containing the result of element-wise equality comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.eq(self, other)

    async def __ne__(self, other: Object) -> Object:
        """
        Check inequality between two objects.

        Creates a new Object with a table containing the result of element-wise inequality comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.ne(self, other)

    async def __lt__(self, other: Object) -> Object:
        """
        Check if one object is less than another.

        Creates a new Object with a table containing the result of element-wise less than comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.lt(self, other)

    async def __le__(self, other: Object) -> Object:
        """
        Check if one object is less than or equal to another.

        Creates a new Object with a table containing the result of element-wise less than or equal comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.le(self, other)

    async def __gt__(self, other: Object) -> Object:
        """
        Check if one object is greater than another.

        Creates a new Object with a table containing the result of element-wise greater than comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.gt(self, other)

    async def __ge__(self, other: Object) -> Object:
        """
        Check if one object is greater than or equal to another.

        Creates a new Object with a table containing the result of element-wise greater than or equal comparison.

        Args:
            other: Another Object to compare with

        Returns:
            Object: New Object instance pointing to result table (boolean values)
        """
        from . import operators

        self.checkstale()
        return await operators.ge(self, other)

    async def __and__(self, other: Object) -> Object:
        """
        Bitwise AND operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise AND.

        Args:
            other: Another Object to AND with

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.and_(self, other)

    async def __or__(self, other: Object) -> Object:
        """
        Bitwise OR operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise OR.

        Args:
            other: Another Object to OR with

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.or_(self, other)

    async def __xor__(self, other: Object) -> Object:
        """
        Bitwise XOR operation between two objects.

        Creates a new Object with a table containing the result of element-wise bitwise XOR.

        Args:
            other: Another Object to XOR with

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        self.checkstale()
        return await operators.xor(self, other)

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
        from . import ingest
        return await ingest.copy(self)

    async def concat(self, other: Union["Object", "ValueType"]) -> "Object":
        """
        Concatenate another object or value to this object.

        Creates a new Object with rows from self followed by rows/values from other.
        Self must have array fieldtype. Other can be:
        - An Object (array or scalar)
        - A ValueType (scalar or list)

        When other is a ValueType, the function first copies self, then inserts the value(s).

        Args:
            other: Another Object or value to concatenate

        Returns:
            Object: New Object instance with concatenated data

        Raises:
            ValueError: If self does not have array fieldtype

        Examples:
            >>> # Concatenate with another Object
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_b = await ctx.create_object_from_value([4, 5, 6])
            >>> result = await obj_a.concat(obj_b)
            >>> await result.data()  # Returns [1, 2, 3, 4, 5, 6]
            >>>
            >>> # Concatenate with a scalar value
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> result = await obj_a.concat(42)
            >>> await result.data()  # Returns [1, 2, 3, 42]
            >>>
            >>> # Concatenate with a list of values
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> result = await obj_a.concat([4, 5])
            >>> await result.data()  # Returns [1, 2, 3, 4, 5]
        """
        from . import ingest
        return await ingest.concat(self, other)

    async def insert(self, other: Union["Object", "ValueType"]) -> None:
        """
        Insert another object or value into this object in place.

        Modifies self's table directly by appending rows/values from other.
        Self must have array fieldtype. Other can be:
        - An Object (array or scalar)
        - A ValueType (scalar or list)

        Unlike concat, this method modifies the table in place without creating a new object.

        Args:
            other: Another Object or value to insert

        Raises:
            ValueError: If self does not have array fieldtype

        Examples:
            >>> # Insert another Object
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> obj_b = await ctx.create_object_from_value([4, 5, 6])
            >>> await obj_a.insert(obj_b)
            >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5, 6]
            >>>
            >>> # Insert a scalar value
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> await obj_a.insert(42)
            >>> await obj_a.data()  # Returns [1, 2, 3, 42]
            >>>
            >>> # Insert a list of values
            >>> obj_a = await ctx.create_object_from_value([1, 2, 3])
            >>> await obj_a.insert([4, 5])
            >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5]
        """
        from . import ingest
        await ingest.insert(self, other)

    async def min(self) -> float:
        """
        Calculate the minimum value from the object's table.

        Returns:
            float: Minimum value from the 'value' column
        """
        self.checkstale()
        result = await self.ch_client.query(f"SELECT min(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def max(self) -> float:
        """
        Calculate the maximum value from the object's table.

        Returns:
            float: Maximum value from the 'value' column
        """
        self.checkstale()
        result = await self.ch_client.query(f"SELECT max(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def sum(self) -> float:
        """
        Calculate the sum of values from the object's table.

        Returns:
            float: Sum of values from the 'value' column
        """
        self.checkstale()
        result = await self.ch_client.query(f"SELECT sum(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def mean(self) -> float:
        """
        Calculate the mean (average) value from the object's table.

        Returns:
            float: Mean value from the 'value' column
        """
        self.checkstale()
        result = await self.ch_client.query(f"SELECT avg(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def std(self) -> float:
        """
        Calculate the standard deviation of values from the object's table.

        Returns:
            float: Standard deviation from the 'value' column
        """
        self.checkstale()
        result = await self.ch_client.query(f"SELECT stddevPop(value) FROM {self.table}")
        return result.result_rows[0][0]

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"
