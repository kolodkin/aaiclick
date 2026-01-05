"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from typing import Optional, Dict, List, Tuple, Any, Union, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .context import Context
    from .factories import ValueType

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

    # Methods that access the database and should check stale status
    _DB_METHODS = frozenset({
        'result', 'data', 'copy', 'concat', 'min', 'max', 'sum', 'mean', 'std',
        '_apply_operator', '_get_fieldtype',
        '__add__', '__sub__', '__mul__', '__truediv__', '__floordiv__',
        '__mod__', '__pow__', '__eq__', '__ne__', '__lt__', '__le__',
        '__gt__', '__ge__', '__and__', '__or__', '__xor__'
    })

    def __getattribute__(self, name):
        """
        Override attribute access to check stale status before database operations.

        Raises:
            RuntimeError: If attempting to call a database method on a stale object
        """
        # Get the attribute using object's __getattribute__ to avoid recursion
        attr = object.__getattribute__(self, name)

        # Check if this is a DB method and if object is stale (context is None)
        if name in object.__getattribute__(self, '_DB_METHODS'):
            if object.__getattribute__(self, '_ctx') is None:
                table_name = object.__getattribute__(self, '_table_name')
                raise RuntimeError(
                    f"Cannot use stale Object. Table '{table_name}' has been deleted."
                )

        return attr

    def __init__(self, ctx: "Context", table: Optional[str] = None):
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
        return await self._ctx.ch_client.query(f"SELECT * FROM {self.table}")

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
        from . import data_extraction

        # Query column names and comments
        columns_query = f"""
        SELECT name, comment
        FROM system.columns
        WHERE table = '{self.table}'
        ORDER BY position
        """
        columns_result = await self._ctx.ch_client.query(columns_query)

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
        columns_query = f"""
        SELECT comment FROM system.columns
        WHERE table = '{self.table}' AND name = 'value'
        """
        result = await self._ctx.ch_client.query(columns_query)
        if result.result_rows:
            meta = ColumnMeta.from_yaml(result.result_rows[0][0])
            return meta.fieldtype
        return None

    async def _apply_operator(self, obj_b: "Object", operator: str) -> "Object":
        """
        Apply an operator on two objects using SQL templates.

        Args:
            obj_b: Another Object to operate with
            operator: Operator symbol (e.g., '+', '-', '**', '==', '&')

        Returns:
            Object: New Object instance pointing to result table
        """
        from . import operators

        # Get SQL expression from operator mapping
        expression = operators.OPERATOR_EXPRESSIONS[operator]

        # Check if operating on scalars or arrays
        fieldtype = await self._get_fieldtype()

        # Get value column types from both operands
        type_query_a = f"""
        SELECT type FROM system.columns
        WHERE table = '{self.table}' AND name = 'value'
        """
        type_result_a = await self._ctx.ch_client.query(type_query_a)
        type_a = type_result_a.result_rows[0][0] if type_result_a.result_rows else "Float64"

        type_query_b = f"""
        SELECT type FROM system.columns
        WHERE table = '{obj_b.table}' AND name = 'value'
        """
        type_result_b = await self._ctx.ch_client.query(type_query_b)
        type_b = type_result_b.result_rows[0][0] if type_result_b.result_rows else "Float64"

        # Determine result type: promote to Float64 if mixing integer and float types
        int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
        float_types = {"Float32", "Float64"}

        if (type_a in int_types and type_b in float_types) or (type_a in float_types and type_b in int_types):
            # Mixed types: promote to Float64
            value_type = "Float64"
        elif type_a in float_types or type_b in float_types:
            # At least one float: use Float64
            value_type = "Float64"
        else:
            # Both same category: use first operand's type
            value_type = type_a

        # Build schema for result table
        schema = Schema(
            fieldtype=fieldtype,
            columns={"aai_id": "UInt64", "value": value_type}
        )

        # Create result object with schema (not registered - caller manages lifecycle)
        result = await self._ctx.create_object(schema, register=False)

        # Insert data based on fieldtype
        if fieldtype == FIELDTYPE_ARRAY:
            # Array operation
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT a.rn as aai_id, {expression} AS value
            FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {self.table}) AS a
            INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {obj_b.table}) AS b
            ON a.rn = b.rn
            """
        else:
            # Scalar operation
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT 1 AS aai_id, {expression} AS value
            FROM {self.table} AS a, {obj_b.table} AS b
            """

        await self._ctx.ch_client.command(insert_query)

        return result

    async def __add__(self, other: "Object") -> "Object":
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

    async def __sub__(self, other: "Object") -> "Object":
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

    async def __mul__(self, other: "Object") -> "Object":
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

    async def __truediv__(self, other: "Object") -> "Object":
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

    async def __floordiv__(self, other: "Object") -> "Object":
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

    async def __mod__(self, other: "Object") -> "Object":
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

    async def __pow__(self, other: "Object") -> "Object":
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

    async def __eq__(self, other: "Object") -> "Object":
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

    async def __ne__(self, other: "Object") -> "Object":
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

    async def __lt__(self, other: "Object") -> "Object":
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

    async def __le__(self, other: "Object") -> "Object":
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

    async def __gt__(self, other: "Object") -> "Object":
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

    async def __ge__(self, other: "Object") -> "Object":
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

    async def __and__(self, other: "Object") -> "Object":
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

    async def __or__(self, other: "Object") -> "Object":
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

    async def __xor__(self, other: "Object") -> "Object":
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

    async def min(self) -> float:
        """
        Calculate the minimum value from the object's table.

        Returns:
            float: Minimum value from the 'value' column
        """
        result = await self._ctx.ch_client.query(f"SELECT min(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def max(self) -> float:
        """
        Calculate the maximum value from the object's table.

        Returns:
            float: Maximum value from the 'value' column
        """
        result = await self._ctx.ch_client.query(f"SELECT max(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def sum(self) -> float:
        """
        Calculate the sum of values from the object's table.

        Returns:
            float: Sum of values from the 'value' column
        """
        result = await self._ctx.ch_client.query(f"SELECT sum(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def mean(self) -> float:
        """
        Calculate the mean (average) value from the object's table.

        Returns:
            float: Mean value from the 'value' column
        """
        result = await self._ctx.ch_client.query(f"SELECT avg(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def std(self) -> float:
        """
        Calculate the standard deviation of values from the object's table.

        Returns:
            float: Standard deviation from the 'value' column
        """
        result = await self._ctx.ch_client.query(f"SELECT stddevPop(value) FROM {self.table}")
        return result.result_rows[0][0]

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"
