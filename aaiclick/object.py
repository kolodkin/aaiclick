"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
import yaml
from .client import get_client
from .snowflake import get_snowflake_id
from .sql_template_loader import load_sql_template
from . import operators


# Fieldtype constants
FIELDTYPE_SCALAR = "s"
FIELDTYPE_ARRAY = "a"
FIELDTYPE_DICT = "d"

# Orient constants for data() method
ORIENT_DICT = "dict"
ORIENT_RECORDS = "records"


@dataclass
class ColumnMeta:
    """
    Metadata for a column parsed from YAML comment.

    Attributes:
        fieldtype: 's' for scalar, 'a' for array
    """

    fieldtype: Optional[str] = None

    def to_yaml(self) -> str:
        """
        Convert metadata to single-line YAML format for column comment.

        Returns:
            str: YAML string like "{fieldtype: a}"
        """
        if self.fieldtype is None:
            return ""

        return yaml.dump({"fieldtype": self.fieldtype}, default_flow_style=True).strip()

    @classmethod
    def from_yaml(cls, comment: str) -> "ColumnMeta":
        """
        Parse YAML from column comment string.

        Args:
            comment: Column comment string containing YAML

        Returns:
            ColumnMeta: Parsed metadata
        """
        if not comment or not comment.strip():
            return cls()

        try:
            data = yaml.safe_load(comment)
            if not isinstance(data, dict):
                return cls()

            return cls(fieldtype=data.get("fieldtype"))
        except yaml.YAMLError:
            return cls()


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

    def __init__(self, table: Optional[str] = None):
        """
        Initialize an Object.

        Args:
            table: Optional table name. If not provided, generates unique table name
                  using Snowflake ID prefixed with 't' for ClickHouse compatibility
        """
        self._table_name = table if table is not None else f"t{get_snowflake_id()}"

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    async def result(self):
        """
        Query and return all data from the object's table.

        Returns:
            Query result with all rows from the table
        """
        client = await get_client()
        return await client.query(f"SELECT * FROM {self.table}")

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
        client = await get_client()

        # Query column names and comments
        columns_query = f"""
        SELECT name, comment
        FROM system.columns
        WHERE table = '{self.table}'
        ORDER BY position
        """
        columns_result = await client.query(columns_query)

        # Parse YAML from comments and get column names
        columns: Dict[str, ColumnMeta] = {}
        column_names: List[str] = []
        for name, comment in columns_result.result_rows:
            columns[name] = ColumnMeta.from_yaml(comment)
            column_names.append(name)

        # Determine data type based on columns
        has_aai_id = "aai_id" in columns

        # Query data (order by aai_id for arrays)
        if has_aai_id:
            data_result = await client.query(f"SELECT * FROM {self.table} ORDER BY aai_id")
        else:
            data_result = await self.result()
        rows = data_result.result_rows

        is_simple_structure = set(column_names) <= {"aai_id", "value"}

        if not is_simple_structure:
            # Dict type (scalar or arrays)
            # Filter out aai_id from output
            output_columns = [name for name in column_names if name != "aai_id"]
            col_indices = {name: column_names.index(name) for name in output_columns}

            # Check if this is dict of arrays by looking at fieldtype
            first_col = output_columns[0] if output_columns else None
            is_dict_of_arrays = first_col and columns.get(first_col, ColumnMeta()).fieldtype == FIELDTYPE_ARRAY

            if orient == ORIENT_RECORDS:
                # Return list of dicts (one per row)
                return [{name: row[col_indices[name]] for name in output_columns} for row in rows]
            else:
                # ORIENT_DICT
                if is_dict_of_arrays:
                    # Dict of arrays: return dict with arrays as values
                    return {name: [row[col_indices[name]] for row in rows] for name in output_columns}
                elif rows:
                    # Dict of scalars: return single dict (first row)
                    return {name: rows[0][col_indices[name]] for name in output_columns}
                return {}

        value_meta = columns.get("value")
        if value_meta and value_meta.fieldtype == FIELDTYPE_SCALAR:
            # Scalar: return single value
            return rows[0][0] if rows else None
        else:
            # Array: return list of values
            if has_aai_id:
                return [row[1] for row in rows]
            else:
                return [row[0] for row in rows]

    async def _has_aai_id(self) -> bool:
        """Check if this object's table has a aai_id column."""
        client = await get_client()
        columns_query = f"""
        SELECT name FROM system.columns
        WHERE table = '{self.table}' AND name = 'aai_id'
        """
        result = await client.query(columns_query)
        return len(result.result_rows) > 0

    async def _get_fieldtype(self) -> Optional[str]:
        """Get the fieldtype of the value column."""
        client = await get_client()
        columns_query = f"""
        SELECT comment FROM system.columns
        WHERE table = '{self.table}' AND name = 'value'
        """
        result = await client.query(columns_query)
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
        # Get SQL expression from operator mapping
        expression = operators.OPERATOR_EXPRESSIONS[operator]

        result = Object()
        client = await get_client()

        # Check if operating on scalars or arrays
        has_aai_id = await self._has_aai_id()
        fieldtype = await self._get_fieldtype()
        comment = ColumnMeta(fieldtype=fieldtype).to_yaml()

        if has_aai_id:
            # Array operation with aai_id - use array template
            template = load_sql_template("apply_op_array")
            create_query = template.format(
                result_table=result.table,
                expression=expression,
                left_table=self.table,
                right_table=obj_b.table
            )
            await client.command(create_query)

            # Add comments
            aai_id_comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN aai_id '{aai_id_comment}'")
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")
        else:
            # Scalar operation - use scalar template
            template = load_sql_template("apply_op_scalar")
            create_query = template.format(
                result_table=result.table,
                expression=expression,
                left_table=self.table,
                right_table=obj_b.table
            )
            await client.command(create_query)
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")

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
        return await operators.xor(self, other)

    async def delete_table(self) -> None:
        """
        Delete the ClickHouse table associated with this object.
        """
        client = await get_client()
        await client.command(f"DROP TABLE IF EXISTS {self.table}")

    async def min(self) -> float:
        """
        Calculate the minimum value from the object's table.

        Returns:
            float: Minimum value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT min(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def max(self) -> float:
        """
        Calculate the maximum value from the object's table.

        Returns:
            float: Maximum value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT max(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def sum(self) -> float:
        """
        Calculate the sum of values from the object's table.

        Returns:
            float: Sum of values from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT sum(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def mean(self) -> float:
        """
        Calculate the mean (average) value from the object's table.

        Returns:
            float: Mean value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT avg(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def std(self) -> float:
        """
        Calculate the standard deviation of values from the object's table.

        Returns:
            float: Standard deviation from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT stddevPop(value) FROM {self.table}")
        return result.result_rows[0][0]

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"
