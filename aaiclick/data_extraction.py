"""
aaiclick.data_extraction - Functions for extracting data from Object tables.

This module provides specialized extraction functions for different table types.
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from .object import Object

from .object import ColumnMeta, FIELDTYPE_ARRAY, ORIENT_RECORDS
from .client import get_client


async def extract_scalar_data(obj: "Object") -> Any:
    """
    Extract data from a scalar table (single row with aai_id and value).

    Args:
        obj: Object instance with scalar data

    Returns:
        Single scalar value or None if empty
    """
    ch_client = await get_client()
    data_result = await ch_client.query(f"SELECT value FROM {obj.table} ORDER BY aai_id")
    rows = data_result.result_rows
    return rows[0][0] if rows else None


async def extract_array_data(obj: "Object") -> List[Any]:
    """
    Extract data from an array table (multiple rows with aai_id and value).

    Args:
        obj: Object instance with array data

    Returns:
        List of values ordered by aai_id
    """
    ch_client = await get_client()
    data_result = await ch_client.query(f"SELECT value FROM {obj.table} ORDER BY aai_id")
    rows = data_result.result_rows
    return [row[0] for row in rows]


async def extract_dict_data(
    obj: "Object",
    column_names: List[str],
    columns: Dict[str, ColumnMeta],
    orient: str
):
    """
    Extract data from a dict table (multiple columns with aai_id).

    Args:
        obj: Object instance with dict data
        column_names: List of column names in order
        columns: Dict mapping column names to metadata
        orient: Output format (ORIENT_DICT or ORIENT_RECORDS)

    Returns:
        Dict or list of dicts based on orient parameter
    """
    ch_client = await get_client()
    data_result = await ch_client.query(f"SELECT * FROM {obj.table} ORDER BY aai_id")
    rows = data_result.result_rows

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
