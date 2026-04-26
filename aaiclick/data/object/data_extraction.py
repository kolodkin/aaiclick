"""
aaiclick.data.data_extraction - Functions for extracting data from Object tables.

This module provides specialized extraction functions for different table types.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ..models import FIELDTYPE_ARRAY, ORIENT_RECORDS, ColumnInfo

if TYPE_CHECKING:
    from .object import Object


def _convert_value(value):
    """Convert ClickHouse result values to Python types.

    - Tuples (from Array columns) are recursively converted to lists.
    - Naive datetimes (from DateTime64 UTC columns) get UTC timezone attached.
    - Lists containing naive datetimes (from Array(DateTime64) columns)
      get UTC timezone attached to each element.
    """
    if isinstance(value, tuple):
        return [_convert_value(v) for v in value]
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    if isinstance(value, list) and value and isinstance(value[0], datetime):
        return [v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v for v in value]
    return value


def _has_nested_columns(column_names: list[str]) -> bool:
    """Check if any column names use dot-star notation for nested arrays."""
    return any(".*." in name for name in column_names)


def _unflatten_record(flat_record: dict) -> dict:
    """Reconstruct nested dict structure from flat dot-star notation columns.

    Converts parallel arrays back into list-of-dicts (reverse of flattening).

    Example:
        ``{"a": 2, "b.*.c": [[1,2,3], [4,5,6]], "b.*.d": [5, 10]}``
        becomes
        ``{"a": 2, "b": [{"c": [1,2,3], "d": 5}, {"c": [4,5,6], "d": 10}]}``
    """
    plain: dict = {}
    nested_groups: dict[str, dict[str, Any]] = {}

    for col, val in flat_record.items():
        # Split on first ".*." occurrence
        star_pos = col.find(".*.")
        if star_pos == -1:
            plain[col] = val
        else:
            prefix = col[:star_pos]
            suffix = col[star_pos + 3 :]
            if prefix not in nested_groups:
                nested_groups[prefix] = {}
            nested_groups[prefix][suffix] = val

    result = dict(plain)

    for prefix, sub_fields in nested_groups.items():
        first_val = next(iter(sub_fields.values()))
        length = len(first_val) if isinstance(first_val, (list, tuple)) else 1

        items = []
        for i in range(length):
            item = {}
            for suffix, values in sub_fields.items():
                item[suffix] = values[i] if isinstance(values, (list, tuple)) else values
            items.append(item)

        # Recursively unflatten if deeper nesting exists
        result[prefix] = [_unflatten_record(item) for item in items]

    return result


async def extract_scalar_data(obj: Object) -> Any:
    """
    Extract data from a scalar table (single 'value' row).

    Args:
        obj: Object instance with scalar data

    Returns:
        Single scalar value or None if empty
    """
    query = obj._build_select(columns="value", default_order_by=None)
    data_result = await obj.ch_client.query(query)
    rows = data_result.result_rows
    return _convert_value(rows[0][0]) if rows else None


async def extract_array_data(obj: Object, **build_select_kwargs: Any) -> list[Any]:
    """
    Extract data from an array table (multiple 'value' rows).

    Extra kwargs (``order_by``, ``limit``, ``offset``) are forwarded to
    ``Object._build_select`` to override View-stored attrs per-call.
    """
    query = obj._build_select(columns="value", default_order_by=None, **build_select_kwargs)
    data_result = await obj.ch_client.query(query)
    rows = data_result.result_rows
    return [_convert_value(row[0]) for row in rows]


async def extract_dict_data(
    obj: Object,
    column_names: list[str],
    columns: dict[str, ColumnInfo],
    orient: str,
    **build_select_kwargs: Any,
):
    """
    Extract data from a dict table.

    Handles nested structures by detecting dot-star notation in column names
    and unflattening them back to nested dicts.

    Args:
        obj: Object instance with dict data
        column_names: List of column names in order
        columns: Dict mapping column names to ColumnInfo (with fieldtype)
        orient: Output format (ORIENT_DICT or ORIENT_RECORDS)

    Returns:
        Dict or list of dicts based on orient parameter
    """
    query = obj._build_select(columns="*", default_order_by=None, **build_select_kwargs)
    data_result = await obj.ch_client.query(query)
    rows = data_result.result_rows

    output_columns = list(column_names)
    col_indices = {name: column_names.index(name) for name in output_columns}

    nested = _has_nested_columns(output_columns)

    # Dict-of-arrays when the first column carries fieldtype=ARRAY.
    first_col = output_columns[0] if output_columns else None
    is_dict_of_arrays = bool(first_col and columns.get(first_col) and columns[first_col].fieldtype == FIELDTYPE_ARRAY)

    if nested:
        return _extract_nested_dict_data(rows, output_columns, col_indices, is_dict_of_arrays, orient)

    if orient == ORIENT_RECORDS:
        # Return list of dicts (one per row)
        return [{name: _convert_value(row[col_indices[name]]) for name in output_columns} for row in rows]
    else:
        # ORIENT_DICT
        if is_dict_of_arrays:
            # Dict of arrays: return dict with arrays as values
            return {name: [_convert_value(row[col_indices[name]]) for row in rows] for name in output_columns}
        elif rows:
            # Dict of scalars: return single dict (first row)
            return {name: _convert_value(rows[0][col_indices[name]]) for name in output_columns}
        return {}


def _extract_nested_dict_data(
    rows: list,
    output_columns: list[str],
    col_indices: dict[str, int],
    is_dict_of_arrays: bool,
    orient: str,
):
    """Extract data from a table with nested dot-star columns.

    Builds flat dicts from each row, then unflattens them to restore
    the nested structure.
    """
    if orient == ORIENT_RECORDS:
        result = []
        for row in rows:
            flat = {name: _convert_value(row[col_indices[name]]) for name in output_columns}
            result.append(_unflatten_record(flat))
        return result
    else:
        # ORIENT_DICT
        if is_dict_of_arrays:
            records = []
            for row in rows:
                flat = {name: _convert_value(row[col_indices[name]]) for name in output_columns}
                records.append(_unflatten_record(flat))
            # Transpose list of records to dict of arrays
            if records:
                keys = list(records[0].keys())
                return {key: [r[key] for r in records] for key in keys}
            return {}
        elif rows:
            flat = {name: _convert_value(rows[0][col_indices[name]]) for name in output_columns}
            return _unflatten_record(flat)
        return {}
