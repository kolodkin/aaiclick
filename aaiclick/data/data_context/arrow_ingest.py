"""
aaiclick.data.data_context.arrow_ingest - Arrow-based ingest schema evaluation.

``pa.array(records)`` infers one unified nested type across ALL records in
C++ (no first-record sampling; ``pa.Table.from_pylist`` must NOT be used -
it takes top-level keys from the first record only). The type tree maps
1:1 onto dot notation: struct field -> ``x.y`` (no Array level),
list<struct> -> ``x.*.y`` (one Array level per star). Keys missing in some
records/items surface as nulls in the unified type and are rejected -
strict identical-keys semantics with no per-item Python work. Leaf data
stays in arrow end to end: flat leaf arrays are assembled into a
``pa.Table`` (cast to the ClickHouse target schema) and inserted via
``ChClient.insert_arrow`` - no Python-list round trip.
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pyarrow.compute as pc

from ..models import ColumnInfo
from .arrow_types import ch_type_to_pa


def infer_struct_array(records: list) -> pa.StructArray:
    """Infer a unified StructArray from a list of dict records.

    Arrow conversion errors (mixed dict/non-dict items, cross-record type
    conflicts) are translated to ``ValueError``.
    """
    try:
        arr = pa.array(records)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise ValueError(f"Cannot infer a uniform schema from records: {e}") from e
    if not pa.types.is_struct(arr.type):
        raise ValueError(f"Records must be dicts, got arrow type {arr.type}")
    return arr


def leaf_column_info(pa_type: pa.DataType, array_depth: int = 0) -> ColumnInfo:
    """Map an arrow leaf type to a ColumnInfo (parity with legacy inference)."""
    if pa.types.is_boolean(pa_type):
        ch_type = "Bool"
    elif pa.types.is_timestamp(pa_type):
        ch_type = "DateTime64(3, 'UTC')"
    elif pa.types.is_integer(pa_type):
        ch_type = "Int64"
    elif pa.types.is_floating(pa_type):
        ch_type = "Float64"
    elif pa.types.is_null(pa_type):
        return ColumnInfo("String", nullable=True, array=array_depth)
    else:
        ch_type = "String"
    return ColumnInfo(ch_type, array=array_depth)


def _is_list_type(pa_type: pa.DataType) -> bool:
    return pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type)


def list_leaf_column_info(pa_type: pa.DataType, array_depth: int, key_path: str) -> ColumnInfo:
    """Descend list nesting to the leaf ColumnInfo; a dict below a list level cannot round-trip."""
    while _is_list_type(pa_type):
        array_depth += 1
        pa_type = pa_type.value_type
    if pa.types.is_struct(pa_type):
        raise ValueError(f"Lists of lists of dicts are not supported: {key_path!r}")
    return leaf_column_info(pa_type, array_depth)


def struct_type_to_columns(struct_type: pa.StructType) -> dict[str, ColumnInfo]:
    """Walk the arrow type tree into flat dot-notation ColumnInfos.

    Raises ValueError for field names containing ``.`` and for empty
    structs (no representable columns).
    """
    columns: dict[str, ColumnInfo] = {}
    for i in range(struct_type.num_fields):
        field = struct_type.field(i)
        _walk_type(field.name, field.type, "", 0, columns)
    return columns


def _walk_type(
    name: str,
    pa_type: pa.DataType,
    prefix: str,
    array_depth: int,
    columns: dict[str, ColumnInfo],
) -> None:
    key_path = f"{prefix}{name}"
    if "." in name:
        raise ValueError(f"Dict keys must not contain '.': {key_path!r}")
    if pa.types.is_struct(pa_type):
        if pa_type.num_fields == 0:
            raise ValueError(f"Empty dict values are not supported: {key_path!r}")
        for i in range(pa_type.num_fields):
            field = pa_type.field(i)
            _walk_type(field.name, field.type, f"{key_path}.", array_depth, columns)
    elif _is_list_type(pa_type):
        elem = pa_type.value_type
        if pa.types.is_struct(elem):
            if elem.num_fields == 0:
                raise ValueError(f"Empty dict values are not supported: {key_path!r}")
            for i in range(elem.num_fields):
                field = elem.field(i)
                _walk_type(field.name, field.type, f"{key_path}.*.", array_depth + 1, columns)
        else:
            columns[key_path] = list_leaf_column_info(pa_type, array_depth, key_path)
    else:
        columns[key_path] = leaf_column_info(pa_type, array_depth)


def arrow_table_for_insert(
    col_map: Mapping[str, pa.Array | list],
    columns: dict[str, ColumnInfo],
) -> pa.Table:
    """Assemble ingest columns into a ``pa.Table`` matching the CH schema.

    ``col_map`` values are either arrow leaf arrays (from
    :func:`struct_array_to_columns` or inference) or plain Python lists.
    Each column is brought to the arrow type of its ``ColumnInfo`` target:
    arrow arrays are cast (covers ``FieldSpec`` type overrides and all-null
    leaves landing in ``Nullable(String)``), Python lists are constructed
    directly at the target type. Casts allow time truncation — the
    ``DateTime64(3)`` target intentionally drops sub-millisecond input —
    but reject every other lossy conversion.
    """
    arrays = []
    for name, value in col_map.items():
        target = ch_type_to_pa(columns[name].ch_type())
        if isinstance(value, list):
            arr = pa.array(value, type=target)
        elif value.type != target:
            arr = value.cast(options=pc.CastOptions(target_type=target, allow_time_truncate=True))
        else:
            arr = value
        arrays.append(arr)
    return pa.table(arrays, names=list(col_map))


def _missing(key_path: str) -> ValueError:
    return ValueError(f"All records must have identical keys: field {key_path!r} is missing or None in some records")


def struct_array_to_columns(arr: pa.StructArray, nullable_keys: frozenset[str] = frozenset()) -> dict[str, pa.Array]:
    """Extract flat leaf columns as arrow arrays, enforcing strictness.

    Any null at a struct/list level, or in a typed leaf, means a key was
    missing (or None) in some records/items -> ValueError. Leaves whose
    dot-notation key is in ``nullable_keys`` (from ``FieldSpec(nullable=True)``)
    are exempt — their nulls ingest as NULL. Struct- and list-level nulls
    always raise: a None dict or list item cannot round-trip. All-null leaves
    (arrow ``null`` type, e.g. from empty lists or all-None values) pass
    through as-is and are cast to Nullable(String) by
    :func:`arrow_table_for_insert`, matching legacy behavior.
    """
    if arr.null_count:
        raise ValueError("Records must all be dicts (found a null record)")
    # Offsets-based rewrapping assumes an unsliced array, i.e. fresh from pa.array().
    return _extract_struct(arr, "", nullable_keys)


def _extract_struct(arr: pa.StructArray, prefix: str, nullable_keys: frozenset[str]) -> dict[str, pa.Array]:
    out: dict[str, pa.Array] = {}
    for i in range(arr.type.num_fields):
        field = arr.type.field(i)
        _extract_field(f"{prefix}{field.name}", arr.field(i), out, nullable_keys)
    return out


def _extract_field(key_path: str, arr: pa.Array, out: dict[str, pa.Array], nullable_keys: frozenset[str]) -> None:
    pa_type = arr.type
    if pa.types.is_struct(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        out.update(_extract_struct(arr, f"{key_path}.", nullable_keys))
    elif _is_list_type(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        if pa.types.is_struct(pa_type.value_type):
            values = arr.values
            if values.null_count:
                raise _missing(key_path)
            sub = _extract_struct(values, f"{key_path}.*.", nullable_keys)
            for name, leaf in sub.items():
                out[name] = pa.ListArray.from_arrays(arr.offsets, leaf)
        else:
            inner = arr
            while _is_list_type(inner.type):
                inner = inner.values
                if inner.null_count and not pa.types.is_null(inner.type) and key_path not in nullable_keys:
                    raise _missing(key_path)
            out[key_path] = arr
    else:
        if arr.null_count and not pa.types.is_null(pa_type) and key_path not in nullable_keys:
            raise _missing(key_path)
        out[key_path] = arr
