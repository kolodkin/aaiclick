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

import pyarrow as pa

from ..models import ColumnInfo


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
            depth = array_depth + 1
            while _is_list_type(elem):
                depth += 1
                elem = elem.value_type
            columns[key_path] = leaf_column_info(elem, depth)
    else:
        columns[key_path] = leaf_column_info(pa_type, array_depth)


def _contains_timestamp(pa_type: pa.DataType) -> bool:
    if pa.types.is_timestamp(pa_type):
        return True
    if _is_list_type(pa_type):
        return _contains_timestamp(pa_type.value_type)
    return False


def arrow_table_for_insert(
    col_map: dict[str, pa.Array | list],
    columns: dict[str, ColumnInfo],
) -> pa.Table:
    """Assemble ingest columns into a ``pa.Table`` matching the CH schema.

    ``col_map`` values are either arrow leaf arrays (from
    :func:`struct_array_to_columns` or inference) or plain Python lists.
    Each column is brought to the arrow type of its ``ColumnInfo`` target:
    arrow arrays are cast (covers ``FieldSpec`` type overrides and all-null
    leaves landing in ``Nullable(String)``), Python lists are constructed
    directly at the target type. Timestamp casts are unchecked because the
    ``DateTime64(3)`` target intentionally truncates sub-millisecond input;
    all other casts reject lossy conversions.
    """
    arrays = []
    for name, value in col_map.items():
        target = ch_type_to_pa(columns[name].ch_type())
        if isinstance(value, list):
            arr = pa.array(value, type=target)
        elif value.type != target:
            arr = value.cast(target, safe=not _contains_timestamp(target))
        else:
            arr = value
        arrays.append(arr)
    return pa.table(arrays, names=list(col_map))


_PA_BASE_TYPES: dict[str, pa.DataType] = {
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "Float32": pa.float32(),
    "Float64": pa.float64(),
    "String": pa.string(),
    "Bool": pa.bool_(),
}


def ch_type_to_pa(ch_type: str) -> pa.DataType:
    """Convert a ClickHouse type string to a pyarrow DataType."""
    if ch_type.startswith("Nullable("):
        return ch_type_to_pa(ch_type[9:-1])
    if ch_type.startswith("LowCardinality("):
        return ch_type_to_pa(ch_type[15:-1])
    if ch_type in _PA_BASE_TYPES:
        return _PA_BASE_TYPES[ch_type]
    if ch_type.startswith("DateTime64"):
        return pa.timestamp("ms", tz="UTC")
    if ch_type.startswith("Array("):
        return pa.list_(ch_type_to_pa(ch_type[6:-1]))
    if ch_type.startswith("Map("):
        key_type, val_type = _split_map_args(ch_type[4:-1])
        return pa.map_(ch_type_to_pa(key_type), ch_type_to_pa(val_type))
    if ch_type.startswith("Tuple("):
        elem_types = _split_top_level(ch_type[6:-1])
        return pa.struct([(f"f{i}", ch_type_to_pa(t)) for i, t in enumerate(elem_types)])
    return pa.string()


def _split_map_args(inner: str) -> tuple[str, str]:
    """Split Map(K, V) arguments respecting nested parentheses."""
    depth = 0
    for i, ch in enumerate(inner):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            return inner[:i].strip(), inner[i + 1 :].strip()
    return inner, ""


def _split_top_level(inner: str) -> list[str]:
    """Split comma-separated type arguments respecting nested parentheses."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(inner):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(inner[start:i].strip())
            start = i + 1
    parts.append(inner[start:].strip())
    return parts


def _missing(key_path: str) -> ValueError:
    return ValueError(f"All records must have identical keys: field {key_path!r} is missing or None in some records")


def struct_array_to_columns(arr: pa.StructArray) -> dict[str, pa.Array]:
    """Extract flat leaf columns as arrow arrays, enforcing strictness.

    Any null at a struct/list level, or in a typed leaf, means a key was
    missing (or None) in some records/items -> ValueError. All-null leaves
    (arrow ``null`` type, e.g. from empty lists or all-None values) pass
    through as-is and are cast to Nullable(String) by
    :func:`arrow_table_for_insert`, matching legacy behavior.
    """
    if arr.null_count:
        raise ValueError("Records must all be dicts (found a null record)")
    # Offsets-based rewrapping assumes an unsliced array, i.e. fresh from pa.array().
    return _extract_struct(arr, "")


def _extract_struct(arr: pa.StructArray, prefix: str) -> dict[str, pa.Array]:
    out: dict[str, pa.Array] = {}
    for i in range(arr.type.num_fields):
        field = arr.type.field(i)
        _extract_field(f"{prefix}{field.name}", arr.field(i), out)
    return out


def _extract_field(key_path: str, arr: pa.Array, out: dict[str, pa.Array]) -> None:
    pa_type = arr.type
    if pa.types.is_struct(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        out.update(_extract_struct(arr, f"{key_path}."))
    elif _is_list_type(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        if pa.types.is_struct(pa_type.value_type):
            values = arr.values
            if values.null_count:
                raise _missing(key_path)
            sub = _extract_struct(values, f"{key_path}.*.")
            for name, leaf in sub.items():
                out[name] = pa.ListArray.from_arrays(arr.offsets, leaf)
        else:
            inner = arr
            while _is_list_type(inner.type):
                inner = inner.values
                if inner.null_count and not pa.types.is_null(inner.type):
                    raise _missing(key_path)
            out[key_path] = arr
    else:
        if arr.null_count and not pa.types.is_null(pa_type):
            raise _missing(key_path)
        out[key_path] = arr
