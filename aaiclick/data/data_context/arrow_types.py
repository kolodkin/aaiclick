"""
aaiclick.data.data_context.arrow_types - ClickHouse type-string → pyarrow mapping.

Backend-neutral type infrastructure shared by the ingest layer
(``arrow_ingest``) and the chdb backend adapter (``chdb_client``).
"""

from __future__ import annotations

import pyarrow as pa

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
        key_type, val_type = _split_top_level(ch_type[4:-1])
        return pa.map_(ch_type_to_pa(key_type), ch_type_to_pa(val_type))
    if ch_type.startswith("Tuple("):
        elem_types = _split_top_level(ch_type[6:-1])
        return pa.struct([(f"f{i}", ch_type_to_pa(t)) for i, t in enumerate(elem_types)])
    return pa.string()


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
