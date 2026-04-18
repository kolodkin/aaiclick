"""
aaiclick.data.object.join - Object.join() implementation.

Database-level helpers for joining two Objects on one or more key columns.
Mirrors the shape of ``concat_objects_db`` in ``aaiclick/data/object/ingest.py``:

    Object.join()  →  resolve_join_keys() → build_join_schema() → join_objects_db()

All key/schema resolution is pure Python and fully unit-testable without a
ClickHouse client; only ``join_objects_db`` touches SQL.
"""

from __future__ import annotations

from typing import Literal, NamedTuple

from aaiclick.oplog.oplog_api import oplog_record_sample

from ..data_context import create_object
from ..models import (
    FIELDTYPE_DICT,
    ColumnInfo,
    IngestQueryInfo,
    Schema,
)
from .ingest import _are_types_compatible

JoinHow = Literal["inner", "left", "right", "full", "cross"]

HOW_TO_SQL: dict[str, str] = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "cross": "CROSS JOIN",
}


class JoinKeys(NamedTuple):
    """Resolved left/right join key lists.

    Attributes:
        left: Ordered list of column names on the left-hand Object.
        right: Ordered list of column names on the right-hand Object. Same
            length as ``left``; positions are paired.
    """

    left: list[str]
    right: list[str]


def _as_key_list(keys: str | list[str] | None) -> list[str] | None:
    """Normalize a key argument to list[str] or None.

    ``"k"`` → ``["k"]``; ``["k"]`` passes through; ``None`` stays ``None``.
    Rejects empty strings and empty lists as invalid inputs.
    """
    if keys is None:
        return None
    if isinstance(keys, str):
        if not keys:
            raise ValueError("join key must be a non-empty string")
        return [keys]
    if not keys:
        raise ValueError("join key list must not be empty")
    for k in keys:
        if not isinstance(k, str) or not k:
            raise ValueError(f"join key must be a non-empty string, got {k!r}")
    return list(keys)


def resolve_join_keys(
    on: str | list[str] | None,
    left_on: str | list[str] | None,
    right_on: str | list[str] | None,
    how: JoinHow,
) -> JoinKeys:
    """Resolve the ``on`` / ``left_on`` / ``right_on`` triangle to paired key lists.

    Rules:
    - Exactly one of ``{on}`` or ``{left_on, right_on}`` may be set (except
      ``how="cross"`` which forbids all three).
    - If ``left_on`` is given, ``right_on`` must be given with equal length.
    - Single-string keys are normalized to one-element lists.
    - ``how="cross"`` returns empty key lists.

    Raises:
        ValueError: On any violation of the above.
    """
    if how not in HOW_TO_SQL:
        raise ValueError(f"join: unknown how={how!r}; expected one of {sorted(HOW_TO_SQL)}")

    on_list = _as_key_list(on)
    left_list = _as_key_list(left_on)
    right_list = _as_key_list(right_on)

    if how == "cross":
        if on_list is not None or left_list is not None or right_list is not None:
            raise ValueError("join: how='cross' must not be combined with on / left_on / right_on")
        return JoinKeys(left=[], right=[])

    if on_list is not None and (left_list is not None or right_list is not None):
        raise ValueError("join: pass either on= or left_on=/right_on=, not both")

    if on_list is not None:
        return JoinKeys(left=list(on_list), right=list(on_list))

    if left_list is None and right_list is None:
        raise ValueError("join: must pass on= or left_on=/right_on= (unless how='cross')")

    if left_list is None or right_list is None:
        raise ValueError("join: left_on and right_on must both be set")

    if len(left_list) != len(right_list):
        raise ValueError(
            f"join: left_on and right_on must be same length, got {len(left_list)} vs {len(right_list)}"
        )

    return JoinKeys(left=left_list, right=right_list)


def _promote_nullable(col: ColumnInfo) -> ColumnInfo:
    """Return ``col`` with ``nullable=True`` (no-op if already nullable)."""
    if col.nullable:
        return col
    return ColumnInfo(
        type=col.type,
        nullable=True,
        array=col.array,
        low_cardinality=col.low_cardinality,
        description=col.description,
    )


def _nullable_sides(how: str) -> tuple[bool, bool]:
    """Return (promote_left, promote_right) per the join-type nullability rules."""
    if how == "left":
        return (False, True)
    if how == "right":
        return (True, False)
    if how == "full":
        return (True, True)
    return (False, False)


def build_join_schema(
    left_cols: dict[str, ColumnInfo],
    right_cols: dict[str, ColumnInfo],
    keys: JoinKeys,
    how: JoinHow,
    suffixes: tuple[str, str] | None,
) -> tuple[Schema, list[tuple[str, str]], list[tuple[str, str]]]:
    """Compute the result schema + output-column mappings for a join.

    Returns:
        A tuple ``(schema, left_projection, right_projection)`` where each
        projection is a list of ``(source_column, output_column)`` pairs.
        Key columns appear in ``left_projection`` only when the result holds
        a single copy (``on=`` form); under ``left_on``/``right_on`` both sides
        project their key columns under their original names.

    Raises:
        ValueError: Key missing on either side, key types incompatible,
            non-key column collision without suffixes, or empty suffix.
    """
    for k in keys.left:
        if k not in left_cols:
            raise ValueError(f"join: left key {k!r} not found in left columns {sorted(left_cols)}")
    for k in keys.right:
        if k not in right_cols:
            raise ValueError(f"join: right key {k!r} not found in right columns {sorted(right_cols)}")

    for lk, rk in zip(keys.left, keys.right):
        lc = left_cols[lk]
        rc = right_cols[rk]
        if not _are_types_compatible(lc.type, rc.type):
            raise ValueError(
                f"join: key types incompatible for {lk!r} ({lc.type}) vs {rk!r} ({rc.type})"
            )

    using_form = keys.left == keys.right

    promote_left, promote_right = _nullable_sides(how)

    result_columns: dict[str, ColumnInfo] = {"aai_id": ColumnInfo("UInt64")}
    left_projection: list[tuple[str, str]] = []
    right_projection: list[tuple[str, str]] = []

    key_out_names: set[str] = set()
    if using_form:
        for k in keys.left:
            col = left_cols[k]
            if promote_left and promote_right:
                col = _promote_nullable(col)
            result_columns[k] = col
            left_projection.append((k, k))
            key_out_names.add(k)
    else:
        for lk in keys.left:
            col = left_cols[lk]
            if promote_left:
                col = _promote_nullable(col)
            result_columns[lk] = col
            left_projection.append((lk, lk))
            key_out_names.add(lk)
        for rk in keys.right:
            col = right_cols[rk]
            if promote_right:
                col = _promote_nullable(col)
            if rk in result_columns:
                raise ValueError(
                    f"join: right key {rk!r} collides with an existing result column; "
                    f"rename the right key or use on= when names match"
                )
            result_columns[rk] = col
            right_projection.append((rk, rk))
            key_out_names.add(rk)

    left_nonkey = [c for c in left_cols if c != "aai_id" and c not in keys.left]
    right_nonkey = [c for c in right_cols if c != "aai_id" and c not in keys.right]

    collisions = set(left_nonkey) & set(right_nonkey)
    collisions |= {c for c in left_nonkey if c in key_out_names and c not in keys.left}
    collisions |= {c for c in right_nonkey if c in key_out_names and c not in keys.right}

    if collisions and suffixes is None:
        raise ValueError(
            f"join: column collision on {sorted(collisions)}; "
            f"pass suffixes=('_x', '_y') or rename first"
        )

    if suffixes is not None:
        lsuf, rsuf = suffixes
        if not lsuf or not rsuf:
            raise ValueError(f"join: suffixes must both be non-empty, got {suffixes!r}")
    else:
        lsuf = rsuf = ""

    def _suffix(col: str, suf: str) -> str:
        return f"{col}{suf}" if col in collisions else col

    for col in left_nonkey:
        out = _suffix(col, lsuf)
        if out in result_columns:
            raise ValueError(f"join: suffixed left column {out!r} still collides; rename first")
        info = left_cols[col]
        if promote_left:
            info = _promote_nullable(info)
        result_columns[out] = info
        left_projection.append((col, out))

    for col in right_nonkey:
        out = _suffix(col, rsuf)
        if out in result_columns:
            raise ValueError(f"join: suffixed right column {out!r} still collides; rename first")
        info = right_cols[col]
        if promote_right:
            info = _promote_nullable(info)
        result_columns[out] = info
        right_projection.append((col, out))

    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=result_columns)
    return schema, left_projection, right_projection


async def join_objects_db(
    left: IngestQueryInfo,
    right: IngestQueryInfo,
    *,
    keys: JoinKeys,
    how: JoinHow,
    suffixes: tuple[str, str] | None,
    ch_client,
):
    """Materialize a join into a new Object via CREATE + INSERT...SELECT...JOIN.

    Follows the ``concat_objects_db`` pattern: build the result ``Schema`` in
    Python, call ``create_object(schema)`` to CREATE the destination, then
    issue a single ``INSERT INTO {result} ({cols}) SELECT ... FROM {left} AS l
    {HOW} JOIN {right} AS r {USING|ON}``. Fresh Snowflake IDs are generated
    by the destination table's DEFAULT; source ``aai_id`` columns are never
    projected.

    Self-join is supported via the ``AS l`` / ``AS r`` aliases.

    Args:
        left: Left-hand source.
        right: Right-hand source.
        keys: Resolved join keys from ``resolve_join_keys``.
        how: Join type.
        suffixes: Suffix pair applied to non-key collisions; None means
            collisions raise.
        ch_client: Active ClickHouse client.

    Returns:
        Object: New Object containing the join result.
    """
    schema, left_proj, right_proj = build_join_schema(
        left_cols=left.columns,
        right_cols=right.columns,
        keys=keys,
        how=how,
        suffixes=suffixes,
    )

    result = await create_object(schema)

    using_form = keys.left == keys.right and keys.left != []
    insert_cols = [out for _, out in left_proj] + [out for _, out in right_proj]
    insert_cols_sql = ", ".join(insert_cols)

    # Always emit ON form internally so outer-join NULL semantics line up with
    # our schema: join_use_nulls=1 makes ClickHouse's USING-merged key column
    # nullable regardless of which side drives, which conflicts with the
    # non-nullable key we model for LEFT / RIGHT. Under ON form we can pick
    # the key from the driving side (or COALESCE for FULL) explicitly.
    select_parts: list[str] = []
    using_key_names = set(keys.left) if using_form else set()

    for src, out in left_proj:
        target_type = schema.columns[out].ch_type()
        if using_form and out in using_key_names:
            if how == "full":
                expr = f"coalesce(l.{src}, r.{src})"
            elif how == "right":
                expr = f"r.{src}"
            else:
                expr = f"l.{src}"
        else:
            expr = f"l.{src}"
        select_parts.append(f"CAST({expr} AS {target_type}) AS {out}")
    for src, out in right_proj:
        target_type = schema.columns[out].ch_type()
        select_parts.append(f"CAST(r.{src} AS {target_type}) AS {out}")
    select_sql = ", ".join(select_parts)

    join_sql = HOW_TO_SQL[how]

    if how == "cross":
        on_clause = ""
    else:
        conds = " AND ".join(f"l.{lk} = r.{rk}" for lk, rk in zip(keys.left, keys.right))
        on_clause = f" ON {conds}"

    # For outer joins, make the misses materialize as NULL instead of the
    # missing side's type default. Our schema already marks the outer-side
    # columns Nullable; this setting aligns the runtime with the schema.
    settings_clause = " SETTINGS join_use_nulls = 1" if how in ("left", "right", "full") else ""

    await ch_client.command(
        f"INSERT INTO {result.table} ({insert_cols_sql}) "
        f"SELECT {select_sql} FROM {left.source} AS l "
        f"{join_sql} {right.source} AS r{on_clause}{settings_clause}"
    )

    oplog_record_sample(
        result.table,
        "join",
        kwargs={
            "left": left.base_table,
            "right": right.base_table,
            "left_on": list(keys.left),
            "right_on": list(keys.right),
            "how": how,
        },
    )

    return result
