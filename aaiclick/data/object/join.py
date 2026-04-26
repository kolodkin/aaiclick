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
from .ingest import _are_types_compatible, promote_nullable

JoinHow = Literal["inner", "left", "right", "full", "cross"]

DEFAULT_SUFFIXES: tuple[str, str] = ("_l", "_r")

SuffixesArg = tuple[str, str] | bool | None

HOW_TO_SQL: dict[str, str] = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "cross": "CROSS JOIN",
}

# Under USING form (keys share names across sides), the merged key column in
# the output comes from the side that is guaranteed non-null for that row.
# FULL joins can have misses on either side, so coalesce.
_USING_KEY_TEMPLATE: dict[str, str] = {
    "inner": "l.{0}",
    "left": "l.{0}",
    "right": "r.{0}",
    "full": "coalesce(l.{0}, r.{0})",
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


class JoinSchema(NamedTuple):
    """Result of ``build_join_schema``.

    Attributes:
        schema: Result-table ``Schema``.
        left_projection: ``(source_col, output_col)`` pairs for columns
            drawn from the left source.
        right_projection: ``(source_col, output_col)`` pairs for columns
            drawn from the right source.
        using_form: True when left and right keys share names (and will be
            merged into a single output column).
    """

    schema: Schema
    left_projection: list[tuple[str, str]]
    right_projection: list[tuple[str, str]]
    using_form: bool


def _as_key_list(keys: str | list[str] | None) -> list[str] | None:
    """Normalize a key argument to ``list[str]`` or ``None``.

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
        raise ValueError(f"join: left_on and right_on must be same length, got {len(left_list)} vs {len(right_list)}")

    return JoinKeys(left=left_list, right=right_list)


def _nullable_sides(how: str) -> tuple[bool, bool]:
    """Return ``(promote_left, promote_right)`` per the join-type nullability rules."""
    if how == "left":
        return (False, True)
    if how == "right":
        return (True, False)
    if how == "full":
        return (True, True)
    return (False, False)


def _resolve_suffixes(suffixes: SuffixesArg) -> tuple[str, str] | None:
    """Normalize the ``suffixes`` argument.

    ``True`` expands to the default ``("_l", "_r")`` pair. ``False`` is a
    synonym for ``None`` (collisions raise). A tuple passes through.
    """
    if suffixes is True:
        return DEFAULT_SUFFIXES
    if suffixes is False or suffixes is None:
        return None
    return suffixes


def build_join_schema(
    left_cols: dict[str, ColumnInfo],
    right_cols: dict[str, ColumnInfo],
    keys: JoinKeys,
    how: JoinHow,
    suffixes: SuffixesArg,
) -> JoinSchema:
    """Compute the result schema and per-side output-column mappings.

    Under USING form (left keys == right keys), the merged key survives
    once in the output schema and is projected by the left side. Under
    separate ``left_on`` / ``right_on``, both key columns survive under
    their original names.

    Raises:
        ValueError: Key missing on either side, key types incompatible,
            non-key column collision without ``suffixes``, or empty suffix.
    """
    for k in keys.left:
        if k not in left_cols:
            raise ValueError(f"join: left key {k!r} not found in left columns {sorted(left_cols)}")
    for k in keys.right:
        if k not in right_cols:
            raise ValueError(f"join: right key {k!r} not found in right columns {sorted(right_cols)}")

    for lk, rk in zip(keys.left, keys.right, strict=True):
        lc = left_cols[lk]
        rc = right_cols[rk]
        if not _are_types_compatible(lc.type, rc.type):
            raise ValueError(f"join: key types incompatible for {lk!r} ({lc.type}) vs {rk!r} ({rc.type})")

    using_form = keys.left == keys.right
    promote_left, promote_right = _nullable_sides(how)

    result_columns: dict[str, ColumnInfo] = {}
    left_projection: list[tuple[str, str]] = []
    right_projection: list[tuple[str, str]] = []

    if using_form:
        for k in keys.left:
            col = left_cols[k]
            if promote_left and promote_right:
                col = promote_nullable(col)
            result_columns[k] = col
            left_projection.append((k, k))
    else:
        for lk in keys.left:
            col = left_cols[lk]
            if promote_left:
                col = promote_nullable(col)
            result_columns[lk] = col
            left_projection.append((lk, lk))
        for rk in keys.right:
            if rk in result_columns:
                raise ValueError(
                    f"join: right key {rk!r} collides with an existing result column; "
                    f"rename the right key or use on= when names match"
                )
            col = right_cols[rk]
            if promote_right:
                col = promote_nullable(col)
            result_columns[rk] = col
            right_projection.append((rk, rk))

    left_nonkey = [c for c in left_cols if c not in keys.left]
    right_nonkey = [c for c in right_cols if c not in keys.right]

    collisions = (
        (set(left_nonkey) & set(right_nonkey))
        | (set(left_nonkey) & set(result_columns))
        | (set(right_nonkey) & set(result_columns))
    )

    resolved = _resolve_suffixes(suffixes)

    if collisions and resolved is None:
        raise ValueError(
            f"join: column collision on {sorted(collisions)}; "
            f"pass suffixes=True or suffixes={DEFAULT_SUFFIXES!r} or rename first"
        )

    if resolved is not None:
        lsuf, rsuf = resolved
        if not lsuf or not rsuf:
            raise ValueError(f"join: suffixes must both be non-empty, got {resolved!r}")
    else:
        lsuf = rsuf = ""

    def _add_nonkey(
        source_cols: dict[str, ColumnInfo],
        names: list[str],
        suf: str,
        promote: bool,
        projection: list[tuple[str, str]],
    ) -> None:
        for col in names:
            out = f"{col}{suf}" if col in collisions else col
            if out in result_columns:
                raise ValueError(f"join: suffixed column {out!r} still collides; rename first")
            info = source_cols[col]
            if promote:
                info = promote_nullable(info)
            result_columns[out] = info
            projection.append((col, out))

    _add_nonkey(left_cols, left_nonkey, lsuf, promote_left, left_projection)
    _add_nonkey(right_cols, right_nonkey, rsuf, promote_right, right_projection)

    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=result_columns)
    return JoinSchema(schema, left_projection, right_projection, using_form)


def _project_expr(side: str, src: str, source_type: str, target_type: str) -> str:
    """Build a column projection, skipping the CAST when source/target match."""
    if source_type == target_type:
        return f"{side}.{src}"
    return f"CAST({side}.{src} AS {target_type})"


async def join_objects_db(
    left: IngestQueryInfo,
    right: IngestQueryInfo,
    *,
    keys: JoinKeys,
    how: JoinHow,
    suffixes: SuffixesArg,
    ch_client,
):
    """Materialize a join into a new Object via CREATE + INSERT...SELECT...JOIN.

    Follows the ``concat_objects_db`` pattern: build the result ``Schema`` in
    Python, call ``create_object(schema)`` to CREATE the destination, then
    issue a single ``INSERT INTO {result} ({cols}) SELECT ... FROM {left} AS l
    {HOW} JOIN {right} AS r ON ...``.  Self-join is supported via the
    ``AS l`` / ``AS r`` aliases.

    The SQL always uses ON form (never USING) even when the user passed
    ``on=``, because ClickHouse's ``SETTINGS join_use_nulls=1`` makes the
    USING-merged key nullable in the result set regardless of which side
    drives, conflicting with the non-nullable key we model for LEFT/RIGHT.
    Under ON form we can pick the key from the driving side (or coalesce
    for FULL) explicitly — see ``_USING_KEY_TEMPLATE``.
    """
    jschema = build_join_schema(
        left_cols=left.columns,
        right_cols=right.columns,
        keys=keys,
        how=how,
        suffixes=suffixes,
    )
    schema, left_proj, right_proj, using_form = jschema
    using_keys = set(keys.left) if using_form else set()

    result = await create_object(schema)

    insert_cols = [out for _, out in left_proj] + [out for _, out in right_proj]
    insert_cols_sql = ", ".join(insert_cols)

    select_parts: list[str] = []
    for src, out in left_proj:
        target_col = schema.columns[out]
        target_type = target_col.ch_type()
        if using_form and out in using_keys:
            expr = _USING_KEY_TEMPLATE[how].format(out)
            # For USING form, both sides share the key type, so CAST only if
            # the target differs from the left source type (e.g., promoted to
            # Nullable under FULL).
            if target_type != left.columns[out].ch_type():
                expr = f"CAST({expr} AS {target_type})"
        else:
            expr = _project_expr("l", src, left.columns[src].ch_type(), target_type)
        select_parts.append(f"{expr} AS {out}")
    for src, out in right_proj:
        target_col = schema.columns[out]
        expr = _project_expr("r", src, right.columns[src].ch_type(), target_col.ch_type())
        select_parts.append(f"{expr} AS {out}")
    select_sql = ", ".join(select_parts)

    if how == "cross":
        on_clause = ""
    else:
        conds = " AND ".join(f"l.{lk} = r.{rk}" for lk, rk in zip(keys.left, keys.right, strict=True))
        on_clause = f" ON {conds}"

    # Outer joins need join_use_nulls=1 so misses materialize as NULL against
    # the Nullable-promoted result schema instead of the missing side's type
    # default.
    settings_clause = " SETTINGS join_use_nulls = 1" if how in ("left", "right", "full") else ""

    await ch_client.command(
        f"INSERT INTO {result.table} ({insert_cols_sql}) "
        f"SELECT {select_sql} FROM {left.source} AS l "
        f"{HOW_TO_SQL[how]} {right.source} AS r{on_clause}{settings_clause}"
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
