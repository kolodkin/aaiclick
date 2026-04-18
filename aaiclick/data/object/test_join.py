"""Tests for Object.join() — key resolution, schema builder, end-to-end SQL."""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import FIELDTYPE_DICT, ColumnInfo, Schema
from aaiclick.data.object.join import (
    JoinKeys,
    build_join_schema,
    resolve_join_keys,
)

# =============================================================================
# Phase 1: resolve_join_keys
# =============================================================================


def test_resolve_on_string_normalizes_to_list():
    keys = resolve_join_keys(on="k", left_on=None, right_on=None, how="inner")
    assert keys == JoinKeys(left=["k"], right=["k"])


def test_resolve_on_list_passes_through():
    keys = resolve_join_keys(on=["a", "b"], left_on=None, right_on=None, how="inner")
    assert keys == JoinKeys(left=["a", "b"], right=["a", "b"])


def test_resolve_left_right_on_independent_names():
    keys = resolve_join_keys(on=None, left_on="id", right_on="tconst", how="inner")
    assert keys == JoinKeys(left=["id"], right=["tconst"])


def test_resolve_cross_no_keys():
    keys = resolve_join_keys(on=None, left_on=None, right_on=None, how="cross")
    assert keys == JoinKeys(left=[], right=[])


def test_resolve_missing_keys_raises():
    with pytest.raises(ValueError, match="must pass on="):
        resolve_join_keys(on=None, left_on=None, right_on=None, how="inner")


def test_resolve_on_with_left_on_raises():
    with pytest.raises(ValueError, match="not both"):
        resolve_join_keys(on="k", left_on="k", right_on=None, how="inner")


def test_resolve_left_on_without_right_on_raises():
    with pytest.raises(ValueError, match="left_on and right_on must both be set"):
        resolve_join_keys(on=None, left_on="id", right_on=None, how="inner")


def test_resolve_key_length_mismatch_raises():
    with pytest.raises(ValueError, match="same length"):
        resolve_join_keys(on=None, left_on=["a", "b"], right_on=["x"], how="inner")


def test_resolve_cross_with_keys_raises():
    with pytest.raises(ValueError, match="cross"):
        resolve_join_keys(on="k", left_on=None, right_on=None, how="cross")


def test_resolve_unknown_how_raises():
    with pytest.raises(ValueError, match="unknown how"):
        resolve_join_keys(on="k", left_on=None, right_on=None, how="semi")  # type: ignore[arg-type]


def test_resolve_empty_string_key_raises():
    with pytest.raises(ValueError, match="non-empty string"):
        resolve_join_keys(on="", left_on=None, right_on=None, how="inner")


def test_resolve_empty_key_list_raises():
    with pytest.raises(ValueError, match="must not be empty"):
        resolve_join_keys(on=[], left_on=None, right_on=None, how="inner")


# =============================================================================
# Phase 2: build_join_schema
# =============================================================================


def _cols(**kwargs: ColumnInfo) -> dict[str, ColumnInfo]:
    return {"aai_id": ColumnInfo("UInt64"), **kwargs}


def test_schema_using_form_dedupes_key():
    left = _cols(k=ColumnInfo("Int64"), a=ColumnInfo("String"))
    right = _cols(k=ColumnInfo("Int64"), b=ColumnInfo("Float64"))

    schema, lproj, rproj, _ = build_join_schema(
        left, right, JoinKeys(left=["k"], right=["k"]), how="inner", suffixes=None
    )

    assert isinstance(schema, Schema)
    assert schema.fieldtype == FIELDTYPE_DICT
    assert list(schema.columns) == ["aai_id", "k", "a", "b"]
    assert lproj == [("k", "k"), ("a", "a")]
    assert rproj == [("b", "b")]


def test_schema_on_form_keeps_both_keys():
    left = _cols(id=ColumnInfo("Int64"), name=ColumnInfo("String"))
    right = _cols(tconst=ColumnInfo("Int64"), total=ColumnInfo("Float64"))

    schema, lproj, rproj, _ = build_join_schema(
        left, right, JoinKeys(left=["id"], right=["tconst"]), how="inner", suffixes=None
    )

    assert list(schema.columns) == ["aai_id", "id", "tconst", "name", "total"]
    assert lproj == [("id", "id"), ("name", "name")]
    assert rproj == [("tconst", "tconst"), ("total", "total")]


def test_schema_missing_left_key_raises():
    with pytest.raises(ValueError, match="left key 'missing'"):
        build_join_schema(
            _cols(a=ColumnInfo("Int64")),
            _cols(missing=ColumnInfo("Int64")),
            JoinKeys(left=["missing"], right=["missing"]),
            how="inner",
            suffixes=None,
        )


def test_schema_missing_right_key_raises():
    with pytest.raises(ValueError, match="right key 'rk'"):
        build_join_schema(
            _cols(lk=ColumnInfo("Int64")),
            _cols(other=ColumnInfo("Int64")),
            JoinKeys(left=["lk"], right=["rk"]),
            how="inner",
            suffixes=None,
        )


def test_schema_incompatible_key_types_raises():
    with pytest.raises(ValueError, match="key types incompatible"):
        build_join_schema(
            _cols(k=ColumnInfo("String")),
            _cols(k=ColumnInfo("Int64")),
            JoinKeys(left=["k"], right=["k"]),
            how="inner",
            suffixes=None,
        )


def test_schema_compatible_int_types_accepted():
    schema, _, _, _ = build_join_schema(
        _cols(k=ColumnInfo("Int32")),
        _cols(k=ColumnInfo("Int64")),
        JoinKeys(left=["k"], right=["k"]),
        how="inner",
        suffixes=None,
    )
    # Left side's ColumnInfo wins for the result key type.
    assert schema.columns["k"].type == "Int32"


def test_schema_collision_without_suffixes_raises():
    with pytest.raises(ValueError, match="column collision on \\['score'\\]"):
        build_join_schema(
            _cols(k=ColumnInfo("Int64"), score=ColumnInfo("Float64")),
            _cols(k=ColumnInfo("Int64"), score=ColumnInfo("Float64")),
            JoinKeys(left=["k"], right=["k"]),
            how="inner",
            suffixes=None,
        )


def test_schema_collision_with_suffixes_renames_both():
    schema, lproj, rproj, _ = build_join_schema(
        _cols(k=ColumnInfo("Int64"), score=ColumnInfo("Float64")),
        _cols(k=ColumnInfo("Int64"), score=ColumnInfo("Float64")),
        JoinKeys(left=["k"], right=["k"]),
        how="inner",
        suffixes=("_l", "_r"),
    )

    assert list(schema.columns) == ["aai_id", "k", "score_l", "score_r"]
    assert lproj == [("k", "k"), ("score", "score_l")]
    assert rproj == [("score", "score_r")]


def test_schema_empty_suffix_raises():
    with pytest.raises(ValueError, match="non-empty"):
        build_join_schema(
            _cols(k=ColumnInfo("Int64"), s=ColumnInfo("Float64")),
            _cols(k=ColumnInfo("Int64"), s=ColumnInfo("Float64")),
            JoinKeys(left=["k"], right=["k"]),
            how="inner",
            suffixes=("", "_r"),
        )


@pytest.mark.parametrize(
    "how,left_nullable,right_nullable",
    [
        ("inner", False, False),
        ("left", False, True),
        ("right", True, False),
        ("full", True, True),
    ],
)
def test_schema_nullable_promotion_per_how(how, left_nullable, right_nullable):
    left = _cols(k=ColumnInfo("Int64"), a=ColumnInfo("String"))
    right = _cols(k=ColumnInfo("Int64"), b=ColumnInfo("Float64"))
    schema, _, _, _ = build_join_schema(
        left, right, JoinKeys(left=["k"], right=["k"]), how=how, suffixes=None
    )

    assert schema.columns["a"].nullable is left_nullable
    assert schema.columns["b"].nullable is right_nullable
    # Key column under USING form becomes nullable only on FULL
    assert schema.columns["k"].nullable is (left_nullable and right_nullable)


def test_schema_low_cardinality_preserved_and_promoted():
    left = _cols(k=ColumnInfo("Int64"), tag=ColumnInfo("String", low_cardinality=True))
    right = _cols(k=ColumnInfo("Int64"))
    schema, _, _, _ = build_join_schema(
        left, right, JoinKeys(left=["k"], right=["k"]), how="right", suffixes=None
    )

    tag = schema.columns["tag"]
    assert tag.low_cardinality is True
    assert tag.nullable is True


def test_schema_cross_join_has_no_keys():
    left = _cols(a=ColumnInfo("String"))
    right = _cols(b=ColumnInfo("Float64"))
    schema, lproj, rproj, _ = build_join_schema(
        left, right, JoinKeys(left=[], right=[]), how="cross", suffixes=None
    )

    assert list(schema.columns) == ["aai_id", "a", "b"]
    assert lproj == [("a", "a")]
    assert rproj == [("b", "b")]


def test_schema_aai_id_never_projected():
    left = _cols(k=ColumnInfo("Int64"))
    right = _cols(k=ColumnInfo("Int64"))
    schema, lproj, rproj, _ = build_join_schema(
        left, right, JoinKeys(left=["k"], right=["k"]), how="inner", suffixes=None
    )

    projected_sources = [src for src, _ in lproj] + [src for src, _ in rproj]
    assert "aai_id" not in projected_sources


# =============================================================================
# Phase 3: end-to-end join via Object.join()
# =============================================================================


async def test_join_inner_records(ctx):
    users = await create_object_from_value(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}
    )
    orders = await create_object_from_value(
        {"id": [1, 1, 4], "total": [9.5, 14.0, 2.0]}
    )

    joined = await users.join(orders, on="id")
    rows = await joined.data(orient="records")

    by_total = sorted((r["id"], r["name"], r["total"]) for r in rows)
    assert by_total == [(1, "Alice", 9.5), (1, "Alice", 14.0)]
    assert set(rows[0].keys()) == {"id", "name", "total"}


async def test_join_left_fills_null(ctx):
    users = await create_object_from_value(
        {"id": [1, 2], "name": ["Alice", "Bob"]}
    )
    orders = await create_object_from_value(
        {"id": [1], "total": [9.5]}
    )

    joined = await users.join(orders, on="id", how="left")
    rows = await joined.data(orient="records")
    by_id = {r["id"]: r["total"] for r in rows}

    assert by_id == {1: 9.5, 2: None}
    assert joined.schema.columns["total"].nullable is True


async def test_join_right_promotes_left(ctx):
    users = await create_object_from_value({"id": [1], "name": ["Alice"]})
    orders = await create_object_from_value({"id": [1, 2], "total": [9.5, 2.0]})

    joined = await users.join(orders, on="id", how="right")
    rows = await joined.data(orient="records")
    by_id = {r["id"]: r["name"] for r in rows}

    assert by_id == {1: "Alice", 2: None}


async def test_join_full_outer(ctx):
    a = await create_object_from_value({"k": [1, 2], "x": [10, 20]})
    b = await create_object_from_value({"k": [2, 3], "y": [200, 300]})

    joined = await a.join(b, on="k", how="full")
    rows = await joined.data(orient="records")
    by_k = {r["k"]: (r["x"], r["y"]) for r in rows}

    assert by_k == {1: (10, None), 2: (20, 200), 3: (None, 300)}


async def test_join_left_on_right_on_keeps_both_keys(ctx):
    users = await create_object_from_value({"id": [1, 2], "name": ["A", "B"]})
    orders = await create_object_from_value({"user_id": [1, 1], "total": [9.5, 14.0]})

    joined = await users.join(orders, left_on="id", right_on="user_id")
    rows = await joined.data(orient="records")

    assert all(r["id"] == r["user_id"] for r in rows)
    assert set(rows[0].keys()) == {"id", "user_id", "name", "total"}


async def test_join_suffixes_on_collision(ctx):
    a = await create_object_from_value({"id": [1, 2], "score": [10, 20]})
    b = await create_object_from_value({"id": [1, 2], "score": [99, 88]})

    merged = await a.join(b, on="id", suffixes=("_l", "_r"))
    rows = await merged.data(orient="records")
    by_id = {r["id"]: (r["score_l"], r["score_r"]) for r in rows}

    assert by_id == {1: (10, 99), 2: (20, 88)}


async def test_join_collision_without_suffixes_raises(ctx):
    a = await create_object_from_value({"id": [1], "score": [10]})
    b = await create_object_from_value({"id": [1], "score": [99]})

    with pytest.raises(ValueError, match="column collision"):
        await a.join(b, on="id")


async def test_join_cross(ctx):
    colors = await create_object_from_value({"c": ["red", "blue"]})
    sizes = await create_object_from_value({"s": ["S", "M", "L"]})

    skus = await colors.join(sizes, how="cross")
    rows = await skus.data(orient="records")

    pairs = {(r["c"], r["s"]) for r in rows}
    assert len(pairs) == 6
    assert ("red", "M") in pairs


async def test_join_self_join_aliases(ctx):
    a = await create_object_from_value({"id": [1, 2, 3], "val": [10, 20, 30]})

    joined = await a.join(a, on="id", suffixes=("_l", "_r"))
    rows = await joined.data(orient="records")
    by_id = {r["id"]: (r["val_l"], r["val_r"]) for r in rows}

    assert by_id == {1: (10, 10), 2: (20, 20), 3: (30, 30)}


async def test_join_missing_key_raises(ctx):
    a = await create_object_from_value({"id": [1, 2]})
    b = await create_object_from_value({"other": [1, 2]})

    with pytest.raises(ValueError, match="left key 'nope'"):
        await a.join(b, on="nope")


async def test_join_type_incompatible_key_raises(ctx):
    a = await create_object_from_value({"k": [1, 2]})
    b = await create_object_from_value({"k": ["a", "b"]})

    with pytest.raises(ValueError, match="key types incompatible"):
        await a.join(b, on="k")


async def test_join_cross_rejects_keys(ctx):
    a = await create_object_from_value({"c": ["red"]})
    b = await create_object_from_value({"s": ["S"]})

    with pytest.raises(ValueError, match="cross"):
        await a.join(b, on="c", how="cross")


async def test_join_unknown_how_raises(ctx):
    a = await create_object_from_value({"id": [1]})
    b = await create_object_from_value({"id": [1]})

    with pytest.raises(ValueError, match="unknown how"):
        await a.join(b, on="id", how="semi")  # type: ignore[arg-type]
