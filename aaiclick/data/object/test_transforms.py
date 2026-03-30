"""Tests for Computed column helper functions and Object transform methods."""

import pytest

from aaiclick import cast, create_object_from_value, split_by_char


async def test_cast_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "abc"}, {"n": "100"}])
    result = await obj.with_columns({"n_int": cast("n", "UInt32")}).data()
    assert result["n_int"] == [42, None, 100]


async def test_cast_not_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "100"}])
    result = await obj.with_columns({"n_int": cast("n", "UInt32", nullable=False)}).data()
    assert result["n_int"] == [42, 100]


async def test_cast_returns_computed(ctx):
    c = cast("col", "UInt32")
    assert c.type == "Nullable(UInt32)"
    assert c.expression == "toUInt32OrNull(col)"


async def test_cast_not_nullable_returns_computed(ctx):
    c = cast("col", "Float64", nullable=False)
    assert c.type == "Float64"
    assert c.expression == "toFloat64(col)"


async def test_split_by_char_returns_computed(ctx):
    c = split_by_char("genres", ",")
    assert c.type == "Array(String)"
    assert c.expression == "splitByChar(',', genres)"


async def test_split_by_char_element_type(ctx):
    c = split_by_char("tags", ",", element_type="LowCardinality(String)")
    assert c.type == "Array(LowCardinality(String))"


async def test_split_by_char_explode(ctx):
    obj = await create_object_from_value([{"s": "a,b,c"}, {"s": "d,e"}])
    result = await obj.with_columns({"parts": split_by_char("s", ",")}).explode("parts").data()
    assert sorted(result["parts"]) == ["a", "b", "c", "d", "e"]


async def test_with_cast_method_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "bad"}, {"n": "7"}])
    result = await obj.with_cast("n", "UInt32", nullable=True).data()
    assert result["n_uint32"] == [42, None, 7]


async def test_with_cast_method_non_nullable(ctx):
    obj = await create_object_from_value([{"n": "42"}, {"n": "7"}])
    result = await obj.with_cast("n", "UInt32").data()
    assert result["n_uint32"] == [42, 7]


async def test_with_cast_method_string(ctx):
    obj = await create_object_from_value([{"n": 42}, {"n": 7}])
    result = await obj.with_cast("n", "String").data()
    assert result["n_string"] == ["42", "7"]


async def test_with_cast_method_alias(ctx):
    obj = await create_object_from_value([{"n": "10"}, {"n": "20"}])
    result = await obj.with_cast("n", "UInt32", alias="n_int").data()
    assert result["n_int"] == [10, 20]


async def test_with_split_by_char_method(ctx):
    obj = await create_object_from_value([{"genres": "Drama,Comedy"}, {"genres": "Action"}])
    result = await obj.with_split_by_char("genres", ",").explode("genres_parts").data()
    assert sorted(result["genres_parts"]) == ["Action", "Comedy", "Drama"]


async def test_with_split_by_char_method_alias(ctx):
    obj = await create_object_from_value([{"genres": "Drama,Comedy"}])
    result = await obj.with_split_by_char("genres", ",", alias="genre").explode("genre").data()
    assert sorted(result["genre"]) == ["Comedy", "Drama"]
