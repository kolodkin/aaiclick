"""Tests for count_if() — conditional counting via countIf()."""


from aaiclick import create_object_from_value
from aaiclick.data.data_context import create_object
from aaiclick.data.models import FIELDTYPE_ARRAY, ColumnInfo, Schema


async def test_count_if_str_basic(ctx):
    """count_if with a str condition returns scalar Object."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.count_if("value > 3")
    assert await result.data() == 2


async def test_count_if_str_all_match(ctx):
    """count_if where all rows match."""
    obj = await create_object_from_value([10, 20, 30])
    result = await obj.count_if("value > 0")
    assert await result.data() == 3


async def test_count_if_str_none_match(ctx):
    """count_if where no rows match."""
    obj = await create_object_from_value([1, 2, 3])
    result = await obj.count_if("value > 100")
    assert await result.data() == 0


async def test_count_if_str_always_true(ctx):
    """count_if('1') counts all rows (equivalent to count())."""
    obj = await create_object_from_value([10, 20, 30, 40])
    result = await obj.count_if("1")
    assert await result.data() == 4


async def test_count_if_dict_basic(ctx):
    """count_if with a dict returns dict Object with one row."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.count_if({
        "small": "value <= 2",
        "large": "value >= 4",
    })
    data = await result.data()
    assert data["small"] == 2
    assert data["large"] == 2


async def test_count_if_dict_total_via_always_true(ctx):
    """Dict form with '1' condition acts as total count."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    result = await obj.count_if({
        "total": "1",
        "gt3": "value > 3",
    })
    data = await result.data()
    assert data["total"] == 5
    assert data["gt3"] == 2


async def test_count_if_dict_on_dict_object(ctx):
    """count_if works on dict Objects with named columns."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "name": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
        },
    )
    obj = await create_object(schema)
    from aaiclick.data.data_context import get_ch_client
    ch = get_ch_client()
    await ch.command(
        f"INSERT INTO {obj.table} (name, score) VALUES "
        f"('alice', 90), ('bob', 45), ('carol', 80), ('dave', 30)"
    )

    result = await obj.count_if({
        "passing": "score >= 50",
        "failing": "score < 50",
    })
    data = await result.data()
    assert data["passing"] == 2
    assert data["failing"] == 2


async def test_count_if_on_view_with_where(ctx):
    """count_if works on a View (Object.where())."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    view = obj.where("value <= 6")
    result = await view.count_if("value > 3")
    assert await result.data() == 3
