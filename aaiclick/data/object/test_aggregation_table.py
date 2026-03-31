"""
Tests for the aggregation table pattern: multi-source INSERT + GROUP BY collapse.

Validates the pattern where multiple data sources with different schemas
insert() into a shared table (missing nullable columns auto-fill with NULL),
then collapse via group_by().agg() with any() to merge into one row per key.
"""

from aaiclick import create_object, create_object_from_value
from aaiclick.data.models import FIELDTYPE_ARRAY, ColumnInfo, Computed, Schema


async def test_aggregation_table_two_sources(ctx):
    """Two sources insert into shared table, collapse picks non-NULL values."""
    # Source A: has name and score
    source_a = await create_object_from_value({
        "key": ["CVE-1", "CVE-2"],
        "name": ["heartbleed", "shellshock"],
        "score": [9.8, 7.5],
    })

    # Source B: has key and label (different columns)
    source_b = await create_object_from_value({
        "key": ["CVE-1", "CVE-3"],
        "label": ["critical", "medium"],
    })

    # Create aggregation table with all columns
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "key": ColumnInfo("String"),
            "in_a": ColumnInfo("UInt8"),
            "in_b": ColumnInfo("UInt8"),
            "name": ColumnInfo("String", nullable=True),
            "score": ColumnInfo("Float64", nullable=True),
            "label": ColumnInfo("String", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Insert source A with computed flag columns; label auto-fills NULL
    view_a = source_a.with_columns({
        "in_a": Computed("UInt8", "1"),
        "in_b": Computed("UInt8", "0"),
    })
    await agg.insert(view_a)

    # Insert source B with computed flag columns; name/score auto-fill NULL
    view_b = source_b.with_columns({
        "in_a": Computed("UInt8", "0"),
        "in_b": Computed("UInt8", "1"),
    })
    await agg.insert(view_b)

    # Collapse: GROUP BY key, merge with max/any
    result = await agg.group_by("key").agg({
        "in_a": "max",
        "in_b": "max",
        "name": "any",
        "score": "any",
        "label": "any",
    })

    data = await result.data()
    rows = {
        k: {col: data[col][i] for col in data}
        for i, k in enumerate(data["key"])
    }

    # CVE-1: present in both sources
    assert rows["CVE-1"]["in_a"] == 1
    assert rows["CVE-1"]["in_b"] == 1
    assert rows["CVE-1"]["name"] == "heartbleed"
    assert rows["CVE-1"]["score"] == 9.8
    assert rows["CVE-1"]["label"] == "critical"

    # CVE-2: only in source A
    assert rows["CVE-2"]["in_a"] == 1
    assert rows["CVE-2"]["in_b"] == 0
    assert rows["CVE-2"]["name"] == "shellshock"
    assert rows["CVE-2"]["score"] == 7.5

    # CVE-3: only in source B
    assert rows["CVE-3"]["in_a"] == 0
    assert rows["CVE-3"]["in_b"] == 1
    assert rows["CVE-3"]["label"] == "medium"


async def test_aggregation_table_three_sources(ctx):
    """Three sources merging via subset insert, no with_columns needed."""
    src1 = await create_object_from_value({
        "id": ["A", "B"],
        "val1": [10, 20],
    })
    src2 = await create_object_from_value({
        "id": ["B", "C"],
        "val2": [200, 300],
    })
    src3 = await create_object_from_value({
        "id": ["A", "C"],
        "val3": ["x", "z"],
    })

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "id": ColumnInfo("String"),
            "val1": ColumnInfo("Int64", nullable=True),
            "val2": ColumnInfo("Int64", nullable=True),
            "val3": ColumnInfo("String", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Subset insert: each source only has id + one val column
    await agg.insert(src1)
    await agg.insert(src2)
    await agg.insert(src3)

    result = await agg.group_by("id").agg({
        "val1": "any",
        "val2": "any",
        "val3": "any",
    })

    data = await result.data()
    rows = {
        k: {col: data[col][i] for col in data}
        for i, k in enumerate(data["id"])
    }

    assert rows["A"]["val1"] == 10
    assert rows["A"]["val3"] == "x"
    assert rows["B"]["val1"] == 20
    assert rows["B"]["val2"] == 200
    assert rows["C"]["val2"] == 300
    assert rows["C"]["val3"] == "z"


async def test_aggregation_table_duplicate_key_same_source(ctx):
    """When same key appears in one source, any() still picks a value."""
    src = await create_object_from_value({
        "key": ["X", "X", "Y"],
        "value": [1, 2, 3],
    })

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "key": ColumnInfo("String"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    agg = await create_object(schema)

    # Direct insert — same schema, no subset needed
    await agg.insert(src)

    result = await agg.group_by("key").agg({"value": "any"})
    data = await result.data()

    pairs = dict(zip(data["key"], data["value"]))
    # any() picks an arbitrary value from the group — either 1 or 2
    assert pairs["X"] in (1, 2)
    assert pairs["Y"] == 3
