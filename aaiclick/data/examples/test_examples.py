"""The data examples print what their ``# →`` comments say.

Each ``print(...)  # → <expected>`` line in an example is an acceptance
check: the printed text must contain ``<expected>``. ``...`` in the
expected text matches anything (ids, table names, timings, a row order that
is undefined).
"""

import ast
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest

from aaiclick.data.examples import (
    aggregation_table,
    array_operators,
    basic_join,
    basic_operators,
    data_manipulation,
    dict_selectors,
    explode,
    group_by,
    isin,
    nested_arrays,
    nullable,
    order_by,
    statistics_ops,
    transforms,
    views,
)

ARROW = "# →"


def expected_outputs(path: Path) -> dict[int, str]:
    """Map each commented ``print(`` call's first line to its expected text."""
    lines = path.read_text().splitlines()
    out: dict[int, str] = {}
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
            for line in lines[node.lineno - 1 : node.end_lineno]:
                if ARROW in line:
                    out[node.lineno] = line.rsplit(ARROW, 1)[1].strip()
    return out


async def run_and_capture(module: ModuleType) -> dict[int, str]:
    """Run ``module.amain()``; return the last text each line of the module printed."""
    printed: dict[int, str] = {}
    source = module.__file__

    def record(*args: object, sep: str = " ", end: str = "\n", **_: object) -> None:
        frame = sys._getframe(1)
        if frame.f_code.co_filename == source:
            printed[frame.f_lineno] = sep.join(str(a) for a in args)

    module.print = record  # module globals shadow the builtin
    try:
        await module.amain()
    finally:
        del module.print
    return printed


def _matches(text: str, expected: str) -> bool:
    pattern = ".*".join(re.escape(part) for part in expected.split("..."))
    return re.search(pattern, text, re.S) is not None


async def check_example(module: ModuleType) -> list[str]:
    """Mismatches between an example's ``# →`` comments and what it prints."""
    expected = expected_outputs(Path(module.__file__))
    printed = await run_and_capture(module)
    problems = []
    for lineno, want in sorted(expected.items()):
        if lineno not in printed:
            problems.append(f"line {lineno}: never printed (expected {want!r})")
        elif not _matches(printed[lineno], want):
            problems.append(f"line {lineno}: printed {printed[lineno].rstrip()!r}, expected {want!r}")
    return problems


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("comment", "expected"),
    [
        pytest.param("# → 3", 1, id="wrong-comment"),
        pytest.param("# → 2", 0, id="right-comment"),
        pytest.param("# → ...", 0, id="wildcard"),
    ],
)
async def test_check_example_reports_a_wrong_comment(tmp_path, comment, expected):
    src = tmp_path / "fake_example.py"
    src.write_text(f"async def amain():\n    print('total:', 2)  {comment}\n")
    assert len(await check_example(_load(src))) == expected


# req: user_guide/object/operators/scalar-operands, user_guide/object/operators/arithmetic-operators, user_guide/object/operators/comparison-operators, user_guide/object/operators/bitwise-operators
async def test_basic_operators_example(ctx):
    assert await check_example(basic_operators) == []


# req: user_guide/object/operators/arithmetic-operators, user_guide/object/operators/aggregation-operators
async def test_array_operators_example(ctx):
    assert await check_example(array_operators) == []


# req: user_guide/object/operators/unary-transforms
async def test_transforms_example(ctx):
    assert await check_example(transforms) == []


# req: user_guide/object/operators/aggregation-operators
async def test_statistics_ops_example(ctx):
    assert await check_example(statistics_ops) == []


# req: user_guide/object/operators/group-by-aggregations
async def test_group_by_example(ctx):
    assert await check_example(group_by) == []


# req: user_guide/object/operators/group-by-agg-spec
async def test_aggregation_table_example(ctx):
    assert await check_example(aggregation_table) == []


# req: user_guide/object/views/views, user_guide/object/views/chained-where
async def test_views_example(ctx):
    assert await check_example(views) == []


# req: user_guide/object/views/column-selection
async def test_dict_selectors_example(ctx):
    assert await check_example(dict_selectors) == []


# req: user_guide/ingest/concat-sources, user_guide/ingest/insert-sources, user_guide/object/views/copy
async def test_data_manipulation_example(ctx):
    assert await check_example(data_manipulation) == []


# req: user_guide/object/views/explode
async def test_explode_example(ctx):
    assert await check_example(explode) == []


# req: user_guide/ingest/nested-flattening
async def test_nested_arrays_example(ctx):
    assert await check_example(nested_arrays) == []


# req: user_guide/ingest/concat-nullable-promotion
async def test_nullable_example(ctx):
    assert await check_example(nullable) == []


# req: user_guide/ingest/join-types
async def test_basic_join_example(ctx):
    assert await check_example(basic_join) == []


# req: user_guide/object/operators/isin
async def test_isin_example(ctx):
    assert await check_example(isin) == []


# req: user_guide/ingest/row-order
async def test_order_by_example(ctx):
    assert await check_example(order_by) == []
