"""
Tests for examples to ensure they run without errors.
"""

import importlib
import tempfile
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).parent / "examples"

EXCLUDED = {"__init__.py", "run_all.py", "orchestration_basic.py"}

EXAMPLE_MODULES = sorted(
    f.stem
    for f in EXAMPLES_DIR.glob("*.py")
    if f.name not in EXCLUDED
)


@pytest.mark.parametrize("module_name", EXAMPLE_MODULES)
async def test_example(ctx, module_name):
    """Test that each example runs successfully."""
    mod = importlib.import_module(f"aaiclick.examples.{module_name}")
    await mod.main()


async def test_orchestration_basic_example(orch_ctx, monkeypatch):
    """Test that the orchestration_basic example runs successfully."""
    from aaiclick.examples.orchestration_basic import async_main

    # Use a temp directory for logs
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # Run the async version (main() uses job.test() which calls asyncio.run()
        # and cannot be used inside an already-running event loop)
        await async_main()
