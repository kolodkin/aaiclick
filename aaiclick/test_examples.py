"""
Tests for examples to ensure they run without errors.
"""

import importlib
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).parent / "examples"
EXCLUDED = {"__init__.py", "run_all.py"}

EXAMPLE_MODULES = sorted(
    f.stem
    for f in EXAMPLES_DIR.glob("*.py")
    if f.name not in EXCLUDED
)


@pytest.fixture
async def example_env(monkeypatch, tmp_path):
    """Set up environment for all examples."""
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))
    yield


@pytest.mark.parametrize("module_name", EXAMPLE_MODULES)
async def test_example(example_env, module_name):
    """Test that each example runs successfully."""
    mod = importlib.import_module(f"aaiclick.examples.{module_name}")
    await mod.amain()
