"""Tests for the basic worker example project."""

import subprocess
from pathlib import Path


def test_basic_worker_script():
    """Test that basic_worker.sh runs successfully and produces expected output."""
    script_path = Path(__file__).parent / "basic_worker.sh"

    result = subprocess.run(
        [str(script_path)],
        capture_output=True,
        text=True,
        timeout=30,
        cwd=script_path.parent,
    )

    # Combine stdout and stderr for checking (worker output may go to either)
    output = result.stdout + result.stderr

    # Script should exit cleanly
    assert result.returncode == 0, f"Script failed with code {result.returncode}. Output:\n{output}"

    # Check all expected output in one step
    expected = [
        "=== Basic Worker Example ===",
        "Registering job...",
        "Starting worker in background...",
        "Tick 1/6",
        "Tick 2/6",
        "Tick 3/6",
        "Tick 4/6",
        "Tick 5/6",
        "Tick 6/6",
        "Done!",
        "=== Example completed ===",
    ]
    missing = [s for s in expected if s not in output]
    assert not missing, f"Missing: {missing}. Output:\n{output}"
