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

    # Check script structure messages
    assert "=== Basic Worker Example ===" in output, f"Missing header. Output:\n{output}"
    assert "Registering job..." in output, f"Missing registration. Output:\n{output}"
    assert "Starting worker in background..." in output, f"Missing worker start. Output:\n{output}"

    # Check all 6 tick messages from the periodic_print task
    for i in range(1, 7):
        assert f"Tick {i}/6" in output, f"Missing Tick {i}/6. Output:\n{output}"

    assert "Done!" in output, f"Missing Done message. Output:\n{output}"
    assert "=== Example completed ===" in output, f"Missing completion. Output:\n{output}"
