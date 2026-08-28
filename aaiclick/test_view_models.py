from __future__ import annotations

import subprocess
import sys


def test_import_order_independent_of_orchestration():
    """``view_models`` must be importable before ``orchestration``.

    Regression for the old ``view_models`` ↔ ``orchestration`` cycle: any
    import path that reached ``view_models`` first (e.g. the AI stack) died
    with a partially-initialized-module ImportError. Each ordering runs in a
    fresh interpreter because import-order bugs are invisible once the modules
    are cached in this process.
    """
    for stmt in (
        "import aaiclick.view_models",
        "import aaiclick.ai.ollama, aaiclick.view_models",
    ):
        proc = subprocess.run([sys.executable, "-c", stmt], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
