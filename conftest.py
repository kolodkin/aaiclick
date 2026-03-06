"""
Root conftest for pytest-xdist worker isolation.

This runs before aaiclick/conftest.py, setting SNOWFLAKE_MACHINE_ID
based on the xdist worker ID to prevent table name collisions.
"""

import os


def pytest_configure(config):
    worker_id = os.getenv("PYTEST_XDIST_WORKER", "")
    if worker_id:
        # "gw0" -> 1, "gw1" -> 2, etc. (reserve 0 for non-xdist runs)
        machine_id = int(worker_id.replace("gw", "")) + 1
        os.environ["SNOWFLAKE_MACHINE_ID"] = str(machine_id)
