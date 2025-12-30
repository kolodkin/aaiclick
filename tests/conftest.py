"""
Pytest configuration for aaiclick tests.

This module sets up the test environment including:
- Starting ClickHouse via docker-compose
- Cleaning up test tables after tests
"""

import asyncio
import subprocess
import time
import pytest
import os


def pytest_configure(config):
    """
    Run docker-compose up before any tests.
    """
    print("\n" + "=" * 50)
    print("Starting ClickHouse with docker-compose...")
    print("=" * 50)

    # Run docker-compose up -d
    result = subprocess.run(
        ["docker-compose", "up", "-d"],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error starting docker-compose: {result.stderr}")
        raise RuntimeError("Failed to start docker-compose")

    print("Docker-compose started successfully")

    # Wait for ClickHouse to be ready
    print("Waiting for ClickHouse to be healthy...")
    max_retries = 30
    for i in range(max_retries):
        result = subprocess.run(
            ["docker-compose", "ps", "--filter", "health=healthy", "--services"],
            cwd=os.path.dirname(os.path.dirname(__file__)),
            capture_output=True,
            text=True,
        )

        if "clickhouse" in result.stdout:
            print("ClickHouse is ready!")
            break

        time.sleep(1)
        print(f"Waiting... ({i + 1}/{max_retries})")
    else:
        raise RuntimeError("ClickHouse did not become healthy in time")


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for async tests.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def cleanup_tables():
    """
    Fixture to track and cleanup test tables after each test.
    """
    tables_to_cleanup = []

    def register_table(table_name):
        tables_to_cleanup.append(table_name)

    yield register_table

    # Cleanup after test
    if tables_to_cleanup:
        from aaiclick import get_client

        client = await get_client()
        for table in tables_to_cleanup:
            try:
                await client.command(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                print(f"Warning: Failed to drop table {table}: {e}")
