"""Sample tasks for map() execution tests."""

from pathlib import Path

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object, View
from aaiclick.orchestration.decorators import task
from aaiclick.orchestration.orch_helpers import map


@task
async def create_test_data() -> Object:
    """Create an Object with integer values [1, 2, 3, 4, 5]."""
    return await create_object_from_value([10, 20, 30, 40, 50])


@task
async def row_writer(row, output_file: str):
    """Write each row value to a file, one per line."""
    path = Path(output_file)
    with path.open("a") as f:
        f.write(f"{row}\n")


@task
async def row_writer_with_extra(row, factor: int, output_file: str):
    """Write row * factor to a file."""
    path = Path(output_file)
    with path.open("a") as f:
        f.write(f"{row * factor}\n")
