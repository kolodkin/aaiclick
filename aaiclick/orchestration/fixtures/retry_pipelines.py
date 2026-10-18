"""A consumer whose first attempt fails, for the retry-keeps-its-inputs test.

Module-level so a worker can import every task by entrypoint.
"""

from pathlib import Path

from aaiclick import Object, create_object_from_value
from aaiclick.orchestration import job, task, task_result


@task
async def make_values() -> Object:
    return await create_object_from_value([1, 2, 3])


@task(max_retries=1)
async def total_after_one_failure(values: Object, marker_file: str) -> int:
    marker = Path(marker_file)
    if not marker.exists():
        marker.touch()
        raise RuntimeError("first attempt fails")
    return sum(await values.data())


@job("retry_reads_input")
def retry_pipeline(marker_file: str):
    values = make_values()
    total = total_after_one_failure(values=values, marker_file=marker_file)
    return task_result(data=total, tasks=[values, total])
