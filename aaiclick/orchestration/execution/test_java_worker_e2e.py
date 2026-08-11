"""End-to-end: the Java worker claims and completes a shell task.

Requires distributed backends plus ``AAICLICK_JAVA_WORKER_JAR`` pointing at
the shaded jar built from ``java/aaiclick-worker``. This is the schema-drift
guard for ``java/aaiclick-worker/src/test/resources/schema.sql`` — here the
worker runs against the real Python-migrated schema.

Uses the full ``orch_ctx`` fixture: ``read_task_logs`` needs the context's CH
client, and the chdb single-session constraint behind the ``no_ch`` variants
does not apply to a remote ClickHouse server.
"""

import asyncio
import os
import subprocess

import pytest

from aaiclick.backend import is_local
from aaiclick.data.data_context import get_ch_client
from aaiclick.orchestration.jobs import get_job
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.models import JOB_COMPLETED, TASK_COMPLETED
from aaiclick.orchestration.registered_jobs import run_job
from aaiclick.orchestration.runner_config import ENTRY_SHELL

JAR = os.environ.get("AAICLICK_JAVA_WORKER_JAR")

pytestmark = pytest.mark.skipif(
    is_local() or not JAR,
    reason="requires distributed backends and AAICLICK_JAVA_WORKER_JAR",
)


async def _log_rows_diagnostic(task_id: int) -> str:
    """Everything task_logs holds for a task, ignoring run_id — shown when the
    expected line is missing so a run_id/flush mismatch is visible in CI."""
    result = await get_ch_client().query(
        "SELECT run_id, seq, stream, line FROM task_logs WHERE task_id = {task_id:UInt64} ORDER BY seq",
        parameters={"task_id": task_id},
    )
    total = await get_ch_client().query("SELECT COUNT(*) FROM task_logs")
    return f"rows for task {task_id}: {result.result_rows!r}; task_logs total: {total.result_rows!r}"


async def test_java_worker_completes_shell_task(orch_ctx):
    job = await run_job(
        "java-e2e",
        "",
        entry_type=ENTRY_SHELL,
        command=["sh", "-c", "echo from-java-worker"],
    )

    proc = subprocess.Popen(
        ["java", "-jar", JAR, "--max-tasks", "1"],
        env=os.environ.copy(),
    )
    try:
        refreshed = None
        for _ in range(60):
            await asyncio.sleep(1)
            refreshed = await get_job(job.id)
            if refreshed is not None and refreshed.status == JOB_COMPLETED:
                break
        assert refreshed is not None
        assert refreshed.status == JOB_COMPLETED

        # --max-tasks 1 exits after the task: waiting here guarantees the
        # final log flush (part of the worker's task teardown) has run.
        returncode = proc.wait(timeout=30)
        assert returncode == 0, f"Java worker exited with {returncode}"

        tasks = await get_tasks_for_job(job.id)
        assert len(tasks) == 1
        entry_task = tasks[0]
        assert entry_task.status == TASK_COMPLETED
        assert entry_task.run_ids, "Java worker must register a run_id"

        logs = await read_task_logs(entry_task.id, entry_task.run_ids[-1])
        assert any("from-java-worker" in line.text for line in logs), (
            f"log line missing for run_ids={entry_task.run_ids}; "
            + await _log_rows_diagnostic(entry_task.id)
        )
    finally:
        proc.terminate()
        proc.wait(timeout=30)
