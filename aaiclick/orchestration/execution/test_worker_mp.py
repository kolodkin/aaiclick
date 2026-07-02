"""ExecutionWorker tests that spawn multiprocessing workers.

Kept in a dedicated module so ``orch_ctx_no_ch`` can be module-scoped
(the parent process never opens chdb, leaving the file lock free for
each spawned child).
"""

from ..models import EXECUTION_WORKER_STOPPED
from .mp_worker import mp_worker_main_loop
from .execution_worker import get_execution_worker, register_execution_worker, request_execution_worker_stop


async def test_worker_main_loop_stops_on_stop_request(orch_ctx_no_ch, monkeypatch, fast_poll):
    """Test that the main loop exits when a stop request is detected."""
    worker = await register_execution_worker()
    await request_execution_worker_stop(worker.id)

    monkeypatch.setattr("aaiclick.orchestration.execution.execution_worker.HEARTBEAT_INTERVAL", 0)

    tasks_executed = await mp_worker_main_loop(
        execution_worker_id=worker.id,
        install_signal_handlers=False,
        max_empty_polls=50,
    )

    assert tasks_executed == 0

    db_worker = await get_execution_worker(worker.id)
    assert db_worker is not None
    assert db_worker.status == EXECUTION_WORKER_STOPPED
