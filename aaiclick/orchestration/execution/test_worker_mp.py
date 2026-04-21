"""Worker tests that spawn multiprocessing workers.

Kept in a dedicated module so ``orch_ctx_no_ch`` can be module-scoped
(the parent process never opens chdb, leaving the file lock free for
each spawned child).
"""

from ..models import WorkerStatus
from .mp_worker import mp_worker_main_loop
from .worker import get_worker, register_worker, request_worker_stop


async def test_worker_main_loop_stops_on_stop_request(orch_ctx_no_ch, monkeypatch, fast_poll):
    """Test that the main loop exits when a stop request is detected."""
    worker = await register_worker()
    await request_worker_stop(worker.id)

    monkeypatch.setattr("aaiclick.orchestration.execution.worker.HEARTBEAT_INTERVAL", 0)

    tasks_executed = await mp_worker_main_loop(
        worker_id=worker.id,
        install_signal_handlers=False,
        max_empty_polls=50,
    )

    assert tasks_executed == 0

    db_worker = await get_worker(worker.id)
    assert db_worker is not None
    assert db_worker.status == WorkerStatus.STOPPED
