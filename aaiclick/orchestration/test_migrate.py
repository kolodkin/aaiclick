from __future__ import annotations

import logging

from alembic import command

from aaiclick.orchestration import logging as orch_logging
from aaiclick.orchestration.migrate import get_alembic_config


def test_programmatic_migration_leaves_process_logging_alone():
    """Migrations run from code must not reconfigure the host process's
    logging. ``env.py``'s ``fileConfig`` used to disable every already-imported
    ``aaiclick.*`` logger — dropping, e.g., the failed-run traceback that
    ``capture_task_output`` writes into the task's log — and reset the root
    logger's level and handlers.

    Offline mode (``sql=True``) loads ``env.py`` without needing a database."""
    root = logging.getLogger()
    handlers_before, level_before = root.handlers[:], root.level

    command.upgrade(get_alembic_config(), "head", sql=True)

    assert orch_logging.logger.disabled is False
    assert root.handlers == handlers_before
    assert root.level == level_before
