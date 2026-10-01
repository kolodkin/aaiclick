from __future__ import annotations

import logging

from alembic import command

from aaiclick.orchestration import logging as orch_logging
from aaiclick.orchestration.migrate import get_alembic_config


def test_migrations_keep_existing_loggers_enabled():
    """Loading the migrations' ``env.py`` must not disable loggers created
    before it. ``fileConfig`` does that by default, so a process that migrated
    in-process silently lost every ``aaiclick.*`` logger — e.g. the failed-run
    traceback that ``capture_task_output`` writes into the task's log.

    Offline mode (``sql=True``) loads ``env.py`` without needing a database."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        command.upgrade(get_alembic_config(), "head", sql=True)
    finally:
        root.handlers, root.level = saved_handlers, saved_level

    assert orch_logging.logger.disabled is False
