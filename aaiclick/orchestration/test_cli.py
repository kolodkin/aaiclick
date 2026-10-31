"""Tests for the orchestration CLI entry points."""

from __future__ import annotations

import logging
import logging.config

import pytest

from aaiclick.orchestration.cli import configure_logging, local_log_config

WORKER_LOGGER = "aaiclick.orchestration.execution.execution_worker"


@pytest.fixture
def restore_aaiclick_logger():
    """``dictConfig`` binds a handler to ``capsys``'s stream and turns propagation
    off; put the logger back for later ``caplog`` tests."""
    logger = logging.getLogger("aaiclick")
    saved = (logger.level, list(logger.handlers), logger.propagate)
    yield
    logger.setLevel(saved[0])
    logger.handlers = saved[1]
    logger.propagate = saved[2]


def _assert_worker_line_on_stderr(capsys) -> None:
    logging.getLogger(WORKER_LOGGER).info("ExecutionWorker 1 executing task 7")
    stamp, sep, rest = capsys.readouterr().err.strip().partition(" INFO ")
    assert sep and rest == f"{WORKER_LOGGER}: ExecutionWorker 1 executing task 7"
    assert stamp[:4].isdigit(), stamp


def test_configure_logging_prints_worker_info_lines(capsys, restore_aaiclick_logger):
    """Python's WARNING default drops the worker's INFO lines; the standalone
    loops write them to stderr, timestamped."""
    configure_logging()
    _assert_worker_line_on_stderr(capsys)


def test_local_log_config_prints_worker_info_lines(capsys, restore_aaiclick_logger):
    """The same lines reach stderr when layered onto uvicorn's config, whose
    own loggers are kept."""
    base = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": "%(message)s"}},
        "handlers": {"default": {"class": "logging.StreamHandler", "formatter": "default"}},
        "loggers": {"uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False}},
    }
    config = local_log_config(base)
    assert config["loggers"]["uvicorn"] == base["loggers"]["uvicorn"]

    logging.config.dictConfig(config)
    _assert_worker_line_on_stderr(capsys)
