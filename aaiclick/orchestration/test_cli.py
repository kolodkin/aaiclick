"""Tests for the orchestration CLI entry points."""

from __future__ import annotations

import logging
import logging.config
import re

import pytest

from aaiclick.orchestration.cli import local_log_config, start_local


async def test_start_local_refuses_non_loopback_host(monkeypatch):
    """Local mode has auth off, so binding a reachable address opens the server to the network."""
    monkeypatch.setattr("aaiclick.orchestration.cli.is_local", lambda: True)
    with pytest.raises(RuntimeError, match="loopback"):
        await start_local(host="0.0.0.0")


@pytest.fixture
def restore_aaiclick_logger():
    """``dictConfig`` rewires the ``aaiclick`` logger for the whole process; put it
    back so later tests' ``caplog`` (which needs propagation) keeps working."""
    logger = logging.getLogger("aaiclick")
    saved = (logger.level, list(logger.handlers), logger.propagate)
    yield
    logger.level, logger.handlers, logger.propagate = saved[0], saved[1], saved[2]


def test_local_log_config_prints_worker_info_lines(capsys, restore_aaiclick_logger):
    """uvicorn's config alone drops aaiclick's INFO lines; the extended config
    writes them to stderr, timestamped, and keeps uvicorn's own loggers."""
    base = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": "%(message)s"}},
        "handlers": {"default": {"class": "logging.StreamHandler", "formatter": "default"}},
        "loggers": {"uvicorn": {"handlers": ["default"], "level": "INFO"}},
    }
    config = local_log_config(base)
    assert config["loggers"]["uvicorn"] == base["loggers"]["uvicorn"]
    assert "aaiclick" not in base["loggers"]

    logging.config.dictConfig(config)
    logging.getLogger("aaiclick.orchestration.execution.execution_worker").info("ExecutionWorker 1 executing task 7")

    line = capsys.readouterr().err.strip()
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} INFO aaiclick\.orchestration\.execution\.execution_worker: "
        r"ExecutionWorker 1 executing task 7",
        line,
    ), line
