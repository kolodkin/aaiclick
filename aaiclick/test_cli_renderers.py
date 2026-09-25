"""Tests for the CLI output renderers in ``aaiclick/cli_renderers.py``."""

from aaiclick.ai.ollama import OLLAMA_PULLED, OllamaBootstrapResult
from aaiclick.cli_renderers import render_job_failure, render_setup_result
from aaiclick.orchestration.view_models import TaskStatsView
from aaiclick.testing import make_job_stats
from aaiclick.view_models import SetupResult, SetupStep


def test_render_job_failure_shows_full_error_not_the_table_truncation(capsys):
    """``render_job_stats`` clips errors to 60 chars for its table; the final
    failure message must not, or the traceback is unusable."""
    long_error = "ValueError: " + "x" * 200
    stats = make_job_stats(
        "FAILED",
        [
            TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error=long_error),
            TaskStatsView(id=8, entrypoint="mod.ok", status="COMPLETED"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert long_error in out
    assert "mod.ok" not in out
    assert "task get 7" in out


def test_render_job_failure_is_not_silent_on_a_cascade_only_failure(capsys):
    """A cancelled origin leaves no directly-FAILED task, but the rollup still
    fails the job — the block must still name what went wrong."""
    stats = make_job_stats(
        "FAILED",
        [
            TaskStatsView(id=9, entrypoint="mod.downstream", status="UPSTREAM_FAILED", error="Upstream task failed"),
            TaskStatsView(id=8, entrypoint="mod.origin", status="CANCELLED"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert "mod.downstream" in out
    assert "mod.origin" in out


def test_render_job_failure_hides_cascade_victims_when_a_real_failure_exists(capsys):
    """A wide fan-out can cascade to hundreds of UPSTREAM_FAILED tasks whose
    error is one constant string — noise next to the task that actually broke."""
    stats = make_job_stats(
        "FAILED",
        [
            TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error="ValueError: real"),
            TaskStatsView(id=9, entrypoint="mod.downstream", status="UPSTREAM_FAILED", error="Upstream task failed"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert "mod.boom" in out
    assert "mod.downstream" not in out


def test_render_setup_result(capsys):
    """Known step names get their display label, unknown ones print verbatim;
    a non-ok status is upper-cased; the Ollama block follows the steps."""
    result = SetupResult(
        root="/r",
        ch_url="chdb:///r/chdb",
        sql_url="sqlite+aiosqlite:///r/db.sqlite",
        mode="local",
        steps=[
            SetupStep(name="chdb", status="ok"),
            SetupStep(name="sqlite", status="failed", detail="disk full"),
            SetupStep(name="custom", status="skipped"),
        ],
        ollama=OllamaBootstrapResult(
            model="ollama/llama3", server_url="http://localhost:11434", status=OLLAMA_PULLED, detail="pulled llama3"
        ),
    )
    render_setup_result(result)

    assert capsys.readouterr().out == (
        "Root:    /r\n"
        "CH URL:  chdb:///r/chdb\n"
        "SQL URL: sqlite+aiosqlite:///r/db.sqlite\n"
        "Mode:    local\n"
        "  chdb: OK\n"
        "  SQLite DB: FAILED (disk full)\n"
        "  custom: SKIPPED\n"
        "\n"
        "AI model: ollama/llama3\n"
        "  ollama server: running\n"
        "  pulled llama3\n"
        "Setup complete.\n"
    )
