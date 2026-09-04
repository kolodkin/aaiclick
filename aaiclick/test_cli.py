"""Tests for the argparse CLI: new shell/image flags and their forwarding."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aaiclick.__main__ import (
    _load_lineage_ai,
    _parse_command_env,
    _parse_set_kwargs,
    _run_data_api,
    _run_debug,
    _run_explain,
    _run_job_wait,
    _run_register_job,
    _run_run_job,
    build_parser,
    main,
)
from aaiclick.cli_wait import JobWaitTimeout
from aaiclick.orchestration.models import JobStatus
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.orchestration.view_models import JobStatsView, TaskStatsView


async def _noop_run_internal_api(awaitable):
    """Stand-in for ``_run_internal_api`` that consumes the coroutine it is given."""
    if hasattr(awaitable, "__await__"):
        awaitable.close()
    return MagicMock()


def test_run_job_parser_shell_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run-job",
            "j",
            "--entry-type",
            "shell",
            "--command",
            "python main.py",
            "--command-env",
            "K=v",
            "--command-env",
            "OTHER=2",
            "--image",
            "python:3.12",
        ]
    )
    assert args.command == "run-job"
    assert args.entry_type == "shell"
    assert args.command_str == "python main.py"
    assert args.command_env == ["K=v", "OTHER=2"]
    assert args.image == "python:3.12"


def test_run_job_parser_entry_type_default_module():
    parser = build_parser()
    args = parser.parse_args(["run-job", "j"])
    assert args.command == "run-job"
    assert args.entry_type == "module"
    assert args.command_str is None
    assert args.command_env is None
    assert args.image is None


def test_register_job_parser_image_flag():
    parser = build_parser()
    args = parser.parse_args(["register-job", "myapp.jobs.etl", "--image", "myrepo/img:1"])
    assert args.image == "myrepo/img:1"


def test_parse_command_env_pairs():
    assert _parse_command_env(["K=v", "A=b=c"]) == {"K": "v", "A": "b=c"}
    assert _parse_command_env(None) is None
    assert _parse_command_env([]) is None


def test_parse_command_env_rejects_missing_equals():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_command_env(["bad"])


async def test_run_run_job_forwards_shell_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run-job",
            "j",
            "--entry-type",
            "shell",
            "--command",
            "python main.py --flag",
            "--command-env",
            "K=v",
            "--image",
            "python:3.12",
        ]
    )
    with (
        patch("aaiclick.__main__.internal_api.run_job") as run_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_run_job(args)

    request = run_job.call_args.args[0]
    assert request.entry_type == "shell"
    assert request.command == ["python", "main.py", "--flag"]
    assert request.command_env == {"K": "v"}
    assert request.image == "python:3.12"


async def test_run_register_job_forwards_image():
    parser = build_parser()
    args = parser.parse_args(["register-job", "myapp.jobs.etl", "--image", "myrepo/img:1"])
    with (
        patch("aaiclick.__main__.internal_api.register_job") as register_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_register_job(args)

    request = register_job.call_args.args[0]
    assert request.image == "myrepo/img:1"


def test_run_job_command_dest_not_shadowed_by_command_flag():
    """Regression: ``run-job --command`` must not overwrite the top-level
    subparser ``dest="command"``.

    The subparsers action and the ``--command`` flag both defaulted to
    ``dest="command"``, so parsing any ``run-job`` invocation left
    ``args.command`` as ``None`` (or the ``--command`` value) instead of
    ``"run-job"``. ``main()`` then matched no dispatch branch and silently
    fell through to ``print_help()`` — exit 0, nothing persisted. Only the
    distributed e2e (which calls ``run-job`` via a subprocess) caught it."""
    parser = build_parser()
    for argv in (
        ["run-job", "j"],
        ["run-job", "j", "--git-sha", "a" * 40],
        ["run-job", "j", "--command", "python main.py"],
    ):
        args = parser.parse_args(argv)
        assert args.command == "run-job", argv


def test_main_dispatches_run_job_to_handler():
    """Full ``main()`` dispatch path: ``run-job`` must route to its handler,
    not the top-level ``print_help()`` no-op fallback.

    Guards the silent-no-op regression end-to-end (argv -> dispatch ->
    handler), which the parse-only tests above would miss if ``main()``'s
    branch comparison ever drifted from the subparser name."""
    with (
        patch("sys.argv", ["aaiclick", "run-job", "myjob", "--git-sha", "b" * 40]),
        patch("aaiclick.__main__.asyncio.run"),
        # Patching an async def defaults to AsyncMock, whose call creates a
        # coroutine the mocked asyncio.run never awaits — an unraisable
        # RuntimeWarning that filterwarnings=["error"] fails the run.
        patch("aaiclick.__main__._run_run_job", new_callable=MagicMock) as handler,
    ):
        main()

    handler.assert_called_once()
    dispatched_args = handler.call_args.args[0]
    assert dispatched_args.command == "run-job"
    assert dispatched_args.name == "myjob"
    assert dispatched_args.git_sha == "b" * 40


@pytest.mark.parametrize(
    "argv, expected",
    [
        pytest.param(
            ["explain", "p_revenue"],
            {"command": "explain", "table": "p_revenue", "question": None, "json": False},
            id="explain-default-question",
        ),
        pytest.param(
            ["explain", "p_revenue", "Which join fed this?", "--json"],
            {"command": "explain", "table": "p_revenue", "question": "Which join fed this?", "json": True},
            id="explain-custom-question",
        ),
        pytest.param(
            ["debug", "p_revenue", "Why is total negative?"],
            {"command": "debug", "table": "p_revenue", "question": "Why is total negative?", "max_iterations": 10},
            id="debug-default-iterations",
        ),
        pytest.param(
            ["debug", "p_revenue", "Why?", "--max-iterations", "3", "--json"],
            {"command": "debug", "table": "p_revenue", "question": "Why?", "max_iterations": 3, "json": True},
            id="debug-flags",
        ),
    ],
)
def test_ai_lineage_parsers(argv, expected):
    args = build_parser().parse_args(argv)
    assert {key: getattr(args, key) for key in expected} == expected


def test_debug_parser_requires_question():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["debug", "p_revenue"])


@pytest.mark.parametrize(
    "argv, handler_name",
    [
        pytest.param(["explain", "p_revenue"], "_run_explain", id="explain"),
        pytest.param(["debug", "p_revenue", "Why?"], "_run_debug", id="debug"),
    ],
)
def test_main_dispatches_ai_lineage_commands(argv, handler_name):
    with (
        patch("sys.argv", ["aaiclick", *argv]),
        patch("aaiclick.__main__.asyncio.run"),
        patch(f"aaiclick.__main__.{handler_name}", new_callable=MagicMock) as handler,
    ):
        main()

    handler.assert_called_once()
    assert handler.call_args.args[0].table == "p_revenue"


def _fake_lineage_ai() -> MagicMock:
    """Stand-in for ``internal_api.lineage_ai`` so these tests need no ``ai`` extra."""
    module = MagicMock()
    module.explain_lineage = AsyncMock(return_value="explain-answer")
    module.debug_result = AsyncMock(return_value="debug-answer")
    return module


async def test_run_explain_forwards_question_and_renders(capsys):
    args = build_parser().parse_args(["explain", "p_revenue", "Which join fed this?"])
    lineage_ai = _fake_lineage_ai()
    with (
        patch("aaiclick.__main__._load_lineage_ai", return_value=lineage_ai),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
        patch("aaiclick.__main__._render") as render,
    ):
        await _run_explain(args)

    lineage_ai.explain_lineage.assert_awaited_once_with("p_revenue", question="Which join fed this?")
    assert render.call_args.args[1] == "explain-answer"


async def test_run_debug_forwards_max_iterations():
    args = build_parser().parse_args(["debug", "p_revenue", "Why?", "--max-iterations", "3"])
    lineage_ai = _fake_lineage_ai()
    with (
        patch("aaiclick.__main__._load_lineage_ai", return_value=lineage_ai),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
        patch("aaiclick.__main__._render") as render,
    ):
        await _run_debug(args)

    lineage_ai.debug_result.assert_awaited_once_with("p_revenue", question="Why?", max_iterations=3)
    assert render.call_args.args[1] == "debug-answer"


async def test_ai_lineage_commands_run_with_clickhouse_attached():
    """The oplog graph and the debug agent's live queries read ClickHouse."""
    args = build_parser().parse_args(["explain", "p_revenue"])
    seen_with_ch: list[bool] = []

    async def recording_run_internal_api(coro, *, with_ch: bool = False):
        seen_with_ch.append(with_ch)
        return await coro

    with (
        patch("aaiclick.__main__._load_lineage_ai", return_value=_fake_lineage_ai()),
        patch("aaiclick.__main__._run_internal_api", new=recording_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_explain(args)

    assert seen_with_ch == [True]


def test_load_lineage_ai_exits_with_install_hint_without_ai_extra(capsys):
    with (
        patch("aaiclick.__main__.importlib.import_module", side_effect=ImportError("No module named 'litellm'")),
        pytest.raises(SystemExit) as exc_info,
    ):
        _load_lineage_ai()

    assert exc_info.value.code == 1
    assert "pip install aaiclick[ai]" in capsys.readouterr().err


def test_tenant_parser():
    parser = build_parser()
    args = parser.parse_args(["tenant", "create", "acme", "--name", "Acme Corp"])
    assert args.command == "tenant" and args.tenant_command == "create"
    assert args.slug == "acme" and args.name == "Acme Corp"
    args = parser.parse_args(["tenant", "list"])
    assert args.tenant_command == "list"


def test_member_parser():
    parser = build_parser()
    args = parser.parse_args(["member", "add", "--tenant", "acme", "--username", "u", "--role", "admin"])
    assert args.command == "member" and args.member_command == "add"
    assert args.tenant == "acme" and args.username == "u" and args.role == "admin"
    args = parser.parse_args(["member", "remove", "--tenant", "acme", "--username", "u"])
    assert args.member_command == "remove"


def test_global_tenant_flag():
    parser = build_parser()
    args = parser.parse_args(["--tenant", "acme", "registered-job", "list"])
    assert args.global_tenant == "acme" and args.command == "registered-job"
    args = parser.parse_args(["registered-job", "list"])
    assert args.global_tenant is None


def test_global_and_subcommand_tenant_flags_do_not_collide():
    """The member subcommand owns ``--tenant``; the global flag keeps its own
    slot, so neither silently overwrites the other."""
    parser = build_parser()
    args = parser.parse_args(["--tenant", "global_t", "member", "add", "--tenant", "acme", "--username", "u"])
    assert args.global_tenant == "global_t" and args.tenant == "acme"


async def test_run_data_api_provides_a_sql_session():
    """``aaiclick data`` commands need an orch context, not a bare data context.

    Deliberately takes no ``orch_ctx`` fixture: an ambient orch context leaves
    the SQL engine contextvar set and hides the defect.
    """

    async def _uses_sql_session() -> bool:
        async with get_sql_session():
            return True

    assert await _run_data_api(_uses_sql_session())


def test_parse_set_kwargs_coerces_json_typed_values():
    """``--set`` has no type annotations to lean on, so values are JSON-parsed:
    a job expecting ``int`` must not receive the string ``"300"``."""
    assert _parse_set_kwargs(["corpus_size=300", "generate=true", "ratio=0.5", "ids=[1,2]"]) == {
        "corpus_size": 300,
        "generate": True,
        "ratio": 0.5,
        "ids": [1, 2],
    }


def test_parse_set_kwargs_falls_back_to_raw_string():
    assert _parse_set_kwargs(["name=hello", "path=/tmp/x", 'quoted="hi"']) == {
        "name": "hello",
        "path": "/tmp/x",
        "quoted": "hi",
    }


def test_parse_set_kwargs_splits_on_first_equals_only():
    assert _parse_set_kwargs(["expr=a=b"]) == {"expr": "a=b"}


def test_parse_set_kwargs_empty_when_unset():
    assert _parse_set_kwargs(None) == {}


def test_parse_set_kwargs_rejects_missing_equals():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_set_kwargs(["bad"])


async def test_run_run_job_set_takes_precedence_over_kwargs_json():
    """Documented precedence: default_kwargs < --kwargs < --set."""
    parser = build_parser()
    args = parser.parse_args(["run-job", "j", "--kwargs", '{"corpus_size": 10, "keep": 1}', "--set", "corpus_size=300"])
    with (
        patch("aaiclick.__main__.internal_api.run_job") as run_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_run_job(args)

    assert run_job.call_args.args[0].kwargs == {"corpus_size": 300, "keep": 1}


async def test_run_register_job_set_merges_into_default_kwargs():
    parser = build_parser()
    args = parser.parse_args(["register-job", "myapp.jobs.etl", "--set", "corpus_size=300"])
    with (
        patch("aaiclick.__main__.internal_api.register_job") as register_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_register_job(args)

    assert register_job.call_args.args[0].default_kwargs == {"corpus_size": 300}


async def _passthrough_run_internal_api(coro, *, with_ch: bool = False):
    """Stand-in for ``_run_internal_api`` that awaits instead of opening a context."""
    return await coro


def _stats_view(job_status: JobStatus, tasks: list[TaskStatsView] | None = None) -> JobStatsView:
    return JobStatsView(
        job_id=77,
        job_name="j",
        job_status=job_status,
        total_tasks=len(tasks or []),
        status_counts={},
        tasks=tasks or [],
    )


async def test_job_wait_exits_nonzero_and_reports_the_failed_task(capsys):
    """CI and ``set -e`` scripts key off the exit code; the operator keys off
    knowing which task blew up."""
    args = build_parser().parse_args(["job", "wait", "77"])
    stats = _stats_view("FAILED", [TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error="boom")])

    with (
        patch("aaiclick.__main__.cli_wait.wait_for_job", new=AsyncMock(return_value=stats)),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
    ):
        with pytest.raises(SystemExit) as exc:
            await _run_job_wait(args)

    assert exc.value.code == 1
    assert "mod.boom" in capsys.readouterr().out


async def test_job_wait_returns_cleanly_when_job_completed():
    args = build_parser().parse_args(["job", "wait", "77"])
    with (
        patch("aaiclick.__main__.cli_wait.wait_for_job", new=AsyncMock(return_value=_stats_view("COMPLETED"))),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
    ):
        await _run_job_wait(args)


async def test_job_wait_timeout_exits_nonzero(capsys):
    args = build_parser().parse_args(["job", "wait", "77"])
    boom = AsyncMock(side_effect=JobWaitTimeout("stuck", _stats_view("RUNNING")))
    with (
        patch("aaiclick.__main__.cli_wait.wait_for_job", new=boom),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
    ):
        with pytest.raises(SystemExit) as exc:
            await _run_job_wait(args)

    assert exc.value.code == 1
    assert "stuck" in capsys.readouterr().err


async def test_run_job_does_not_block_without_progress():
    """``run-job`` stays fire-and-forget by default."""
    args = build_parser().parse_args(["run-job", "j"])
    with (
        patch("aaiclick.__main__.internal_api.run_job"),
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
        patch("aaiclick.__main__.cli_wait.wait_for_job") as waiter,
    ):
        await _run_run_job(args)

    waiter.assert_not_called()


def test_main_dispatches_job_wait_to_handler():
    with (
        patch("sys.argv", ["aaiclick", "job", "wait", "77"]),
        patch("aaiclick.__main__.asyncio.run"),
        patch("aaiclick.__main__._run_job_wait", new_callable=MagicMock) as handler,
    ):
        main()

    handler.assert_called_once()
    assert handler.call_args.args[0].ref == "77"


async def test_job_wait_json_output_stays_parseable_on_failure(capsys):
    """``--json`` must emit exactly one JSON document — a trailing human-readable
    failure block would break ``jq`` consumers."""
    args = build_parser().parse_args(["job", "wait", "77", "--json"])
    stats = _stats_view("FAILED", [TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error="boom")])

    with (
        patch("aaiclick.__main__.cli_wait.wait_for_job", new=AsyncMock(return_value=stats)),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
    ):
        with pytest.raises(SystemExit):
            await _run_job_wait(args)

    json.loads(capsys.readouterr().out)


async def test_job_wait_json_timeout_keeps_stdout_clean_and_diagnoses_on_stderr(capsys):
    """Under --json nothing has streamed progress, so the timeout still has to
    name the stuck task — on stderr, leaving stdout parseable."""
    args = build_parser().parse_args(["job", "wait", "77", "--json"])
    stuck = _stats_view("RUNNING", [TaskStatsView(id=7, entrypoint="mod.stuck", status="RUNNING")])
    boom = AsyncMock(side_effect=JobWaitTimeout("stuck", stuck))

    with (
        patch("aaiclick.__main__.cli_wait.wait_for_job", new=boom),
        patch("aaiclick.__main__._run_internal_api", new=_passthrough_run_internal_api),
    ):
        with pytest.raises(SystemExit):
            await _run_job_wait(args)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "mod.stuck" in captured.err


def _stub_setup_cli(monkeypatch, *, isatty: bool) -> list[bool]:
    """Stub the setup CLI's collaborators; returns the ``force`` values forwarded."""
    calls: list[bool] = []
    monkeypatch.setattr("aaiclick.__main__.setup_api.stale_local_db", lambda: ["jobs.tenant_id"])
    monkeypatch.setattr("aaiclick.__main__.setup_api.stale_local_db_message", lambda stale: f"stale: {stale}")
    monkeypatch.setattr("sys.stdin.isatty", lambda: isatty)
    monkeypatch.setattr("aaiclick.__main__.setup_api.setup", lambda *, ai, force: calls.append(force))
    monkeypatch.setattr("aaiclick.__main__._render", lambda *a, **k: None)
    return calls


def _run_setup_main():
    with patch("sys.argv", ["aaiclick", "setup"]):
        main()


def test_setup_prompts_before_recreating_stale_local_db(monkeypatch, capsys):
    """An interactive run offers to recreate a local.db left by an older
    version, and forwards the answer as ``force``."""
    calls = _stub_setup_cli(monkeypatch, isatty=True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "y")

    _run_setup_main()

    assert calls == [True]
    assert "jobs.tenant_id" in capsys.readouterr().err


def test_setup_declined_prompt_leaves_database_alone(monkeypatch):
    """Answering no forwards ``force=False``, so setup refuses rather than
    deleting the database behind the user's back."""
    calls = _stub_setup_cli(monkeypatch, isatty=True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "")

    _run_setup_main()

    assert calls == [False]


def test_setup_does_not_prompt_when_not_a_tty(monkeypatch):
    """A piped run must never block on input — setup() reports instead."""
    calls = _stub_setup_cli(monkeypatch, isatty=False)

    def _no_input(_prompt=""):
        raise AssertionError("must not prompt without a TTY")

    monkeypatch.setattr("builtins.input", _no_input)

    _run_setup_main()

    assert calls == [False]
