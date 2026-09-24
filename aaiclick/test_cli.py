"""Tests for the argparse CLI: parsers, ``main()`` dispatch, and handler output."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlmodel import select

from aaiclick.__main__ import (
    _load_lineage_ai,
    _parse_command_env,
    _parse_set_kwargs,
    _run_data_api,
    build_parser,
    main,
)
from aaiclick.data.data_context.ch_client import get_ch_client
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_FAILED, Job, JobStatus, RegisteredJob, Task
from aaiclick.orchestration.registered_jobs import run_job
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.testing import run_cli
from aaiclick.view_models import LineageAnswer

# A real importable callable, so ``register-job`` passes entrypoint validation.
_VALID_ENTRYPOINT = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _only_task() -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task))).scalar_one()


async def _only_registered_job() -> RegisteredJob:
    async with get_sql_session() as session:
        return (await session.execute(select(RegisteredJob))).scalar_one()


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


def test_parse_command_env_pairs():
    assert _parse_command_env(["K=v", "A=b=c"]) == {"K": "v", "A": "b=c"}
    assert _parse_command_env(None) is None
    assert _parse_command_env([]) is None


def test_parse_command_env_rejects_missing_equals():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_command_env(["bad"])


async def test_run_job_persists_shell_flags(orch_ctx):
    await run_cli(
        "run-job",
        "j",
        "--entry-type",
        "shell",
        "--command",
        "python main.py --flag",
        "--command-env",
        "K=v",
    )

    task = await _only_task()
    assert task.entry_type == "shell"
    assert task.command == ["python", "main.py", "--flag"]
    assert task.command_env == {"K": "v"}


async def test_run_job_forwards_image_to_the_runner_check(orch_ctx):
    """``--image`` reaches ``run_job``, whose subprocess runner has no image to
    run and refuses it rather than silently dropping it."""
    with pytest.raises(ValueError, match="image require a docker/kubernetes registered job"):
        await run_cli("run-job", "j", "--entry-type", "shell", "--command", "true", "--image", "python:3.12")


async def test_register_job_persists_image(orch_ctx, capsys):
    await run_cli("register-job", "myapp.jobs.etl", "--image", "myrepo/img:1")

    registered = await _only_registered_job()
    assert registered.image == "myrepo/img:1"
    assert f"Registered job 'etl' (id={registered.id})" in capsys.readouterr().out


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


def _fake_lineage_ai() -> MagicMock:
    """Fakes ``internal_api.lineage_ai`` (the LLM seam), so no ``ai`` extra or model is needed.

    Each fake reads ``get_ch_client()``, as the real oplog graph and debug agent do, so the
    CLI must run them with ClickHouse attached or the fake raises.
    """

    async def explain_lineage(table: str, *, question: str | None = None) -> LineageAnswer:
        get_ch_client()
        return LineageAnswer(target_table=table, question=question, answer="explain-answer")

    async def debug_result(table: str, *, question: str, max_iterations: int) -> LineageAnswer:
        get_ch_client()
        return LineageAnswer(target_table=table, question=question, answer="debug-answer")

    module = MagicMock()
    module.explain_lineage = AsyncMock(side_effect=explain_lineage)
    module.debug_result = AsyncMock(side_effect=debug_result)
    return module


async def test_explain_forwards_question_and_prints_answer(orch_ctx, capsys):
    lineage_ai = _fake_lineage_ai()
    with patch("aaiclick.__main__._load_lineage_ai", return_value=lineage_ai):
        await run_cli("explain", "p_revenue", "Which join fed this?")

    lineage_ai.explain_lineage.assert_awaited_once_with("p_revenue", question="Which join fed this?")
    assert capsys.readouterr().out == "explain-answer\n"


async def test_debug_forwards_max_iterations_and_prints_answer(orch_ctx, capsys):
    lineage_ai = _fake_lineage_ai()
    with patch("aaiclick.__main__._load_lineage_ai", return_value=lineage_ai):
        await run_cli("debug", "p_revenue", "Why?", "--max-iterations", "3")

    lineage_ai.debug_result.assert_awaited_once_with("p_revenue", question="Why?", max_iterations=3)
    assert capsys.readouterr().out == "debug-answer\n"


def test_load_lineage_ai_exits_with_install_hint_without_ai_extra(capsys):
    with (
        patch("aaiclick.ai.importing.importlib.import_module", side_effect=ImportError("No module named 'litellm'")),
        pytest.raises(SystemExit) as exc_info,
    ):
        _load_lineage_ai()

    assert exc_info.value.code == 1
    assert "pip install aaiclick[ai]" in capsys.readouterr().err


def test_user_role_parsers():
    parser = build_parser()
    args = parser.parse_args(["user", "create", "u", "--role", "admin"])
    assert args.user_command == "create" and args.role == "admin"
    args = parser.parse_args(["user", "set-role", "7", "member"])
    assert args.user_command == "set-role" and args.user_id == 7 and args.role == "member"
    with pytest.raises(SystemExit):
        parser.parse_args(["user", "set-role", "7", "superadmin"])


async def test_run_data_api_provides_a_sql_session():
    """``aaiclick data`` commands need an orch context, not a bare data context.

    Deliberately takes no ``orch_ctx`` fixture: an ambient orch context leaves
    the SQL engine contextvar set and hides the defect.
    """

    async def _uses_sql_session() -> bool:
        async with get_sql_session():
            return True

    assert await _run_data_api(_uses_sql_session())


@pytest.mark.parametrize(
    "pairs, expected",
    [
        # ``--set`` has no type annotations to lean on, so values are
        # JSON-parsed: a job expecting ``int`` must not receive ``"300"``.
        pytest.param(
            ["corpus_size=300", "generate=true", "ratio=0.5", "ids=[1,2]"],
            {"corpus_size": 300, "generate": True, "ratio": 0.5, "ids": [1, 2]},
            id="json-typed-values",
        ),
        pytest.param(
            ["name=hello", "path=/tmp/x", 'quoted="hi"'],
            {"name": "hello", "path": "/tmp/x", "quoted": "hi"},
            id="raw-string-fallback",
        ),
        pytest.param(["expr=a=b"], {"expr": "a=b"}, id="splits-on-first-equals-only"),
        pytest.param(None, {}, id="unset-is-empty"),
    ],
)
def test_parse_set_kwargs(pairs, expected):
    """``_parse_set_kwargs`` is a pure parser; its output is the contract."""
    assert _parse_set_kwargs(pairs) == expected


def test_parse_set_kwargs_rejects_missing_equals():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_set_kwargs(["bad"])


async def test_run_job_set_takes_precedence_over_kwargs_json(orch_ctx):
    """Documented precedence: default_kwargs < --kwargs < --set."""
    await run_cli("run-job", "j", "--kwargs", '{"corpus_size": 10, "keep": 1}', "--set", "corpus_size=300")

    assert (await _only_task()).kwargs == {"corpus_size": 300, "keep": 1}


async def test_register_job_set_merges_into_default_kwargs(orch_ctx):
    await run_cli("register-job", _VALID_ENTRYPOINT, "--set", "corpus_size=300")

    assert (await _only_registered_job()).default_kwargs == {"corpus_size": 300}


async def _seed_job(job_status: JobStatus | None, entrypoint: str, *, task_error: str | None = None) -> int:
    """Create a one-task job; set it to ``job_status`` with a failed task when
    ``task_error`` is given. ``None`` leaves the fresh job non-terminal."""
    job = await run_job("waitable", entrypoint)
    async with get_sql_session() as session:
        if job_status is not None:
            row = await session.get(Job, job.id)
            assert row is not None
            row.status = job_status
            session.add(row)
        if task_error is not None:
            task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
            task.status = TASK_FAILED
            task.error = task_error
            session.add(task)
        await session.commit()
    return job.id


async def test_job_wait_exits_nonzero_and_reports_the_failed_task(orch_ctx, capsys):
    """CI and ``set -e`` scripts key off the exit code; the operator keys off
    knowing which task blew up."""
    job_id = await _seed_job(JOB_FAILED, "mod.exploding_task", task_error="boom")

    with pytest.raises(SystemExit) as exc:
        await run_cli("job", "wait", str(job_id))

    assert exc.value.code == 1
    assert "exploding_task" in capsys.readouterr().out


async def test_job_wait_returns_cleanly_when_job_completed(orch_ctx):
    job_id = await _seed_job(JOB_COMPLETED, "mod.ok")

    await run_cli("job", "wait", str(job_id))


async def test_job_wait_timeout_exits_nonzero(orch_ctx, capsys):
    job_id = await _seed_job(None, "mod.stuck_task")

    with pytest.raises(SystemExit) as exc:
        await run_cli("job", "wait", str(job_id), "--timeout", "0")

    assert exc.value.code == 1
    assert "did not reach a terminal" in capsys.readouterr().err


async def test_run_job_does_not_block_without_progress(orch_ctx):
    """``run-job`` stays fire-and-forget by default."""
    with patch("aaiclick.__main__.cli_wait.wait_for_job") as waiter:
        await run_cli("run-job", "j")

    waiter.assert_not_called()


async def test_job_wait_json_output_stays_parseable_on_failure(orch_ctx, capsys):
    """``--json`` must emit exactly one JSON document — a trailing human-readable
    failure block would break ``jq`` consumers."""
    job_id = await _seed_job(JOB_FAILED, "mod.exploding_task", task_error="boom")

    with pytest.raises(SystemExit):
        await run_cli("job", "wait", str(job_id), "--json")

    json.loads(capsys.readouterr().out)


async def test_job_wait_json_timeout_keeps_stdout_clean_and_diagnoses_on_stderr(orch_ctx, capsys):
    """Under --json nothing has streamed progress, so the timeout still has to
    name the stuck task — on stderr, leaving stdout parseable."""
    job_id = await _seed_job(None, "mod.stuck_task")

    with pytest.raises(SystemExit):
        await run_cli("job", "wait", str(job_id), "--json", "--timeout", "0")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "stuck_task" in captured.err


def _stub_setup_cli(monkeypatch, *, isatty: bool) -> list[bool]:
    """Stub the setup CLI's collaborators; returns the ``force`` values forwarded."""
    calls: list[bool] = []
    monkeypatch.setattr("aaiclick.__main__.setup_api.stale_local_db_reason", lambda: "stale: jobs.error")
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
    assert "jobs.error" in capsys.readouterr().err


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


def test_token_parser_flags():
    parser = build_parser()
    args = parser.parse_args(["token", "create", "alice", "--name", "ci", "--scope", "write", "--expires-days", "30"])
    assert args.command == "token" and args.token_command == "create"
    assert args.username == "alice" and args.name == "ci" and args.scope == "write" and args.expires_days == 30
    args = parser.parse_args(["token", "create", "alice", "--name", "ro"])
    assert args.scope == "read" and args.expires_days is None
    args = parser.parse_args(["token", "revoke", "alice", "42"])
    assert args.token_command == "revoke" and args.token_id == 42


def test_user_parser_new_commands():
    parser = build_parser()
    args = parser.parse_args(["user", "create", "sso_only", "--email", "s@example.com"])
    assert args.password is None and args.email == "s@example.com"
    assert parser.parse_args(["user", "enable", "7"]).user_command == "enable"
    assert parser.parse_args(["user", "reset-mfa", "7"]).user_command == "reset-mfa"
    args = parser.parse_args(["user", "set-email", "7"])
    assert args.user_command == "set-email" and args.email is None
    assert parser.parse_args(["user", "reset-link", "7"]).user_command == "reset-link"


def test_audit_parser_flags():
    parser = build_parser()
    args = parser.parse_args(
        ["audit", "list", "--username", "alice", "--method", "POST", "--path", "/api/v0/jobs", "--since", "2026-01-01"]
    )
    assert args.command == "audit" and args.audit_command == "list"
    assert (
        args.username == "alice"
        and args.method == "POST"
        and args.path == "/api/v0/jobs"
        and args.since == "2026-01-01"
    )


def test_token_parser_accepts_every_scope():
    parser = build_parser()
    for level in ("read", "write", "admin"):
        args = parser.parse_args(["token", "create", "alice", "--name", "ci", "--scope", level])
        assert args.scope == level
    assert parser.parse_args(["token", "create", "alice", "--name", "ci"]).scope == "read"


def test_user_invite_parser():
    parser = build_parser()
    args = parser.parse_args(["user", "invite", "alice", "--email", "a@example.com", "--role", "admin"])
    assert args.user_command == "invite" and args.username == "alice"
    assert args.email == "a@example.com" and args.role == "admin"
    assert parser.parse_args(["user", "invite", "bob"]).role == "viewer"
