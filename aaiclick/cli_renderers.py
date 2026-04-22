"""Text renderers for the CLI.

Each CLI verb has two output modes: the default human-readable table (one
function per verb here) and ``--json`` (``view.model_dump_json()`` invoked
directly in ``__main__``). Renderers read fields off the view model — never
from SQLModel rows — so the JSON schema and text columns cannot drift.
"""

from __future__ import annotations

from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.orchestration.view_models import (
    JobDetail,
    JobStatsView,
    JobView,
    RegisteredJobView,
    TaskDetail,
    WorkerView,
)
from aaiclick.view_models import (
    MigrationAction,
    MigrationResult,
    ObjectDeleted,
    OllamaBootstrapResult,
    OllamaBootstrapStatus,
    Page,
    PurgeObjectsResult,
    SetupResult,
)


def _fmt_ms(ms: int | None) -> str:
    """Format a millisecond duration as ``123ms`` / ``1.2s`` / ``1m 30.5s``."""
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rem = divmod(seconds, 60)
    return f"{int(minutes)}m {rem:.1f}s"


def _fmt_optional(value: object) -> str:
    """Render ``None`` as a dash; everything else via ``str()``."""
    return "-" if value is None else str(value)


def render_jobs_page(page: Page[JobView], offset: int) -> None:
    """Print a paged list of jobs as an aligned text table."""
    if not page.items:
        print("No jobs found")
        return

    print(f"{'ID':<20} {'Name':<25} {'Status':<12} {'Type':<10} {'Created':<20}")
    print("-" * 87)
    for j in page.items:
        created = j.created_at.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{j.id:<20} {j.name:<25} {j.status.value:<12} {j.run_type.value:<10} {created:<20}")
    total = page.total if page.total is not None else len(page.items)
    print(f"\nShowing {offset + 1}-{offset + len(page.items)} of {total}")


def render_job_detail(detail: JobDetail) -> None:
    """Print full job details — ID, name, status, timestamps, registration."""
    print(f"ID:           {detail.id}")
    print(f"Name:         {detail.name}")
    print(f"Status:       {detail.status.value}")
    print(f"Run type:     {detail.run_type.value}")
    print(f"Registered:   {_fmt_optional(detail.registered_job_id)}")
    print(f"Created at:   {detail.created_at}")
    print(f"Started at:   {_fmt_optional(detail.started_at)}")
    print(f"Completed at: {_fmt_optional(detail.completed_at)}")
    if detail.error:
        print(f"Error:        {detail.error}")


def render_job_stats(stats: JobStatsView) -> None:
    """Print formatted job stats as a markdown table to stdout."""
    print(f"\n## Job: {stats.job_name} (ID: {stats.job_id})")
    print()
    breakdown = [f"{status}: {count}" for status, count in sorted(stats.status_counts.items())]
    print("| Status | Tasks | Wall Time | Exec Time | Breakdown |")
    print("|--------|-------|-----------|-----------|-----------|")
    print(
        f"| {stats.job_status.value} "
        f"| {stats.total_tasks} "
        f"| {_fmt_ms(stats.wall_time_ms)} "
        f"| {_fmt_ms(stats.exec_time_ms)} "
        f"| {', '.join(breakdown)} |"
    )
    print()
    print("| Task | Status | Queue | Exec |")
    print("|------|--------|-------|------|")
    for t in stats.tasks:
        error_suffix = f" `{t.error[:60]}`" if t.error else ""
        print(
            f"| {t.entrypoint} "
            f"| {t.status.value} "
            f"| {_fmt_ms(t.queue_time_ms)} "
            f"| {_fmt_ms(t.exec_time_ms)}{error_suffix} |"
        )
    print()


def render_job_cancelled(view: JobView) -> None:
    """Single-line confirmation that ``internal_api.cancel_job`` succeeded."""
    print(f"Job {view.id} cancelled")


def render_job_created(view: JobView) -> None:
    """Single-line confirmation that ``internal_api.run_job`` created a job."""
    print(f"Job '{view.name}' created (id={view.id}, run_type={view.run_type.value})")


def render_registered_jobs_page(page: Page[RegisteredJobView]) -> None:
    """Print a paged list of registered jobs as an aligned text table."""
    if not page.items:
        print("No registered jobs found")
        return

    print(f"{'ID':<20} {'Name':<25} {'Enabled':<9} {'Schedule':<15} {'Next Run':<20}")
    print("-" * 89)
    for j in page.items:
        next_run = j.next_run_at.strftime("%Y-%m-%d %H:%M:%S") if j.next_run_at else "-"
        print(f"{j.id:<20} {j.name:<25} {str(j.enabled):<9} {_fmt_optional(j.schedule):<15} {next_run:<20}")


def render_registered_job(view: RegisteredJobView) -> None:
    """Print registered-job details — mirrors the old ``register-job`` output."""
    print(f"Registered job '{view.name}' (id={view.id})")
    if view.schedule:
        print(f"  Schedule:         {view.schedule}")
    if view.preservation_mode:
        print(f"  Preservation:     {view.preservation_mode.value}")
    if view.next_run_at:
        print(f"  Next run at:      {view.next_run_at}")


def render_registered_job_enabled(view: RegisteredJobView) -> None:
    """Single-line confirmation that ``internal_api.enable_job`` succeeded."""
    print(f"Job '{view.name}' enabled (id={view.id})")


def render_registered_job_disabled(view: RegisteredJobView) -> None:
    """Single-line confirmation that ``internal_api.disable_job`` succeeded."""
    print(f"Job '{view.name}' disabled (id={view.id})")


def render_task_detail(detail: TaskDetail) -> None:
    """Print full task details — ID, job, entrypoint, status, timings, worker."""
    print(f"ID:           {detail.id}")
    print(f"Job:          {detail.job_id}")
    print(f"Name:         {detail.name}")
    print(f"Entrypoint:   {detail.entrypoint}")
    print(f"Status:       {detail.status.value}")
    print(f"Attempt:      {detail.attempt}")
    print(f"Max retries:  {detail.max_retries}")
    print(f"Created at:   {detail.created_at}")
    print(f"Started at:   {_fmt_optional(detail.started_at)}")
    print(f"Completed at: {_fmt_optional(detail.completed_at)}")
    print(f"Worker:       {_fmt_optional(detail.worker_id)}")
    print(f"Log path:     {_fmt_optional(detail.log_path)}")
    if detail.kwargs:
        print(f"Kwargs:       {detail.kwargs}")
    if detail.result is not None:
        print(f"Result:       {detail.result}")
    if detail.error:
        print(f"Error:        {detail.error}")


def render_workers_page(page: Page[WorkerView], offset: int) -> None:
    """Print a paged list of workers as an aligned text table."""
    if not page.items:
        print("No workers found")
        return

    print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
    print("-" * 80)
    for w in page.items:
        print(
            f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}"
        )
    total = page.total if page.total is not None else len(page.items)
    print(f"\nShowing {offset + 1}-{offset + len(page.items)} of {total}")


def render_worker_stopped(view: WorkerView) -> None:
    """Single-line confirmation that ``internal_api.stop_worker`` succeeded."""
    print(f"Stop requested for worker {view.id}")


def render_objects_page(page: Page[ObjectView]) -> None:
    """Print a paged list of persistent objects as an aligned text table."""
    if not page.items:
        print("No persistent objects found")
        return

    print(f"{'Name':<40} {'Scope':<8} {'Rows':<12} {'Bytes':<12}")
    print("-" * 72)
    for o in page.items:
        print(f"{o.name:<40} {o.scope:<8} {_fmt_optional(o.row_count):<12} {_fmt_optional(o.size_bytes):<12}")
    total = page.total if page.total is not None else len(page.items)
    print(f"\nTotal: {total}")


def render_object_detail(detail: ObjectDetail) -> None:
    """Print full object details — table, scope, schema columns."""
    print(f"Name:      {detail.name}")
    print(f"Table:     {detail.table}")
    print(f"Scope:     {detail.scope}")
    print(f"Rows:      {_fmt_optional(detail.row_count)}")
    print(f"Bytes:     {_fmt_optional(detail.size_bytes)}")
    print(f"Created:   {_fmt_optional(detail.created_at)}")
    print("Columns:")
    for col in detail.table_schema.columns:
        if col.name == "aai_id":
            continue
        print(f"  {col.name}: {col.type}")


def render_object_deleted(view: ObjectDeleted) -> None:
    """Single-line confirmation that ``internal_api.delete_object`` succeeded."""
    print(f"Deleted persistent object '{view.name}'")


def render_objects_purged(result: PurgeObjectsResult) -> None:
    """Print the list of tables dropped by ``internal_api.purge_objects``."""
    if not result.deleted:
        print("No persistent objects matched the filter")
        return
    print(f"Deleted {len(result.deleted)} persistent object(s):")
    for name in result.deleted:
        print(f"  {name}")


_SETUP_STEP_LABELS = {
    "chdb": "chdb",
    "clickhouse": "ClickHouse",
    "sqlite": "SQLite DB",
    "postgres": "PostgreSQL",
}


def render_setup_result(result: SetupResult) -> None:
    """Print ``internal_api.setup`` output — resolved config + per-step outcomes."""
    print(f"Root:    {result.root}")
    print(f"CH URL:  {result.ch_url}")
    print(f"SQL URL: {result.sql_url}")
    print(f"Mode:    {result.mode}")
    for step in result.steps:
        label = _SETUP_STEP_LABELS.get(step.name, step.name)
        marker = "OK" if step.status == "ok" else step.status.upper()
        detail = f" ({step.detail})" if step.detail else ""
        print(f"  {label}: {marker}{detail}")
    if result.ollama is not None:
        print()
        render_ollama_bootstrap(result.ollama)
    print("Setup complete.")


def render_ollama_bootstrap(result: OllamaBootstrapResult) -> None:
    """Print ``internal_api.bootstrap_ollama`` output — server + model status."""
    print(f"AI model: {result.model}")
    if result.status == OllamaBootstrapStatus.NOT_OLLAMA:
        print(f"  {result.detail or 'not an Ollama model'}")
        return
    if result.status == OllamaBootstrapStatus.SERVER_UNREACHABLE:
        print("  ollama server: NOT RUNNING")
        print("  Start with:    ollama serve &")
        print("  Or install:    curl -fsSL https://ollama.com/install.sh | sh")
        return
    print("  ollama server: running")
    if result.status == OllamaBootstrapStatus.ALREADY_PRESENT:
        print(f"  {result.detail}")
    elif result.status == OllamaBootstrapStatus.PULLED:
        print(f"  {result.detail}")
    elif result.status == OllamaBootstrapStatus.FAILED:
        print(f"  {result.detail}")


def render_migration_result(result: MigrationResult) -> None:
    """Confirm upgrade/downgrade success — alembic logs the rest on its own."""
    if result.action == MigrationAction.UPGRADE:
        print(f"Database upgraded to {result.revision}")
    elif result.action == MigrationAction.DOWNGRADE:
        print(f"Database downgraded to {result.revision}")
