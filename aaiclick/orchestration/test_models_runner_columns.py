from aaiclick.orchestration.models import Job, RegisteredJob, Task


def test_task_has_entry_columns():
    cols = Task.__table__.columns.keys()
    assert {"entry_type", "command", "command_env"} <= set(cols)


def test_job_has_runner_column():
    assert "runner" in Job.__table__.columns.keys()


def test_registered_job_has_runner_column():
    assert "runner" in RegisteredJob.__table__.columns.keys()
