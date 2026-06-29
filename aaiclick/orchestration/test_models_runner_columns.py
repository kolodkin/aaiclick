from aaiclick.orchestration.models import Job, RegisteredJob, Task


def test_task_has_entry_columns():
    cols = set(Task.model_fields)
    assert {"entry_type", "command", "command_env"} <= cols


def test_job_has_runner_column():
    assert "runner" in Job.model_fields


def test_registered_job_has_runner_column():
    assert "runner" in RegisteredJob.model_fields


def test_job_flat_runner_columns_removed():
    gone = {"git_remote", "git_sha", "git_branch", "dockerfile", "image_tag", "kubernetes_config"}
    assert gone.isdisjoint(Job.model_fields)


def test_registered_job_keeps_default_columns():
    cols = set(RegisteredJob.model_fields)
    assert {"git_remote", "dockerfile", "kubernetes_config", "runner"} <= cols
