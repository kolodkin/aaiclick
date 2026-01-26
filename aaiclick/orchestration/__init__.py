"""
aaiclick.orchestration - Orchestration backend for job and task management.

This module provides orchestration capabilities for aaiclick, including
job creation, task execution, and workflow management.
"""

from .claiming import claim_next_task, update_job_status, update_task_status
from .context import OrchContext, get_orch_context
from .debug_execution import ajob_test, job_test
from .execution import execute_task, run_job_tasks
from .factories import create_job, create_task
from .logging import capture_task_output, get_logs_dir
from .models import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    Dependency,
    DependencyType,
    Group,
    Job,
    JobStatus,
    Task,
    TasksType,
    TaskStatus,
    Worker,
    WorkerStatus,
)
from .worker import (
    deregister_worker,
    get_worker,
    list_workers,
    register_worker,
    run_worker,
    worker_heartbeat,
    worker_main_loop,
)
