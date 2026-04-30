"""Tests for job query functions."""

from ..factories import create_job
from ..models import JOB_COMPLETED, JOB_PENDING, JobStatus
from .queries import count_jobs, get_job, list_jobs


async def test_get_job(orch_ctx):
    """Test retrieving a job by ID."""
    job = await create_job("test_get_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    result = await get_job(job.id)
    assert result is not None
    assert result.id == job.id
    assert result.name == "test_get_job"
    assert result.status == JOB_PENDING


async def test_get_job_not_found(orch_ctx):
    """Test get_job returns None for non-existent ID."""
    result = await get_job(999999999)
    assert result is None


async def test_list_jobs_basic(orch_ctx):
    """Test listing jobs returns results."""
    await create_job("list_basic_a", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await create_job("list_basic_b", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    jobs = await list_jobs()
    names = [j.name for j in jobs]
    assert "list_basic_a" in names
    assert "list_basic_b" in names


async def test_list_jobs_filter_by_status(orch_ctx):
    """Test filtering jobs by status."""
    await create_job("status_filter", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    pending = await list_jobs(status=JOB_PENDING)
    pending_names = [j.name for j in pending]
    assert "status_filter" in pending_names

    completed = await list_jobs(status=JOB_COMPLETED)
    completed_names = [j.name for j in completed]
    assert "status_filter" not in completed_names


async def test_list_jobs_filter_by_name_like(orch_ctx):
    """Test filtering jobs by name pattern."""
    await create_job("etl_pipeline_daily", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await create_job("ml_training_run", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    etl_jobs = await list_jobs(name_like="%etl%")
    names = [j.name for j in etl_jobs]
    assert "etl_pipeline_daily" in names
    assert "ml_training_run" not in names


async def test_list_jobs_limit_and_offset(orch_ctx):
    """Test pagination with limit and offset."""
    for i in range(5):
        await create_job(f"paginate_{i}", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    page1 = await list_jobs(name_like="paginate_%", limit=2, offset=0)
    page2 = await list_jobs(name_like="paginate_%", limit=2, offset=2)

    assert len(page1) == 2
    assert len(page2) == 2
    # Pages should not overlap
    page1_ids = {j.id for j in page1}
    page2_ids = {j.id for j in page2}
    assert page1_ids.isdisjoint(page2_ids)


async def test_count_jobs(orch_ctx):
    """Test counting jobs with filters."""
    await create_job("count_test_a", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    await create_job("count_test_b", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    total = await count_jobs(name_like="count_test_%")
    assert total >= 2

    pending = await count_jobs(status=JOB_PENDING, name_like="count_test_%")
    assert pending >= 2

    completed = await count_jobs(status=JOB_COMPLETED, name_like="count_test_%")
    assert completed == 0
