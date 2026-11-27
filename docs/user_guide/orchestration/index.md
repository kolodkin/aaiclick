Orchestration
---

Define pipelines with `@task` and `@job`, run them locally with zero
infrastructure, and scale the same code to distributed workers that execute
tasks in subprocesses, Docker containers, or Kubernetes Pods. Backends,
worker commands and environment variables: [Deployment](../deployment.md).

# Quick Start

```python
from aaiclick.orchestration import job, task, job_test, TaskResult

@task
async def add(a: int, b: int) -> int:
    return a + b

@task
async def multiply(x: int, y: int) -> int:
    return x * y

@job("pipeline")
def pipeline(x: int, y: int):
    sum_result = add(a=x, b=y)      # no dependency between tasks → run in parallel
    product = multiply(x=x, y=y)
    return TaskResult(tasks=[sum_result, product])

job_test(pipeline, x=3, y=4)  # create the job and execute it synchronously
```

<!-- req: result-argument-dependency -->
Passing one task's result as another task's argument creates the dependency.
Full script: [Examples: Orchestration Basics](../../examples/orchestration_basic.md);
guided walk-through: [tutorial chapter](../../tutorial/orchestration.md).

# Internal Design

Scheduler, task claiming, table lifecycle, and runner plumbing are covered in
the [orchestration design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/orchestration.md)
(not part of this site).
