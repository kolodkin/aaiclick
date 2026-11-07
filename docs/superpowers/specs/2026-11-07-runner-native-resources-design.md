Runner-native `resources`
---

# Intent

A deployment runs one container runner (`AAICLICK_RUNNER=docker` or
`kubernetes`); mixed fleets are not supported. The per-job `resources` setting
therefore does not need a shape both runners understand. Today it is a
Kubernetes `ResourceRequirements` dict for every runner, parsed with a
Kubernetes quantity parser at submission, and the docker runner translates
`limits.cpu` / `limits.memory` into `--cpus` / `--memory`. Drop the translation:
each runner takes its own native shape.

# Goals

- Docker `resources` is written in docker's own terms, with no quantity
  conversion.
- Kubernetes `resources` stays the verbatim Pod container `resources`.
- Less code: the shared quantity parser and the docker translation go away.

# Non-goals

- Supporting one `resources` value that works on both runners.
- Per-runner sections inside one value (`{"docker": …, "kubernetes": …}`).
- Submission-time validation of the value's contents. The server does not know
  the deployment's runner, and adding that knowledge is out of scope.

# Approaches

| Approach                                                   | Trade-off                                                                        |
|------------------------------------------------------------|----------------------------------------------------------------------------------|
| **A. One column, runner-specific shape** (chosen)          | Smallest diff; a malformed value fails the task at dispatch, not the submission |
| B. Two columns (`resources` k8s, `docker_resources`)       | Submission validation per field, but a migration, two API fields, both unused on any given deployment |
| C. Keep today's shared k8s shape                           | Nothing to do, but keeps a parser and translation the deployment model does not need |

A was picked: the job row is runner-agnostic by design (the runner is the
worker's, never the job's), so one column whose meaning follows the deployment
matches the existing model.

# Design

**Schema**: no change. `RegisteredJob.resources` / `Job.resources` stay a
nullable JSON dict, resolved `run_job` kwarg → registration default → `None`.

**Submission** (`runner_config.validate_resources`): shrinks to "a mapping or
`None`". `parse_quantity`, `_QUANTITY_*` and `_RESOURCE_SECTIONS` are deleted.
The "resources require an image source" rule stays.

**Docker** (`docker_config.resource_flags`): the dict is a flat map of
`docker run` resource flags, keys without the leading dashes, values passed
through as strings:

```python
run_job(name, image="python:3.12", resources={"cpus": "1.5", "memory": "512m"})
# → docker run --cpus 1.5 --memory 512m …
```

Any key is accepted (`memory-swap`, `cpu-shares`, `gpus`, …); docker itself
rejects what it does not know, and that error fails the task. The implicit
`--memory-swap = --memory` pin is dropped: a user who wants it sets it. Values
are not validated beyond being scalars.

**Kubernetes**: unchanged. The dict is copied verbatim into the Pod container
`resources`.

**Docs**: the user guide "Resources" section becomes two short blocks, one per
runner. `kubernetes_runner.md` "Configuration" drops the "because the docker
runner applies its limits too" rationale. `registered_jobs` docstrings follow.

**Tests**: `test_parse_quantity`, `test_validate_resources_*` quantity cases
and the docker translation cases are replaced by one parametrized
`test_resource_flags` over the flat map and one `validate_resources` mapping
check.

# Left to the builder

- Exact wording of the docker error for a non-scalar value.
- Whether `view_models` needs a docstring touch (it only mentions the field).
