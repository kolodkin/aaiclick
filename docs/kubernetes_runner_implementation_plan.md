Kubernetes Runner — Implementation Plan
---

Phased plan for the Kubernetes runner specified in
[kubernetes_runner.md](kubernetes_runner.md). Each phase lands working,
tested code. Mark phases ✅ with file references as they complete.

# Phase 0 — Runner abstraction ✅

Extract the shared lifecycle so a third runner is ~6 methods, not a
near-duplicate module.

- ✅ `TaskVehicle` protocol, `RunnerResult`, `drive_vehicle` in
  `aaiclick/orchestration/execution/worker.py`.
- ✅ Docker and subprocess runners refactored onto the driver
  (`docker_worker._DockerVehicle`, `mp_worker._MpVehicle`).
- ✅ Full execution suite passes unchanged.

# Phase 1 — Schema and config

- `RunnerMode` gains `"kubernetes"` + `RUNNER_KUBERNETES`; widen the
  `runner_mode` CHECK constraints on `jobs` and `registered_jobs`.
- New `TaskRunResult` model (`task_run_results`, PK `(task_id, run_epoch)`).
- New cluster columns on `Job` / `RegisteredJob` (namespace, service_account,
  image_pull_secret, resources).
- `KubernetesJobConfig` + `resolve_kubernetes_config` in a new
  `kubernetes_config.py` (extends the shared Docker git/image resolution).
- Migration via the `generate-migration` skill — never hand-written.

**Deliverable**: models + migration + config resolution, with unit tests for
the three-layer precedence and the constraint widening.

# Phase 2 — Vehicle and Pod entrypoint

- `kubernetes_worker.py`: `KubernetesVehicle` (the six `TaskVehicle` methods)
  and `_pod_main` (mirrors `docker_worker._container_main`, writing a
  `TaskRunResult` row instead of `result.json`).
- `create_kubernetes_job` factory (mirrors `create_docker_job`; same
  build-task injection).
- `run_job` and `dispatch_execute` branches for `RUNNER_KUBERNETES`.

**Deliverable**: a Kubernetes job runs end-to-end against a local cluster,
including cancellation (`poll_cancelled` → Pod delete).

**Success criteria**: vehicle unit tests cover `collect` reading the result
row, synthesized failure on a missing row, cancellation override, and timeout
— the Docker vehicle's test matrix, re-pointed at the Pod transport.

# Phase 3 — CLI and scaffolding

- `register-job --runner kubernetes`; `--namespace` / resource flags on
  `register-job` and `run-job`.
- A `kubernetes init` helper if Pod-spec scaffolding proves useful (the image
  build reuses the existing `docker init` Dockerfile).

# Phase 4 — E2E and CI

- Extract the shared `sample_job` git-daemon fixture so `test_e2e/kubernetes/`
  and `test_e2e/docker/` both use it.
- `test_e2e/kubernetes/test_runner_e2e.py` + `conftest.py` with a
  `kubernetes_e2e` marker and a `kubectl cluster-info` skip guard.
- `_kubernetes-e2e-reusable.yaml` (minikube via `setup-minikube`) and a nightly
  caller, mirroring the Docker workflows.

!!! warning "Resolve minikube↔host-service networking early in Phase 4"
    Pods must reach the registry, Postgres, and ClickHouse. Prototype this
    before wiring the full test — it is the likeliest source of churn.
