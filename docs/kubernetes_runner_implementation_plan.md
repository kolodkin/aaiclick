Kubernetes Runner — Implementation Plan
---

Phased plan for the Kubernetes runner specified in
[kubernetes_runner.md](kubernetes_runner.md). Each phase lands working, tested
code. Mark phases ✅ with file references as they complete.

# Phase 0 — Runner abstraction ✅

Extract the shared lifecycle so a third runner is ~6 methods, not a near-duplicate module.

- ✅ `TaskVehicle` protocol, `RunnerResult`, `drive_vehicle` in
  `aaiclick/orchestration/execution/worker.py`.
- ✅ Docker and subprocess runners refactored onto the driver
  (`docker_worker._DockerVehicle`, `mp_worker._MpVehicle`).
- ✅ Full execution suite passes unchanged.

# Phase 0.5 — Networking spike ✅

Throwaway CI spike (`k8s-net-spike.yaml`, since removed) that de-risked the
minikube↔host paths before any runner code. Findings:

- Pods reach host-side Postgres + ClickHouse via `host.minikube.internal`
  (→ node gateway) — **but only after the `coredns` deployment is ready**;
  launching a Pod too early gives it no DNS.
- The cluster pulls the job image from `host.minikube.internal:5000` over HTTP
  with `--insecure-registry`.
- `kubectl logs -f` streaming and `kubectl delete` work as the vehicle needs.

Conclusion: Phase 5 uses host-side DBs (no in-cluster Postgres/ClickHouse), and
its setup must `kubectl -n kube-system rollout status deploy/coredns` before
submitting task Pods.

# Phase 1 — Shared CLI primitive

Prep refactor (same playbook as Phase 0): extract before adding k8s.

- `execution/cli.py` with `run(...)` (capture) and `run(..., stream=True)` (live
  tee, from `docker_build._stream_to_stdio`) and one error type.
- `docker_worker`, `docker_build`, `docker_config` refactored onto it.
- Rename the shared `AAICLICK_DOCKER_REGISTRY` → `AAICLICK_REGISTRY` (3 code
  sites, the docker e2e workflow, 2 tests); no back-compat shim.

**Deliverable**: one async-subprocess primitive, existing duplication removed,
the registry var renamed, docker tests green.

# Phase 2 — Schema and config

- `RunnerMode` gains `"kubernetes"` + `RUNNER_KUBERNETES`; widen the
  `runner_mode` CHECK constraints on `jobs` and `registered_jobs`.
- `TaskRunResult` model (`task_run_results`, PK `(task_id, run_epoch)`).
- One nullable `kubernetes_config` JSON column on `Job` / `RegisteredJob`.
- `KubernetesConfig` + `resolve_kubernetes_config` in `kubernetes_config.py`
  (reuses the shared Docker git/image resolution).
- Migration via the `generate-migration` skill — never hand-written.

**Deliverable**: models + migration + config resolution, with unit tests for the
three-layer precedence and the constraint widening.

# Phase 3 — Vehicle, logs, and Pod entrypoint

- `kubernetes_worker.py`: `KubernetesVehicle` (the six `TaskVehicle` methods,
  bare Pod with `restartPolicy: Never`) and `_pod_main` (writes a `TaskRunResult`
  row instead of `result.json`).
- Log handling: Pod output to stdout (tee in `capture_task_output`, env-gated);
  `wait()` streams `kubectl logs -f` into a temp file, does an authoritative
  final `kubectl logs` before delete, relocates to the canonical `log_path`.
- `create_kubernetes_job` factory (mirrors `create_docker_job`; same build-task
  injection); `run_job` and `dispatch_execute` branches for `RUNNER_KUBERNETES`.

**Deliverable**: a Kubernetes job runs end-to-end against a local cluster,
including cancellation (`poll_cancelled` → Pod delete) and streamed logs.

**Success criteria**: vehicle unit tests cover `collect` reading the result row,
synthesized failure on a missing row, cancellation override, timeout, and the
log reconcile/relocate — the Docker vehicle's test matrix, re-pointed at the Pod
transport.

# Phase 4 — CLI and scaffolding

- `register-job --runner kubernetes`; `--namespace` / resource flags on
  `register-job` and `run-job`.
- The image build reuses the existing `docker init` Dockerfile — no new scaffold
  unless Pod-spec templating proves useful.

# Phase 5 — E2E and CI

- Extract the shared `sample_job` git-daemon fixture so `test_e2e/kubernetes/`
  and `test_e2e/docker/` both use it.
- `test_e2e/kubernetes/test_runner_e2e.py` + `conftest.py` with a
  `kubernetes_e2e` marker and a `kubectl cluster-info` skip guard.
- `_kubernetes-e2e-reusable.yaml` (minikube via `setup-minikube`) and a nightly
  caller, mirroring the Docker workflows. Apply the Phase 0.5 recipe:
  `--insecure-registry=host.minikube.internal:5000`, wait for `coredns`
  rollout before submitting Pods, and point task DSNs at `host.minikube.internal`.
