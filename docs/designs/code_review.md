Code Review — 2026-10-18
---

Open findings from a whole-repo audit (the first round's security findings are fixed),
assuming a distributed deployment with several workers. All come from reading the code, none reproduced at runtime.
Remove an entry once its fix lands.

# Open Findings

- **Upstream refs have no `job_id` filter:** `upstream` / `group_results` resolution reads any task's result by id; only the returned-handle check stops a shell/jvm child naming another job's task (`aaiclick/orchestration/execution/runner.py`).

# Not Covered

- **Not read:**
  - the Java SDK and the web UI (`src/`)
  - migrations and examples
  - most of `aaiclick/data/object/object.py` and `operators.py`
  - the Docker and Kubernetes image builds
- **Not run:** the project's test suite.
