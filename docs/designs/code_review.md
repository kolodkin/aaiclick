Code Review — 2026-10-18
---

Open findings from a whole-repo audit (the first round's security findings are fixed),
assuming a distributed deployment with several workers. All come from reading the code, none reproduced at runtime.
Remove an entry once its fix lands.

# Open Findings

- **mp parent's "never query ClickHouse" is unenforced:** since #489 every `orch_context` installs a `LazyChClient`, so an accidental parent-side query silently takes the chdb lock. A blanket refusal won't do: under chdb the parent's `ChLogSink` writes container task logs through that client (`aaiclick/orchestration/execution/mp_worker.py`).

# Not Covered

- **Not read:**
  - the Java SDK and the web UI (`src/`)
  - migrations and examples
  - most of `aaiclick/data/object/object.py` and `operators.py`
  - the Docker and Kubernetes image builds
- **Not run:** the project's test suite.
