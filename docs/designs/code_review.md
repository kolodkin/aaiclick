Code Review — 2026-10-18
---

Open findings from a whole-repo audit (the first round's security findings are fixed),
assuming a distributed deployment with several workers. All come from reading the code, none reproduced at runtime.
Remove an entry once its fix lands.

# Open Findings

- **Returned shell children choose their own build:** an `out.json` child may declare any `git_remote` / `dockerfile`, so a container can make the worker build any image. Upstream and `group_results` resolution still has no `job_id` filter; only the returned-handle check guards it (`aaiclick/orchestration/execution/runner.py`).
- **`stage_in_json` leaks its temp dir on failure:** the `aaiclick-io-*` dir is created before kwarg resolution; a resolve error propagates before any caller learns the path, leaking one empty dir per failed attempt (`aaiclick/orchestration/execution/runner.py`).
- **`LazyChClient.close()` before first use can reconnect:** with `_client` still `None` nothing is marked closed, so a holder calling `query()` later opens a connection nothing closes; add a `_closed` flag (`aaiclick/data/object_context/ch_client.py`).
- **mp parent's "never query ClickHouse" is unenforced:** since #489 every `orch_context` installs a `LazyChClient`, so an accidental parent-side query silently takes the chdb lock instead of raising (`aaiclick/orchestration/orch_context.py`, `aaiclick/orchestration/execution/mp_worker.py`).
- **Username race:** a concurrent duplicate username returns 500 instead of 409 (`aaiclick/auth/store.py`).
- **Reset links outlive a password change:** they stay usable after the password is set (`aaiclick/internal_api/password_reset.py`).
- **Duplicate insert helpers:** `create_user` and `create_refresh_token` repeat `_insert` (`aaiclick/auth/store.py`).
- **Header parsed twice:** the Authorization header is parsed in both `PrincipalAuthMiddleware` and `resolve_principal` (`aaiclick/server/auth.py`).

# Not Covered

- **Not read:**
  - the Java SDK and the web UI (`src/`)
  - migrations and examples
  - most of `aaiclick/data/object/object.py` and `operators.py`
  - the Docker and Kubernetes image builds
- **Not run:** the project's test suite.
