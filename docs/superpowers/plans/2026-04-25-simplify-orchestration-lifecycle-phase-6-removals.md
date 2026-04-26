Phase 6 — Removals
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Delete dead code now that the new lifecycle is the active one.

Targets:

- `OrchLifecycleHandler` (entire class) and any `_lifecycle_var.set(OrchLifecycleHandler(...))` call sites.
- `PreservationMode` enum + every reference to it.
- `resolve_job_config()` and the `Job.preservation_mode` / `RegisteredJob.preservation_mode` ORM columns.
- `AAICLICK_DEFAULT_PRESERVATION_MODE` env var + `get_default_preservation_mode()`.
- `TableRunRef` and `TableContextRef` SQLModel classes (the SQL tables are already gone via the Phase 1 migration; only the Python classes remain).
- `BackgroundWorker._cleanup_unreferenced_tables()` (replaced by `_cleanup_orphan_scratch_tables` and `_cleanup_at_job_completion`).
- The `oplog_dispatch` / `DBLifecycleOp` / `DBLifecycleMessage` machinery if it was only used by `OrchLifecycleHandler` (verify before deleting).

After this phase the codebase has no references to the old lifecycle.

---

## Task 1: Delete `_cleanup_unreferenced_tables()`

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`
- Modify: any test that exercises it directly.

- [ ] **Step 1: Find call sites**

```bash
grep -rn "_cleanup_unreferenced_tables\|cleanup_unreferenced_tables" /home/user/aaiclick/aaiclick/
```

- [ ] **Step 2: Delete the method**

Remove the `async def _cleanup_unreferenced_tables` definition from `background_worker.py`. Remove the call from `BackgroundWorker._run_loop` (or whichever method drives the polling cycle).

- [ ] **Step 3: Delete or repurpose its tests**

If `aaiclick/orchestration/background/test_cleanup.py` has tests targeting this method specifically, delete them. The new methods (`_cleanup_orphan_scratch_tables`, `_cleanup_at_job_completion`) have their own coverage from Phase 5.

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_cleanup.py
git commit -m "cleanup: remove _cleanup_unreferenced_tables"
```

---

## Task 2: Delete `OrchLifecycleHandler` and its call sites

**Files:**
- Modify: `aaiclick/orchestration/orch_context.py`
- Modify: anything that imports `OrchLifecycleHandler`.

- [ ] **Step 1: Find references**

```bash
grep -rn "OrchLifecycleHandler" /home/user/aaiclick/aaiclick/
```

- [ ] **Step 2: Delete the class**

Remove the `class OrchLifecycleHandler(LifecycleHandler):` block (and all its methods) from `orch_context.py`. Verify that the rewritten `task_scope()` (Phase 4) doesn't reference it.

- [ ] **Step 3: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/orch_context.py
git commit -m "cleanup: remove OrchLifecycleHandler"
```

---

## Task 3: Delete `TableRunRef` and `TableContextRef`

**Files:**
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py`
- Modify: any importer.

- [ ] **Step 1: Find references**

```bash
grep -rn "TableRunRef\|TableContextRef\|table_run_refs\|table_context_refs" /home/user/aaiclick/aaiclick/
```

The Phase 1 migration already dropped the SQL tables; this step removes the Python ORM classes and any helper functions that talked to them.

- [ ] **Step 2: Delete classes + helpers**

Remove the `TableRunRef` and `TableContextRef` class definitions from `db_lifecycle.py`. Remove any module-level helper functions whose only purpose was to operate on these tables (e.g. `incref_table_run`, `migrate_table_registry_to_sql` if it touches these — verify).

- [ ] **Step 3: Delete `DBLifecycleOp` / `DBLifecycleMessage` if unused**

```bash
grep -rn "DBLifecycleOp\|DBLifecycleMessage" /home/user/aaiclick/aaiclick/
```

If the only references were in `OrchLifecycleHandler` (now deleted), drop these too.

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/lifecycle/db_lifecycle.py
git commit -m "cleanup: remove TableRunRef, TableContextRef, and DBLifecycleOp scaffolding"
```

---

## Task 4: Delete `PreservationMode` enum and column

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Modify: `aaiclick/orchestration/factories.py`
- Modify: any other importer.

- [ ] **Step 1: Find references**

```bash
grep -rn "PreservationMode\|preservation_mode" /home/user/aaiclick/aaiclick/
```

- [ ] **Step 2: Remove the enum and the columns**

In `models.py`:
- Delete `class PreservationMode(StrEnum): ...`.
- Delete `preservation_mode` column from `Job` and `RegisteredJob`.

In `factories.py`:
- Delete `resolve_job_config()`.
- Remove any `job.preservation_mode = ...` assignments.

- [ ] **Step 3: Update `docs/future.md`**

Open `docs/future.md`. Find the entry "Switch DB Enums from StrEnum to Literal + sa_column" — remove `PreservationMode` from its list of affected enums (or delete the bullet entirely if `PreservationMode` was the only one).

- [ ] **Step 4: Update `CLAUDE.md` if applicable**

```bash
grep -n "PreservationMode\|preservation_mode\|resolve_job_config" /home/user/aaiclick/CLAUDE.md
```

If found, replace the example with one using `resolve_preserve()` or remove the snippet entirely.

- [ ] **Step 5: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/models.py aaiclick/orchestration/factories.py docs/future.md CLAUDE.md
git commit -m "$(cat <<'EOF'
cleanup: remove PreservationMode enum and resolve_job_config()

The Phase 1 migration already dropped the SQL columns; this commit
removes the Python types and the resolution helper. resolve_preserve()
is the new entry point.
EOF
)"
```

---

## Task 5: Delete `AAICLICK_DEFAULT_PRESERVATION_MODE`

**Files:**
- Modify: `aaiclick/orchestration/env.py`

- [ ] **Step 1: Find references**

```bash
grep -rn "AAICLICK_DEFAULT_PRESERVATION_MODE\|get_default_preservation_mode" /home/user/aaiclick/
```

Includes docs and tests.

- [ ] **Step 2: Delete the helper**

Remove `get_default_preservation_mode()` from `env.py`. Remove any documentation references.

- [ ] **Step 3: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/env.py
git commit -m "cleanup: drop AAICLICK_DEFAULT_PRESERVATION_MODE env var"
```

---

## Task 6: Confirm clean grep

- [ ] **Step 1: Run the acceptance grep**

```bash
grep -rn "PreservationMode\|OrchLifecycleHandler\|table_run_refs\|table_context_refs\|resolve_job_config\|AAICLICK_DEFAULT_PRESERVATION_MODE" aaiclick/
```

Expected: no output (the acceptance criterion from the master plan).

If there are lingering hits in tests that exercised removed code, delete those tests outright — don't try to "adapt" them to a different surface; their purpose is gone.

- [ ] **Step 2: Run full repo test suite**

```bash
cd /home/user/aaiclick && pytest aaiclick/ -x --no-cov -q
```

Expected: PASS.

- [ ] **Step 3: Commit any final cleanup**

```bash
git status
# If anything remains, commit it.
git add -p   # review hunks
git commit -m "cleanup: residual references to removed lifecycle types"
```

- [ ] **Step 4: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-gwqt4
```

---

# Done When

- The acceptance grep returns empty.
- `pytest aaiclick/ -x` is green.
- `docs/future.md` and `CLAUDE.md` no longer reference `PreservationMode`.
