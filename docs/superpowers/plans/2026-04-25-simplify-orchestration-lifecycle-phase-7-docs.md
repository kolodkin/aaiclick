Phase 7 — Docs & Examples
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Bring docs and examples in line with the new lifecycle. After this phase, anyone reading `docs/orchestration.md` learns the new model and there are no stale references to `PreservationMode` outside of the spec itself.

---

## Task 1: Rewrite the lifecycle section of `docs/orchestration.md`

**Files:**
- Modify: `docs/orchestration.md`

- [ ] **Step 1: Skim current content**

```bash
grep -n "^# \|^## " /home/user/aaiclick/docs/orchestration.md
```

Note which sections cover lifecycle, table scoping, `PreservationMode`. These are the targets.

- [ ] **Step 2: Update the lifecycle section**

Replace the lifecycle prose with a tightened version. Use the `# Cleanup ownership` section from the spec as the source of truth — copy the table verbatim. Add a short prose intro and a link back to the spec for the deeper rationale.

```markdown
# Lifecycle

Tasks are conceptually `DataContext`s with three orchestration extras: pin/unpin
for cross-task Object handoff, `task_name_locks` for non-preserved named tables,
and a `preserve` list of named tables that survive the run.

| Table type                              | On successful task exit          | On task failure / worker death | At job completion          |
|-----------------------------------------|----------------------------------|--------------------------------|----------------------------|
| `t_*` not pinned (pure scratch)         | Inline DROP by task              | BackgroundWorker sweeps        | n/a                        |
| `t_*` pinned (Object output)            | Leave (pin-controlled)           | Leave (pin-controlled)         | BackgroundWorker drops     |
| `j_<id>_<name>`, not preserved          | Inline DROP by task              | Leave for job-end cleanup      | BackgroundWorker drops     |
| `j_<id>_<name>`, preserved              | Leave (job-end)                  | Leave (job-end)                | BackgroundWorker drops     |
| `p_*` (persistent)                      | Never                            | Never                          | Never (user-managed)       |

See the design spec at `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`
for the rationale and migration history.
```

- [ ] **Step 3: Update the API section**

Replace the `PreservationMode` section with `preserve`:

````markdown
## Declaring preserved tables

`create_job(preserve=...)` declares which named tables should survive the run.

```python
job = await create_job(
    registered_name="train_embeddings",
    preserve=["training_set", "vocab"],
)
```

`preserve` accepts:

- `None` — nothing preserved (default).
- `list[str]` — those names are preserved.
- `"*"` — every `j_<id>_<name>` created during the job is preserved.
- `[]` — explicitly nothing preserved (does NOT fall through to the RegisteredJob default).

Resolution order: explicit > `RegisteredJob.preserve` > `None`.

Inside a task, `ctx.table("name")` either:

- creates `j_<job_id>_<name>` once for the job (when the name is preserved or
  `preserve == "*"`), with idempotent `CREATE IF NOT EXISTS`, or
- creates `j_<job_id>_<name>` task-locally (when not preserved), guarded by
  `task_name_locks` so two concurrent tasks can't take the same name. Collision
  raises `TableNameCollision`.
````

- [ ] **Step 4: Run a markdown lint pass if configured**

```bash
grep -n "markdownlint\|prettier" /home/user/aaiclick/pyproject.toml /home/user/aaiclick/.pre-commit-config.yaml 2>/dev/null
```

If a linter is configured, run it on `docs/orchestration.md`. Otherwise skip.

- [ ] **Step 5: Commit**

```bash
git add docs/orchestration.md
git commit -m "$(cat <<'EOF'
cleanup: rewrite lifecycle section in docs/orchestration.md

Replaces the PreservationMode-era lifecycle description with the new
preserve-list / task_name_locks model. Cleanup-ownership table aligns
with the design spec.
EOF
)"
```

---

## Task 2: Update example scripts

**Files:**
- Modify any file in `aaiclick/orchestration/examples/` that references `PreservationMode` (Phase 6 should have already removed direct uses, but example output / comments may still mention the old name).

- [ ] **Step 1: Find references**

```bash
grep -rn "PreservationMode\|preservation_mode\|preserve" /home/user/aaiclick/aaiclick/orchestration/examples/
```

- [ ] **Step 2: Add a `preserve=` example**

Pick the most-representative example (likely `orchestration_basic.py`). Add a small block demonstrating `preserve`:

```python
# Declare a preserved named table; survives the entire run, dropped at job end.
job = await create_job(
    registered_name="example_preserve",
    preserve=["training_set"],
)
```

Match the file's existing style (output annotations, etc.). Add the file's `# →` output comment for any `print()` calls that show the resolved value, per the project's example-file convention in `CLAUDE.md`.

- [ ] **Step 3: Run the example to verify it works**

If the project has a runner for examples (check the README or CLAUDE.md), use it. Otherwise:

```bash
cd /home/user/aaiclick && python -m aaiclick.orchestration.examples.orchestration_basic
```

Expected: clean run.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/examples/
git commit -m "feature: example showing preserve=[...] in create_job"
```

---

## Task 3: Final acceptance pass

- [ ] **Step 1: Re-read the spec**

```bash
cat /home/user/aaiclick/docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md
```

For each "Done When" criterion in the master plan, verify it's met.

- [ ] **Step 2: Run the full test suite**

```bash
cd /home/user/aaiclick && pytest aaiclick/ -x --no-cov
```

Expected: PASS.

- [ ] **Step 3: Run the migration round-trip one more time**

```bash
cd /home/user/aaiclick && rm -f .pytest_aaiclick.sqlite && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini upgrade head && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini downgrade -1 && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini upgrade head
```

Expected: all three commands succeed.

- [ ] **Step 4: Run the acceptance grep**

```bash
grep -rn "PreservationMode\|OrchLifecycleHandler\|table_run_refs\|table_context_refs\|resolve_job_config\|AAICLICK_DEFAULT_PRESERVATION_MODE" /home/user/aaiclick/aaiclick/
```

Expected: no hits.

- [ ] **Step 5: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-plab-lifecycle-YRXOm
```

- [ ] **Step 6: Run the project's check-pr skill**

```
/check-pr
```

(Or the equivalent invocation per CLAUDE.md.) Investigate any CI failure.

---

# Done When

- `docs/orchestration.md` reflects the new lifecycle.
- Examples demonstrate `preserve=`.
- Full test suite passes.
- Migration round-trip works on a fresh DB.
- Acceptance grep is empty.
- CI is green.
