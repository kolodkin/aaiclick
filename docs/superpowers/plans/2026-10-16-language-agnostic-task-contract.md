# Language-Agnostic Task Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `_aaiclick_type` discriminator for every tagged dict on the wire, a JSON form for returned tasks/groups with local handles, and an `in.json`/`out.json` file data plane for shell and jvm tasks on every runner.

**Architecture:** `refs.py` stays the single vocabulary; `register_returned_tasks` grows to accept tagged dicts next to Python objects. Shell tasks keep running in the mp child on the subprocess runner (`execute_shell_task` gains the IO dir) and move to host-side `TaskVehicle`s on docker/kubernetes, where the multi-step create/cp/start/cp sequences live. The JVM shim becomes a file-plane shell task with its class name as argv.

**Tech Stack:** Python 3 / asyncio / SQLModel / Pydantic, docker CLI, kubectl, Java 21 + Jackson (shim), pytest.

**Spec:** `docs/superpowers/specs/2026-10-16-language-agnostic-task-contract-design.md`

## Global Constraints

- Discriminator key is exactly `_aaiclick_type`; it is reserved — a dict carrying an unknown value raises `ValueError`.
- `_aaiclick_type` values: `upstream`, `group_results`, `callable`, `object`, `view`, `native`, `pydantic`, `task`, `group`, `task_result`.
- `native` wrapper is `{"_aaiclick_type": "native", "value": v}`; pydantic is `{"_aaiclick_type": "pydantic", "type": "mod.Cls", "data": {...}}`.
- Env var `AAICLICK_IO_DIR`; files `in.json` and `out.json`; container IO dir `/aaiclick`.
- No dual-read of old keys; no DB migration (JSON columns are untyped).
- A string `task_id`/`group_id` is a handle scoped to one return value; an int is a committed id.
- Shell and jvm kwargs may carry only plain values, `native` and `upstream`; everything else is rejected at commit.
- Shell/jvm containers receive `command_env` only — never the runner env.
- Kubernetes helper image default `busybox:1.36`, override `AAICLICK_K8S_IO_IMAGE`; no resources requested for helpers.
- CLAUDE.md rules apply: imports at top, `Literal` over enums, `NamedTuple` over tuples, no `Any` shortcuts, no history comments.

## Review Focus

1. `out.json` that is not valid JSON → the task fails with an error naming the file; the worker does not crash. (Task 4)
2. Two `task` dicts in one return share an `id` → the parent fails naming the duplicate handle. (Task 2)
3. A shell task that exits nonzero after writing `out.json` → the file is ignored; no children are registered. (Task 4)
4. A `task` dict whose `kwargs` carry an `object`/`view`/`callable` ref → rejected at commit by the plain-data-plane validator, naming the kwarg. (Task 3)
5. A kwarg dict that merely *looks* tagged — `{"_aaiclick_type": "banana"}` — raises `ValueError` at deserialization rather than passing through as data. (Task 1)

---

### Task 1: Rename the vocabulary to `_aaiclick_type`

**Files:**
- Modify: `aaiclick/data/object/refs.py`
- Modify: `aaiclick/data/object/test_refs.py`
- Modify: `aaiclick/orchestration/execution/runner.py` (`_deserialize_value`, `serialize_task_result`, `_referenced_tables`)
- Modify: `aaiclick/orchestration/image_injection.py` (`_unsupported_jvm_ref`)
- Modify: `aaiclick/orchestration/jobs/queries.py` (`get_job_result`)
- Modify: `aaiclick/orchestration/execution/test_execution.py`, `test_kubernetes_worker.py`, `aaiclick/orchestration/test_image_injection.py`
- Modify: `test_e2e/runner_flow.py`, `test_e2e/sandbox_e2e.py`, `test_e2e/docker/test_docker_runner_e2e.py` (literal asserts only)

**Interfaces:**
- Produces, in `refs.py`:
  - `AAICLICK_TYPE = "_aaiclick_type"`; value constants `UPSTREAM`, `GROUP_RESULTS`, `CALLABLE`, `OBJECT`, `VIEW`, `NATIVE = "native"`, `PYDANTIC = "pydantic"`, `TASK = "task"`, `GROUP = "group"`, `TASK_RESULT = "task_result"`; field keys `TASK_ID`, `GROUP_ID`, `ENTRYPOINT`, `TABLE`, `PERSISTENT`, `JOB_ID`, `VALUE = "value"`, `PYDANTIC_CLASS = "type"`, `PYDANTIC_DATA = "data"`, `HANDLE = "id"`, `DEPENDS_ON = "depends_on"`, `TASKS = "tasks"`, `DATA = "data"`.
  - `RefKind = Literal[...]` over the ten values; `ref_kind(value: Any) -> RefKind | None` — `None` for a non-dict or a dict without the key; `ValueError` for an unknown value.
  - Models `UpstreamRef`, `GroupResultsRef`, `CallableRef`, `ObjectRef`, `ViewRef` carry `aaiclick_type: Literal[...] = Field(alias=AAICLICK_TYPE)` with `populate_by_name=True`; `to_dict()` dumps `by_alias=True`.
  - Constructors `upstream_ref(task_id: int | str)`, `group_results_ref(group_id: int | str)`, `callable_ref(entrypoint)`, `native_ref(value) -> {"_aaiclick_type": "native", "value": value}`, `pydantic_ref(model: BaseModel) -> dict`.
  - Predicates `is_upstream_ref`, `is_native_ref`, `is_persistent_object_ref` keyed on `AAICLICK_TYPE`.
  - Removed: `REF_TYPE`, `OBJECT_TYPE`, `NATIVE_VALUE`, `PYDANTIC_TYPE`, `native_value_ref`, `is_native_value_ref`.

- [ ] **Step 1: Rewrite `test_refs.py` for the new shapes**

```python
def test_object_ref_to_dict(ref, expected):  # params now {"_aaiclick_type": "object", "table": "t_123"} etc.

def test_ref_kind_unknown_type_raises():
    with pytest.raises(ValueError, match="_aaiclick_type"):
        ref_kind({"_aaiclick_type": "banana"})

def test_ref_kind_untagged_dict_is_plain():
    assert ref_kind({"id": "w1", "value": 3}) is None

def test_models_round_trip_through_alias():
    assert UpstreamRef.model_validate({"_aaiclick_type": "upstream", "task_id": 7}).to_dict() == {"_aaiclick_type": "upstream", "task_id": 7}
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/data/object/test_refs.py -q`
Expected: FAIL (old keys / missing names)

- [ ] **Step 3: Rewrite `refs.py` per the Interfaces block; update the module docstring's "Reference shapes" to the new dicts, including `task`, `group`, `task_result` with the handle rule**

`UpstreamRef.task_id: int | str`, `GroupResultsRef.group_id: int | str` (handles).

- [ ] **Step 4: Update consumers**

- `runner.py`: `_deserialize_value` dispatches on the new kinds (`NATIVE` → `value[VALUE]`; `PYDANTIC` → `_import_class(value[PYDANTIC_CLASS]).model_validate(value[PYDANTIC_DATA])`); `serialize_task_result` emits `pydantic_ref(result)` / `native_ref(...)`; `_referenced_tables` checks `ref_kind(value) in (OBJECT, VIEW)`.
- `image_injection._unsupported_jvm_ref`: drop the `REF_TYPE in value` branch (unknown types now raise in `ref_kind`; let that propagate).
- `jobs/queries.py`: `ref_kind(result) in (OBJECT, VIEW)` guard.
- Replace literals in the listed tests and e2e asserts (`{"native_value": X}` → `{"_aaiclick_type": "native", "value": X}`, `{"object_type": "object", ...}` → `{"_aaiclick_type": "object", ...}`, `{"pydantic_type": ..}` → `{"_aaiclick_type": "pydantic", "type": ..}`).

- [ ] **Step 5: Run the unit suite**

Run: `uv run pytest aaiclick/data/object/test_refs.py aaiclick/orchestration -q -x`
Expected: PASS

- [ ] **Step 6: Grep for stragglers**

Run: `grep -rn "ref_type\|native_value\|object_type\|pydantic_type" aaiclick test_e2e --include=*.py`
Expected: no output

- [ ] **Step 7: Commit**

```bash
git add -A aaiclick test_e2e && git commit -m "Rename ref vocabulary to a single _aaiclick_type discriminator"
```

---

### Task 2: Register returned task/group dicts with local handles

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py` (`register_returned_tasks`, `_tasks_from`, `_hold_dependencies`)
- Test: `aaiclick/orchestration/execution/test_execution.py`
- Create: `aaiclick/orchestration/fixtures/dict_return_pipelines.py` (task functions used by the tests)

**Interfaces:**
- Consumes: Task 1 constants; `create_task()` (`factories.py`); `Task.depends_on`, `Group(id=get_snowflake_id(), name=...)`, `Group.add_task`.
- Produces, in `runner.py`:
  - `_build_dag_from_dicts(items: list[dict]) -> tuple[list[Task | Group], dict[str, int]]` — mints snowflake ids, builds each `task` through `create_task(callback=spec.get("entrypoint"), **rest)` after popping `id`/`depends_on`, returns the nodes and the handle → id map; raises `ValueError("duplicate handle 'w1'")` / `ValueError("unknown handle 'x'")`.
  - `_swap_handles(value: Any, handles: dict[str, int]) -> Any` — walks kwargs/data; a string `task_id`/`group_id` inside an `upstream`/`group_results` ref becomes the mapped int.
  - `register_returned_tasks(result, parent_task_id, job_id)` accepts: a `task_result` dict, a `task`/`group` dict, a list of them, in addition to today's Python shapes. `data` that is an `upstream` ref to a handle gets `_hold_dependencies` on that child's id.

- [ ] **Step 1: Write the fixture tasks and failing tests**

`dict_return_pipelines.py`:

```python
@task
async def spawn_dicts(n: int):
    return {"_aaiclick_type": "task_result",
            "data": {"_aaiclick_type": "upstream", "task_id": "fin"},
            "tasks": [
                {"_aaiclick_type": "task", "id": "w1", "entrypoint": "aaiclick.orchestration.fixtures.dict_return_pipelines.double", "kwargs": {"x": n}},
                {"_aaiclick_type": "task", "id": "w2", "entrypoint": "...double", "kwargs": {"x": n + 1}},
                {"_aaiclick_type": "task", "id": "fin", "entrypoint": "...total",
                 "kwargs": {"parts": [{"_aaiclick_type": "upstream", "task_id": "w1"}, {"_aaiclick_type": "upstream", "task_id": "w2"}]}},
            ]}

@task
async def spawn_group(n: int):  # group with handle "g"; "fin" kwargs {"parts": group_results_ref("g")}

@task
async def spawn_duplicate_handle(): ...   # two tasks with id "w1"
@task
async def spawn_unknown_handle(): ...     # fin references "nope"
```

Tests in `test_execution.py`:

```python
async def test_run_job_tasks_dict_return_spawns_children(orch_ctx):
    job = await create_job("dict_return", create_task(spawn_dicts, {"n": 1}))
    await run_job_tasks(job)
    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == 6   # 2 + 4

async def test_run_job_tasks_dict_return_group_results(orch_ctx):  # == [2, 4]

@pytest.mark.parametrize("entry, match", [
    pytest.param(spawn_duplicate_handle, "duplicate handle 'w1'", id="duplicate"),
    pytest.param(spawn_unknown_handle, "unknown handle 'nope'", id="unknown"),
])
async def test_run_job_tasks_dict_return_bad_handles_fail_parent(orch_ctx, entry, match):
    job = await create_job("bad_handles", create_task(entry))
    await run_job_tasks(job)
    assert job.status == JOB_FAILED
    assert match in (job.error or "")
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/orchestration/execution/test_execution.py -k dict_return -q`
Expected: FAIL (dict returned as plain data; `get_job_result` is the dict)

- [ ] **Step 3: Implement `_build_dag_from_dicts`, `_swap_handles`, and the dict branches in `register_returned_tasks`**

`depends_on` resolves through the same handle map via `Task.depends_on`. `group` dicts: `Group(id=..., name=spec.get("name", "group"))`, members built recursively, each `group.add_task(member)`; the group's handle maps to its id. Reuse `_tasks_from` for the flattening/validation step by feeding it the built nodes.

- [ ] **Step 4: Run the new tests and the orchestration suite**

Run: `uv run pytest aaiclick/orchestration -q -x`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A aaiclick && git commit -m "Register task/group dicts returned by tasks, with local handles"
```

---

### Task 3: One plain-data-plane validator for shell and jvm, plus host-side kwarg resolution

**Files:**
- Modify: `aaiclick/orchestration/image_injection.py` (`validate_jvm_tasks` → `validate_plain_data_plane_tasks`)
- Modify: `aaiclick/orchestration/orch_context.py` (`commit_tasks` call site)
- Modify: `aaiclick/orchestration/execution/runner.py` (add `resolve_plain_kwargs`)
- Test: `aaiclick/orchestration/test_image_injection.py`, `aaiclick/orchestration/execution/test_execution.py`

**Interfaces:**
- Produces:
  - `validate_plain_data_plane_tasks(tasks: list[Task]) -> None` — for `entry_type in (ENTRY_SHELL, ENTRY_JVM)`: kwargs may hold only plain values, `native`, `upstream` (per kwarg, any depth); jvm additionally needs a non-empty entrypoint and its own `image_source` (unchanged rules). Error text: `f"{entry_type} task {name!r} receives a {kind} ref in kwarg {kwarg!r}; the shell/jvm data plane is plain values and upstream refs only"`.
  - `async resolve_plain_kwargs(kwargs: dict) -> dict` in `runner.py` — resolves `upstream` (recursively, through the producer's stored result) and unwraps `native`; a resolved value of any other kind raises `ValueError(f"kwarg {name!r} resolves to a {kind} ref, which a shell task cannot receive")`.

- [ ] **Step 1: Write failing tests**

In `test_image_injection.py`, rename the `validate_jvm_*` tests to `validate_plain_data_plane_*` and add a shell case to the rejects parametrization:

```python
pytest.param({"data": OBJECT_REF}, None, "object ref", id="shell-object-ref")  # entry_type="shell", no image needed
```

In `test_execution.py`:

```python
async def test_resolve_plain_kwargs_unwraps_upstream_and_native(orch_ctx):
    # commit a COMPLETED task whose result is native_ref({"a": 1}); resolve {"x": upstream_ref(id), "y": native_ref(2), "z": 3}
    assert resolved == {"x": {"a": 1}, "y": 2, "z": 3}

async def test_resolve_plain_kwargs_rejects_object_result(orch_ctx):
    # upstream result stored as {"_aaiclick_type": "object", "table": "t_1"}
    with pytest.raises(ValueError, match="resolves to a object ref"):
        await resolve_plain_kwargs({"d": upstream_ref(id)})
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/orchestration/test_image_injection.py aaiclick/orchestration/execution/test_execution.py -k "plain" -q`
Expected: FAIL

- [ ] **Step 3: Implement the validator rename and `resolve_plain_kwargs`**

`resolve_plain_kwargs` is a sibling of `_deserialize_value`: same recursion over lists/dicts, only two kinds allowed; it opens its own `get_sql_session()`.

- [ ] **Step 4: Run the orchestration suite**

Run: `uv run pytest aaiclick/orchestration -q -x`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A aaiclick && git commit -m "Validate shell and jvm kwargs as one plain data plane; add resolve_plain_kwargs"
```

---

### Task 4: File data plane on the subprocess runner

**Files:**
- Create: `aaiclick/orchestration/execution/io_plane.py`
- Modify: `aaiclick/orchestration/execution/runner.py` (`execute_shell_task`, `execute_task` shell branch)
- Modify: `aaiclick/orchestration/execution/mp_worker.py` (`_child_run_task` shell branch)
- Test: `aaiclick/orchestration/execution/test_execution.py`, `aaiclick/orchestration/execution/test_mp_worker_shell.py`

**Interfaces:**
- Produces, in `io_plane.py`:
  - `IO_DIR_ENV = "AAICLICK_IO_DIR"`, `IN_JSON = "in.json"`, `OUT_JSON = "out.json"`, `CONTAINER_IO_DIR = "/aaiclick"`.
  - `write_in_json(io_dir: Path, kwargs: dict) -> Path`.
  - `read_out_json(io_dir: Path) -> Any` — missing file → `None`; unparsable → `ValueError(f"{OUT_JSON} is not valid JSON: {err}")`.
- Produces, in `runner.py` (not `io_plane.py` — it needs `register_returned_tasks`, and `runner` imports `io_plane`):
  - `async finish_shell_result(task: Task, out_value: Any) -> Any` — `set_current_task_info(...)`, unwrap a top-level `native`, `register_returned_tasks(out_value, task.id, task.job_id)`, return the data portion.
  - `execute_shell_task(task, spec=None) -> Any` — creates a per-attempt temp dir, `write_in_json(dir, await resolve_plain_kwargs(task.kwargs))`, overlays `{IO_DIR_ENV: dir}` on the spec env, runs, on exit 0 returns `await finish_shell_result(task, read_out_json(dir))`, removes the dir in `finally`. Nonzero exit still raises `RuntimeError("exit N")` before the file is read.
- `_child_run_task` shell branch puts `RunnerResult(True, serialize_task_result(data, job_id), None)`.

- [ ] **Step 1: Write failing end-to-end tests in `test_execution.py`**

```python
WRITE_DOUBLE = ["python", "-c", "import json,os; d=os.environ['AAICLICK_IO_DIR']; k=json.load(open(f'{d}/in.json')); json.dump({'_aaiclick_type':'native','value':k['n']*2}, open(f'{d}/out.json','w'))"]

async def test_shell_task_reads_in_json_and_writes_out_json(orch_ctx):
    job = await create_job("shell_io", create_task(None, {"n": 21}, entry_type="shell", command=WRITE_DOUBLE))
    await run_job_tasks(job)
    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == 42

async def test_shell_task_without_out_json_has_no_result(orch_ctx):  # command: ["true"]; task.result is None

async def test_shell_task_out_json_spawns_children(orch_ctx):
    # entry writes a task_result with two shell children (WRITE_DOUBLE, n=1 / n=2) and "fin"
    # whose command sums in.json["parts"]; data → upstream "fin"
    ... assert await get_job_result(job) == 6

async def test_shell_task_nonzero_exit_ignores_out_json(orch_ctx):
    # writes a task_result then `exit 3`
    assert job.status == JOB_FAILED
    assert len(await get_tasks_for_job(job.id)) == 1

async def test_shell_task_invalid_out_json_fails_task(orch_ctx):
    # command writes "not json" to out.json
    assert job.status == JOB_FAILED and "out.json is not valid JSON" in (job.error or "")
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/orchestration/execution/test_execution.py -k "shell_task" -q`
Expected: FAIL (`AAICLICK_IO_DIR` unset → KeyError in the child; result None)

- [ ] **Step 3: Implement `io_plane.py` and the `execute_shell_task` changes**

- [ ] **Step 4: Update `_child_run_task` and add an mp test**

In `test_mp_worker_shell.py` add `test_mp_shell_task_result_reaches_task_row` running `WRITE_DOUBLE` through the mp worker and asserting `task.result == native_ref(42)`.

- [ ] **Step 5: Run the suites**

Run: `uv run pytest aaiclick/orchestration/execution -q -x`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A aaiclick && git commit -m "Shell tasks read in.json and write out.json under AAICLICK_IO_DIR"
```

---

### Task 5: Docker shell vehicle

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py` (replace `build_shell_run_spec` with `_DockerShellVehicle` + `_run_shell_in_container`)
- Modify: `aaiclick/orchestration/execution/dispatch.py` (route docker shell tasks to the vehicle; drop the docker branch from `build_shell_spec`)
- Test: `aaiclick/orchestration/execution/test_docker_worker.py`, `test_dispatch.py`; `test_e2e/runner_flow.py` (`run_shell_command_env_flow` gains an `out.json` round trip)

**Interfaces:**
- Consumes: `io_plane` (Task 4), `resolve_plain_kwargs` (Task 3), `drive_vehicle`, `collect_remote_result`, `write_command_env_file`, `register_host_log_run`.
- Produces:
  - `build_docker_create_cmd(task: Task, image_tag: str, resources: dict | None, env_file: str | None) -> list[str]` — `docker create --name aaiclick-task-<id>-<epoch> [--env-file F] -e AAICLICK_IO_DIR=/aaiclick <image> <argv>`.
  - `_DockerShellVehicle(image_tag, resources)`: `launch` = create → `docker cp <tmp>/in.json <name>:/aaiclick/in.json` → `docker start`; `wait` = `_wait_for_container` then `docker cp <name>:/aaiclick/out.json <tmp>/out.json` (ignore a missing file), payload = `read_out_json(tmp)`; `collect` = `RunnerResult(exit_code == 0 and error is None and not cancelled, None, error or f"exit {exit_code}")` carrying the payload in a `_ShellPayload` NamedTuple `(exit_code, out_value)`; `cleanup` = `docker rm --force`, remove tmp dir and env file.
  - `_run_shell_in_container(task, execution_worker_id, dispatch, log_run_id) -> ExecuteResult` — on success `data = await finish_shell_result(task, out_value)` and `serialize_task_result(data, task.job_id)`.
  - `dispatch._SHELL_RUNNERS: dict[WorkerRunner, ...]` with `RUNNER_DOCKER: _run_shell_in_container`; `dispatch_execute` sends `ENTRY_SHELL` with a runner in that map through `register_host_log_run` + handler, otherwise through the mp child as today.

- [ ] **Step 1: Write failing unit tests**

Replace `test_build_shell_run_spec_*` with:

```python
def test_build_docker_create_cmd_sets_io_dir_and_env_file():
    cmd = build_docker_create_cmd(task, "img:1", None, "/tmp/x.env")
    assert cmd[:3] == ["docker", "create", "--name"]
    assert "-e" in cmd and "AAICLICK_IO_DIR=/aaiclick" in cmd
    assert cmd[cmd.index("--env-file") + 1] == "/tmp/x.env"
    assert cmd[-(len(task.command) + 1):] == ["img:1", *task.command]
```

Keep `test_build_shell_run_spec_rejects_newline_in_command_env` as a test of `write_command_env_file`. In `test_dispatch.py`, adapt `test_dispatch_execute_removes_shell_env_file` to the vehicle's cleanup.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/orchestration/execution/test_docker_worker.py aaiclick/orchestration/execution/test_dispatch.py -q`
Expected: FAIL (names missing)

- [ ] **Step 3: Implement the vehicle, the ExecuteFn and the dispatch route**

`docker cp` into a container that lacks `/aaiclick` creates it; copy the directory (`docker cp <tmp>/. <name>:/aaiclick`) so no mkdir step is needed.

- [ ] **Step 4: Extend the shared e2e flow**

In `test_e2e/runner_flow.py`, `ASSERT_COMMAND_ENV` also writes `{"_aaiclick_type": "native", "value": {"k": "$K"}}` to `$AAICLICK_IO_DIR/out.json`; `run_shell_command_env_flow` asserts `tasks[0].result == native_ref({"k": "v"})`.

- [ ] **Step 5: Run unit suites**

Run: `uv run pytest aaiclick/orchestration/execution -q -x`
Expected: PASS (docker e2e runs in its own workflow)

- [ ] **Step 6: Commit**

```bash
git add -A aaiclick test_e2e && git commit -m "Run docker shell tasks through a host vehicle with the in/out file plane"
```

---

### Task 6: Kubernetes shell vehicle with io helpers

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (replace `build_shell_pod_spec`; add `build_shell_pod_manifest`, `_KubernetesShellVehicle`, `_run_shell_in_pod`; `_pod_status` keyed by container name)
- Modify: `aaiclick/orchestration/execution/dispatch.py` (`RUNNER_KUBERNETES: _run_shell_in_pod`; `build_shell_spec` keeps only the subprocess case — inline it)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Produces:
  - `IO_IMAGE_ENV = "AAICLICK_K8S_IO_IMAGE"`, `DEFAULT_IO_IMAGE = "busybox:1.36"`, `io_image() -> str`.
  - `build_shell_pod_manifest(*, name, namespace, image_tag, command, service_account, image_pull_secret, resources, command_env_secret) -> dict` — `initContainers: [io-in: sh -c 'until [ -f /aaiclick/in.json ]; do sleep 0.2; done']`, `containers: [task (user argv, env AAICLICK_IO_DIR=/aaiclick, envFrom secret), io (sleep infinity)]`, one `emptyDir` volume `aaiclick-io` mounted at `/aaiclick` in all three; `restartPolicy: Never`; helpers carry no `resources`.
  - `_container_state(handle, container: str, init: bool = False) -> tuple[str, int]` — `("running"|"terminated"|"waiting"|"", exit_code)` from one `kubectl get -o jsonpath` on `(init)containerStatuses[?(@.name=="…")]`; `POD_NOT_FOUND` as today.
  - `_KubernetesShellVehicle(spec)`: `launch` = create Secret if `command_env`, apply manifest, poll until `io-in` is `running`, `kubectl cp <tmp>/in.json <ns>/<pod>:/aaiclick/in.json -c io-in`; `wait` = poll `task` until `terminated` (timeout / `POD_NOT_FOUND` as the module vehicle), then `kubectl exec <pod> -c io -- cat /aaiclick/out.json` (missing → `None`) parsed through `json.loads` with the same error text as `read_out_json`; `output_argv` = `kubectl logs --follow -c task`; `terminate`/`cleanup` = delete pod and secret.
  - `_run_shell_in_pod(task, execution_worker_id, dispatch, log_run_id) -> ExecuteResult` mirroring Task 5.

- [ ] **Step 1: Write failing manifest tests**

Replace `test_shell_pod_runs_argv_only_command_env` and `test_build_shell_pod_spec_*` with:

```python
def test_shell_pod_manifest_has_io_helpers_and_shared_volume(monkeypatch):
    monkeypatch.setenv("AAICLICK_K8S_IO_IMAGE", "registry.local/busybox:1")
    m = build_shell_pod_manifest(name="p", namespace="ns", image_tag="img", command=["sh", "-c", "x"], service_account=None, image_pull_secret=None, resources={"limits": {"cpu": "1"}}, command_env_secret="p")
    names = [c["name"] for c in m["spec"]["containers"]]
    assert names == ["task", "io"]
    assert m["spec"]["initContainers"][0]["name"] == "io-in"
    assert {c["image"] for c in (*m["spec"]["initContainers"], m["spec"]["containers"][1])} == {"registry.local/busybox:1"}
    task = m["spec"]["containers"][0]
    assert task["env"] == [{"name": "AAICLICK_IO_DIR", "value": "/aaiclick"}]
    assert task["envFrom"] == [{"secretRef": {"name": "p"}}]
    assert "resources" not in m["spec"]["containers"][1]
    assert all(v["mountPath"] == "/aaiclick" for c in (*m["spec"]["initContainers"], *m["spec"]["containers"]) for v in c["volumeMounts"])
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -q`
Expected: FAIL

- [ ] **Step 3: Implement the manifest builder, status helper, vehicle, ExecuteFn and dispatch route; delete `build_shell_pod_spec`**

- [ ] **Step 4: Run the suite**

Run: `uv run pytest aaiclick/orchestration/execution -q -x`
Expected: PASS (`test_kubernetes_runner_shell_command_env` covers the live path in the k8s workflow via the shared flow extended in Task 5)

- [ ] **Step 5: Commit**

```bash
git add -A aaiclick && git commit -m "Run kubernetes shell tasks in a pod with io helper containers"
```

---

### Task 7: JVM on the file plane

**Files:**
- Modify: `java/aaiclick-task-api/src/main/java/.../AaiTaskShim.java`; `KwargsResolver.java` (becomes a pure-JSON unwrapper of `native`); delete `TaskStore.java`, `Db.java`, `SqlConfig.java`, `NamedParamSql.java`, `TaskRow.java` and their tests (`NamedParamSqlTest`, `SqlConfigTest`, `testsupport/TestDb.java`)
- Modify: `java/aaiclick-task-api/pom.xml` (drop `postgresql`, `sqlite-jdbc`)
- Modify: `.github/workflows/test.yaml` (`java-sdk` job: drop the schema-fixture step and `AAICLICK_TEST_SQLITE_DB`)
- Modify: `aaiclick/orchestration/execution/dispatch.py` (jvm → the shell vehicles with argv `[task.entrypoint]`), `docker_worker.py` and `kubernetes_worker.py` (remove the jvm branches of `_build_docker_run_cmd` / `_build_pod_manifest`), `image_injection.py` (`stamp_inherited_image` docstring only), `docker_scaffold.py` (`_JVM_TEMPLATE` comment)
- Modify: `test_e2e/docker/test_docker_runner_e2e.py::test_docker_runner_jvm_task` (docstring + result shape), `test_e2e/fixtures/sample_job/sample_jobs.py` (docstrings)
- Test: `java/.../AaiTaskShimTest.java`, `aaiclick/orchestration/execution/test_dispatch.py`

**Interfaces:**
- Shim: `AaiTaskShim.run(String[] args, Map<String,String> env) -> int`; `args[0]` is the class name (`Class` or `Class#method`), `env.get("AAICLICK_IO_DIR")` the directory. Reads `in.json` (plain kwargs; `native` wrappers unwrapped by `KwargsResolver.unwrap(JsonNode)`), binds via `KwargsBinder`, invokes, writes `out.json`: a `Map`/`JsonNode` whose top level carries `_aaiclick_type` is written as is; `void`/`null` writes nothing; any other value is written as `{"_aaiclick_type": "native", "value": …}`. Exceptions print `Type: message` to stderr and return 1.
- Python: the jvm `JobDispatch` reaches `_run_shell_in_container` / `_run_shell_in_pod` with `command=[task.entrypoint]`, so the image `ENTRYPOINT` (the shim) receives the class name. `require_container_runner` and the commit-time image rule are unchanged.

- [ ] **Step 1: Rewrite `AaiTaskShimTest` against a `@TempDir`**

```java
@Test void successWritesNativeOutJson()   // in.json {"date":"2026-08-20","window":7} → out.json {"_aaiclick_type":"native","value":{"date":"2026-08-20","window":14}}
@Test void taggedMapIsWrittenUnwrapped()  // SampleTasks#spawn returns Map.of("_aaiclick_type","task_result","data",1,"tasks",List.of()) → out.json equals it
@Test void voidResultWritesNoOutJson()
@Test void taskExceptionReturnsOneAndWritesNothing()
@Test void nativeWrappedKwargIsUnwrapped()  // in.json {"values": {"_aaiclick_type":"native","value":[1.0,2.0]}} → sum 3.0
```

Add `spawn` to `SampleTasks`.

- [ ] **Step 2: Run to verify failure**

Run: `mvn -B -q -f java/pom.xml test`
Expected: compile failure on the removed signature

- [ ] **Step 3: Rewrite the shim and resolver, delete the SQL classes, trim the pom and the CI job**

- [ ] **Step 4: Run the Java suite**

Run: `mvn -B -f java/pom.xml verify`
Expected: BUILD SUCCESS

- [ ] **Step 5: Route jvm through the shell vehicles in Python; add `test_dispatch_jvm_uses_shell_vehicle_with_class_argv`**

The test monkeypatches `_run_shell_in_container` and asserts it is called with a `JobDispatch` whose `command == ["com.example.Pipeline"]` for a jvm task on `RUNNER_DOCKER`.

- [ ] **Step 6: Update the docker jvm e2e assertion to `native_ref({"total": 60.0, "count": 3})` and its docstring (file plane, no JDBC)**

- [ ] **Step 7: Run the Python suites and grep**

Run: `uv run pytest aaiclick/orchestration -q -x && grep -rn "remote_task_results\|TaskStore\|JDBC" java docs/designs/java-sdk.md`
Expected: PASS; grep hits only in `java-sdk.md` (fixed in Task 8)

- [ ] **Step 8: Commit**

```bash
git add -A java .github aaiclick test_e2e && git commit -m "Move the JVM shim to the in/out file plane and drop its JDBC path"
```

---

### Task 8: Documentation

**Files:**
- Modify: `docs/designs/orchestration.md` ("Task Parameter Serialization", "Shell entry type", "Execution layers", env table)
- Modify: `docs/designs/java-sdk.md` (architecture, data-plane contract, Java module table, testing)
- Modify: `docs/designs/future.md` (add "Object support for non-Python tasks — unsupported" under Deferred)
- Delete: `docs/superpowers/specs/2026-10-16-language-agnostic-task-contract-design.md`, `docs/superpowers/plans/2026-10-16-language-agnostic-task-contract.md`

- [ ] **Step 1: Rewrite the four doc sections**

- `orchestration.md` "Task Parameter Serialization": the ten-row `_aaiclick_type` table from the spec, the handle rule, implementation refs `refs.py` / `register_returned_tasks()`.
- "Shell entry type": `AAICLICK_IO_DIR`, `in.json`/`out.json`, the Node example, per-runner transport (mp child / docker create-cp-start / pod with `io-in`+`io`), env table row `shell` + container = `command_env` + `AAICLICK_IO_DIR`; jvm row = same as shell.
- "Execution layers" table: jvm = image `ENTRYPOINT` shim + class-name argv, reads/writes the IO files.
- `java-sdk.md`: shim steps become read `in.json` → bind → invoke → write `out.json`; module table drops the SQL rows; testing section describes the temp-dir suite; remove the schema-fixture paragraph.

- [ ] **Step 2: Run the `shortify` and `markdown-style` skills over the edited docs**

- [ ] **Step 3: Delete the spec and plan; grep for references**

Run: `grep -rn "superpowers/specs/2026-10-16\|superpowers/plans/2026-10-16" docs aaiclick`
Expected: no output

- [ ] **Step 4: Commit and push; run `check-pr`**

```bash
git add -A docs && git commit -m "Document the language-agnostic task contract and file data plane"
git push -u origin ccr-7be4cff3-4si2qx
```
