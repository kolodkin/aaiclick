Task Interface
---

Write a task in any language: a process that reads its inputs from one JSON
file, writes its result to another, and can spawn more tasks by describing
them as JSON. No aaiclick library, no database access. The
[Task API](task_api.md) packages sit on top of this contract.

Design: [task_interface.md](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/task_interface.md).

# The contract

| What                       | Where                                                   |
|----------------------------|---------------------------------------------------------|
| Your inputs                | `$AAICLICK_IO_DIR/in.json` — a JSON object of kwargs    |
| Your result (optional)     | `$AAICLICK_IO_DIR/out.json` — any JSON value            |
| Your logs                  | stdout / stderr, as usual                               |
| Success                    | exit code 0 (`out.json` is ignored on any other exit)   |

`in.json` is plain JSON: upstream results have already been resolved into
their values. No `out.json` means no result.

```js
// worker.js — Node, no dependencies
const fs = require("fs");
const dir = process.env.AAICLICK_IO_DIR;
const {n} = JSON.parse(fs.readFileSync(`${dir}/in.json`));
fs.writeFileSync(`${dir}/out.json`, JSON.stringify(n * 2));
```

```bash
# the same in bash
jq '.n * 2' "$AAICLICK_IO_DIR/in.json" > "$AAICLICK_IO_DIR/out.json"
```

Submit it as a shell task — on the host, in a Docker image, or in a
Kubernetes Pod, the files work the same way:

```python
run_job("double", entry_type="shell", command=["node", "worker.js"], kwargs={"n": 21}, image="node:22")
```

A downstream Python task receives the result as a plain value:

```python
@task
async def report(value: int):   # 42
    ...
```

# Vocabulary

Kwargs and results are JSON. Plain values are data; a dict carrying the
reserved key `_aaiclick_type` is one of these shapes:

| `_aaiclick_type` | Fields                                                | Meaning                                         |
|------------------|-------------------------------------------------------|-------------------------------------------------|
| `upstream`       | `task_id` (int = database id; string = sibling `id`)  | Another task's result                           |
| `group_results`  | `group_id` (int = database id; string = sibling `id`) | The list of a group's results                   |
| `native`         | `value`                                               | A plain value, wrapped; same as writing it bare |
| `task`           | See [Spawning tasks](#spawning-tasks)                 | A child task to spawn                           |
| `group`          | `id`, `name`, `tasks: [task…]`                        | A set of child tasks                            |
| `task_result`    | `data`, `tasks: [task \| group …]`                    | A result plus children to spawn                 |
| `callable`       | `entrypoint`                                          | A Python function — Python tasks only           |
| `object`         | `table`, `persistent`, `job_id`                       | A ClickHouse table — Python tasks only          |
| `view`           | `table`, `view_schema`                                | A filtered `object` — Python tasks only         |
| `pydantic`       | `type` (dotted class path), `data`                    | A Pydantic model — Python tasks only            |

!!! warning "`_aaiclick_type` is reserved"
    A dict with that key is interpreted, never passed through as data, and an
    unknown value fails the task.

# Spawning tasks

Write a `task_result` to `out.json`: `data` is your result, `tasks` the
children. Children reference each other by `id`, which the framework
replaces with a database id.

```json
{"_aaiclick_type": "task_result",
 "data": {"_aaiclick_type": "upstream", "task_id": "fin"},
 "tasks": [
   {"_aaiclick_type": "task", "id": "w1", "entry_type": "shell", "command": ["node", "worker.js"], "kwargs": {"n": 1}},
   {"_aaiclick_type": "task", "id": "w2", "entry_type": "shell", "command": ["node", "worker.js"], "kwargs": {"n": 2}},
   {"_aaiclick_type": "task", "id": "fin", "entry_type": "shell", "command": ["node", "sum.js"],
    "kwargs": {"values": [{"_aaiclick_type": "upstream", "task_id": "w1"},
                          {"_aaiclick_type": "upstream", "task_id": "w2"}]}}
 ]}
```

Here `fin` runs after both workers and reads `{"values": [2, 4]}` from its
`in.json`; `data` names `fin`, so whoever consumes this task's result waits
for `fin` and gets its value (`6`).

`task` fields:

| Field                                   | Meaning                                                     |
|-----------------------------------------|-------------------------------------------------------------|
| `id`                                    | Name siblings refer to (optional)                           |
| `entry_type`                            | `"shell"`, `"module"` (Python) or `"jvm"`                   |
| `command` / `entrypoint`                | Argv for shell; dotted path or Java class name otherwise    |
| `kwargs`                                | JSON object; values may be `upstream` refs to sibling `id`s |
| `command_env`, `max_retries`, `name`    | Env vars for the process, retries on failure, display name  |
| `image` or `git_remote` / `git_sha` / … | Image source; omitted → the child runs in your image        |
| `depends_on`                            | Sibling `id`s to run after, without data flow               |

A `group` dict (`{"_aaiclick_type": "group", "id": "g", "tasks": [...]}`)
groups children; a `group_results` ref (`{"_aaiclick_type": "group_results",
"group_id": "g"}`) in a kwarg delivers the list of their results. Returning
a single `task`, or a list of them, works too when there is no data.

# What a task can and cannot receive

Only plain values and upstream results. An `Object` or Python callable in a
shell or jvm task's kwargs is rejected at commit, or at launch when an
upstream returned one.

Inside a container the process sees only `command_env` and
`AAICLICK_IO_DIR` — never aaiclick's own database credentials.

# Python and Java

Typed packages that write these files for you: [Task API](task_api.md).

# Kubernetes requirements

Shell and jvm Pods move `in.json` / `out.json` through two small helper
containers (`busybox:1.36`; set `AAICLICK_K8S_IO_IMAGE` on an air-gapped
cluster), so the worker's Role needs `pods/exec` in addition to the verbs in
[Container Images](container_images.md#kubernetes-dispatching-worker).
