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

# Spawning tasks

To fan out, write a `task_result` to `out.json`: `tasks` are the children,
`data` is your result. Children refer to each other through string handles
on `id`, used inside `upstream` refs; the orchestrator assigns the real ids
and wires the dependencies.

```json
{"_aaiclick_type": "task_result",
 "data": {"_aaiclick_type": "upstream", "task_id": "fin"},
 "tasks": [
   {"_aaiclick_type": "task", "id": "w1", "entry_type": "shell", "command": ["node", "worker.js"], "kwargs": {"n": 1}},
   {"_aaiclick_type": "task", "id": "w2", "entry_type": "shell", "command": ["node", "worker.js"], "kwargs": {"n": 2}},
   {"_aaiclick_type": "task", "id": "fin", "entry_type": "shell", "command": ["node", "sum.js"],
    "kwargs": {"parts": [{"_aaiclick_type": "upstream", "task_id": "w1"},
                         {"_aaiclick_type": "upstream", "task_id": "w2"}]}}
 ]}
```

Here `fin` runs after both workers and reads `{"parts": [2, 4]}` from its
`in.json`; `data` names `fin`, so whoever consumes this task's result waits
for `fin` and gets its value (`6`).

A `task` takes the same fields as `create_task()` in Python:

| Field                                     | Meaning                                                   |
|-------------------------------------------|-----------------------------------------------------------|
| `id`                                      | Handle other children refer to (optional)                 |
| `entry_type`                              | `"shell"`, `"module"` (Python) or `"jvm"`                 |
| `command` / `entrypoint`                  | Argv for shell; dotted path or Java class name otherwise  |
| `kwargs`                                  | JSON object; values may be `upstream` refs to handles     |
| `command_env`, `max_retries`, `name`      | As in Python                                              |
| `image` or `git_remote` / `git_sha` / …   | Image source; omitted → the child runs in your image      |
| `depends_on`                              | Handles to run after, without data flow                   |

A `group` dict (`{"_aaiclick_type": "group", "id": "g", "tasks": [...]}`)
groups children; a `group_results` ref (`{"_aaiclick_type": "group_results",
"group_id": "g"}`) in a kwarg delivers the list of their results. Returning
a single `task`, or a list of them, works too when there is no data.

!!! warning "`_aaiclick_type` is reserved"
    A dict with that key is interpreted, never passed through as data, and an
    unknown value fails the task. Plain JSON (`42`, `[1, 2]`, `{"k": "v"}`)
    is always data; wrapping it as `{"_aaiclick_type": "native", "value": …}`
    is accepted and means the same thing.

# What a task can and cannot receive

Kwargs on the file plane carry plain values and the results of upstream
tasks. ClickHouse `Object`s and Python callables cannot cross into a
non-Python process: a job that passes one to a shell or jvm task is rejected
when the task is committed, and a shell task whose upstream Python producer
returned an `Object` fails at launch.

Inside a container the process sees only `command_env` and
`AAICLICK_IO_DIR` — never aaiclick's own database credentials.

# Python and Java

Typed packages that write these files for you: [Task API](task_api.md).

# Kubernetes requirements

Shell and jvm Pods move `in.json` / `out.json` through two small helper
containers (`busybox:1.36`; set `AAICLICK_K8S_IO_IMAGE` on an air-gapped
cluster), so the worker's Role needs `pods/exec` in addition to the verbs in
[Container Images](container_images.md#kubernetes-dispatching-worker).
