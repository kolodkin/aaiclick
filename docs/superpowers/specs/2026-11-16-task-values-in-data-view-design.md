Task Values in the Data View
---

A job's `@data` view lists only ClickHouse tables, so a task's Python inputs
and results — JSON in `tasks.kwargs` / `tasks.result` — are invisible. This
spec lists every task input and result next to the job's objects and opens
each one in the existing results grid. Nothing new is stored.

Vocabulary of the stored values: `docs/designs/task_interface.md`.
Mock: https://claude.ai/artifact/LvWF6XQRGHjNvpai2KuQPp

# Rows

With `kind="all"`, a job listing returns the job's objects as today, then
one row per top-level kwarg and one per non-null result of every task in the
job. The default (`kind="object"`) is today's listing, so the CLI, MCP and
the job page's objects menu are unchanged. The `persistent` and `all` scopes
never list task rows.

| Kind     | Row is                                    | Value / Target column                |
|----------|-------------------------------------------|--------------------------------------|
| `object` | a `table_registry` table (today's rows)   | —                                    |
| `value`  | an untagged value, `native` or `pydantic` | JSON preview, cut at 120 characters  |
| `ref`    | any other `_aaiclick_type`                | ref type tag, then the target        |

A `ref` row's tag and target come from its `_aaiclick_type`:

| `_aaiclick_type`  | Tag        | Target                          | Link opens              |
|-------------------|------------|---------------------------------|-------------------------|
| `object`, `view`  | `object`   | the object's name               | that object             |
| `upstream`        | `task`     | `<task_name>.result`            | that task's result row  |
| `group_results`   | `group`    | `group <id> results (<n> tasks)`| nothing                 |
| `callable`        | `function` | the entrypoint, plain text      | nothing                 |

An unknown `_aaiclick_type` is a `ref` tagged with the raw type and no link,
so a new ref type shows up without a code change.

Only the top-level value decides the kind: a list or dict holding refs is a
`value` and shows its JSON as stored. `pydantic` previews as
`<type> <data>`.

## Naming

| Row    | Display name         | Address in prompt, URL and API |
|--------|----------------------|--------------------------------|
| input  | `<task_name>.<kwarg>`| `<task_id>.<kwarg>`            |
| result | `<task_name>.result` | `<task_id>`                    |

Task names repeat within a job (`_map_part` per partition), so the address
uses the task id. Object names match `^[a-zA-Z_][a-zA-Z0-9_]*$`, so an
address — it starts with a digit — never collides with one. Rows carry an
`in` / `out` marker; a kwarg named `result` stays distinct from the result.

# API

## `list_objects`

`ObjectView` gains fields, all optional so object rows are unchanged:

```python
OBJECT_KIND_OBJECT = "object"
OBJECT_KIND_VALUE = "value"
OBJECT_KIND_REF = "ref"
ObjectKind = Literal["object", "value", "ref"]

class ObjectView(ApiModel):
    ...
    kind: ObjectKind = "object"
    label: str | None = None      # display name of a task row
    task_id: SnowflakeId | None = None
    direction: Literal["in", "out"] | None = None
    ref_type: str | None = None   # "object" | "task" | "group" | "function" | raw type
    preview: str | None = None    # value preview or ref target text
    target: str | None = None     # object name or address the link opens; None = no link
```

For task rows `name` is the address, `table` is empty and `row_count` /
`size_bytes` are null. `ObjectFilter` gains
`kind: ObjectKind | Literal["all"] = "object"`; any value but `"object"`
requires `job`. The SPA requests `all` and filters by chip client-side.

Task rows are built in Python from one query over the job's tasks, ordered by
task id, inputs by kwarg name then the result, and appended after the object
rows. `total` counts both parts and `limit` cuts the combined list; `prefix`
matches a task row's label.

## Reading one

`query_object` accepts an address in `object` (job scopes only);
`get_object` does not — the list row carries all the detail there is. For an
address, no SQL runs: the stored JSON is returned in the kernel's shape,

```json
{"meta": [{"name": "value", "type": "JSON"}], "data": [[<stored value>]]}
```

`native` is unwrapped; `pydantic` and refs are returned as stored. `where`,
`order_by` and `fields` on an address are rejected with `Invalid`; `fmt="csv"`
returns the JSON text in one cell. A value over 1 MiB is replaced by
`{"truncated": true, "bytes": <n>}` — the task API still returns it whole.

REST, MCP and CLI get this through the existing verbs.

# SPA

- `ObjectsTable`: Kind and Value / Target columns, shown in job scopes; the
  kind chips above it. Rows / Size / Created show `—` for task rows.
- `Data` object view, for an address: header panel (kind, task, direction,
  ref type, points-to link) from the list row, then `ResultsTable` with the
  one cell. Limit, paging, Fields and Order by are hidden.
- The prompt accepts `@data job <ref> <address>`.

# Out of Scope

- Task rows in `persistent` / `all` scopes and in dashboards.
- Inputs / Result panels on `@task <id>` — a follow-up that links here.
- Linking a `function` ref to tasks: the operators call it inline, so there
  is no task to open.

# Tests

- `internal_api/test_objects.py`: a job with a native kwarg, a pydantic
  kwarg, an upstream kwarg, a callable kwarg, an object result and a native
  result lists the expected kinds, ref types and targets; `kind` filters.
- `internal_api/test_viewer.py`: `query_object` on an input address and a
  result address returns the one-cell shape; `where` is rejected.
- `test_e2e`: the job's `@data` view shows a value row and opens it.
