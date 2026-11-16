Task Values in the Data View
---

A job's `@data` view lists only ClickHouse tables, so a task's Python inputs
and results — JSON in `tasks.kwargs` / `tasks.result` — are invisible. This
spec lists every task input and result next to the job's objects and opens
each one in the existing results grid. Nothing new is stored.

Vocabulary of the stored values: `docs/designs/task_interface.md`.
Mock: https://claude.ai/artifact/LvWF6XQRGHjNvpai2KuQPp

# Rows

A `job:<ref>` scope lists the job's objects as today, then one row per
top-level kwarg and one per non-null result of every task in the job.
`persistent` lists no task rows; `all` is unchanged.

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
| `group_results`   | `group`    | `group <id> results (<n> tasks)`| the group in `@job`     |
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
    task_id: SnowflakeId | None = None
    direction: Literal["in", "out"] | None = None
    ref_type: str | None = None   # "object" | "task" | "group" | "function" | raw type
    preview: str | None = None    # value preview or ref target text
    target: str | None = None     # address the link opens; None = no link
```

For task rows `name` is the address, `table` is empty and `row_count` /
`size_bytes` are null. `ObjectFilter` gains `kind: ObjectKind | None`; the
SPA's all / objects / values / refs chips set it.

Task rows are built in Python from one `SELECT id, name, kwargs, result FROM
tasks WHERE job_id = …`, ordered by task id then kwarg name, and appended
after the object rows. The cursor covers both parts.

## Reading one

`query_object` and `get_object` accept an address in `object`. For one, no
SQL runs: the stored JSON is returned in the kernel's shape,

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
  ref type, points-to link), then `ResultsTable` with the one cell. Limit,
  paging, Fields and Order by are hidden.
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
