# Example Projects Guidelines

**Example projects MUST use the aaiclick API exclusively.**

- Use `Object` methods: `concat()`, `insert()`, `group_by()`, `agg()`, `with_columns()`, `rename()`, `view()`, operators, etc.
- Use `create_object()`, `create_object_from_url()`, `create_object_from_value()`
- **Do NOT use raw ClickHouse queries** (`ch.command()`, `ch.query()`) unless there is absolutely no aaiclick API equivalent
- If raw SQL seems necessary, **ask the user for approval first** — it likely means the API needs extending
- Example projects serve as the public API showcase; they should demonstrate best practices, not internal workarounds
