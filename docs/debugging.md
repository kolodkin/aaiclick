Debugging
---

# VS Code Setup

Add a launch configuration to `.vscode/launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: IMDB",
            "type": "debugpy",
            "request": "launch",
            "module": "aaiclick.example_projects.imdb_dataset_builder.imdb_dataset_builder",
            "args": ["--run"],
            "env": {
                "AAICLICK_DEBUGGER": "1"
            }
        }
    ]
}
```

# Debug Console and ContextVars

aaiclick uses Python `ContextVar` for managing database clients (ClickHouse, SQLAlchemy)
within `data_context()` and `orch_context()`. The VS Code debug console evaluates each
`await` expression in a **fresh Context copy**, which means `ContextVar` values set by the
running task are not visible.

## Symptoms

Evaluating `await obj.data()` or `await obj.count()` in the debug console raises:

```
RuntimeError: No active data or orch context
```

Even though the same code works fine during normal execution.

## AAICLICK_DEBUGGER env var

Set `AAICLICK_DEBUGGER=1` to enable automatic chdb client creation when the `ContextVar`
is empty. This is safe because it only activates when:

1. The `ContextVar` has no client (normal code paths always have one)
2. The `AAICLICK_DEBUGGER` env var is explicitly set
3. The backend is chdb (local mode)

With this env var set, `await` expressions in the debug console work transparently.

## What works without AAICLICK_DEBUGGER

- Inspecting local variables in the Variables panel
- Evaluating sync expressions (`obj.columns`, `obj.name`, `len(data)`)
- Evaluating `await` on coroutines that don't use ContextVars

## Manual fallback

If `AAICLICK_DEBUGGER` is not set, you can manually set up the client in the debug console:

```python
from aaiclick.orchestration.execution.debug import debug_orch
await debug_orch()
```

!!! warning "Each `await` runs in a fresh Context"
    The `debug_orch()` call sets ContextVars, but subsequent `await` evaluations
    may not see them. The `AAICLICK_DEBUGGER` env var approach is more reliable
    because it uses a module-level global instead of ContextVars.
