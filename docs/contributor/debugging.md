Debugging
---

# VS Code

Example `.vscode/launch.json`:

```json
{
    "configurations": [
        {
            "name": "Debug: Basic Orchestration",
            "type": "debugpy",
            "request": "launch",
            "module": "aaiclick.orchestration.examples.orchestration_basic",
            "env": { "AAICLICK_DEBUGGER": "1" }
        }
    ]
}
```

# AAICLICK_DEBUGGER

The VS Code debug console evaluates each `await` in a fresh Python Context,
making `ContextVar`-based clients invisible. `AAICLICK_DEBUGGER=1` creates a
fallback chdb client when the `ContextVar` is empty and the backend is chdb
(local mode), so `await obj.data()` works in the debug console.
