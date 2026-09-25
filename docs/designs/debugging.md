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

The VS Code debug console evaluates each `await` in a fresh Python Context, making
`ContextVar`-based clients invisible. Set `AAICLICK_DEBUGGER=1` to auto-create a
chdb client fallback, so `await obj.data()` works in the debug console.

Only activates when the `ContextVar` is empty and the backend is chdb (local mode).

# AAICLICK_DEBUG

The CLI turns an unexpected exception into one `error: <Type>: <message>`
line on stderr and exit code 1. Set `AAICLICK_DEBUG=1` to get the full
traceback instead — include it in bug reports.

**Implementation**: `aaiclick/__main__.py` (`main`)
