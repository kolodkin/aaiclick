# Local-mode Server Lifespan — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `BackgroundWorker` + execution `worker_main_loop` into the FastAPI / FastMCP `lifespan` so that `python -m aaiclick local start` (and `uvicorn aaiclick.server.app:app`) run the combined REST + MCP server with workers in a single local-mode process.

**Architecture:** A new `local_runtime()` async context manager owns startup / shutdown sequencing for the workers. The server's existing `app.py` swaps its lifespan for one that chains FastMCP's lifespan with `local_runtime()` when `is_local()` is true. The CLI verb `local start` becomes a thin wrapper that launches uvicorn against `app`.

**Tech Stack:** FastAPI, FastMCP, uvicorn, asyncio, asynccontextmanager, httpx + asgi-lifespan for tests.

**Spec:** `docs/superpowers/specs/2026-04-26-local-mode-server-lifespan-design.md`
**PR:** https://github.com/kolodkin/aaiclick/pull/262
**Branch:** `claude/fastapi-lifespan-worker-X8KZm`

---

## Phases

| Phase | Subject | Plan file |
|-------|---------|-----------|
| 1 | `local_runtime()` helper module + tests | `2026-04-26-local-mode-server-lifespan-phase-1-runtime.md` |
| 2 | Server-side lifespan wiring (`app.py`) + lifespan tests | `2026-04-26-local-mode-server-lifespan-phase-2-lifespan.md` |
| 3 | CLI rewrite (`start_local`) + docs updates | `2026-04-26-local-mode-server-lifespan-phase-3-cli-docs.md` |

After each phase:

- Run `/check-pr` — confirm CI is green on the branch.
- Run `/simplify` — review the changed code for reuse, quality, and efficiency; fix any issues found.
- Move to the next phase only when both pass.

## Independence between phases

- **Phase 1** introduces a new module that nothing imports. Merging Phase 1 alone does not change runtime behaviour.
- **Phase 2** rewires the server's lifespan to call `local_runtime()`. After Phase 2, `uvicorn aaiclick.server.app:app` works in local mode; the CLI verb `local start` still runs the legacy standalone-worker path (unchanged from today).
- **Phase 3** rewrites `start_local()` to launch uvicorn against `app`. This is the breaking change for CLI behaviour. Removes `--max-tasks` from the `local start` argparse.

Each phase ends with the project building, all tests passing, and CI green on the PR.

## Out of scope (tracked elsewhere)

- Splitting REST and MCP into separate ASGI apps. The spec records why the combined app is preferred (chdb single-process constraint in local mode).
- Drain-with-timeout for in-flight tasks on shutdown — `docs/future.md`.
- Single-instance file lock — chdb's lock is the existing guard; an explicit lock can land later.
