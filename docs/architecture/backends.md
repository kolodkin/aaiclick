Backends
---

aaiclick runs on two backend pairs. The Object and task APIs are the same on
both; these are the differences.

<!-- req: backend-capabilities -->
| Concern                         | Local: chdb + SQLite                            | Distributed: ClickHouse + PostgreSQL         |
|---------------------------------|-------------------------------------------------|----------------------------------------------|
| Processes                       | one: `local start` runs server and both workers | server, execution workers, background worker |
| Auth                            | off: a synthetic admin, bound to `127.0.0.1`    | required: session or API token               |
| Container runners               | none: tasks run in a child process              | `AAICLICK_RUNNER=docker` or `kubernetes`     |
| Local file paths                | read and export                                 | raise: a server would use its own disk       |
| `.bz2` export                   | raises `ValueError`                             | supported                                    |
| S3 `connection=` named sessions | ignored                                         | used                                         |
| `QueryStats`                    | `read_rows`, `read_bytes`, `elapsed_s` only     | also result and written counts               |
| Change signal for live views    | in-process bus                                  | PostgreSQL `LISTEN` / `NOTIFY`               |

<!-- req: one-chdb-session-per-process -->
chdb holds one embedded engine per process. A nested context reuses the
outer client rather than opening a second session, and a session cannot be
reopened in the same process after cleanup — see
`docs/contributor/testing.md` — "chdb Session Constraint".
