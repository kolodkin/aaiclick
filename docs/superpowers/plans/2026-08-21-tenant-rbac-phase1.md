# Tenant RBAC Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Multi-tenant RBAC backend core: tenant + membership tables, superadmin flag, tenant-aware JWTs and principals, `X-Tenant-Id` request scoping, tenant-filtered queries, `/tenants` API, CLI, migration, docs.

**Architecture:** A neutral `aaiclick/tenancy.py` module holds the active-tenant ContextVar (default tenant id `1`) so both orchestration and auth can import it without cycles. Tenant/membership tables live in `aaiclick/auth/models.py`; `users.role` becomes `users.superadmin`. JWTs carry `superadmin` + a `tenants` membership map; a new `require_tenant` FastAPI dependency resolves the active tenant from `X-Tenant-Id` and sets the ContextVar; `internal_api` query filters enforce isolation.

**Tech Stack:** SQLModel/SQLAlchemy (async), FastAPI, PyJWT, Alembic, pytest (chdb + SQLite local backend).

**Spec:** `docs/designs/tenant_rbac.md` (committed on this branch). Read it before starting.

## Global Constraints

- Follow root `CLAUDE.md`: all imports at top of file; `Literal` over enums; no `__all__`; no `Any` shortcuts; NamedTuples over plain tuples; no history comments.
- Follow the `python-testing-style` skill for every test file; router tests assert HTTP plumbing only (see `aaiclick/server/CLAUDE.md`), business logic tests live in `aaiclick/internal_api/test_*.py` / `aaiclick/auth/test_*.py`.
- Never hand-write Alembic migration files — use the `generate-migration` skill (Task 9).
- All work on branch `claude/tenant-rbac-ct6m28`; commit after every task; do not push until the final task says so.
- Run the fast test scope after each task: the test files you touched plus `aaiclick/auth`, `aaiclick/internal_api`, `aaiclick/server`.
- `DEFAULT_TENANT_ID = 1` everywhere — the default tenant row is seeded with this fixed id (not a snowflake), slug `default`.

---

### Task 1: Tenancy module (ContextVar + constants)

**Files:**
- Create: `aaiclick/tenancy.py`
- Test: `aaiclick/test_tenancy.py`

**Interfaces:**
- Produces: `DEFAULT_TENANT_ID: int = 1`, `DEFAULT_TENANT_SLUG: str = "default"`, `get_active_tenant_id() -> int`, `active_tenant(tenant_id: int)` context manager.
- Consumed by: internal_api query filters (Task 6), `factories.new_job_row` (Task 6), `require_tenant` (Task 5), CLI (Task 8).

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the active-tenant contextvar helpers."""

from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant, get_active_tenant_id


def test_default_tenant_when_unset():
    assert get_active_tenant_id() == DEFAULT_TENANT_ID


def test_active_tenant_sets_and_restores():
    with active_tenant(42):
        assert get_active_tenant_id() == 42
    assert get_active_tenant_id() == DEFAULT_TENANT_ID


def test_active_tenant_nests():
    with active_tenant(2):
        with active_tenant(3):
            assert get_active_tenant_id() == 3
        assert get_active_tenant_id() == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest aaiclick/test_tenancy.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aaiclick.tenancy'`

- [ ] **Step 3: Write the implementation**

```python
"""Active-tenant context shared by the server, CLI, and orchestration.

A neutral module (no auth or orchestration imports) so both sides can read
the active tenant without an import cycle. The server's ``require_tenant``
dependency and the CLI set it; ``internal_api`` query filters and the job
factories read it. Unset means the default tenant — local mode and existing
single-tenant deployments keep working with zero configuration.
See ``docs/designs/tenant_rbac.md``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

DEFAULT_TENANT_ID = 1
DEFAULT_TENANT_SLUG = "default"

_active_tenant_id: ContextVar[int] = ContextVar("active_tenant_id", default=DEFAULT_TENANT_ID)


def get_active_tenant_id() -> int:
    """Tenant id all tenant-scoped reads/writes are filtered by."""
    return _active_tenant_id.get()


@contextmanager
def active_tenant(tenant_id: int) -> Iterator[None]:
    """Set the active tenant for the duration of the block."""
    token = _active_tenant_id.set(tenant_id)
    try:
        yield
    finally:
        _active_tenant_id.reset(token)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest aaiclick/test_tenancy.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/tenancy.py aaiclick/test_tenancy.py
git commit -m "Add tenancy module: active-tenant contextvar + default tenant constants"
```

---

### Task 2: Tenant and membership models + store CRUD

**Files:**
- Modify: `aaiclick/auth/models.py` (add `Tenant`, `TenantMembership`; leave `User` untouched for now)
- Modify: `aaiclick/auth/store.py` (add tenant/membership CRUD)
- Test: `aaiclick/auth/test_store.py` (extend), `aaiclick/auth/test_models.py` (extend)

**Interfaces:**
- Produces models:
  - `Tenant(id: int, slug: str, name: str, created_at: datetime)` — table `tenants`, `slug` unique+indexed.
  - `TenantMembership(id: int, tenant_id: int, user_id: int, role: Role, created_at: datetime)` — table `tenant_memberships`, unique `(tenant_id, user_id)`.
- Produces store functions (all `async`, all raising the module's `ValueError` subclasses on domain errors):
  - `create_tenant(*, slug: str, name: str, tenant_id: int | None = None) -> Tenant` (raises new `SlugTaken(ValueError)`; `tenant_id` lets setup seed id `1`, default is `get_snowflake_id()`)
  - `get_tenant_by_id(tenant_id: int) -> Tenant | None`
  - `get_tenant_by_slug(slug: str) -> Tenant | None`
  - `list_tenants() -> list[Tenant]` (ordered by slug)
  - `set_membership(*, tenant_id: int, user_id: int, role: Role) -> TenantMembership` (upsert: update role if the row exists)
  - `remove_membership(*, tenant_id: int, user_id: int) -> bool` (True if a row was deleted)
  - `list_memberships_for_user(user_id: int) -> list[TenantMembership]`
  - `list_memberships_for_tenant(tenant_id: int) -> list[TenantMembership]`

**Model code to add** (append to `aaiclick/auth/models.py`; `UniqueConstraint` comes from `sqlalchemy`):

```python
class Tenant(SQLModel, table=True):
    __tablename__: ClassVar[str] = "tenants"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    slug: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    name: str = Field(sa_column=Column(String, nullable=False))
    created_at: datetime = Field(default_factory=utc_now)


class TenantMembership(SQLModel, table=True):
    __tablename__: ClassVar[str] = "tenant_memberships"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(sa_column=Column(BigInteger, ForeignKey("tenants.id"), nullable=False, index=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    role: Role = Field(sa_column=Column(String, nullable=False))
    created_at: datetime = Field(default_factory=utc_now)
```

**Store code shape** — mirror the existing `create_user` / `get_user_by_*` patterns exactly (open `get_sql_session`, `select`, commit, refresh). Upsert example:

```python
async def set_membership(*, tenant_id: int, user_id: int, role: Role) -> TenantMembership:
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(TenantMembership).where(
                    TenantMembership.tenant_id == tenant_id, TenantMembership.user_id == user_id
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            existing.role = role
            session.add(existing)
            await session.commit()
            await session.refresh(existing)
            return existing
        row = TenantMembership(id=get_snowflake_id(), tenant_id=tenant_id, user_id=user_id, role=role)
        session.add(row)
        await session.commit()
        await session.refresh(row)
        return row
```

- [ ] **Step 1: Write failing tests** in `aaiclick/auth/test_store.py`, following its existing async fixture style: create tenant, duplicate slug raises `SlugTaken`, get by slug/id, `set_membership` creates then updates role on second call, `remove_membership` returns True then False, list functions return rows. In `test_models.py` assert `Tenant.__tablename__ == "tenants"` and `TenantMembership.__tablename__ == "tenant_memberships"` matching the style of existing assertions there.
- [ ] **Step 2: Run** `python -m pytest aaiclick/auth -v` — new tests FAIL (ImportError).
- [ ] **Step 3: Implement** models + store per the shapes above.
- [ ] **Step 4: Run** `python -m pytest aaiclick/auth -v` — all PASS.
- [ ] **Step 5: Commit** `git commit -m "Add tenant and tenant_membership tables with store CRUD"`

---

### Task 3: users.superadmin + membership-aware JWTs + principal

This is the pivot task: `users.role` is replaced by `users.superadmin`, JWT claims change, and everything that referenced a global role is updated in one atomic commit so the tree stays green.

**Files:**
- Modify: `aaiclick/auth/models.py` (`User.role: Role` → `superadmin: bool`)
- Modify: `aaiclick/auth/store.py` (`create_user(..., superadmin: bool = False)`; `set_role` → `set_superadmin(user_id, superadmin: bool)`)
- Modify: `aaiclick/auth/security.py` (claims)
- Modify: `aaiclick/auth/view_models.py` (`MeView`, `UserView`, `CreateUserRequest`, `SetRoleRequest` → `SetSuperadminRequest`; add `TenantRoleView`)
- Modify: `aaiclick/internal_api/auth.py` (mint from memberships)
- Modify: `aaiclick/internal_api/users.py` (`set_role` → `set_superadmin`)
- Modify: `aaiclick/server/auth.py` (`Principal`)
- Modify: `aaiclick/server/routers/auth.py` (`/auth/me` returns tenants), `aaiclick/server/routers/users.py` (role route → superadmin route)
- Modify: `aaiclick/server/app.py` (`_seed_admin` creates a superadmin)
- Modify: `aaiclick/__main__.py` + `aaiclick/cli_renderers.py` (`user create --superadmin`, drop `--role` / `set-role` → `set-superadmin`)
- Test: update `aaiclick/auth/test_security.py`, `aaiclick/auth/test_store.py`, `aaiclick/internal_api/test_auth.py`, `aaiclick/internal_api/test_users.py`, `aaiclick/server/test_auth.py`, `aaiclick/server/routers/test_auth.py`, `aaiclick/server/routers/test_users.py`, `aaiclick/test_cli.py`

**Interfaces (produced, relied on by Tasks 4-8):**

```python
# security.py
class AccessClaims(NamedTuple):
    user_id: int
    superadmin: bool
    tenants: dict[int, str]          # tenant_id -> role

def encode_access_token(*, user_id: int, superadmin: bool, tenants: dict[int, str], secret: str, ttl: int) -> str
def decode_access_token(token: str, secret: str) -> AccessClaims

# server/auth.py
class Principal(NamedTuple):
    user_id: int | None
    username: str | None
    superadmin: bool
    tenants: dict[int, Role]         # membership map from the JWT

_SYNTHETIC_ADMIN = Principal(user_id=None, username=None, superadmin=True, tenants={})

# view_models.py
class TenantRoleView(BaseModel):
    tenant_id: int
    slug: str
    name: str
    role: Role

class MeView(BaseModel):
    id: int | None
    username: str | None
    superadmin: bool
    tenants: list[TenantRoleView]
```

**Key implementation points:**

- `encode_access_token` puts `"superadmin": superadmin` and `"tenants": {str(tid): role for tid, role in tenants.items()}` in the payload (JSON object keys must be strings); `decode_access_token` converts keys back with `int(k)` and defaults missing claims (`payload.get("superadmin", False)`, `payload.get("tenants", {})`) so malformed tokens raise `TokenError` only on truly missing `sub`/type.
- `internal_api/auth._mint_pair` signature becomes `_mint_pair(*, user: User, secret: str)`; it loads `await store.list_memberships_for_user(user.id)` and builds the tenants map. `login` / `refresh` pass the `User` row.
- `internal_api/users.set_superadmin(user_id, superadmin: bool)` keeps the revoke-all-sessions behavior `set_role` had.
- `/auth/me` resolves membership rows to `TenantRoleView` (join to `Tenant` for slug/name via new store helper `list_memberships_for_user` + `get_tenant_by_id`, or one loop — N is tiny). In local mode return `MeView(id=None, username=None, superadmin=True, tenants=[])`.
- `routers/users.py`: `PUT /{user_id}/role` becomes `PUT /{user_id}/superadmin` taking `SetSuperadminRequest {superadmin: bool}`.
- `_seed_admin` in `app.py`: `store.create_user(username=..., password_hash=..., superadmin=True)`.
- CLI: `aaiclick user create <username> --password P [--superadmin]`; `user set-superadmin <user_id> {true,false}` replaces `set-role`; renderers print `superadmin` instead of `role`. Remove now-unused `ROLE_*` imports from `__main__.py` (the `Role` literal itself stays — memberships use it).
- `ROLE_ADMIN` / `ROLE_VIEWER` / `Role` / `ROLES` stay in `auth/models.py` — they now describe membership roles.

- [ ] **Step 1: Update the tests first** across the files listed above to the new shapes (claims carry `superadmin`/`tenants`; login test asserts the minted token decodes to the user's membership map; `/auth/me` asserts the new body; CLI test drops `--role`).
- [ ] **Step 2: Run** `python -m pytest aaiclick/auth aaiclick/internal_api/test_auth.py aaiclick/internal_api/test_users.py aaiclick/server aaiclick/test_cli.py -x -q` — FAIL.
- [ ] **Step 3: Implement** all listed modifications.
- [ ] **Step 4: Run** the same scope — all PASS. Then run `python -m pytest aaiclick -x -q` for the full suite (other areas must be untouched).
- [ ] **Step 5: Commit** `git commit -m "Replace global role with superadmin flag and membership-aware JWTs"`

---

### Task 4: require_tenant / require_superadmin dependencies

**Files:**
- Modify: `aaiclick/server/auth.py`
- Modify: `aaiclick/server/app.py` (router wiring)
- Modify: `aaiclick/server/routers/users.py`, `aaiclick/server/routers/execution_workers.py` (guard swaps)
- Test: `aaiclick/server/test_auth.py`, `aaiclick/server/routers/test_execution_workers.py`, `aaiclick/server/routers/test_users.py`

**Interfaces:**
- Consumes: `Principal` (Task 3), `active_tenant` / `DEFAULT_TENANT_ID` (Task 1).
- Produces in `server/auth.py`:

```python
TENANT_HEADER = "X-Tenant-Id"

class TenantContext(NamedTuple):
    tenant_id: int
    role: Role

def resolve_tenant(principal: Principal, header_value: str | None) -> TenantContext
    # pure function, unit-testable:
    # - auth disabled (principal is synthetic) handled by caller
    # - header set: int() or raise Invalid("X-Tenant-Id must be an integer")
    #   role = principal.tenants.get(tid) or (ROLE_ADMIN if principal.superadmin else raise Forbidden)
    # - header missing: exactly one membership -> that one; else raise Invalid("X-Tenant-Id header required")

async def require_tenant(request: Request, principal: Principal = Depends(require_principal)) -> AsyncIterator[TenantContext]
    # yield-dependency: computes TenantContext (auth disabled -> TenantContext(DEFAULT_TENANT_ID, ROLE_ADMIN)),
    # wraps the request in `with active_tenant(ctx.tenant_id):` and yields ctx

async def require_admin(ctx: TenantContext = Depends(require_tenant)) -> TenantContext
    # tenant admin: ctx.role != ROLE_ADMIN -> Forbidden("tenant admin role required")

async def require_superadmin(principal: Principal = Depends(require_principal)) -> Principal
    # principal.superadmin or Forbidden("superadmin required")
```

- `AdminAuthMiddleware` (the `/mcp` guard) now checks `principal.superadmin` instead of `role == admin`; rename its message to `"superadmin required"`.
- `Invalid` is imported from `aaiclick.internal_api.errors` (it already maps to a 422 `Problem`).

**Wiring changes in `app.py`:** the tenant-scoped routers (`jobs`, `registered_jobs`, `tasks`, `objects`) switch their include-time dependency from `require_principal` to `require_tenant`; `execution_workers` keeps `require_principal` (reads are tenant-less). In `routers/users.py` swap `require_admin` → `require_superadmin`. In `routers/execution_workers.py` add `Depends(require_superadmin)` to the start and stop routes (imports from `..auth`).

**Mutating tenant-scoped routes keep `require_admin`** — it now means tenant admin and rides on `require_tenant`, so no per-route edits are needed in `jobs.py` / `registered_jobs.py` / `tasks.py` / `objects.py` beyond what FastAPI resolves automatically.

- [ ] **Step 1: Write failing tests.** Unit-test `resolve_tenant` in `aaiclick/server/test_auth.py` (header hit, header for non-member superadmin → admin, header for non-member non-superadmin → `Forbidden`, bad int → `Invalid`, missing header with 1 membership → implied, missing with 0 or 2 → `Invalid`). Router tests: with auth disabled (local test mode) tenant-scoped routes still answer without the header; worker start/stop returns 403 for a non-superadmin token when auth is enabled (follow the existing enabled-auth test pattern in `server/routers/test_auth.py` / `test_users.py` for minting tokens).
- [ ] **Step 2: Run** `python -m pytest aaiclick/server -x -q` — FAIL.
- [ ] **Step 3: Implement** per the interface block.
- [ ] **Step 4: Run** `python -m pytest aaiclick/server aaiclick/internal_api -q` — PASS.
- [ ] **Step 5: Commit** `git commit -m "Add require_tenant/require_superadmin; X-Tenant-Id request scoping"`

---

### Task 5: tenant_id columns on registered_jobs and jobs

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Modify: `aaiclick/orchestration/factories.py` (`new_job_row`)
- Modify: `aaiclick/orchestration/registered_jobs.py` (`_build_registered_job`)
- Test: `aaiclick/orchestration/test_orchestration_factories.py` (extend)

**Interfaces:**
- Consumes: `get_active_tenant_id` (Task 1).
- Produces: `RegisteredJob.tenant_id: int`, `Job.tenant_id: int` — both `sa_column=Column(BigInteger, ForeignKey("tenants.id"), nullable=False, index=True, server_default="1")`, model `default=DEFAULT_TENANT_ID`. `RegisteredJob.__table_args__` changes from `UniqueConstraint("name")` to `UniqueConstraint("tenant_id", "name")` (names are unique per tenant).

**Stamping rules:**
- `_build_registered_job` sets `tenant_id=get_active_tenant_id()`.
- `new_job_row` sets `tenant_id=registered.tenant_id if registered is not None else get_active_tenant_id()` — a scheduled run inherits its registration's tenant; a manual/decorator run uses the caller's active tenant.

`aaiclick/orchestration/models.py` must NOT import from `aaiclick.auth` (cycle) — the FK references the table name string `"tenants.id"`, which is fine; `aaiclick.tenancy` is neutral and safe to import in `factories.py` / `registered_jobs.py`.

- [ ] **Step 1: Write failing tests**: `new_job_row(...)` inside `with active_tenant(7)` yields `tenant_id == 7`; with a `registered` row of `tenant_id=9` yields 9; default context yields `DEFAULT_TENANT_ID`.
- [ ] **Step 2: Run** `python -m pytest aaiclick/orchestration/test_orchestration_factories.py -q` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `python -m pytest aaiclick/orchestration -q` (SQLite fixtures build tables from metadata, so the new columns just appear) — PASS.
- [ ] **Step 5: Commit** `git commit -m "Add tenant_id to registered_jobs and jobs; per-tenant name uniqueness"`

---

### Task 6: Tenant-filtered queries in internal_api

**Files:**
- Modify: `aaiclick/internal_api/jobs.py`, `aaiclick/internal_api/registered_jobs.py`, `aaiclick/internal_api/tasks.py`
- Modify: `aaiclick/orchestration/registered_jobs.py` (`get_registered_job` name lookup filters tenant)
- Test: `aaiclick/internal_api/test_jobs.py`, `aaiclick/internal_api/test_registered_jobs.py`, `aaiclick/internal_api/test_tasks.py` (extend)

**Interfaces:** Consumes `get_active_tenant_id` (Task 1) and `tenant_id` columns (Task 5). No signature changes — filtering is invisible to callers.

**Filter rules (cross-tenant access is `NotFound`, never `Forbidden` — no existence leak):**
- `jobs.list_jobs`: add `predicates.append(Job.tenant_id == get_active_tenant_id())` unconditionally.
- `jobs._resolve_job`: every `select(Job)` adds `.where(Job.tenant_id == get_active_tenant_id())` (all three lookups).
- `registered_jobs.list_registered_jobs`: same unconditional predicate on `RegisteredJob.tenant_id`.
- `orchestration/registered_jobs.get_registered_job` (name → row): add the tenant predicate; enable/disable/register flow through it and are covered automatically.
- `tasks.get_task` / `get_task_logs` / `clear_task`: tasks have no tenant column — after loading the `Task`, load its `Job` and treat a tenant mismatch as `NotFound(f"task {task_id} not found")`. Factor one helper in `tasks.py`:

```python
async def _require_task_in_tenant(session: AsyncSession, task: Task) -> None:
    job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None or job.tenant_id != get_active_tenant_id():
        raise NotFound(f"task {task.id} not found")
```

- [ ] **Step 1: Write failing tests**: create a job under `active_tenant(2)` and another under the default tenant; assert `list_jobs` under tenant 2 sees only its job, `get_job` on the other tenant's id raises `NotFound`, same pattern for registered jobs (plus: same name registers cleanly in two tenants), and `get_task` on a task of the other tenant's job raises `NotFound`.
- [ ] **Step 2: Run** `python -m pytest aaiclick/internal_api -q` — new tests FAIL.
- [ ] **Step 3: Implement** the filters.
- [ ] **Step 4: Run** `python -m pytest aaiclick/internal_api aaiclick/orchestration -q` — PASS.
- [ ] **Step 5: Commit** `git commit -m "Filter jobs, registered jobs, and tasks by active tenant"`

---

### Task 7: /tenants API (internal_api + router)

**Files:**
- Create: `aaiclick/internal_api/tenants.py`, `aaiclick/internal_api/test_tenants.py`
- Create: `aaiclick/server/routers/tenants.py`, `aaiclick/server/routers/test_tenants.py`
- Modify: `aaiclick/auth/view_models.py` (requests/views), `aaiclick/internal_api/__init__.py`, `aaiclick/server/app.py` (include router)

**Interfaces:**

```python
# view_models.py additions
class TenantView(BaseModel):
    id: int
    slug: str
    name: str
    created_at: datetime

class CreateTenantRequest(BaseModel):
    slug: str            # validated: ^[a-z0-9_]+$ via pydantic field_validator, raise ValueError
    name: str

class MemberView(BaseModel):
    user_id: int
    username: str
    role: Role

class SetMemberRequest(BaseModel):
    role: Role

# internal_api/tenants.py (async, inside orch_context)
async def create_tenant(request: CreateTenantRequest) -> TenantView          # Conflict on SlugTaken
async def list_tenants() -> Page[TenantView]
async def get_tenant(tenant_id: int) -> TenantView                           # NotFound
async def list_members(tenant_id: int) -> Page[MemberView]                   # NotFound on unknown tenant
async def set_member(tenant_id: int, user_id: int, role: Role) -> MemberView # NotFound on unknown tenant/user; revokes user sessions
async def remove_member(tenant_id: int, user_id: int) -> None                # NotFound if no membership; revokes user sessions
```

`set_member` / `remove_member` call `store.revoke_all_for_user(user_id)` — membership changes must not be outlived by refresh tokens minting the old map (same rationale as `set_role` had).

**Router** (`prefix="/tenants"`, `tags=["tenants"]`, `dependencies=[Depends(orch_scope)]`), guards per spec role matrix:

```python
@router.get("", ...)                      # require_superadmin
@router.post("", status_code=201, ...)    # require_superadmin
@router.get("/{tenant_id}", ...)          # member-or-superadmin: check principal.tenants / superadmin inline, else NotFound
@router.get("/{tenant_id}/members", ...)  # _require_tenant_admin(principal, tenant_id)
@router.put("/{tenant_id}/members/{user_id}", ...)     # _require_tenant_admin
@router.delete("/{tenant_id}/members/{user_id}", status_code=204, ...)  # _require_tenant_admin
```

with one helper in the router module (`X-Tenant-Id` is NOT used here — the path names the tenant):

```python
def _require_tenant_admin(principal: Principal, tenant_id: int) -> None:
    if principal.superadmin or principal.tenants.get(tenant_id) == ROLE_ADMIN:
        return
    raise Forbidden("tenant admin role required")
```

Include in `app.py` next to the users router: `app.include_router(tenants_router.router, prefix=API_PREFIX, dependencies=[Depends(require_principal)])`.

- [ ] **Step 1: Write failing internal_api tests** (create/list/get/members lifecycle, duplicate slug → `Conflict`, unknown ids → `NotFound`, set_member revokes refresh rows — assert via `store.get_active_refresh` returning None after).
- [ ] **Step 2: Run** `python -m pytest aaiclick/internal_api/test_tenants.py -q` — FAIL.
- [ ] **Step 3: Implement** internal_api module.
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Write failing router tests** (status codes + Problem envelopes only; local mode = synthetic superadmin passes guards).
- [ ] **Step 6: Run** `python -m pytest aaiclick/server/routers/test_tenants.py -q` — FAIL, then implement router + wiring, re-run — PASS.
- [ ] **Step 7: Run** `python -m pytest aaiclick/server aaiclick/internal_api -q` — PASS.
- [ ] **Step 8: Commit** `git commit -m "Add /tenants API: tenant CRUD and membership management"`

---

### Task 8: CLI + setup seeding

**Files:**
- Modify: `aaiclick/__main__.py` (tenant/member subcommands, global `--tenant`), `aaiclick/cli_renderers.py`
- Modify: `aaiclick/internal_api/setup.py` (seed default tenant)
- Test: `aaiclick/test_cli.py`, `aaiclick/internal_api/test_setup.py` (extend)

**Interfaces:**
- Consumes: `internal_api.tenants` (Task 7), `store` membership CRUD (Task 2), `active_tenant` (Task 1).
- Produces CLI surface:
  - `aaiclick tenant create <slug> --name NAME`, `aaiclick tenant list`
  - `aaiclick member add --tenant SLUG --username U --role {admin,viewer}`, `member set-role` (same args), `member remove --tenant SLUG --username U`
  - Top-level `--tenant SLUG` option (on the root parser, next to the existing global flags): resolves slug → id via `store.get_tenant_by_slug` inside the command's `orch_context` and wraps the command in `active_tenant(...)`; unknown slug exits with an error. Default: unset → default tenant.
- `setup()` seeds the default tenant after `SQLModel.metadata.create_all`: insert `Tenant(id=DEFAULT_TENANT_ID, slug=DEFAULT_TENANT_SLUG, name="Default")` if no row with that id exists (idempotent re-run).

Member commands resolve `--username` → user id via `store.get_user_by_username` and `--tenant` → tenant id via `store.get_tenant_by_slug`, then call `internal_api.tenants.set_member` / `remove_member`. Renderers follow the existing `render_user` table style.

- [ ] **Step 1: Write failing CLI tests** in `aaiclick/test_cli.py` following its existing invocation pattern (tenant create/list round-trip; member add then set-role then remove; `--tenant` scoping: register a job under `--tenant t2`, `registered-job list` under default tenant doesn't show it). Extend `test_setup.py`: after `setup()`, the default tenant exists; running `setup()` twice stays clean.
- [ ] **Step 2: Run** `python -m pytest aaiclick/test_cli.py aaiclick/internal_api/test_setup.py -q` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** same scope, then `python -m pytest aaiclick -q` full suite — PASS.
- [ ] **Step 5: Commit** `git commit -m "Add tenant/member CLI commands, --tenant scoping, default-tenant seed"`

---

### Task 9: Alembic migration

**Files:**
- Created by tooling: `aaiclick/orchestration/migrations/versions/<rev>_tenant_rbac.py`

**Process:** Use the `generate-migration` skill (GitHub Actions autogenerate — never hand-write the file). Autogenerate will emit: `tenants` + `tenant_memberships` tables, `tenant_id` columns (with `server_default="1"`), the `registered_jobs` unique-constraint change, `users.superadmin` add + `users.role` drop. Then **edit the generated file** to add the data steps in `upgrade()` in this order:

1. After `tenants` is created, before the FK columns land: `op.execute("INSERT INTO tenants (id, slug, name, created_at) VALUES (1, 'default', 'Default', CURRENT_TIMESTAMP)")`.
2. After `users.superadmin` is added, before `users.role` is dropped: `op.execute("UPDATE users SET superadmin = (role = 'admin')")`.
3. Before `users.role` is dropped: give existing viewers a default-tenant membership — `op.execute("INSERT INTO tenant_memberships (id, tenant_id, user_id, role, created_at) SELECT id, 1, id, 'viewer', CURRENT_TIMESTAMP FROM users WHERE role = 'viewer'")` (reusing the user id as the membership id is safe: snowflakes are globally unique and the column is just a PK).

`tenant_id` columns need no backfill — `server_default="1"` covers existing rows. Verify `downgrade()` reverses cleanly (drop columns/tables, restore `users.role` from `superadmin` mapping admin/viewer).

- [ ] **Step 1:** Invoke the `generate-migration` skill and follow it end to end.
- [ ] **Step 2:** Edit the generated revision to add the three data steps above; re-read the whole file for ordering.
- [ ] **Step 3:** Sanity-check locally: `python -m pytest aaiclick/internal_api/test_setup.py aaiclick/orchestration -q` (migration tests if the repo has them; setup path unaffected).
- [ ] **Step 4: Commit** `git commit -m "Add tenant RBAC migration: tenants, memberships, tenant_id, superadmin"`

---

### Task 10: Docs

**Files:**
- Modify: `docs/designs/auth.md` (role matrix + pointers to `tenant_rbac.md`; update Data Model `users` table, module layout, claims description)
- Modify: `docs/designs/tenant_rbac.md` (add implementation references by symbol name per `markdown-style` — e.g. `**Implementation**: aaiclick/server/auth.py — see require_tenant`)
- Modify: `docs/designs/future.md` (note phases 2-3 as the pending work, referencing the spec)

Use the `markdown-style` skill rules (setext title, ATX one level, aligned tables) and run the `shortify` skill after editing.

- [ ] **Step 1:** Update the three docs.
- [ ] **Step 2:** Run `shortify` on the edited docs.
- [ ] **Step 3: Commit** `git commit -m "Document tenant RBAC in auth/tenant_rbac/future design docs"`

---

### Task 11: Full verification + push

- [ ] **Step 1:** `python -m pytest aaiclick -q` — full suite green (paste the summary line as evidence).
- [ ] **Step 2:** Repo lint/format checks if configured (check `pyproject.toml` for ruff config; run `ruff check aaiclick` and `ruff format --check aaiclick` if present).
- [ ] **Step 3:** `git push -u origin claude/tenant-rbac-ct6m28` (retry with backoff on network failure only).
- [ ] **Step 4:** Use the `check-pr` skill per root CLAUDE.md to verify GitHub Actions workflows pass; fix failures.
