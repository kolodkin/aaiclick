# Authentication, Users & RBAC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the static `AAICLICK_API_TOKEN` bearer with username/password users, admin/viewer RBAC, and login sessions (short-lived access JWT + rotating refresh token) across the REST + MCP surfaces, the CLI, and the SPA.

**Architecture:** A new self-contained `aaiclick/auth/` package holds the SQL models (`users`, `refresh_tokens`), pure crypto helpers (`security.py`), env config (`config.py`), and the raw DB layer (`store.py`, owns `get_sql_session()` and raises auth domain errors — mirrors `orchestration/registered_jobs.py`). `internal_api/auth.py` + `internal_api/users.py` are thin, transport-agnostic wrappers that map domain errors to the existing `InternalApiError` taxonomy and return pydantic view models. `server/auth.py` is rewritten to resolve a `Principal` from the access JWT via FastAPI's `HTTPBearer`, gate on `AAICLICK_AUTH_ENABLED`, and enforce `require_admin`; the `/mcp` mount keeps an evolved ASGI middleware (admin-only) because `Depends` does not reach mounted sub-apps.

**Tech Stack:** Python 3.11, SQLModel + Alembic (snowflake `BigInteger` PKs), FastAPI 0.136 (`HTTPBearer`), `bcrypt` (passwords), `pyjwt` (HS256 JWT), React 19 + Vite + Tailwind (SPA).

**Spec:** `docs/auth.md` is the design of record. Read it before starting.

**Conventions (do not violate):**
- All imports at top of file (CLAUDE.md). No `Any` shortcuts. `Literal` + `_enum_check`, not enums, for string columns.
- Tests: see `python-testing-style` skill. Async tests depend on the `orch_ctx` fixture. Server tests use the `app_client` fixture (`httpx.AsyncClient` + `ASGITransport`); never `TestClient`. No `caplog` — patch loggers.
- Build URLs in tests with `API_PREFIX` (`from aaiclick.server.app import API_PREFIX`), e.g. `f"{API_PREFIX}/auth/login"`.
- Commit after every green step. Run `git config user.email 2054182+kolodkin@users.noreply.github.com && git config user.name "mark kolodkin"` once at the start (these commits are intentionally Unverified per the user's choice).
- Migrations: use the `generate-migration` skill — never hand-write a migration file.

---

## File Structure

**New files**
- `aaiclick/auth/__init__.py` — re-export public names (no `__all__`).
- `aaiclick/auth/models.py` — `User`, `RefreshToken` tables; `Role` literal + `ROLE_ADMIN`/`ROLE_VIEWER`/`ROLES`.
- `aaiclick/auth/config.py` — env getters: `auth_enabled()`, `jwt_secret()`, `require_jwt_secret()`, `access_ttl()`, `refresh_ttl()`, `admin_seed()`.
- `aaiclick/auth/security.py` — pure: `hash_password`, `verify_password`, `generate_secret`, `sha256_hex`, `encode_access_token`, `decode_access_token`, `TokenError`.
- `aaiclick/auth/store.py` — raw DB CRUD over `users` / `refresh_tokens`; domain errors `UsernameTaken`, `UserNotFound`, `RefreshInvalid`.
- `aaiclick/auth/view_models.py` — `LoginRequest`, `RefreshRequest`, `LogoutRequest`, `TokenPair`, `MeView`, `UserView`, `CreateUserRequest`, `SetRoleRequest`, `SetPasswordRequest`, `UserListFilter`.
- `aaiclick/internal_api/auth.py` — `login`, `refresh`, `logout`.
- `aaiclick/internal_api/users.py` — `create_user`, `list_users`, `get_user`, `set_role`, `disable_user`, `set_password`.
- `aaiclick/server/routers/auth.py` — `/auth/login|refresh|logout|me`.
- `aaiclick/server/routers/users.py` — `/users` (admin-only).
- Tests: `aaiclick/auth/test_security.py`, `aaiclick/internal_api/test_auth.py`, `aaiclick/internal_api/test_users.py`, `aaiclick/server/routers/test_auth.py`, `aaiclick/server/routers/test_users.py`. Rewrite `aaiclick/server/test_auth.py`.
- SPA: `src/lib/auth.ts`, `src/components/Auth.tsx`, `src/views/Login.tsx`.

**Modified files**
- `pyproject.toml` — add `bcrypt`, `pyjwt` to core `dependencies`.
- `aaiclick/orchestration/migrations/env.py` — import `aaiclick.auth.models`.
- `aaiclick/server/auth.py` — full rewrite (Principal resolution, gating, `require_admin`, evolved middleware; remove static token).
- `aaiclick/server/app.py` — swap `require_bearer`→`require_principal`; include `auth`/`users` routers; add `require_admin` to mutating routes; seed admin in lifespan; update description.
- `aaiclick/server/routers/{jobs,workers,objects,registered_jobs}.py` — `Depends(require_admin)` on mutating endpoints.
- `aaiclick/__main__.py` + `aaiclick/cli_renderers.py` — `aaiclick user` subcommands.
- `src/api/client.ts`, `src/App.tsx`, `src/main.tsx` — auth header + 401-refresh, login gate, AuthProvider.
- Docs: `docs/api_server.md`, `docs/future.md`, `aaiclick/server/CLAUDE.md`, `docs/auth.md` (status → IMPLEMENTED).

---

## Phase 0: Dependencies & package scaffold

### Task 0.1: Add auth dependencies

**Files:** Modify `pyproject.toml`

- [ ] **Step 1: Add `bcrypt` and `pyjwt` to core `dependencies`**

In `pyproject.toml`, the `[project].dependencies` list (currently ending `"croniter>=6.0.0",`) gains two entries. They are **core** (not the `server` extra) because the CLI's `aaiclick user create` runs `internal_api` in-process and needs `bcrypt`; the top-imports rule forbids hiding the import.

```toml
    "croniter>=6.0.0",
    "bcrypt>=4.2.0",
    "pyjwt>=2.10.0",
]
```

- [ ] **Step 2: Sync the environment**

Run: `cd /home/user/aaiclick && uv sync --extra server --extra distributed --extra ai`
Expected: resolves and installs `bcrypt` and `pyjwt` (or `PyJWT`).

- [ ] **Step 3: Verify imports**

Run: `.venv/bin/python -c "import bcrypt, jwt; print(bcrypt.__version__, jwt.__version__)"`
Expected: prints two version strings, no `ModuleNotFoundError`.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Add bcrypt and pyjwt core dependencies for auth"
```

### Task 0.2: Create the auth package

**Files:** Create `aaiclick/auth/__init__.py`

- [ ] **Step 1: Create empty package init**

```python
"""Authentication, users, and RBAC primitives.

See docs/auth.md for the design. Public names are re-exported as the
package grows (models, security, config); no __all__ per project style.
"""
```

- [ ] **Step 2: Verify importable**

Run: `.venv/bin/python -c "import aaiclick.auth; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add aaiclick/auth/__init__.py
git commit -m "Scaffold aaiclick.auth package"
```

---

## Phase 1: Config + security primitives (pure, TDD)

### Task 1.1: `auth/config.py`

**Files:** Create `aaiclick/auth/config.py`, `aaiclick/auth/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/auth/test_config.py
import pytest

from aaiclick.auth import config


def test_auth_disabled_by_default(monkeypatch):
    monkeypatch.delenv("AAICLICK_AUTH_ENABLED", raising=False)
    assert config.auth_enabled() is False


def test_auth_enabled_truthy(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    assert config.auth_enabled() is True


def test_ttls_have_defaults(monkeypatch):
    monkeypatch.delenv("AAICLICK_JWT_ACCESS_TTL", raising=False)
    monkeypatch.delenv("AAICLICK_JWT_REFRESH_TTL", raising=False)
    assert config.access_ttl() == 1800
    assert config.refresh_ttl() == 1209600


def test_require_jwt_secret_raises_when_enabled_and_unset(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.delenv("AAICLICK_JWT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_JWT_SECRET"):
        config.require_jwt_secret()


def test_admin_seed_none_when_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_ADMIN_USERNAME", raising=False)
    monkeypatch.delenv("AAICLICK_ADMIN_PASSWORD", raising=False)
    assert config.admin_seed() is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/auth/test_config.py -q`
Expected: FAIL — `ModuleNotFoundError: aaiclick.auth.config`.

- [ ] **Step 3: Implement `config.py`**

```python
"""Environment configuration for auth. Inline env reads, mirroring backend.py."""

from __future__ import annotations

import os
from typing import NamedTuple

ENV_ENABLED = "AAICLICK_AUTH_ENABLED"
ENV_SECRET = "AAICLICK_JWT_SECRET"
ENV_ACCESS_TTL = "AAICLICK_JWT_ACCESS_TTL"
ENV_REFRESH_TTL = "AAICLICK_JWT_REFRESH_TTL"
ENV_ADMIN_USERNAME = "AAICLICK_ADMIN_USERNAME"
ENV_ADMIN_PASSWORD = "AAICLICK_ADMIN_PASSWORD"

DEFAULT_ACCESS_TTL = 1800
DEFAULT_REFRESH_TTL = 1209600

_TRUTHY = {"1", "true", "yes", "on"}


class AdminSeed(NamedTuple):
    username: str
    password: str


def auth_enabled() -> bool:
    return os.getenv(ENV_ENABLED, "").strip().lower() in _TRUTHY


def jwt_secret() -> str | None:
    return os.getenv(ENV_SECRET) or None


def require_jwt_secret() -> str:
    """Return the signing secret, raising if auth is enabled but it is unset."""
    secret = jwt_secret()
    if secret is None:
        raise RuntimeError(f"{ENV_ENABLED}=true requires {ENV_SECRET} to be set")
    return secret


def access_ttl() -> int:
    return int(os.getenv(ENV_ACCESS_TTL, DEFAULT_ACCESS_TTL))


def refresh_ttl() -> int:
    return int(os.getenv(ENV_REFRESH_TTL, DEFAULT_REFRESH_TTL))


def admin_seed() -> AdminSeed | None:
    username = os.getenv(ENV_ADMIN_USERNAME)
    password = os.getenv(ENV_ADMIN_PASSWORD)
    if username and password:
        return AdminSeed(username, password)
    return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/auth/test_config.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/auth/config.py aaiclick/auth/test_config.py
git commit -m "Add auth config env getters"
```

### Task 1.2: `auth/security.py`

**Files:** Create `aaiclick/auth/security.py`, `aaiclick/auth/test_security.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/auth/test_security.py
import time

import pytest

from aaiclick.auth import security

SECRET = "test-secret-key"


def test_password_round_trip():
    h = security.hash_password("hunter2")
    assert h != "hunter2"
    assert security.verify_password("hunter2", h) is True
    assert security.verify_password("wrong", h) is False


def test_sha256_hex_stable():
    assert security.sha256_hex("abc") == security.sha256_hex("abc")
    assert security.sha256_hex("abc") != security.sha256_hex("abd")


def test_generate_secret_unique():
    assert security.generate_secret() != security.generate_secret()


def test_access_token_round_trip():
    token = security.encode_access_token(user_id=42, role="admin", secret=SECRET, ttl=60)
    claims = security.decode_access_token(token, SECRET)
    assert claims.user_id == 42
    assert claims.role == "admin"


def test_decode_rejects_bad_signature():
    token = security.encode_access_token(user_id=1, role="viewer", secret=SECRET, ttl=60)
    with pytest.raises(security.TokenError):
        security.decode_access_token(token, "other-secret")


def test_decode_rejects_expired():
    token = security.encode_access_token(user_id=1, role="viewer", secret=SECRET, ttl=-1)
    with pytest.raises(security.TokenError):
        security.decode_access_token(token, SECRET)


def test_decode_rejects_wrong_type():
    import jwt

    bad = jwt.encode({"sub": "1", "role": "admin", "type": "refresh"}, SECRET, algorithm="HS256")
    with pytest.raises(security.TokenError):
        security.decode_access_token(bad, SECRET)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/auth/test_security.py -q`
Expected: FAIL — `ModuleNotFoundError: aaiclick.auth.security`.

- [ ] **Step 3: Implement `security.py`**

JWT `exp`/`iat` use timezone-aware UTC (pyjwt's native expectation); DB datetime columns elsewhere use the project's naive `utc_now`. Keep these separate — do not mix.

```python
"""Pure auth crypto: password hashing, token secrets, and access-JWT codec.

No DB, no contextvars, no env reads — callers pass secrets/TTLs explicitly.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import bcrypt
import jwt

TOKEN_TYPE_ACCESS = "access"


class TokenError(Exception):
    """Access token is missing, malformed, expired, or wrong type."""


class AccessClaims(NamedTuple):
    user_id: int
    role: str


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), password_hash.encode())


def generate_secret() -> str:
    """Opaque URL-safe secret for refresh tokens."""
    return secrets.token_urlsafe(32)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def encode_access_token(*, user_id: int, role: str, secret: str, ttl: int) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "role": role,
        "type": TOKEN_TYPE_ACCESS,
        "iat": now,
        "exp": now + timedelta(seconds=ttl),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_access_token(token: str, secret: str) -> AccessClaims:
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise TokenError(str(exc)) from exc
    if payload.get("type") != TOKEN_TYPE_ACCESS:
        raise TokenError("not an access token")
    try:
        return AccessClaims(user_id=int(payload["sub"]), role=payload["role"])
    except (KeyError, ValueError) as exc:
        raise TokenError("malformed claims") from exc
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/auth/test_security.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/auth/security.py aaiclick/auth/test_security.py
git commit -m "Add auth security primitives (bcrypt + JWT codec)"
```

---

## Phase 2: Models + migration

### Task 2.1: `auth/models.py`

**Files:** Create `aaiclick/auth/models.py`; Modify `aaiclick/orchestration/migrations/env.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/auth/test_models.py
from sqlmodel import select

from aaiclick.auth.models import ROLE_ADMIN, RefreshToken, User
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


async def test_user_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="alice", password_hash="x", role=ROLE_ADMIN))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(User).where(User.username == "alice"))).scalar_one()
        assert row.id == uid
        assert row.role == ROLE_ADMIN
        assert row.disabled is False


async def test_refresh_token_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="bob", password_hash="x", role=ROLE_ADMIN))
        session.add(RefreshToken(id=get_snowflake_id(), user_id=uid, token_hash="h", expires_at=__import__("aaiclick.datetime_utils", fromlist=["utc_now"]).utc_now()))
        await session.commit()
```

> Note: keep the import clean in the real file — `from aaiclick.datetime_utils import utc_now` at the top; the inline `__import__` above is only to keep this snippet self-contained. Replace with a top-level import when writing the test.

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/auth/test_models.py -q`
Expected: FAIL — `ModuleNotFoundError: aaiclick.auth.models`.

- [ ] **Step 3: Implement `models.py`**

```python
"""SQLModel tables for users and refresh tokens. See docs/auth.md."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Literal, get_args

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String
from sqlmodel import Field, SQLModel

from ..datetime_utils import utc_now
from ..orchestration.models import _enum_check

ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"
Role = Literal["admin", "viewer"]
ROLES: tuple[Role, ...] = (ROLE_ADMIN, ROLE_VIEWER)


class User(SQLModel, table=True):
    __tablename__: ClassVar[str] = "users"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    username: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    password_hash: str = Field(sa_column=Column(String, nullable=False))
    role: Role = Field(
        sa_column=Column(
            String,
            _enum_check("role", get_args(Role), "ck_users_role"),
            nullable=False,
        ),
    )
    disabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="0"), default=False)
    created_at: datetime = Field(default_factory=utc_now)


class RefreshToken(SQLModel, table=True):
    __tablename__: ClassVar[str] = "refresh_tokens"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("users.id"), nullable=False, index=True))
    token_hash: str = Field(sa_column=Column(String, nullable=False, unique=True, index=True))
    expires_at: datetime = Field(nullable=False)
    rotated_at: datetime | None = Field(default=None)
    revoked_at: datetime | None = Field(default=None)
```

> If importing `_enum_check` from `orchestration.models` creates an undesirable dependency direction, copy the 3-line helper into `auth/models.py` instead. Prefer the import unless it breaks; note the decision in the commit.

- [ ] **Step 4: Register models with Alembic metadata**

In `aaiclick/orchestration/migrations/env.py`, below the existing `import aaiclick.orchestration.models  # noqa: F401` (line 9), add:

```python
import aaiclick.auth.models  # noqa: F401  # register users/refresh_tokens with SQLModel.metadata
```

- [ ] **Step 5: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/auth/test_models.py -q`
Expected: PASS (2 passed). (The `orch_ctx` fixture creates all `SQLModel.metadata` tables in the test SQLite DB, so no migration is needed for tests.)

- [ ] **Step 6: Commit**

```bash
git add aaiclick/auth/models.py aaiclick/auth/test_models.py aaiclick/orchestration/migrations/env.py
git commit -m "Add User and RefreshToken models; register with alembic metadata"
```

### Task 2.2: Generate the migration

**Files:** Creates a new revision under `aaiclick/orchestration/migrations/versions/`

- [ ] **Step 1: Invoke the generate-migration skill**

Use the `generate-migration` skill (runs via GitHub Actions) with a message like `add users and refresh_tokens tables`. Do NOT hand-write the file.

- [ ] **Step 2: Review the generated revision**

Confirm it `create_table("users", ...)` with the unique index on `username` and the `ck_users_role` CHECK, and `create_table("refresh_tokens", ...)` with the FK + unique `token_hash`. No unrelated drops.

- [ ] **Step 3: Apply locally and verify**

Run: `cd /home/user/aaiclick && .venv/bin/python -m aaiclick migrate up` (or the repo's documented migrate command)
Expected: applies cleanly; `users` and `refresh_tokens` exist.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/migrations/versions/
git commit -m "Migration: create users and refresh_tokens tables"
```

---

## Phase 3: Raw DB layer + view models

### Task 3.1: `auth/view_models.py`

**Files:** Create `aaiclick/auth/view_models.py`

- [ ] **Step 1: Implement view models** (no test — pure schema; covered by later phases)

```python
"""Pydantic request/response models for the auth + users surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from .models import Role


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class MeView(BaseModel):
    id: int
    username: str
    role: Role


class UserView(BaseModel):
    id: int
    username: str
    role: Role
    disabled: bool
    created_at: datetime


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: Role = "viewer"


class SetRoleRequest(BaseModel):
    role: Role


class SetPasswordRequest(BaseModel):
    password: str


class UserListFilter(BaseModel):
    limit: int = 50
    offset: int = 0
    cursor: str | None = None
```

- [ ] **Step 2: Verify importable + commit**

Run: `.venv/bin/python -c "from aaiclick.auth import view_models; print('ok')"`

```bash
git add aaiclick/auth/view_models.py
git commit -m "Add auth view models"
```

### Task 3.2: `auth/store.py` (raw DB CRUD, TDD)

**Files:** Create `aaiclick/auth/store.py`, `aaiclick/auth/test_store.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/auth/test_store.py
import pytest

from aaiclick.auth import store
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER


async def test_create_and_get_user(orch_ctx):
    created = await store.create_user(username="alice", password_hash="h", role=ROLE_ADMIN)
    fetched = await store.get_user_by_username("alice")
    assert fetched is not None and fetched.id == created.id


async def test_duplicate_username_raises(orch_ctx):
    await store.create_user(username="alice", password_hash="h", role=ROLE_VIEWER)
    with pytest.raises(store.UsernameTaken):
        await store.create_user(username="alice", password_hash="h2", role=ROLE_VIEWER)


async def test_set_role_and_disable(orch_ctx):
    u = await store.create_user(username="bob", password_hash="h", role=ROLE_VIEWER)
    await store.set_role(u.id, ROLE_ADMIN)
    await store.set_disabled(u.id, True)
    again = await store.get_user_by_id(u.id)
    assert again.role == ROLE_ADMIN and again.disabled is True


async def test_set_role_missing_user_raises(orch_ctx):
    with pytest.raises(store.UserNotFound):
        await store.set_role(999, ROLE_ADMIN)


async def test_refresh_token_lifecycle(orch_ctx):
    u = await store.create_user(username="carol", password_hash="h", role=ROLE_VIEWER)
    rt = await store.create_refresh_token(user_id=u.id, token_hash="hash1", ttl=3600)
    found = await store.get_active_refresh("hash1")
    assert found is not None and found.id == rt.id
    await store.rotate_refresh(rt.id)
    assert await store.get_active_refresh("hash1") is None  # rotated => inactive
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/auth/test_store.py -q`
Expected: FAIL — `ModuleNotFoundError: aaiclick.auth.store`.

- [ ] **Step 3: Implement `store.py`**

Mirrors `orchestration/registered_jobs.py`: owns `get_sql_session()`, raises plain domain errors.

```python
"""Raw DB access for users and refresh tokens. Domain errors only; the
internal_api layer maps these to InternalApiError / Problem responses."""

from __future__ import annotations

from datetime import timedelta

from sqlmodel import col, select

from ..datetime_utils import utc_now
from ..orchestration.orch_context import get_sql_session
from ..snowflake import get_snowflake_id
from .models import RefreshToken, Role, User


class UsernameTaken(ValueError):
    """A user with this username already exists."""


class UserNotFound(ValueError):
    """No user matches the given id/username."""


class RefreshInvalid(ValueError):
    """Refresh token is missing, expired, rotated, or revoked."""


async def create_user(*, username: str, password_hash: str, role: Role) -> User:
    user = User(id=get_snowflake_id(), username=username, password_hash=password_hash, role=role)
    async with get_sql_session() as session:
        existing = await session.execute(select(User).where(User.username == username))
        if existing.scalar_one_or_none() is not None:
            raise UsernameTaken(f"username '{username}' already exists")
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


async def get_user_by_username(username: str) -> User | None:
    async with get_sql_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()


async def get_user_by_id(user_id: int) -> User | None:
    async with get_sql_session() as session:
        result = await session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()


async def list_users(*, limit: int, offset: int) -> tuple[list[User], int]:
    async with get_sql_session() as session:
        rows = (
            await session.execute(
                select(User).order_by(col(User.username).asc()).limit(limit).offset(offset)
            )
        ).scalars().all()
        total = len((await session.execute(select(User.id))).scalars().all())
    return list(rows), total


async def set_role(user_id: int, role: Role) -> User:
    return await _update_user(user_id, role=role)


async def set_disabled(user_id: int, disabled: bool) -> User:
    return await _update_user(user_id, disabled=disabled)


async def set_password_hash(user_id: int, password_hash: str) -> User:
    return await _update_user(user_id, password_hash=password_hash)


async def _update_user(user_id: int, **fields) -> User:
    async with get_sql_session() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if user is None:
            raise UserNotFound(f"user {user_id} not found")
        for key, value in fields.items():
            setattr(user, key, value)
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


async def create_refresh_token(*, user_id: int, token_hash: str, ttl: int) -> RefreshToken:
    token = RefreshToken(
        id=get_snowflake_id(),
        user_id=user_id,
        token_hash=token_hash,
        expires_at=utc_now() + timedelta(seconds=ttl),
    )
    async with get_sql_session() as session:
        session.add(token)
        await session.commit()
        await session.refresh(token)
    return token


async def get_active_refresh(token_hash: str) -> RefreshToken | None:
    """Return the row only if it is unrotated, unrevoked, and unexpired."""
    async with get_sql_session() as session:
        row = (
            await session.execute(select(RefreshToken).where(RefreshToken.token_hash == token_hash))
        ).scalar_one_or_none()
    if row is None or row.rotated_at is not None or row.revoked_at is not None:
        return None
    if row.expires_at <= utc_now():
        return None
    return row


async def rotate_refresh(token_id: int) -> None:
    await _stamp_refresh(token_id, "rotated_at")


async def revoke_refresh(token_id: int) -> None:
    await _stamp_refresh(token_id, "revoked_at")


async def _stamp_refresh(token_id: int, field: str) -> None:
    async with get_sql_session() as session:
        row = (
            await session.execute(select(RefreshToken).where(RefreshToken.id == token_id))
        ).scalar_one_or_none()
        if row is None:
            raise RefreshInvalid(f"refresh token {token_id} not found")
        setattr(row, field, utc_now())
        session.add(row)
        await session.commit()
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/auth/test_store.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/auth/store.py aaiclick/auth/test_store.py
git commit -m "Add auth store (raw DB CRUD for users + refresh tokens)"
```

---

## Phase 4: internal_api (users + auth, TDD)

### Task 4.1: `internal_api/users.py`

**Files:** Create `aaiclick/internal_api/users.py`, `aaiclick/internal_api/test_users.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/internal_api/test_users.py
import pytest

from aaiclick.auth.view_models import CreateUserRequest, UserView
from aaiclick.internal_api import users
from aaiclick.internal_api.errors import Conflict, NotFound
from aaiclick.view_models import Page


async def test_create_user_returns_view(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="alice", password="pw", role="admin"))
    assert isinstance(view, UserView)
    assert view.username == "alice" and view.role == "admin"


async def test_create_duplicate_raises_conflict(orch_ctx):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    with pytest.raises(Conflict):
        await users.create_user(CreateUserRequest(username="alice", password="pw"))


async def test_list_users_paginated(orch_ctx):
    await users.create_user(CreateUserRequest(username="a", password="pw"))
    await users.create_user(CreateUserRequest(username="b", password="pw"))
    page = await users.list_users()
    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2


async def test_set_role_missing_raises_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await users.set_role(12345, "admin")
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/internal_api/test_users.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `users.py`**

```python
"""Internal API for user administration (admin-only at the HTTP layer)."""

from __future__ import annotations

from aaiclick.auth import security, store
from aaiclick.auth.models import Role, User
from aaiclick.auth.view_models import CreateUserRequest, UserListFilter, UserView
from aaiclick.view_models import Page

from .errors import Conflict, NotFound


def _to_view(user: User) -> UserView:
    return UserView(
        id=user.id,
        username=user.username,
        role=user.role,
        disabled=user.disabled,
        created_at=user.created_at,
    )


async def create_user(request: CreateUserRequest) -> UserView:
    try:
        user = await store.create_user(
            username=request.username,
            password_hash=security.hash_password(request.password),
            role=request.role,
        )
    except store.UsernameTaken as exc:
        raise Conflict(str(exc)) from exc
    return _to_view(user)


async def list_users(filter: UserListFilter | None = None) -> Page[UserView]:
    filter = filter or UserListFilter()
    rows, total = await store.list_users(limit=filter.limit, offset=filter.offset)
    return Page[UserView](items=[_to_view(u) for u in rows], total=total)


async def get_user(user_id: int) -> UserView:
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise NotFound(f"user {user_id} not found")
    return _to_view(user)


async def set_role(user_id: int, role: Role) -> UserView:
    try:
        return _to_view(await store.set_role(user_id, role))
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc


async def disable_user(user_id: int, disabled: bool = True) -> UserView:
    try:
        return _to_view(await store.set_disabled(user_id, disabled))
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc


async def set_password(user_id: int, password: str) -> UserView:
    try:
        return _to_view(await store.set_password_hash(user_id, security.hash_password(password)))
    except store.UserNotFound as exc:
        raise NotFound(str(exc)) from exc
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/internal_api/test_users.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api/users.py aaiclick/internal_api/test_users.py
git commit -m "Add internal_api.users (user administration)"
```

### Task 4.2: `internal_api/auth.py`

**Files:** Create `aaiclick/internal_api/auth.py`, `aaiclick/internal_api/test_auth.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/internal_api/test_auth.py
import pytest

from aaiclick.auth.view_models import (
    CreateUserRequest, LoginRequest, LogoutRequest, RefreshRequest, TokenPair,
)
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Unauthorized

SECRET = "itest-secret"


async def _make_user(username="alice", password="pw", role="admin"):
    await users.create_user(CreateUserRequest(username=username, password=password, role=role))


async def test_login_success(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    assert isinstance(pair, TokenPair)
    assert pair.access_token and pair.refresh_token


async def test_login_bad_password_raises(orch_ctx):
    await _make_user()
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="alice", password="nope"), secret=SECRET)


async def test_login_unknown_user_raises(orch_ctx):
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="ghost", password="pw"), secret=SECRET)


async def test_disabled_user_cannot_login(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="dis", password="pw"))
    await users.disable_user(view.id, True)
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="dis", password="pw"), secret=SECRET)


async def test_refresh_rotates_and_rejects_reuse(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    rotated = await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
    assert rotated.refresh_token != pair.refresh_token
    with pytest.raises(Unauthorized):  # reuse of the old token
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)


async def test_logout_revokes_refresh(orch_ctx):
    await _make_user()
    pair = await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    await auth.logout(LogoutRequest(refresh_token=pair.refresh_token))
    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/internal_api/test_auth.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `auth.py`**

The `secret`/`ttl` are passed in by the caller (the router supplies them from `auth.config`), keeping this layer free of env reads and easy to test.

```python
"""Internal API for login/refresh/logout. Transport-agnostic; the server
router supplies the JWT secret + TTLs from aaiclick.auth.config."""

from __future__ import annotations

from aaiclick.auth import config, security, store
from aaiclick.auth.view_models import LoginRequest, LogoutRequest, RefreshRequest, TokenPair

from .errors import Unauthorized


def _issue(*, user_id: int, role: str, secret: str, access_ttl: int) -> str:
    return security.encode_access_token(user_id=user_id, role=role, secret=secret, ttl=access_ttl)


async def _mint_pair(*, user_id: int, role: str, secret: str, access_ttl: int, refresh_ttl: int) -> TokenPair:
    refresh_secret = security.generate_secret()
    await store.create_refresh_token(
        user_id=user_id, token_hash=security.sha256_hex(refresh_secret), ttl=refresh_ttl
    )
    return TokenPair(
        access_token=_issue(user_id=user_id, role=role, secret=secret, access_ttl=access_ttl),
        refresh_token=refresh_secret,
        expires_in=access_ttl,
    )


async def login(
    request: LoginRequest, *, secret: str, access_ttl: int | None = None, refresh_ttl: int | None = None
) -> TokenPair:
    access_ttl = access_ttl if access_ttl is not None else config.access_ttl()
    refresh_ttl = refresh_ttl if refresh_ttl is not None else config.refresh_ttl()
    user = await store.get_user_by_username(request.username)
    if user is None or user.disabled or not security.verify_password(request.password, user.password_hash):
        raise Unauthorized("invalid username or password")
    return await _mint_pair(
        user_id=user.id, role=user.role, secret=secret, access_ttl=access_ttl, refresh_ttl=refresh_ttl
    )


async def refresh(
    request: RefreshRequest, *, secret: str, access_ttl: int | None = None, refresh_ttl: int | None = None
) -> TokenPair:
    access_ttl = access_ttl if access_ttl is not None else config.access_ttl()
    refresh_ttl = refresh_ttl if refresh_ttl is not None else config.refresh_ttl()
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is None:
        raise Unauthorized("invalid refresh token")
    user = await store.get_user_by_id(row.user_id)
    if user is None or user.disabled:
        raise Unauthorized("user is disabled")
    await store.rotate_refresh(row.id)  # rotation: old token becomes inactive
    return await _mint_pair(
        user_id=user.id, role=user.role, secret=secret, access_ttl=access_ttl, refresh_ttl=refresh_ttl
    )


async def logout(request: LogoutRequest) -> None:
    row = await store.get_active_refresh(security.sha256_hex(request.refresh_token))
    if row is not None:
        await store.revoke_refresh(row.id)
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/internal_api/test_auth.py -q`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api/auth.py aaiclick/internal_api/test_auth.py
git commit -m "Add internal_api.auth (login/refresh/logout)"
```

---

## Phase 5: Server auth core (Principal, gating, RBAC, middleware)

### Task 5.1: Rewrite `server/auth.py`

**Files:** Rewrite `aaiclick/server/auth.py`; rewrite `aaiclick/server/test_auth.py`

- [ ] **Step 1: Write the failing tests**

```python
# aaiclick/server/test_auth.py  (full rewrite)
import jwt
import pytest

from aaiclick.auth import security
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER
from aaiclick.server import auth as srv_auth

SECRET = "srv-secret"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def test_disabled_returns_synthetic_admin(monkeypatch):
    monkeypatch.delenv("AAICLICK_AUTH_ENABLED", raising=False)
    principal = srv_auth.resolve_principal(authorization=None)
    assert principal.role == ROLE_ADMIN


def test_enabled_missing_token_unauthorized(enabled):
    from aaiclick.internal_api.errors import Unauthorized

    with pytest.raises(Unauthorized):
        srv_auth.resolve_principal(authorization=None)


def test_enabled_valid_jwt(enabled):
    token = security.encode_access_token(user_id=7, role=ROLE_VIEWER, secret=SECRET, ttl=60)
    principal = srv_auth.resolve_principal(authorization=_bearer(token))
    assert principal.user_id == 7 and principal.role == ROLE_VIEWER


def test_enabled_bad_signature_unauthorized(enabled):
    from aaiclick.internal_api.errors import Unauthorized

    token = jwt.encode({"sub": "1", "role": "admin", "type": "access"}, "other", algorithm="HS256")
    with pytest.raises(Unauthorized):
        srv_auth.resolve_principal(authorization=_bearer(token))
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/server/test_auth.py -q`
Expected: FAIL — `resolve_principal` not defined.

- [ ] **Step 3: Implement the rewritten `server/auth.py`**

```python
"""Auth for the REST surface and the /mcp mount.

When AAICLICK_AUTH_ENABLED is off, every request is allowed (synthetic admin
principal) and startup logs a WARNING. When on, the Authorization: Bearer
access JWT is required; HTTPBearer (auto_error=False) extracts it and registers
the OpenAPI scheme. The /mcp mount keeps an ASGI middleware (admin-only)
because Depends does not propagate into mounted sub-apps. See docs/auth.md.
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from aaiclick.auth import config, security
from aaiclick.auth.models import ROLE_ADMIN, Role
from aaiclick.internal_api.errors import Forbidden, Unauthorized
from aaiclick.view_models import ProblemCode

from .errors import problem_response

BEARER_CHALLENGE = {"WWW-Authenticate": "Bearer"}
logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


class Principal(NamedTuple):
    user_id: int | None
    username: str | None
    role: Role


_SYNTHETIC_ADMIN = Principal(user_id=None, username=None, role=ROLE_ADMIN)


def resolve_principal(authorization: str | None) -> Principal:
    """Core principal resolution, shared by the dependency and the middleware."""
    if not config.auth_enabled():
        return _SYNTHETIC_ADMIN
    scheme, credentials = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not credentials:
        raise Unauthorized("missing bearer token")
    try:
        claims = security.decode_access_token(credentials, config.require_jwt_secret())
    except security.TokenError as exc:
        raise Unauthorized(str(exc)) from exc
    return Principal(user_id=claims.user_id, username=None, role=claims.role)


async def require_principal(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """FastAPI dependency → resolves the Principal or raises Unauthorized."""
    header = f"Bearer {creds.credentials}" if creds else None
    return resolve_principal(header)


async def require_admin(principal: Principal = Depends(require_principal)) -> Principal:
    if principal.role != ROLE_ADMIN:
        raise Forbidden("admin role required")
    return principal


def warn_if_open() -> None:
    if not config.auth_enabled():
        logger.warning("%s is off — server is open", config.ENV_ENABLED)


class AdminAuthMiddleware:
    """ASGI guard for the /mcp mount: admin-only when auth is enabled."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        authorization = Headers(scope=scope).get("authorization")
        try:
            principal = resolve_principal(authorization)
            if principal.role != ROLE_ADMIN:
                raise Forbidden("admin role required")
        except Unauthorized as exc:
            await problem_response("Unauthorized", 401, str(exc), ProblemCode.UNAUTHORIZED, BEARER_CHALLENGE)(scope, receive, send)
            return
        except Forbidden as exc:
            await problem_response("Forbidden", 403, str(exc), ProblemCode.FORBIDDEN)(scope, receive, send)
            return
        await self.app(scope, receive, send)
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/server/test_auth.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/auth.py aaiclick/server/test_auth.py
git commit -m "Rewrite server auth: Principal resolution, gating, require_admin, admin /mcp middleware"
```

---

## Phase 6: Server routers + app wiring + RBAC on mutations

### Task 6.1: `server/routers/auth.py`

**Files:** Create `aaiclick/server/routers/auth.py`, `aaiclick/server/routers/test_auth.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/server/routers/test_auth.py
import pytest

from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

SECRET = "router-secret"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


async def test_login_then_access_protected(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw", role="admin"))

    login = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})
    assert login.status_code == 200
    access = login.json()["access_token"]

    me = await app_client.get(f"{API_PREFIX}/auth/me", headers={"Authorization": f"Bearer {access}"})
    assert me.status_code == 200 and me.json()["username"] == "alice"


async def test_login_bad_password_401(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "x"})
    assert res.status_code == 401
    assert res.json()["code"] == "unauthorized"


async def test_refresh_flow(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    login = (await app_client.post(f"{API_PREFIX}/auth/login", json={"username": "alice", "password": "pw"})).json()
    res = await app_client.post(f"{API_PREFIX}/auth/refresh", json={"refresh_token": login["refresh_token"]})
    assert res.status_code == 200 and res.json()["refresh_token"] != login["refresh_token"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/server/routers/test_auth.py -q`
Expected: FAIL — 404 (route not registered) / import error.

- [ ] **Step 3: Implement `routers/auth.py`**

`/auth/login` and `/auth/refresh` are public (no `require_principal`); `/auth/me` needs the principal.

```python
"""Auth routes: login, refresh, logout, me."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth import config
from aaiclick.auth.view_models import (
    LoginRequest, LogoutRequest, MeView, RefreshRequest, TokenPair,
)
from aaiclick.internal_api import auth as auth_api

from ..auth import Principal, require_principal
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/auth", tags=["auth"], dependencies=[Depends(orch_scope)])


@router.post("/login", response_model=TokenPair, responses=problem_responses(401))
async def login(request: LoginRequest) -> TokenPair:
    return await auth_api.login(request, secret=config.require_jwt_secret())


@router.post("/refresh", response_model=TokenPair, responses=problem_responses(401))
async def refresh(request: RefreshRequest) -> TokenPair:
    return await auth_api.refresh(request, secret=config.require_jwt_secret())


@router.post("/logout", status_code=204)
async def logout(request: LogoutRequest) -> None:
    await auth_api.logout(request)


@router.get("/me", response_model=MeView)
async def me(principal: Principal = Depends(require_principal)) -> MeView:
    return MeView(id=principal.user_id or 0, username=principal.username or "admin", role=principal.role)
```

> The `/me` view: when auth is disabled the synthetic admin has `user_id=None`; surface a stable placeholder (`0` / `"admin"`). When enabled, the JWT carries `sub` (id) and `role` but not username — acceptable for `/me` (the SPA mainly needs `role`). If a real username is required, add a `get_user` lookup here.

- [ ] **Step 4: Wire the router in `app.py`** (see Task 6.3) — then run:

Run: `.venv/bin/pytest aaiclick/server/routers/test_auth.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/routers/auth.py aaiclick/server/routers/test_auth.py
git commit -m "Add /auth router (login/refresh/logout/me)"
```

### Task 6.2: `server/routers/users.py` (admin-only)

**Files:** Create `aaiclick/server/routers/users.py`, `aaiclick/server/routers/test_users.py`

- [ ] **Step 1: Write the failing test**

```python
# aaiclick/server/routers/test_users.py
import pytest

from aaiclick.auth import security
from aaiclick.auth.models import ROLE_ADMIN, ROLE_VIEWER
from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

SECRET = "users-secret"


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", SECRET)


def _admin_header():
    return {"Authorization": f"Bearer {security.encode_access_token(user_id=1, role=ROLE_ADMIN, secret=SECRET, ttl=60)}"}


def _viewer_header():
    return {"Authorization": f"Bearer {security.encode_access_token(user_id=2, role=ROLE_VIEWER, secret=SECRET, ttl=60)}"}


async def test_admin_can_create_user(orch_ctx, app_client, enabled):
    res = await app_client.post(f"{API_PREFIX}/users", json={"username": "newbie", "password": "pw", "role": "viewer"}, headers=_admin_header())
    assert res.status_code == 201 and res.json()["username"] == "newbie"


async def test_viewer_forbidden(orch_ctx, app_client, enabled):
    res = await app_client.post(f"{API_PREFIX}/users", json={"username": "x", "password": "pw"}, headers=_viewer_header())
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_list_users(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.get(f"{API_PREFIX}/users", headers=_admin_header())
    assert res.status_code == 200 and res.json()["total"] >= 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/server/routers/test_users.py -q`
Expected: FAIL — 404 / import error.

- [ ] **Step 3: Implement `routers/users.py`**

```python
"""User administration routes (admin-only)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth.view_models import (
    CreateUserRequest, SetPasswordRequest, SetRoleRequest, UserListFilter, UserView,
)
from aaiclick.internal_api import users as users_api
from aaiclick.view_models import Page

from ..auth import require_admin
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(
    prefix="/users",
    tags=["users"],
    dependencies=[Depends(orch_scope), Depends(require_admin)],
)


@router.get("", response_model=Page[UserView])
async def list_users(filter: UserListFilter = Depends()) -> Page[UserView]:
    return await users_api.list_users(filter)


@router.post("", response_model=UserView, status_code=201, responses=problem_responses(409))
async def create_user(request: CreateUserRequest) -> UserView:
    return await users_api.create_user(request)


@router.put("/{user_id}/role", response_model=UserView, responses=problem_responses(404))
async def set_role(user_id: int, request: SetRoleRequest) -> UserView:
    return await users_api.set_role(user_id, request.role)


@router.put("/{user_id}/password", response_model=UserView, responses=problem_responses(404))
async def set_password(user_id: int, request: SetPasswordRequest) -> UserView:
    return await users_api.set_password(user_id, request.password)


@router.post("/{user_id}/disable", response_model=UserView, responses=problem_responses(404))
async def disable_user(user_id: int) -> UserView:
    return await users_api.disable_user(user_id, True)
```

- [ ] **Step 4: Wire in `app.py` (Task 6.3), then run**

Run: `.venv/bin/pytest aaiclick/server/routers/test_users.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/routers/users.py aaiclick/server/routers/test_users.py
git commit -m "Add /users admin router (RBAC enforced)"
```

### Task 6.3: Wire routers + swap dependency + seed admin in `app.py`

**Files:** Modify `aaiclick/server/app.py`

- [ ] **Step 1: Update imports**

Replace the `from .auth import BearerAuthMiddleware, require_bearer, warn_if_open` line with:

```python
from .auth import AdminAuthMiddleware, require_principal, warn_if_open
from .routers import auth as auth_router
from .routers import users as users_router
```

- [ ] **Step 2: Swap the router-level dependency**

In the `for router in (...)` loop, change `dependencies=[Depends(require_bearer)]` to `dependencies=[Depends(require_principal)]`.

- [ ] **Step 3: Include the new routers**

After the loop, add (note: `auth_router` is public — no global principal dep; `users_router` already carries `require_admin`):

```python
app.include_router(auth_router.router, prefix=API_PREFIX)
app.include_router(users_router.router, prefix=API_PREFIX)
```

- [ ] **Step 4: Update the MCP mount + description**

Change `app.mount(MCP_PATH, BearerAuthMiddleware(_mcp_app))` to `app.mount(MCP_PATH, AdminAuthMiddleware(_mcp_app))`, and update the FastAPI `description` to drop the `AAICLICK_API_TOKEN` mention (point at `docs/auth.md`).

- [ ] **Step 5: Seed admin in lifespan**

In `_lifespan`, after `warn_if_open()`, add a seed call (define `_seed_admin` in `app.py`):

```python
async def _seed_admin() -> None:
    seed = config.admin_seed()
    if seed is None or not config.auth_enabled():
        return
    async with orch_context(with_ch=False):
        existing, total = await store.list_users(limit=1, offset=0)
        if total == 0:
            await store.create_user(
                username=seed.username,
                password_hash=security.hash_password(seed.password),
                role=ROLE_ADMIN,
            )
```

Add imports at top of `app.py`: `from aaiclick.auth import config, security, store`, `from aaiclick.auth.models import ROLE_ADMIN`, and ensure `orch_context` is imported. Call `await _seed_admin()` inside `_lifespan` before `yield`.

- [ ] **Step 6: Run the full server test suite**

Run: `.venv/bin/pytest aaiclick/server -q`
Expected: PASS — including the router tests from 6.1/6.2 and the existing router tests (which run with auth disabled by default, so the synthetic admin keeps them green).

- [ ] **Step 7: Commit**

```bash
git add aaiclick/server/app.py
git commit -m "Wire auth/users routers, swap to require_principal, seed admin, admin /mcp"
```

### Task 6.4: RBAC on mutating endpoints

**Files:** Modify `aaiclick/server/routers/{jobs,workers,objects,registered_jobs}.py`

- [ ] **Step 1: Write a failing test for one mutation**

```python
# add to aaiclick/server/routers/test_workers.py
import pytest

from aaiclick.auth import security
from aaiclick.auth.models import ROLE_VIEWER
from aaiclick.server.app import API_PREFIX


async def test_viewer_cannot_start_worker(orch_ctx, app_client, monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.setenv("AAICLICK_JWT_SECRET", "x")
    token = security.encode_access_token(user_id=2, role=ROLE_VIEWER, secret="x", ttl=60)
    res = await app_client.post(
        f"{API_PREFIX}/workers", json={}, headers={"Authorization": f"Bearer {token}"}
    )
    assert res.status_code == 403
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest aaiclick/server/routers/test_workers.py::test_viewer_cannot_start_worker -q`
Expected: FAIL — currently 202/422, not 403.

- [ ] **Step 3: Add `require_admin` to each mutating endpoint**

Add `from ..auth import require_admin` to each router. Append `Depends(require_admin)` to the `dependencies=[...]` of every write endpoint:
- `workers.py`: `start_worker` (POST), `stop_worker`.
- `jobs.py`: `run_job` (`:run`), `cancel_job`.
- `objects.py`: `delete`/`purge` endpoints.
- `registered_jobs.py`: `register`, `enable`, `disable`.

For router-level-scoped routers (workers, objects), switch the write endpoints to per-endpoint `dependencies=[Depends(orch_scope), Depends(require_admin)]` (or add a router-level `require_admin` only if every endpoint mutates — objects has reads too, so go per-endpoint). GET endpoints stay principal-only.

Example for `workers.start_worker`:

```python
@router.post("", status_code=202, dependencies=[Depends(require_admin)], responses=problem_responses(403, 422, 503))
async def start_worker(request: StartWorkerRequest, http_request: Request) -> Response:
    ...
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest aaiclick/server/routers -q`
Expected: PASS — viewer blocked (403); admin/synthetic-admin still allowed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/routers/
git commit -m "Enforce require_admin on mutating REST endpoints"
```

---

## Phase 7: CLI `aaiclick user ...`

### Task 7.1: Renderers + subcommands

**Files:** Modify `aaiclick/__main__.py`, `aaiclick/cli_renderers.py`

- [ ] **Step 1: Add renderers in `cli_renderers.py`**

```python
def render_user(view: UserView) -> None:
    print(f"{view.id}  {view.username}  role={view.role}  disabled={view.disabled}")


def render_users_page(page: Page[UserView], offset: int) -> None:
    if not page.items:
        print("No users found")
        return
    print(f"{'ID':<20} {'Username':<20} {'Role':<8} {'Disabled':<8}")
    print("-" * 60)
    for u in page.items:
        print(f"{u.id:<20} {u.username:<20} {u.role:<8} {str(u.disabled):<8}")
    _print_page_footer(page, offset)
```

Add `from aaiclick.auth.view_models import UserView` at the top of `cli_renderers.py`.

- [ ] **Step 2: Add the `user` subparser** in `__main__.py` (mirror the `worker` group)

```python
user_parser = subparsers.add_parser("user", help="User administration")
user_sub = user_parser.add_subparsers(dest="user_command", help="User commands")

p = user_sub.add_parser("create", help="Create a user")
p.add_argument("username")
p.add_argument("--password", required=True)
p.add_argument("--role", choices=list(ROLES), default=ROLE_VIEWER)
_add_json_flag(p)

p = user_sub.add_parser("list", help="List users")
p.add_argument("--limit", type=int, default=50)
p.add_argument("--offset", type=int, default=0)
_add_json_flag(p)

p = user_sub.add_parser("set-role", help="Change a user's role")
p.add_argument("user_id", type=int)
p.add_argument("role", choices=list(ROLES))
_add_json_flag(p)

p = user_sub.add_parser("disable", help="Disable a user")
p.add_argument("user_id", type=int)
_add_json_flag(p)

p = user_sub.add_parser("passwd", help="Set a user's password")
p.add_argument("user_id", type=int)
p.add_argument("--password", required=True)
_add_json_flag(p)
```

Add `from aaiclick.auth.models import ROLE_VIEWER, ROLES` to the imports.

- [ ] **Step 3: Add handlers + dispatch**

```python
async def _run_user_create(args):
    view = await _run_internal_api(
        users.create_user(CreateUserRequest(username=args.username, password=args.password, role=args.role))
    )
    _render(args, view, cli_renderers.render_user)


async def _run_user_list(args):
    page = await _run_internal_api(users.list_users(UserListFilter(limit=args.limit, offset=args.offset)))
    _render(args, page, lambda p: cli_renderers.render_users_page(p, args.offset))


async def _run_user_set_role(args):
    view = await _run_internal_api(users.set_role(args.user_id, args.role))
    _render(args, view, cli_renderers.render_user)


async def _run_user_disable(args):
    view = await _run_internal_api(users.disable_user(args.user_id, True))
    _render(args, view, cli_renderers.render_user)


async def _run_user_passwd(args):
    view = await _run_internal_api(users.set_password(args.user_id, args.password))
    _render(args, view, cli_renderers.render_user)
```

In the dispatcher, add:

```python
elif args.command == "user":
    if args.user_command == "create":
        asyncio.run(_run_user_create(args))
    elif args.user_command == "list":
        asyncio.run(_run_user_list(args))
    elif args.user_command == "set-role":
        asyncio.run(_run_user_set_role(args))
    elif args.user_command == "disable":
        asyncio.run(_run_user_disable(args))
    elif args.user_command == "passwd":
        asyncio.run(_run_user_passwd(args))
    else:
        user_parser.print_help()
```

Add imports: `from aaiclick.internal_api import users` and `from aaiclick.auth.view_models import CreateUserRequest, UserListFilter`.

- [ ] **Step 4: Smoke-test the CLI end to end**

Run:
```bash
.venv/bin/python -m aaiclick user create alice --password pw --role admin
.venv/bin/python -m aaiclick user list
```
Expected: first prints the new user line; second lists at least `alice`. (Uses the local SQLite DB; run a migration first if the tables are absent.)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/__main__.py aaiclick/cli_renderers.py
git commit -m "Add 'aaiclick user' CLI subcommands"
```

---

## Phase 8: SPA login + 401 refresh

> No JS test harness exists (no vitest/playwright). Verify each task with `npm run check` (tsc) and a manual dev-server check. Match Tailwind + the `Panel`/`btn` class conventions.

### Task 8.1: Auth store + context

**Files:** Create `src/lib/auth.ts`, `src/components/Auth.tsx`

- [ ] **Step 1: Implement a module-level token store** (`src/lib/auth.ts`)

`fetchJSON`/`postJSON` are plain functions outside React, so the access token lives in a module singleton; the refresh token persists in `localStorage`.

```typescript
const REFRESH_KEY = "aaiclick.refresh";
let accessToken: string | null = null;

export function getAccessToken() { return accessToken; }
export function setAccessToken(t: string | null) { accessToken = t; }
export function getRefreshToken() { return localStorage.getItem(REFRESH_KEY); }
export function setRefreshToken(t: string | null) {
  if (t) localStorage.setItem(REFRESH_KEY, t); else localStorage.removeItem(REFRESH_KEY);
}
export function clearSession() { accessToken = null; setRefreshToken(null); }

export interface TokenPair { access_token: string; refresh_token: string; expires_in: number; }

export async function login(username: string, password: string): Promise<void> {
  const res = await fetch("/api/v0/auth/login", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error("login failed");
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
}

export async function tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;
  const res = await fetch("/api/v0/auth/refresh", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: rt }),
  });
  if (!res.ok) { clearSession(); return false; }
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
  return true;
}
```

- [ ] **Step 2: Implement `AuthContext`** (`src/components/Auth.tsx`) mirroring `Toast.tsx`: exposes `useAuth()` → `{ authed, setAuthed }`, initialised from whether a refresh token / `/auth/me` succeeds.

- [ ] **Step 3: Verify** — `npm run check` passes. Commit.

```bash
git add src/lib/auth.ts src/components/Auth.tsx
git commit -m "SPA: auth token store + context"
```

### Task 8.2: Inject header + 401-refresh in the client

**Files:** Modify `src/api/client.ts`

- [ ] **Step 1: Add an auth header + single-retry-on-401 wrapper** around the `fetch` calls in `fetchJSON`/`postJSON`.

```typescript
import { getAccessToken, tryRefresh, clearSession } from "../lib/auth";

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  const token = getAccessToken();
  return { ...(extra ?? {}), ...(token ? { Authorization: `Bearer ${token}` } : {}) };
}

async function request(path: string, init: RequestInit): Promise<Response> {
  let res = await fetch(`${API}${path}`, { ...init, headers: authHeaders(init.headers as Record<string, string>) });
  if (res.status === 401 && (await tryRefresh())) {
    res = await fetch(`${API}${path}`, { ...init, headers: authHeaders(init.headers as Record<string, string>) });
  }
  if (res.status === 401) { clearSession(); window.dispatchEvent(new Event("aaiclick:unauthorized")); }
  return res;
}
```

Refactor `fetchJSON`/`postJSON` to call `request(...)` and keep `parseError`.

- [ ] **Step 2: Verify** — `npm run check`. Commit.

```bash
git add src/api/client.ts
git commit -m "SPA: attach bearer token, refresh once on 401"
```

### Task 8.3: Login view + route guard

**Files:** Create `src/views/Login.tsx`; Modify `src/views/index.tsx`, `src/App.tsx`, `src/main.tsx`

- [ ] **Step 1: Build `Login.tsx`** — username/password form (Tailwind `Panel`/`field`/`btn btn-primary`), calls `login()` then flips `authed`.

- [ ] **Step 2: Gate `App`** — wrap render: if auth is required and not `authed`, render `<Login/>` instead of `renderRoute(...)`. Listen for the `aaiclick:unauthorized` window event to flip `authed=false`. Bootstrap `authed` on mount by calling `/auth/me` (200 → authed; 401 → show login). When the server has auth disabled, `/auth/me` returns the synthetic admin → authed, so no login wall appears.

- [ ] **Step 3: Wrap the tree** in `<AuthProvider>` in `main.tsx` (beside the React Query + Toast providers).

- [ ] **Step 4: Verify** — `npm run check`; `npm run build`. Manually: with `AAICLICK_AUTH_ENABLED=true` the SPA shows Login; logging in reaches the app; with auth off, no wall. Commit.

```bash
git add src/views/Login.tsx src/views/index.tsx src/App.tsx src/main.tsx
git commit -m "SPA: login view + auth route guard"
```

---

## Phase 9: Remove static token + docs

### Task 9.1: Purge `AAICLICK_API_TOKEN` references

**Files:** repo-wide

- [ ] **Step 1: Find stragglers**

Run: `grep -rn "AAICLICK_API_TOKEN\|require_bearer\|BearerAuthMiddleware" aaiclick/ docs/`
Expected: only intended spots (none in code after the rewrite; docs handled next).

- [ ] **Step 2: Remove any remaining references** and run the full suite.

Run: `.venv/bin/pytest aaiclick -q`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "Remove residual static-token references"
```

### Task 9.2: Documentation

**Files:** `docs/api_server.md`, `docs/future.md`, `aaiclick/server/CLAUDE.md`, `docs/auth.md`

- [ ] **Step 1:** Update `docs/api_server.md` Authentication + Configuration sections to describe the JWT login flow and point to `docs/auth.md`. Apply the `markdown-style` skill; run `shortify` after.
- [ ] **Step 2:** Remove the *Operator UI Auth* item from `docs/future.md`.
- [ ] **Step 3:** Refresh `aaiclick/server/CLAUDE.md` (replace the static-token note with the JWT/Principal model).
- [ ] **Step 4:** Flip `docs/auth.md` status to ✅ IMPLEMENTED and add implementation references (e.g. `**Implementation**: aaiclick/server/auth.py`).
- [ ] **Step 5: Commit**

```bash
git add docs/ aaiclick/server/CLAUDE.md
git commit -m "Docs: document JWT auth, retire static token references"
```

### Task 9.3: Final verification + check-pr

- [ ] **Step 1:** Run the full backend suite + typecheck.

Run: `.venv/bin/pytest aaiclick -q && npm run check`
Expected: all green.

- [ ] **Step 2:** Push and use the `check-pr` skill to confirm GitHub Actions (local + distributed backends) pass; fix failures.

---

## Self-Review (completed by plan author)

- **Spec coverage:** users table ✅ (2.1) · refresh_tokens ✅ (2.1) · PATs removed ✅ (no api_tokens task) · username login ✅ (4.2/6.1) · bcrypt+pyjwt ✅ (0.1) · HTTPBearer + Principal ✅ (5.1) · gating off-by-default ✅ (1.1/5.1) · require_admin RBAC ✅ (5.1/6.4) · admin-only /mcp ✅ (5.1/6.3) · refresh rotation + reuse rejection ✅ (4.2) · logout revocation ✅ (4.2) · seed admin ✅ (6.3) · CLI ✅ (7.1) · SPA login + 401-refresh ✅ (8.x) · remove static token ✅ (9.1) · docs ✅ (9.2) · migration via skill ✅ (2.2).
- **Type consistency:** `Principal` fields (`user_id`, `username`, `role`) consistent across 5.1/6.1; `TokenPair`/`LoginRequest`/`UserView` consistent across auth + users + tests; `store` error names (`UsernameTaken`/`UserNotFound`/`RefreshInvalid`) match their `internal_api` mappings (`Conflict`/`NotFound`/`Unauthorized`).
- **Known judgement calls flagged inline:** `_enum_check` import vs copy (2.1); `/me` username placeholder when auth disabled (6.1); per-endpoint vs router-level `require_admin` for objects (6.4).
