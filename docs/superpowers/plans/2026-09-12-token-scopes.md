# Token Scopes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two-value API-token scope with an ordered four-level ladder (`read` < `write` < `admin` < `superadmin`) whose three tenant-scoped levels are bound to one tenant at mint.

**Architecture:** One `ScopeLevel` literal plus a single `scope_admits` comparison drives both surfaces. REST reads a route's required level off its existing guard and method; MCP reads it off the tool's tag. The level is a *ceiling*, not a grant — the owner's live role in the token's tenant still caps what it delivers, so demotion keeps binding immediately.

**Tech Stack:** Python 3.10+, SQLModel/SQLAlchemy async, FastAPI, FastMCP, Alembic, pytest; React 19 + TypeScript SPA.

**Spec:** `docs/designs/token_scopes.md`

## Global Constraints

- All imports at the top of the file — no function-level imports (CLAUDE.md).
- Closed string sets are `typing.Literal` + module constants, never `StrEnum`, and never a DB CHECK constraint. DB columns stay `sa_column=Column(String, ...)`.
- Never hand-write an Alembic migration — use the `generate-migration` skill (GitHub workflow).
- Fixed tuples in APIs are `NamedTuple`, not plain tuples. Never `Any` as a typing shortcut.
- Tests: business logic in `aaiclick/internal_api/test_*.py` and `aaiclick/auth/test_*.py`; router tests assert HTTP plumbing only (`aaiclick/server/CLAUDE.md`).
- Run `uv run --extra all --extra test pytest <paths> -q --no-cov` and `uv run --extra dev pre-commit run --all-files` before each commit.
- Commit messages end with the session's Co-Authored-By / Claude-Session trailers.

---

### Task 1: The scope ladder

**Files:**
- Modify: `aaiclick/auth/models.py` (replace the `TokenScope` block near the top)
- Test: `aaiclick/auth/test_models.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ScopeLevel` (`Literal["read","write","admin","superadmin"]`), constants `SCOPE_READ` / `SCOPE_WRITE` / `SCOPE_ADMIN` / `SCOPE_SUPERADMIN`, ordered `SCOPE_LEVELS: tuple[ScopeLevel, ...]`, and `scope_admits(held: ScopeLevel, required: ScopeLevel) -> bool`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/auth/test_models.py`:

```python
@pytest.mark.parametrize(
    "held, required, expected",
    [
        pytest.param(SCOPE_READ, SCOPE_READ, True, id="read-admits-read"),
        pytest.param(SCOPE_READ, SCOPE_WRITE, False, id="read-refuses-write"),
        pytest.param(SCOPE_WRITE, SCOPE_READ, True, id="write-admits-read"),
        pytest.param(SCOPE_WRITE, SCOPE_ADMIN, False, id="write-refuses-admin"),
        pytest.param(SCOPE_ADMIN, SCOPE_WRITE, True, id="admin-admits-write"),
        pytest.param(SCOPE_ADMIN, SCOPE_SUPERADMIN, False, id="admin-refuses-superadmin"),
        pytest.param(SCOPE_SUPERADMIN, SCOPE_ADMIN, True, id="superadmin-admits-admin"),
    ],
)
def test_scope_admits(held, required, expected):
    assert scope_admits(held, required) is expected


def test_scope_levels_are_ordered_low_to_high():
    """The tuple order *is* the comparison — a reordering silently changes every gate."""
    assert SCOPE_LEVELS == (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN, SCOPE_SUPERADMIN)
```

Add to that file's imports:

```python
import pytest

from aaiclick.auth.models import (
    SCOPE_ADMIN,
    SCOPE_LEVELS,
    SCOPE_READ,
    SCOPE_SUPERADMIN,
    SCOPE_WRITE,
    scope_admits,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/auth/test_models.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'SCOPE_READ'`

- [ ] **Step 3: Write the implementation**

In `aaiclick/auth/models.py`, replace the four `TOKEN_SCOPE_*` / `TokenScope` / `TOKEN_SCOPES` lines with:

```python
SCOPE_READ = "read"
SCOPE_WRITE = "write"
SCOPE_ADMIN = "admin"
SCOPE_SUPERADMIN = "superadmin"
ScopeLevel = Literal["read", "write", "admin", "superadmin"]
SCOPE_LEVELS: tuple[ScopeLevel, ...] = (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN, SCOPE_SUPERADMIN)
"""Ordered low to high — the index is the comparison in ``scope_admits``."""


def scope_admits(held: ScopeLevel, required: ScopeLevel) -> bool:
    """Whether a token holding ``held`` may perform a ``required``-level operation."""
    return SCOPE_LEVELS.index(held) >= SCOPE_LEVELS.index(required)
```

Then update `ApiToken.scope` to `scope: ScopeLevel = Field(sa_column=Column(String, nullable=False))`.

- [ ] **Step 4: Fix every `TokenScope` reference**

Run: `grep -rn "TokenScope\|TOKEN_SCOPE_" aaiclick/ --include=*.py`

Rename in each hit: `TokenScope` → `ScopeLevel`, `TOKEN_SCOPE_READ` → `SCOPE_READ`, `TOKEN_SCOPE_WRITE` → `SCOPE_WRITE`, `TOKEN_SCOPES` → `SCOPE_LEVELS`. Expected files: `aaiclick/auth/store.py`, `aaiclick/auth/view_models.py`, `aaiclick/server/auth.py`, `aaiclick/__main__.py`.

- [ ] **Step 5: Run tests and lint**

Run: `uv run --extra all --extra test pytest aaiclick/auth aaiclick/internal_api aaiclick/server -q --no-cov -n 4`
Expected: PASS
Run: `uv run --extra dev pre-commit run --all-files`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(auth): ordered four-level scope ladder"
```

---

### Task 2: `api_tokens.tenant_id` column and migration

**Files:**
- Modify: `aaiclick/auth/models.py` (`ApiToken`)
- Create: `aaiclick/orchestration/migrations/versions/<generated>.py` (via workflow)
- Test: `aaiclick/auth/test_models.py`

**Interfaces:**
- Consumes: Task 1's `ScopeLevel`.
- Produces: `ApiToken.tenant_id: int | None` — null exactly when `scope == SCOPE_SUPERADMIN`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/auth/test_models.py`:

```python
async def test_api_token_carries_its_tenant(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="tok", password_hash="x"))
        await session.flush()
        session.add(
            ApiToken(
                id=get_snowflake_id(),
                user_id=uid,
                name="ci",
                prefix="aaic_abc",
                token_hash="h",
                scope=SCOPE_ADMIN,
                tenant_id=DEFAULT_TENANT_ID,
            )
        )
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(ApiToken).where(ApiToken.user_id == uid))).scalar_one()
        assert row.tenant_id == DEFAULT_TENANT_ID and row.scope == SCOPE_ADMIN
```

Add `ApiToken` to the `aaiclick.auth.models` import and `from aaiclick.tenancy import DEFAULT_TENANT_ID`.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/auth/test_models.py -q --no-cov`
Expected: FAIL — `TypeError: 'tenant_id' is an invalid keyword argument`

- [ ] **Step 3: Add the column**

In `ApiToken`, after `scope`:

```python
    tenant_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True), default=None)
    """The tenant this token acts in; ``None`` only for ``superadmin`` scope.

    A plain column, not a DB FK — matching ``jobs`` and ``table_registry``,
    where the reference is enforced at the API boundary.
    """
```

- [ ] **Step 4: Run the test**

Run: `uv run --extra all --extra test pytest aaiclick/auth/test_models.py -q --no-cov`
Expected: PASS (local SQLite builds tables from `SQLModel.metadata`)

- [ ] **Step 5: Generate the migration**

Commit and push the model change first, then use the `generate-migration` skill with message `add api_tokens.tenant_id`. Pull the generated revision and verify:

Run: `uv run --extra all alembic -c aaiclick/orchestration/alembic.ini heads`
Expected: exactly one head

Open the generated file and confirm it adds a nullable `BigInteger` with an index and nothing else. Existing rows are left null; backfill them to the default tenant by appending to `upgrade()`:

```python
    op.execute("UPDATE api_tokens SET tenant_id = 1 WHERE tenant_id IS NULL AND scope <> 'superadmin'")
```

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(auth): bind api tokens to a tenant"
```

---

### Task 3: Store — write the tenant, resolve one membership

**Files:**
- Modify: `aaiclick/auth/store.py` (`ResolvedApiToken`, `create_api_token`, `resolve_api_token`)
- Test: `aaiclick/internal_api/test_api_tokens.py`

**Interfaces:**
- Consumes: Task 2's `ApiToken.tenant_id`.
- Produces: `ResolvedApiToken(token: ApiToken, user: User, role: Role | None)` — `role` is the owner's live role in the token's tenant, `None` when the token is untenanted or the owner is no longer a member. `create_api_token(..., tenant_id: int | None)`.

- [ ] **Step 1: Write the failing test**

Replace `test_resolve_stamps_last_used_once_per_window` in `aaiclick/internal_api/test_api_tokens.py` with:

```python
async def test_resolve_returns_the_live_role_in_the_tokens_tenant(orch_ctx):
    user = await _user()
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=user.id, role=ROLE_ADMIN)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    token_hash = security.sha256_hex(created.token)

    first = await store.resolve_api_token(token_hash)
    assert first is not None
    assert first.token.tenant_id == tenant.id and first.role == ROLE_ADMIN
    assert first.token.last_used_at is not None

    second = await store.resolve_api_token(token_hash)  # inside the window: no second write
    assert second is not None and second.token.last_used_at == first.token.last_used_at


async def test_resolve_role_is_none_once_membership_is_gone(orch_ctx):
    """The ceiling collapses the moment the owner loses the tenant."""
    user = await _user()
    tenant = await store.create_tenant(slug="acme", name="Acme")
    await store.set_membership(tenant_id=tenant.id, user_id=user.id, role=ROLE_ADMIN)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="ci", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    await store.remove_membership(tenant_id=tenant.id, user_id=user.id)

    resolved = await store.resolve_api_token(security.sha256_hex(created.token))
    assert resolved is not None and resolved.role is None
```

Add `ROLE_ADMIN` and `SCOPE_ADMIN` to the `aaiclick.auth.models` import.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/internal_api/test_api_tokens.py -q --no-cov`
Expected: FAIL — `CreateApiTokenRequest` has no `tenant_id`

- [ ] **Step 3: Update the store**

Replace `ResolvedApiToken` and the two functions in `aaiclick/auth/store.py`:

```python
class ResolvedApiToken(NamedTuple):
    token: ApiToken
    user: User
    role: Role | None
    """The owner's live role in the token's tenant; ``None`` when untenanted or no longer a member."""


async def create_api_token(
    *,
    user_id: int,
    name: str,
    prefix: str,
    token_hash: str,
    scope: ScopeLevel,
    tenant_id: int | None,
    expires_at: datetime | None,
) -> ApiToken:
    return await _insert(
        ApiToken(
            id=get_snowflake_id(),
            user_id=user_id,
            name=name,
            prefix=prefix,
            token_hash=token_hash,
            scope=scope,
            tenant_id=tenant_id,
            expires_at=expires_at,
        )
    )


async def resolve_api_token(token_hash: str) -> ResolvedApiToken | None:
    """Everything a request needs to authenticate an API token, in one session:
    the active token, its owner, and the owner's live role in the token's own
    tenant. Also stamps ``last_used_at`` (throttled) without a further session."""
    now = utc_now()
    async with get_sql_session() as session:
        pair = (
            await session.execute(
                select(ApiToken, User)
                .join(User, col(User.id) == col(ApiToken.user_id))
                .where(ApiToken.token_hash == token_hash)
            )
        ).first()
        if pair is None:
            return None
        token, user = pair
        if not _token_active(token, now):
            return None
        role: Role | None = None
        if token.tenant_id is not None:
            membership = (
                await session.execute(
                    select(TenantMembership).where(
                        TenantMembership.user_id == user.id, TenantMembership.tenant_id == token.tenant_id
                    )
                )
            ).scalar_one_or_none()
            role = cast("Role", membership.role) if membership is not None else None
        if token.last_used_at is None or now - token.last_used_at >= API_TOKEN_LAST_USED_GRANULARITY:
            token.last_used_at = now
            session.add(token)
            await session.commit()
    return ResolvedApiToken(token=token, user=user, role=role)
```

- [ ] **Step 4: Run the tests**

Run: `uv run --extra all --extra test pytest aaiclick/internal_api/test_api_tokens.py -q --no-cov`
Expected: still FAIL on `tenant_id` — Task 4 adds it to the request model. That is the expected hand-off; do not add it here.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "refactor(auth): resolve a token against its own tenant"
```

---

### Task 4: The mint ceiling

**Files:**
- Modify: `aaiclick/auth/view_models.py` (`CreateApiTokenRequest`, `ApiTokenView`)
- Modify: `aaiclick/internal_api/api_tokens.py` (`create_token`, `_to_view`)
- Test: `aaiclick/internal_api/test_api_tokens.py`

**Interfaces:**
- Consumes: Task 1's `scope_admits`, Task 3's `create_api_token`.
- Produces: `CreateApiTokenRequest(name, scope, tenant_id, expires_at)`; `create_token(user_id: int, request: CreateApiTokenRequest) -> ApiTokenCreated` raising `Invalid` above the ceiling and `NotFound` for a tenant the caller cannot act in.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/internal_api/test_api_tokens.py`:

```python
async def _member(username: str, tenant_id: int, role: str):
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    await store.set_membership(tenant_id=tenant_id, user_id=view.id, role=role)
    return view


async def test_member_may_mint_up_to_write(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    viewer = await _member("v", tenant.id, ROLE_VIEWER)

    ok = await api_tokens.create_token(
        viewer.id, CreateApiTokenRequest(name="ok", scope=SCOPE_WRITE, tenant_id=tenant.id)
    )
    assert ok.scope == SCOPE_WRITE and ok.tenant_id == tenant.id

    with pytest.raises(Invalid, match="write"):
        await api_tokens.create_token(
            viewer.id, CreateApiTokenRequest(name="no", scope=SCOPE_ADMIN, tenant_id=tenant.id)
        )


async def test_tenant_admin_may_mint_admin_but_not_superadmin(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    admin = await _member("a", tenant.id, ROLE_ADMIN)

    assert (
        await api_tokens.create_token(
            admin.id, CreateApiTokenRequest(name="ok", scope=SCOPE_ADMIN, tenant_id=tenant.id)
        )
    ).scope == SCOPE_ADMIN
    with pytest.raises(Invalid):
        await api_tokens.create_token(admin.id, CreateApiTokenRequest(name="no", scope=SCOPE_SUPERADMIN))


async def test_non_member_tenant_reads_as_missing(orch_ctx):
    """404, never 403 — a token must not be able to probe for tenants."""
    tenant = await store.create_tenant(slug="acme", name="Acme")
    stranger = await users.create_user(CreateUserRequest(username="s", password="pw"))
    with pytest.raises(NotFound):
        await api_tokens.create_token(
            stranger.id, CreateApiTokenRequest(name="no", scope=SCOPE_READ, tenant_id=tenant.id)
        )


async def test_superadmin_mints_anywhere_and_untenanted(orch_ctx):
    tenant = await store.create_tenant(slug="acme", name="Acme")
    root = await users.create_user(CreateUserRequest(username="root", password="pw", superadmin=True))

    bound = await api_tokens.create_token(
        root.id, CreateApiTokenRequest(name="b", scope=SCOPE_ADMIN, tenant_id=tenant.id)
    )
    assert bound.tenant_id == tenant.id

    instance = await api_tokens.create_token(root.id, CreateApiTokenRequest(name="i", scope=SCOPE_SUPERADMIN))
    assert instance.tenant_id is None


async def test_tenant_required_below_superadmin(orch_ctx):
    user = await _user()
    with pytest.raises(Invalid, match="tenant"):
        await api_tokens.create_token(user.id, CreateApiTokenRequest(name="no", scope=SCOPE_READ))
```

Add to imports: `ROLE_ADMIN`, `ROLE_VIEWER`, `SCOPE_READ`, `SCOPE_WRITE`, `SCOPE_ADMIN`, `SCOPE_SUPERADMIN` from `aaiclick.auth.models`; `NotFound` from `aaiclick.internal_api.errors`.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/internal_api/test_api_tokens.py -q --no-cov`
Expected: FAIL — `CreateApiTokenRequest` has no `tenant_id`

- [ ] **Step 3: Widen the view models**

In `aaiclick/auth/view_models.py`:

```python
class ApiTokenView(BaseModel):
    """A token as listed — never carries the secret."""

    id: SnowflakeId
    name: str
    prefix: str
    scope: ScopeLevel
    tenant_id: SnowflakeId | None
    expires_at: datetime | None
    last_used_at: datetime | None
    revoked_at: datetime | None
    created_at: datetime


class CreateApiTokenRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    scope: ScopeLevel = SCOPE_READ
    tenant_id: int | None = None
    """Required below ``superadmin``; ignored (and stored ``None``) at that level."""
    expires_at: datetime | None = None
```

Import `SCOPE_READ, ScopeLevel` from `.models`.

- [ ] **Step 4: Implement the ceiling**

In `aaiclick/internal_api/api_tokens.py`, add above `create_token`:

```python
async def _mint_ceiling(user_id: int, tenant_id: int | None) -> ScopeLevel:
    """The highest level this caller may mint. A superadmin is unbounded; anyone
    else is capped by their role in the tenant they named."""
    user = await store.get_user_by_id(user_id)
    if user is None:
        raise NotFound(f"user {user_id} not found")
    if user.superadmin:
        return SCOPE_SUPERADMIN
    if tenant_id is None:
        raise Invalid("a tenant is required below superadmin scope")
    if await store.get_tenant_by_id(tenant_id) is None:
        raise NotFound(f"tenant {tenant_id} not found")
    membership = await store.get_membership(tenant_id=tenant_id, user_id=user_id)
    if membership is None:
        # Missing, never forbidden — a caller must not be able to probe for tenants.
        raise NotFound(f"tenant {tenant_id} not found")
    return SCOPE_ADMIN if membership.role == ROLE_ADMIN else SCOPE_WRITE
```

Replace `create_token`:

```python
async def create_token(user_id: int, request: CreateApiTokenRequest) -> ApiTokenCreated:
    """Mint a token for ``user_id``. The raw secret is in the response and nowhere else."""
    if request.expires_at is not None and request.expires_at <= utc_now():
        raise Invalid("expires_at must be in the future")
    ceiling = await _mint_ceiling(user_id, request.tenant_id)
    if not scope_admits(ceiling, request.scope):
        raise Invalid(f"cannot mint scope '{request.scope}' — your level here is '{ceiling}'")
    if request.scope != SCOPE_SUPERADMIN and request.tenant_id is None:
        raise Invalid("a tenant is required below superadmin scope")
    tenant_id = None if request.scope == SCOPE_SUPERADMIN else request.tenant_id
    secret = security.generate_api_token()
    row = await store.create_api_token(
        user_id=user_id,
        name=request.name,
        prefix=security.api_token_display_prefix(secret),
        token_hash=security.sha256_hex(secret),
        scope=request.scope,
        tenant_id=tenant_id,
        expires_at=request.expires_at,
    )
    return ApiTokenCreated(**_to_view(row).model_dump(), token=secret)
```

Add `tenant_id=token.tenant_id` to `_to_view`.

- [ ] **Step 5: Add the store helper `_mint_ceiling` needs**

In `aaiclick/auth/store.py`, beside `set_membership`:

```python
async def get_membership(*, tenant_id: int, user_id: int) -> TenantMembership | None:
    async with get_sql_session() as session:
        result = await session.execute(
            select(TenantMembership).where(
                TenantMembership.tenant_id == tenant_id, TenantMembership.user_id == user_id
            )
        )
        return result.scalar_one_or_none()
```

- [ ] **Step 6: Run the tests**

Run: `uv run --extra all --extra test pytest aaiclick/internal_api/test_api_tokens.py aaiclick/auth -q --no-cov`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(auth): cap a mint at the caller's level in the named tenant"
```

---

### Task 5: Principal carries the bound tenant

**Files:**
- Modify: `aaiclick/server/auth.py` (`Principal`, `_principal_from_api_token`, `resolve_tenant`)
- Test: `aaiclick/server/test_auth.py`

**Interfaces:**
- Consumes: Task 3's `ResolvedApiToken`.
- Produces: `Principal(user_id, superadmin, tenants, scope: ScopeLevel | None, kind, tenant_id: int | None)` — `scope is None` means unscoped (session or local mode). `resolve_tenant` honours `principal.tenant_id`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/server/test_auth.py`:

```python
def _bound(level="admin", tenant_id=7, role="admin"):
    return auth.Principal(
        user_id=5, superadmin=False, tenants={tenant_id: role}, scope=level, kind="token", tenant_id=tenant_id
    )


def test_bound_token_ignores_a_missing_header():
    ctx = auth.resolve_tenant(_bound(), None)
    assert ctx == auth.TenantContext(tenant_id=7, role="admin")


def test_bound_token_rejects_a_mismatched_header():
    """A client naming a different tenant has a bug — surface it, don't ignore it."""
    with pytest.raises(Invalid, match="bound"):
        auth.resolve_tenant(_bound(), "8")


def test_bound_token_accepts_a_matching_header():
    assert auth.resolve_tenant(_bound(), "7").tenant_id == 7


def test_bound_token_forbidden_once_membership_is_gone():
    stripped = _bound()._replace(tenants={})
    with pytest.raises(Forbidden):
        auth.resolve_tenant(stripped, None)


def test_session_principal_is_unscoped():
    session = auth.Principal(user_id=1, superadmin=True, tenants={}, kind="session")
    assert session.scope is None and session.tenant_id is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/server/test_auth.py -q --no-cov`
Expected: FAIL — `Principal` has no `tenant_id`

- [ ] **Step 3: Widen `Principal`**

```python
class Principal(NamedTuple):
    user_id: int | None
    superadmin: bool
    tenants: dict[int, Role]
    """Membership map ``tenant_id -> role`` — from the access JWT, or the token's own tenant."""
    scope: ScopeLevel | None = None
    """API-token level; ``None`` means unscoped — a session or local mode, bounded by role alone."""
    kind: AuthKind = AUTH_KIND_SESSION
    tenant_id: int | None = None
    """The tenant a tenant-scoped token is bound to."""
```

- [ ] **Step 4: Build it from the resolved token**

```python
async def _principal_from_api_token(token: str) -> Principal:
    """Look an ``aaic_`` token up by hash and build a Principal from its owner's
    *current* flag and live role in the token's tenant, so revocation and
    demotion bind instantly."""
    async with orch_context(with_ch=False):
        resolved = await store.resolve_api_token(security.sha256_hex(token))
    if resolved is None:
        raise Unauthorized("invalid api token")
    if resolved.user.disabled:
        raise Unauthorized("user is disabled")
    tenants: dict[int, Role] = {}
    if resolved.token.tenant_id is not None and resolved.role is not None:
        tenants[resolved.token.tenant_id] = resolved.role
    return Principal(
        user_id=resolved.user.id,
        superadmin=resolved.user.superadmin,
        tenants=tenants,
        scope=resolved.token.scope,
        kind=AUTH_KIND_TOKEN,
        tenant_id=resolved.token.tenant_id,
    )
```

- [ ] **Step 5: Honour the binding in `resolve_tenant`**

Insert after the `AUTH_KIND_NONE` branch:

```python
    if principal.tenant_id is not None:
        if header_value is not None and header_value != str(principal.tenant_id):
            raise Invalid(f"{TENANT_HEADER} does not match the tenant this token is bound to")
        role = role_in_tenant(principal, principal.tenant_id)
        if role is None:
            raise Forbidden(f"no access to tenant {principal.tenant_id}")
        return TenantContext(tenant_id=principal.tenant_id, role=role)
```

- [ ] **Step 6: Run the tests**

Run: `uv run --extra all --extra test pytest aaiclick/server -q --no-cov -n 4`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(auth): principals carry their token's bound tenant"
```

---

### Task 6: REST enforcement from guard and method

**Files:**
- Modify: `aaiclick/server/auth.py` (`enforce_scope`, `require_principal`, `require_admin`, `require_superadmin`)
- Test: `aaiclick/server/test_auth.py`, `aaiclick/server/routers/test_auth.py`

**Interfaces:**
- Consumes: Task 1's `scope_admits`, Task 5's `Principal.scope`.
- Produces: `enforce_scope(principal: Principal, required: ScopeLevel) -> None`.

- [ ] **Step 1: Write the failing test**

Replace `test_read_scope_blocks_writes` in `aaiclick/server/test_auth.py` with:

```python
@pytest.mark.parametrize(
    "held, required, allowed",
    [
        pytest.param("read", "read", True, id="read-reads"),
        pytest.param("read", "write", False, id="read-cannot-write"),
        pytest.param("write", "admin", False, id="write-cannot-admin"),
        pytest.param("admin", "write", True, id="admin-can-write"),
        pytest.param("admin", "superadmin", False, id="admin-cannot-superadmin"),
        pytest.param("superadmin", "superadmin", True, id="superadmin-can"),
    ],
)
def test_enforce_scope_walks_the_ladder(held, required, allowed):
    principal = auth.Principal(user_id=1, superadmin=True, tenants={}, scope=held, kind="token")
    if allowed:
        auth.enforce_scope(principal, required)
    else:
        with pytest.raises(Forbidden):
            auth.enforce_scope(principal, required)


def test_unscoped_principal_is_never_blocked_by_the_ladder():
    """A session is bounded by its user's role, not by a scope."""
    session = auth.Principal(user_id=1, superadmin=True, tenants={}, kind="session")
    auth.enforce_scope(session, "superadmin")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/server/test_auth.py -q --no-cov`
Expected: FAIL — `enforce_scope() got an unexpected keyword argument` / signature mismatch

- [ ] **Step 3: Rewrite the gate and the guards**

```python
def enforce_scope(principal: Principal, required: ScopeLevel) -> None:
    """Gate a principal's token level against the level an operation needs.
    An unscoped principal (session, local mode) is bounded by role alone."""
    if principal.scope is not None and not scope_admits(principal.scope, required):
        raise Forbidden(f"token scope '{principal.scope}' cannot perform '{required}' operations")
```

In `require_principal`, replace the `enforce_scope(...)` line with:

```python
    enforce_scope(principal, SCOPE_READ if request.method in SAFE_METHODS else SCOPE_WRITE)
```

Then each guard adds its own rung. Note `require_admin` gains a
`require_principal` dependency it does not have today — call sites use
`Depends(require_admin)` and need no change:

```python
async def require_admin(
    ctx: TenantContext = Depends(require_tenant), principal: Principal = Depends(require_principal)
) -> TenantContext:
    """Tenant-admin guard for mutating tenant-scoped routes."""
    enforce_scope(principal, SCOPE_ADMIN)
    check_tenant_admin(ctx)
    return ctx


async def require_superadmin(principal: Principal = Depends(require_principal)) -> Principal:
    enforce_scope(principal, SCOPE_SUPERADMIN)
    check_superadmin(principal)
    return principal
```

- [ ] **Step 4: Add the router-level test**

Append to `aaiclick/server/routers/test_auth.py`:

```python
async def test_write_token_cannot_reach_an_admin_route(orch_ctx, enabled, anon_client):
    """The ladder, not the HTTP verb, is what separates write from admin."""
    from aaiclick.auth.models import SCOPE_WRITE
    from aaiclick.internal_api import api_tokens as api_tokens_api

    user = await users.create_user(CreateUserRequest(username="w", password="pw", superadmin=True))
    created = await api_tokens_api.create_token(
        user.id, CreateApiTokenRequest(name="w", scope=SCOPE_WRITE, tenant_id=DEFAULT_TENANT_ID)
    )
    headers = {"Authorization": f"Bearer {created.token}"}
    assert (await anon_client.get(f"{API_PREFIX}/jobs", headers=headers)).status_code == 200
    denied = await anon_client.post(f"{API_PREFIX}/jobs/1/cancel", headers=headers)
    assert denied.status_code == 403 and denied.json()["code"] == "forbidden"
```

Move the two imports to the top of the file per CLAUDE.md, and add `from aaiclick.auth.view_models import CreateApiTokenRequest` and `from aaiclick.tenancy import DEFAULT_TENANT_ID`.

- [ ] **Step 5: Run the tests and lint**

Run: `uv run --extra all --extra test pytest aaiclick/server aaiclick/internal_api -q --no-cov -n 4`
Expected: PASS
Run: `uv run --extra dev pre-commit run --all-files`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(server): gate REST on the scope ladder"
```

---

### Task 7: MCP tool levels

**Files:**
- Modify: `aaiclick/server/mcp_rbac.py` (`TAG_ADMIN`, `required_level`, `authorize_tool`)
- Modify: `aaiclick/server/mcp.py` (re-tag twelve tools)
- Test: `aaiclick/server/test_mcp_rbac.py`

**Interfaces:**
- Consumes: Task 1's `scope_admits`, Task 6's `enforce_scope`.
- Produces: `TAG_ADMIN = "admin"`; `required_level(tags: set[str]) -> ScopeLevel`.

- [ ] **Step 1: Write the failing test**

Replace `test_authorize_tool_matrix` in `aaiclick/server/test_mcp_rbac.py` with:

```python
@pytest.mark.parametrize(
    "principal, tags, header, expect",
    [
        pytest.param(_principal(tenants={7: "viewer"}, scope="read"), {TAG_READ}, "7", "ok", id="read-token-reads"),
        pytest.param(_principal(tenants={7: "viewer"}, scope="read"), {TAG_WRITE}, "7", Forbidden, id="read-token-no-write"),
        pytest.param(_principal(tenants={7: "viewer"}, scope="write"), {TAG_WRITE}, "7", "ok", id="member-writes"),
        pytest.param(_principal(tenants={7: "viewer"}, scope="write"), {TAG_ADMIN}, "7", Forbidden, id="write-token-no-admin"),
        pytest.param(_principal(tenants={7: "admin"}, scope="admin"), {TAG_ADMIN}, "7", "ok", id="admin-token-admins"),
        pytest.param(_principal(tenants={7: "viewer"}, scope="admin"), {TAG_ADMIN}, "7", Forbidden, id="ceiling-caps-below-token"),
        pytest.param(_principal(superadmin=True, scope="superadmin"), {TAG_SUPERADMIN}, None, "ok", id="superadmin-instance"),
        pytest.param(_principal(tenants={7: "admin"}, scope="admin"), {TAG_SUPERADMIN}, "7", Forbidden, id="admin-not-superadmin"),
    ],
)
def test_authorize_tool_matrix(enabled, principal, tags, header, expect):
    if expect == "ok":
        ctx = authorize_tool(principal, tags, header)
        assert ctx is None if TAG_SUPERADMIN in tags else ctx.tenant_id == int(header)
    else:
        with pytest.raises(expect):
            authorize_tool(principal, tags, header)
```

Update `_principal` to take `scope`:

```python
def _principal(*, superadmin=False, tenants=None, scope="superadmin"):
    return Principal(
        user_id=5, superadmin=superadmin, tenants=tenants or {}, scope=scope, kind="token"
    )
```

Add `TAG_ADMIN` to the `.mcp_rbac` import.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/server/test_mcp_rbac.py -q --no-cov`
Expected: FAIL — `cannot import name 'TAG_ADMIN'`

- [ ] **Step 3: Add the level and rewrite `authorize_tool`**

```python
TAG_READ = "read"
TAG_WRITE = "write"
TAG_ADMIN = "admin"
TAG_SUPERADMIN = "superadmin"

_TAG_LEVELS: tuple[tuple[str, ScopeLevel], ...] = (
    (TAG_SUPERADMIN, SCOPE_SUPERADMIN),
    (TAG_ADMIN, SCOPE_ADMIN),
    (TAG_WRITE, SCOPE_WRITE),
    (TAG_READ, SCOPE_READ),
)


def required_level(tags: set[str]) -> ScopeLevel:
    """The level a tool's tag demands. Highest tag wins, so a mistagged tool
    fails closed rather than open."""
    for tag, level in _TAG_LEVELS:
        if tag in tags:
            return level
    return SCOPE_SUPERADMIN


def authorize_tool(principal: Principal, tags: set[str], tenant_header: str | None) -> TenantContext | None:
    """Decide whether ``principal`` may call a tool with ``tags``.

    Returns the tenant to act in, or ``None`` for instance-level tools. Raises
    ``Forbidden`` / ``Invalid`` exactly like the REST guards.
    """
    required = required_level(tags)
    enforce_scope(principal, required)
    if required == SCOPE_SUPERADMIN:
        check_superadmin(principal)
        return None
    ctx = resolve_tenant(principal, tenant_header)
    if required == SCOPE_ADMIN:
        check_tenant_admin(ctx)
    return ctx
```

- [ ] **Step 4: Re-tag the twelve write tools**

In `aaiclick/server/mcp.py`, change `tags={TAG_WRITE}` to `tags={TAG_ADMIN}` for: `cancel_job`, `run_job`, `register_job`, `enable_job`, `disable_job`, `clear_task`, `delete_object`, `purge_objects`. Leave `save_query`, `delete_saved_query`, `save_dashboard`, `delete_dashboard` at `TAG_WRITE` — those are the member-level viewer writes. Import `TAG_ADMIN`.

Verify: `grep -c "TAG_ADMIN" aaiclick/server/mcp.py` → 9 (8 tools + the import).

- [ ] **Step 5: Run the tests**

Run: `uv run --extra all --extra test pytest aaiclick/server -q --no-cov -n 4`
Expected: PASS after two updates in the same file:

- `test_every_tool_has_exactly_one_rbac_tag` — add `TAG_ADMIN` to its tag set.
- `test_viewer_can_read_but_not_write` — `cancel_job` is now `TAG_ADMIN`, so the
  refusal comes from the ladder before the role check ever runs. Change the
  assertion from `"tenant admin" in ...` to `"cannot perform 'admin'" in ...`.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(mcp): gate tools on the scope ladder"
```

---

### Task 8: `/mcp` takes API tokens only

**Files:**
- Modify: `aaiclick/server/auth.py` (`PrincipalAuthMiddleware`)
- Test: `aaiclick/server/test_auth.py`, `aaiclick/server/test_mcp_rbac.py`

**Interfaces:**
- Consumes: Task 5's `principal_from_credential`.
- Produces: a `401` on `/mcp` for any non-`aaic_` credential while auth is enabled.

- [ ] **Step 1: Write the failing test**

In `aaiclick/server/test_auth.py`, *replace*
`test_mcp_middleware_admits_any_principal_and_stores_it` — it asserts the
behaviour this task removes — with a pair: one proving an API token still
reaches the mount and is recorded, one proving a session JWT no longer does.

```python
async def test_mcp_mount_admits_an_api_token_and_stores_it(orch_ctx, enabled):
    """Per-tool RBAC lives in mcp_rbac.py — the mount only needs a principal."""
    called: list[bool] = []
    user = await users.create_user(CreateUserRequest(username="m", password="pw"))
    await store.set_membership(tenant_id=DEFAULT_TENANT_ID, user_id=user.id, role=ROLE_VIEWER)
    created = await api_tokens.create_token(
        user.id, CreateApiTokenRequest(name="m", scope=SCOPE_READ, tenant_id=DEFAULT_TENANT_ID)
    )
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {created.token}".encode())]}
    await _drive(scope, called)
    assert called == [True]
    recorded = audit_state(scope).principal
    assert recorded is not None and recorded.user_id == user.id


async def test_mcp_mount_refuses_a_session_jwt(enabled):
    """MCP is the machine door; a session JWT belongs on REST."""
    called: list[bool] = []
    token = security.encode_access_token(
        user_id=2, superadmin=True, tenants={}, secret=TEST_JWT_SECRET, ttl=60
    )
    scope = {"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    sent = await _drive(scope, called)
    assert not called
    assert sent[0]["status"] == 401
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/server/test_auth.py -q --no-cov`
Expected: FAIL — the JWT is accepted, so `called == [True]`

- [ ] **Step 3: Restrict the mount**

In `PrincipalAuthMiddleware.__call__`, replace the `try` block:

```python
        try:
            if config.auth_enabled():
                scheme, credentials = get_authorization_scheme_param(authorization)
                if scheme.lower() != "bearer" or not credentials:
                    raise Unauthorized("missing bearer token")
                if not security.is_api_token(credentials):
                    raise Unauthorized("/mcp requires an API token, not a session")
            principal = await resolve_principal(authorization)
```

- [ ] **Step 4: Move the MCP HTTP tests onto real tokens**

In `aaiclick/server/test_mcp_rbac.py`, replace `_token(...)` with a fixture that mints a real token, since JWTs no longer reach the mount:

```python
async def _api_token(scope: str, *, superadmin: bool = False, role: str = ROLE_ADMIN) -> str:
    user = await users.create_user(
        CreateUserRequest(username=f"t_{scope}_{superadmin}", password="pw", superadmin=superadmin)
    )
    if not superadmin:
        await store.set_membership(tenant_id=DEFAULT_TENANT_ID, user_id=user.id, role=role)
    created = await api_tokens.create_token(
        user.id,
        CreateApiTokenRequest(
            name=scope, scope=scope, tenant_id=None if scope == SCOPE_SUPERADMIN else DEFAULT_TENANT_ID
        ),
    )
    return created.token
```

Update the two credentialled HTTP tests — `test_tools_list_is_filtered_by_role`
and `test_viewer_can_read_but_not_write` — to `await _api_token(...)` and build
headers from the returned secret. Both already take `orch_ctx`, which
`_api_token` needs. `test_anonymous_gets_401_problem` sends no credential and
is unchanged.

In `test_tools_list_is_filtered_by_role` the viewer's token is
`_api_token(SCOPE_WRITE, role=ROLE_VIEWER)` and the superadmin's is
`_api_token(SCOPE_SUPERADMIN, superadmin=True)`; the assertions hold as written,
since `run_job` is now `TAG_ADMIN` and a `write` token cannot reach it.

- [ ] **Step 5: Run the tests**

Run: `uv run --extra all --extra test pytest aaiclick/server -q --no-cov -n 4`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(mcp): accept API tokens only on the mount"
```

---

### Task 9: CLI and SPA

**Files:**
- Modify: `aaiclick/__main__.py` (`_run_token_create`, the `token create` parser)
- Modify: `aaiclick/cli_renderers.py` (`render_api_tokens_page`, `render_api_token_created`)
- Modify: `src/views/Tokens.tsx`, `src/api/schema.ts` (generated)
- Test: `aaiclick/test_cli.py`

**Interfaces:**
- Consumes: Task 4's `CreateApiTokenRequest`.
- Produces: `aaiclick token create <username> --name N [--scope read|write|admin|superadmin]`, taking its tenant from the global `--tenant` flag.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/test_cli.py`:

```python
def test_token_parser_accepts_every_scope():
    parser = build_parser()
    for level in ("read", "write", "admin", "superadmin"):
        args = parser.parse_args(["token", "create", "alice", "--name", "ci", "--scope", level])
        assert args.scope == level
    assert parser.parse_args(["token", "create", "alice", "--name", "ci"]).scope == "read"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra all --extra test pytest aaiclick/test_cli.py -q --no-cov`
Expected: FAIL — `argument --scope: invalid choice: 'admin'`

- [ ] **Step 3: Widen the CLI**

The parser already reads `choices=list(SCOPE_LEVELS)` once Task 1's rename lands, so only the default needs checking: `default=SCOPE_READ`. Then teach `_run_token_create` to take its tenant from the one already active.

`_run_internal_api` resolves the global `--tenant` flag and enters
`active_tenant` before awaiting `do()`, so reading the contextvar inside `do()`
gives the named tenant, or `DEFAULT_TENANT_ID` when the flag is absent — no
second slug lookup:

```python
async def _run_token_create(args: argparse.Namespace) -> None:
    async def do():
        user_id = await _resolve_user_id(args.username)
        expires_at = utc_now() + timedelta(days=args.expires_days) if args.expires_days else None
        tenant_id = None if args.scope == SCOPE_SUPERADMIN else get_active_tenant_id()
        return await api_tokens_api.create_token(
            user_id,
            CreateApiTokenRequest(
                name=args.name, scope=args.scope, tenant_id=tenant_id, expires_at=expires_at
            ),
        )

    view = await _run_internal_api(do())
    _render(args, view, cli_renderers.render_api_token_created)
```

Import `SCOPE_SUPERADMIN` from `aaiclick.auth.models` and `get_active_tenant_id` from `aaiclick.tenancy`.

- [ ] **Step 4: Show the tenant in both renderers**

In `cli_renderers.render_api_token_created`, add `tenant={_fmt_optional(view.tenant_id)}` to the first line. In `render_api_tokens_page`, add a `Tenant` column between `Scope` and `Expires`, widening the separator rule to match.

- [ ] **Step 5: Update the SPA**

Run `npm run gen-types`, then in `src/views/Tokens.tsx` widen the scope `<select>` to the four levels and pass `tenant_id` on create — the active tenant from `getActiveTenantId()`, or `null` when `scope === "superadmin"`. Add a `Tenant` column to the table.

Run: `npm run check` — Expected: no errors.

- [ ] **Step 6: Run everything**

Run: `uv run --extra all --extra test pytest aaiclick -q --no-cov -n auto`
Expected: PASS
Run: `npm run build && npm test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(cli,ui): mint tokens at any level in a named tenant"
```

---

### Task 10: Documentation and cleanup

**Files:**
- Modify: `docs/designs/auth.md` (API Tokens, MCP Surface)
- Modify: `docs/designs/tenant_rbac.md` (Role Matrix)
- Delete: `docs/designs/token_scopes.md`, `docs/superpowers/plans/2026-09-12-token-scopes.md`

**Interfaces:**
- Consumes: every task above.
- Produces: nothing — this is the record.

- [ ] **Step 1: Fold the spec into `auth.md`**

Per CLAUDE.md, the spec is deleted once the feature lands and `auth.md` becomes the record. Replace the **Scope** bullet under *API Tokens* with the ladder table from `docs/designs/token_scopes.md` — The Ladder, plus the mint-ceiling table from *Minting* and a sentence on the tenant binding and the ceiling-not-grant rule. Update the *MCP Surface* tag table to four rows. Add implementation references by name:

```markdown
**Implementation**: `aaiclick/auth/models.py` — see `ScopeLevel`, `scope_admits`;
`aaiclick/internal_api/api_tokens.py` — see `_mint_ceiling`, `create_token`;
`aaiclick/server/auth.py` — see `enforce_scope`, `resolve_tenant`;
`aaiclick/server/mcp_rbac.py` — see `required_level`, `authorize_tool`.
```

- [ ] **Step 2: Update the role matrix**

In `docs/designs/tenant_rbac.md`, the `/mcp` row now reads per level rather than per role. Check it still matches `required_level`.

- [ ] **Step 3: Delete the spec and this plan**

```bash
git rm docs/designs/token_scopes.md docs/superpowers/plans/2026-09-12-token-scopes.md
grep -rn "token_scopes" docs/ aaiclick/ || echo "no dangling references"
```

- [ ] **Step 4: Verify the whole tree**

Run: `uv run --extra dev pre-commit run --all-files`
Run: `uv run --extra all --extra test pytest aaiclick -q --no-cov -n auto`
Run: `uv run --extra docs mkdocs build --strict`
Run: `npm run check && npm run build && npm test`
Expected: all PASS

- [ ] **Step 5: Commit and push**

```bash
git add -A && git commit -m "docs(auth): fold the scope ladder into auth.md"
git push -u origin claude/api-auth-rbac-expansion-ih5xtx
```

Then trigger `test.yaml` on the branch and drive it to green.
