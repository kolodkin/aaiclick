"""Internal API for user administration (admin-only at the HTTP layer)."""

from __future__ import annotations

from sqlmodel import col

from aaiclick.auth import security, store
from aaiclick.auth.models import Role, User
from aaiclick.auth.view_models import CreateUserRequest, UserListFilter, UserView
from aaiclick.view_models import Page

from .errors import Conflict, NotFound
from .pagination import paginate


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
    page = await paginate(User, order_by=col(User.username).asc(), limit=filter.limit, offset=filter.offset)
    return Page[UserView](items=[_to_view(u) for u in page.rows], total=page.total)


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
