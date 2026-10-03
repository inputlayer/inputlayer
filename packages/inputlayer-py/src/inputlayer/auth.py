"""Authentication helpers.

Data classes and meta-command compilation for user/key/ACL management.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.-]+$")
# Passwords allow more characters but no whitespace or control chars
_SAFE_PASSWORD = re.compile(r"^\S+$")
# A TTL as the engine accepts it: a number and a unit, e.g. "30s", "90d"
_TTL = re.compile(r"^[0-9]+(ms|s|m|h|d)$")


def _validate_identifier(value: str, name: str) -> str:
    """Validate that a value is safe for use in meta-commands."""
    if not value:
        raise ValueError(f"{name} must not be empty")
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(
            f"{name} contains invalid characters: {value!r}. "
            f"Only letters, digits, underscores, dots, and hyphens are allowed."
        )
    return value


def _validate_password(value: str, name: str) -> str:
    """Validate a password for use in space-delimited meta-commands."""
    if not value:
        raise ValueError(f"{name} must not be empty")
    if not _SAFE_PASSWORD.match(value):
        raise ValueError(
            f"{name} must not contain whitespace: {value!r}"
        )
    return value


@dataclass(frozen=True)
class UserInfo:
    username: str
    role: str


@dataclass(frozen=True)
class ApiKeyInfo:
    """An API key as `.apikey list` reports it. Times are Unix milliseconds;
    ``None`` is unknown (``created_at``), never (``expires_at``) or not yet
    (``last_used_at``)."""

    label: str
    owner: str
    created_at: int | None
    expires_at: int | None
    last_used_at: int | None
    status: str


@dataclass(frozen=True)
class AclEntry:
    username: str
    role: str


# ── Meta command compilation ──────────────────────────────────────────


def compile_create_user(username: str, password: str, role: str = "viewer") -> str:
    _validate_identifier(username, "username")
    _validate_password(password, "password")
    _validate_identifier(role, "role")
    return f".user create {username} {password} {role}"


def compile_drop_user(username: str) -> str:
    _validate_identifier(username, "username")
    return f".user drop {username}"


def compile_set_password(username: str, new_password: str) -> str:
    _validate_identifier(username, "username")
    _validate_password(new_password, "new_password")
    return f".user password {username} {new_password}"


def compile_set_role(username: str, role: str) -> str:
    _validate_identifier(username, "username")
    _validate_identifier(role, "role")
    return f".user role {username} {role}"


def compile_list_users() -> str:
    return ".user list"


def _validate_ttl(value: str) -> str:
    if not _TTL.match(value):
        raise ValueError(
            f"ttl must be a number and a unit (ms, s, m, h or d), e.g. '90d': {value!r}"
        )
    return value


def compile_create_api_key(label: str, ttl: str | None = None) -> str:
    _validate_identifier(label, "label")
    if ttl is None:
        return f".apikey create {label}"
    return f".apikey create {label} {_validate_ttl(ttl)}"


def compile_expire_api_key(label: str, ttl: str) -> str:
    _validate_identifier(label, "label")
    return f".apikey expire {label} {_validate_ttl(ttl)}"


def parse_api_keys(columns: list[str], rows: list[list[object]]) -> list[ApiKeyInfo]:
    """`.apikey list` rows as :class:`ApiKeyInfo`."""
    keys = []
    for row in rows:
        named = dict(zip(columns, row))
        keys.append(
            ApiKeyInfo(
                label=str(named["label"]),
                owner=str(named["owner"]),
                created_at=named.get("created_at"),  # type: ignore[arg-type]
                expires_at=named.get("expires_at"),  # type: ignore[arg-type]
                last_used_at=named.get("last_used_at"),  # type: ignore[arg-type]
                status=str(named["status"]),
            )
        )
    return keys


def compile_list_api_keys() -> str:
    return ".apikey list"


def compile_revoke_api_key(label: str) -> str:
    _validate_identifier(label, "label")
    return f".apikey revoke {label}"


def compile_grant_access(kg: str, username: str, role: str) -> str:
    _validate_identifier(kg, "kg")
    _validate_identifier(username, "username")
    _validate_identifier(role, "role")
    return f".kg acl grant {kg} {username} {role}"


def compile_revoke_access(kg: str, username: str) -> str:
    _validate_identifier(kg, "kg")
    _validate_identifier(username, "username")
    return f".kg acl revoke {kg} {username}"


def compile_list_acl(kg: str) -> str:
    _validate_identifier(kg, "kg")
    return f".kg acl list {kg}"
