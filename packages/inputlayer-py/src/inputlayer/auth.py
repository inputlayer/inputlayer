"""Authentication helpers.

Data classes and meta-command compilation for user/key/ACL management.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.-]+$")
# Passwords allow more characters but no whitespace or control chars
_SAFE_PASSWORD = re.compile(r"^\S+$")


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
    label: str
    created_at: str


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


def compile_create_api_key(label: str) -> str:
    _validate_identifier(label, "label")
    return f".apikey create {label}"


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
