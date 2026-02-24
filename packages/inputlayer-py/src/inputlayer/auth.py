"""Authentication helpers - data classes and meta-command compilation for user/key/ACL management."""

from __future__ import annotations

from dataclasses import dataclass


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
    return f".user create {username} {password} {role}"


def compile_drop_user(username: str) -> str:
    return f".user drop {username}"


def compile_set_password(username: str, new_password: str) -> str:
    return f".user password {username} {new_password}"


def compile_set_role(username: str, role: str) -> str:
    return f".user role {username} {role}"


def compile_list_users() -> str:
    return ".user list"


def compile_create_api_key(label: str) -> str:
    return f".apikey create {label}"


def compile_list_api_keys() -> str:
    return ".apikey list"


def compile_revoke_api_key(label: str) -> str:
    return f".apikey revoke {label}"


def compile_grant_access(kg: str, username: str, role: str) -> str:
    return f".kg acl grant {kg} {username} {role}"


def compile_revoke_access(kg: str, username: str) -> str:
    return f".kg acl revoke {kg} {username}"


def compile_list_acl(kg: str) -> str:
    return f".kg acl list {kg}"
