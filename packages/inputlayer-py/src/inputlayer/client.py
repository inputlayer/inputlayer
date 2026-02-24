"""InputLayer - top-level async client."""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable

from inputlayer.auth import (
    AclEntry,
    ApiKeyInfo,
    UserInfo,
    compile_create_api_key,
    compile_create_user,
    compile_drop_user,
    compile_list_api_keys,
    compile_list_users,
    compile_revoke_api_key,
    compile_set_password,
    compile_set_role,
)
from inputlayer.connection import Connection
from inputlayer.knowledge_graph import KnowledgeGraph
from inputlayer.notifications import NotificationDispatcher, NotificationEvent


class InputLayer:
    """Async client for InputLayer knowledge graph engine.

    Usage::

        async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
            kg = il.knowledge_graph("default")
            await kg.define(Employee)
            await kg.insert(Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True))
            result = await kg.query(Employee)
    """

    def __init__(
        self,
        url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
        auto_reconnect: bool = True,
        reconnect_delay: float = 1.0,
        max_reconnect_attempts: int = 10,
        initial_kg: str | None = None,
        last_seq: int | None = None,
    ) -> None:
        self._conn = Connection(
            url,
            username=username,
            password=password,
            api_key=api_key,
            auto_reconnect=auto_reconnect,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnect_attempts,
            initial_kg=initial_kg,
            last_seq=last_seq,
        )
        self._kgs: dict[str, KnowledgeGraph] = {}

    # ── Connection lifecycle ──────────────────────────────────────────

    async def connect(self) -> None:
        """Connect and authenticate."""
        await self._conn.connect()

    async def close(self) -> None:
        """Close the connection."""
        await self._conn.close()

    async def __aenter__(self) -> InputLayer:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ── Properties ────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._conn.connected

    @property
    def session_id(self) -> str | None:
        return self._conn.session_id

    @property
    def server_version(self) -> str | None:
        return self._conn.server_version

    @property
    def role(self) -> str | None:
        return self._conn.role

    @property
    def last_seq(self) -> int:
        return self._conn.last_seq

    # ── KG management ─────────────────────────────────────────────────

    def knowledge_graph(self, name: str, *, create: bool = True) -> KnowledgeGraph:
        """Get a KnowledgeGraph handle. Switches the session's active KG."""
        if name not in self._kgs:
            self._kgs[name] = KnowledgeGraph(name, self._conn)
        return self._kgs[name]

    async def list_knowledge_graphs(self) -> list[str]:
        """List all knowledge graphs."""
        result = await self._conn.execute(".kg list")
        return [row[0] for row in result.rows] if result.rows else []

    async def drop_knowledge_graph(self, name: str) -> None:
        """Drop a knowledge graph."""
        await self._conn.execute(f".kg drop {name}")
        self._kgs.pop(name, None)

    # ── User management ───────────────────────────────────────────────

    async def create_user(self, username: str, password: str, role: str = "viewer") -> None:
        await self._conn.execute(compile_create_user(username, password, role))

    async def drop_user(self, username: str) -> None:
        await self._conn.execute(compile_drop_user(username))

    async def set_password(self, username: str, new_password: str) -> None:
        await self._conn.execute(compile_set_password(username, new_password))

    async def set_role(self, username: str, role: str) -> None:
        await self._conn.execute(compile_set_role(username, role))

    async def list_users(self) -> list[UserInfo]:
        result = await self._conn.execute(compile_list_users())
        return [
            UserInfo(username=row[0], role=row[1])
            for row in result.rows
            if len(row) >= 2
        ]

    # ── API key management ────────────────────────────────────────────

    async def create_api_key(self, label: str) -> str:
        """Create an API key. Returns the key string."""
        result = await self._conn.execute(compile_create_api_key(label))
        if result.rows and result.rows[0]:
            return str(result.rows[0][0])
        return ""

    async def list_api_keys(self) -> list[ApiKeyInfo]:
        result = await self._conn.execute(compile_list_api_keys())
        return [
            ApiKeyInfo(label=row[0], created_at=str(row[1]) if len(row) > 1 else "")
            for row in result.rows
        ]

    async def revoke_api_key(self, label: str) -> None:
        await self._conn.execute(compile_revoke_api_key(label))

    # ── Notifications ─────────────────────────────────────────────────

    def on(
        self,
        event_type: str,
        *,
        relation: str | None = None,
        knowledge_graph: str | None = None,
    ) -> Callable:
        """Register a notification callback. Use as a decorator."""
        return self._conn.dispatcher.on(
            event_type, relation=relation, knowledge_graph=knowledge_graph
        )

    async def notifications(self) -> AsyncIterator[NotificationEvent]:
        """Async iterator yielding notification events."""
        async for event in self._conn.dispatcher:
            yield event
