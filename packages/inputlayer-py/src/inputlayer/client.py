"""InputLayer - top-level async client."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any

from inputlayer.auth import (
    ApiKeyInfo,
    UserInfo,
    compile_create_api_key,
    compile_create_user,
    compile_drop_user,
    compile_expire_api_key,
    compile_list_api_keys,
    compile_list_users,
    compile_revoke_api_key,
    compile_set_password,
    compile_set_role,
    parse_api_keys,
)
from inputlayer.connection import Connection
from inputlayer.knowledge_graph import KnowledgeGraph
from inputlayer.notifications import EventDispatcher, NotificationDispatcher, NotificationEvent


class InputLayer:
    """Async client for InputLayer knowledge graph engine.

    Usage::

        async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
            kg = il.knowledge_graph("default")
            await kg.define(Employee)
            await kg.insert(
                Employee(id=1, name="Alice", department="eng",
                         salary=120000.0, active=True)
            )
            result = await kg.query(Employee)

    Each knowledge graph handle has its own connection, bound to that graph
    when it opens (on first use) and rebound to it on every reconnect, so
    handles never switch a shared connection between graphs and the
    subscriptions of one survive queries on another. ``connect()`` opens the
    client's own connection (bound to ``initial_kg``, or the server's
    default), which serves user, key and graph administration and the handle
    for that graph.
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
        epoch: str | None = None,
        default_timeout: float | None = 30.0,
        keepalive: float | None = 20.0,
    ) -> None:
        self._dispatcher = NotificationDispatcher()
        self._events = EventDispatcher()
        self._options: dict[str, Any] = {
            "username": username,
            "password": password,
            "api_key": api_key,
            "auto_reconnect": auto_reconnect,
            "reconnect_delay": reconnect_delay,
            "max_reconnect_attempts": max_reconnect_attempts,
            "default_timeout": default_timeout,
            "keepalive": keepalive,
            "dispatcher": self._dispatcher,
            "events": self._events,
        }
        self._url = url
        self._conn = Connection(
            url, initial_kg=initial_kg, last_seq=last_seq, epoch=epoch, **self._options
        )
        # One connection per knowledge graph, opened on first use.
        self._pool: dict[str, Connection] = {}
        self._kgs: dict[str, KnowledgeGraph] = {}

    # ── Connection lifecycle ──────────────────────────────────────────

    async def connect(self) -> None:
        """Connect and authenticate."""
        await self._conn.connect()

    async def close(self) -> None:
        """Close every connection of this client."""
        conns = [self._conn, *(c for c in self._pool.values() if c is not self._conn)]
        for conn in conns:
            await conn.close()
        self._dispatcher.end()
        self._events.end()

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
        return self._dispatcher.last_seq

    @property
    def events(self) -> EventDispatcher:
        """Connection events of every connection: ``disconnected``,
        ``reconnected``, ``session_reset``, ``notification_gap``, ``closed``."""
        return self._events

    # ── KG management ─────────────────────────────────────────────────

    def _connection_for(self, name: str) -> Connection:
        if self._conn.current_kg == name:
            return self._conn
        conn = self._pool.get(name)
        if conn is None:
            conn = Connection(
                self._url, initial_kg=name, lazy=True, create_kg=self._create_kg,
                **self._options,
            )
            self._pool[name] = conn
        return conn

    async def _create_kg(self, name: str) -> bool:
        """Create *name* on the client's own connection; ``True`` if created."""
        if self._conn.state == "idle":
            await self._conn.connect()
        await self._conn.execute(f".kg create {name}")
        return True

    def knowledge_graph(self, name: str, *, create: bool = True) -> KnowledgeGraph:
        """Get a KnowledgeGraph handle, with its own connection bound to *name*.

        The connection opens on the handle's first call; a missing graph is
        created then when ``create`` is true.
        """
        if name not in self._kgs:
            conn = self._connection_for(name)
            if not create and conn is not self._conn:
                conn._create_kg = None
            self._kgs[name] = KnowledgeGraph(name, conn)
        return self._kgs[name]

    async def list_knowledge_graphs(self) -> list[str]:
        """List all knowledge graphs.

        The server's ``.kg list`` command returns a header row followed
        by one indented line per knowledge graph, with the active KG
        marked by a trailing ``*``::

            Knowledge Graphs:
              default *
              demo

        We strip whitespace and the active marker, and skip the header.
        """
        result = await self._conn.execute(".kg list")
        out: list[str] = []
        for row in result.rows or []:
            if not row:
                continue
            text = str(row[0]).strip()
            if not text or text.endswith(":"):
                continue
            if text.endswith(" *"):
                text = text[:-2].rstrip()
            out.append(text)
        return out

    async def drop_knowledge_graph(self, name: str) -> None:
        """Drop a knowledge graph and all its data.

        The handle's own connection to it is closed first. The server rejects
        dropping the graph a connection is bound to, so when it is the
        client's own connection's graph, that connection moves to
        ``default`` first.
        """
        conn = self._pool.pop(name, None)
        if conn is not None and conn is not self._conn:
            await conn.close()
        if self._conn.current_kg == name:
            await self._conn.execute(".kg use default")
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

    async def create_api_key(self, label: str, ttl: str | None = None) -> str:
        """Create an API key, expiring ``ttl`` from now (e.g. ``"90d"``) or
        never. Returns the key, which the server shows only once."""
        result = await self._conn.execute(compile_create_api_key(label, ttl))
        return str(result.rows[0][1])

    async def list_api_keys(self) -> list[ApiKeyInfo]:
        result = await self._conn.execute(compile_list_api_keys())
        return parse_api_keys(result.columns, result.rows)

    async def expire_api_key(self, label: str, ttl: str) -> None:
        """Bring a key's expiry forward to ``ttl`` from now, e.g. the grace
        period of a rotation. An expiry can only be brought forward."""
        await self._conn.execute(compile_expire_api_key(label, ttl))

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
        return self._dispatcher.on(
            event_type, relation=relation, knowledge_graph=knowledge_graph
        )

    async def notifications(self) -> AsyncIterator[NotificationEvent]:
        """Async iterator yielding notification events."""
        async for event in self._dispatcher:
            yield event
