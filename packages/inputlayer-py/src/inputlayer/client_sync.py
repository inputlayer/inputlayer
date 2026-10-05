"""InputLayerSync / KnowledgeGraphSync - synchronous wrappers.

Uses a dedicated background event loop thread so these work safely from
any context: plain scripts, Jupyter notebooks, FastAPI, LangGraph, etc.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any, Generic, TypeVar

from inputlayer._sync import run_sync
from inputlayer.auth import AclEntry, ApiKeyInfo, UserInfo
from inputlayer.client import InputLayer
from inputlayer.index import HnswIndex
from inputlayer.knowledge_graph import (
    ClearResult,
    DebugResult,
    DeleteResult,
    IndexInfo,
    IndexStats,
    InsertResult,
    KnowledgeGraph,
    RelationDescription,
    RelationInfo,
    RuleInfo,
    ServerStatus,
    WhyNotResult,
    WhyResult,
)
from inputlayer.program import Claim, Program, ProgramResult
from inputlayer.relation import Relation
from inputlayer.result import ResultSet
from inputlayer.subscription import (
    Change,
    GroupChange,
    GroupSubscription,
    Live,
    ReadResult,
    Subscription,
    SubscriptionHandle,
    SubscriptionStats,
    _Standing,
)

T = TypeVar("T")
E = TypeVar("E")

R = TypeVar("R", bound=Relation)


class ProgramSync(Program):
    """A ``Program`` whose ``commit()`` blocks: ``KnowledgeGraphSync.program()``."""

    def commit(self, *, strict: bool = True) -> ProgramResult:  # type: ignore[override]
        return run_sync(super().commit(strict=strict))


class KnowledgeGraphSync:
    """Synchronous wrapper around KnowledgeGraph."""

    def __init__(self, kg: KnowledgeGraph) -> None:
        self._kg = kg

    @property
    def name(self) -> str:
        return self._kg.name

    @property
    def session(self) -> Any:
        return self._kg.session

    def define(self, *relations: type[Relation]) -> None:
        run_sync(self._kg.define(*relations))

    def relations(self) -> list[RelationInfo]:
        return run_sync(self._kg.relations())

    def describe(self, relation: type[Relation] | str) -> RelationDescription:
        return run_sync(self._kg.describe(relation))

    def drop_relation(self, relation: type[Relation] | str) -> None:
        run_sync(self._kg.drop_relation(relation))

    def insert(self, facts: Any, data: Any = None) -> InsertResult:
        return run_sync(self._kg.insert(facts, data=data))

    def delete(self, facts: Any, *, where: Callable[..., Any] | None = None) -> DeleteResult:
        return run_sync(self._kg.delete(facts, where=where))

    def retract(self, row_or_relation: Any, **key: Any) -> DeleteResult:
        return run_sync(self._kg.retract(row_or_relation, **key))

    def program(self) -> ProgramSync:
        return ProgramSync(self._kg._commit_program)

    def claim(self, row: R, **kwargs: Any) -> Claim[R]:
        return run_sync(self._kg.claim(row, **kwargs))

    def query(self, *select: Any, **kwargs: Any) -> ResultSet:
        return run_sync(self._kg.query(*select, **kwargs))

    def query_stream(
        self, *select: Any, batch_size: int = 1000, **kwargs: Any
    ) -> list[list]:
        """Synchronous version of query_stream. Returns all batches as a list."""
        async def _collect() -> list[list]:
            batches = []
            async for batch in self._kg.query_stream(
                *select, batch_size=batch_size, **kwargs
            ):
                batches.append(batch)
            return batches
        return run_sync(_collect())

    def vector_search(
        self,
        relation: type[Relation],
        query_vec: list[float],
        *,
        column: str | None = None,
        k: int | None = None,
        radius: float | None = None,
        metric: str = "cosine",
        extra_iql_clauses: list[str] | None = None,
    ) -> ResultSet:
        return run_sync(self._kg.vector_search(
            relation, query_vec,
            column=column, k=k, radius=radius,
            metric=metric, extra_iql_clauses=extra_iql_clauses,
        ))

    def define_rules(self, *targets: Any) -> None:
        run_sync(self._kg.define_rules(*targets))

    def list_rules(self) -> list[RuleInfo]:
        return run_sync(self._kg.list_rules())

    def rule_definition(self, name: str | type) -> list[str]:
        return run_sync(self._kg.rule_definition(name))

    def drop_rule(self, name: str | type) -> None:
        run_sync(self._kg.drop_rule(name))

    def drop_rule_clause(self, name: str | type, index: int) -> None:
        run_sync(self._kg.drop_rule_clause(name, index))

    def edit_rule_clause(self, name: str | type, index: int, clause: Any) -> None:
        run_sync(self._kg.edit_rule_clause(name, index, clause))

    def clear_rule(self, name: str | type) -> None:
        run_sync(self._kg.clear_rule(name))

    def drop_rules_by_prefix(self, prefix: str) -> None:
        run_sync(self._kg.drop_rules_by_prefix(prefix))

    def create_index(self, index: HnswIndex) -> None:
        run_sync(self._kg.create_index(index))

    def list_indexes(self) -> list[IndexInfo]:
        return run_sync(self._kg.list_indexes())

    def index_stats(self, name: str) -> IndexStats:
        return run_sync(self._kg.index_stats(name))

    def drop_index(self, name: str) -> None:
        run_sync(self._kg.drop_index(name))

    def rebuild_index(self, name: str) -> None:
        run_sync(self._kg.rebuild_index(name))

    def grant_access(self, username: str, role: str) -> None:
        run_sync(self._kg.grant_access(username, role))

    def revoke_access(self, username: str) -> None:
        run_sync(self._kg.revoke_access(username))

    def list_acl(self) -> list[AclEntry]:
        return run_sync(self._kg.list_acl())

    def debug(self, *select: Any, **kwargs: Any) -> DebugResult:
        return run_sync(self._kg.debug(*select, **kwargs))

    def why(self, *select: Any, full: bool = False, **kwargs: Any) -> WhyResult:
        return run_sync(self._kg.why(*select, full=full, **kwargs))

    def why_not(self, relation: type, **values: Any) -> WhyNotResult:
        return run_sync(self._kg.why_not(relation, **values))

    def compact(self) -> None:
        run_sync(self._kg.compact())

    def status(self) -> ServerStatus:
        return run_sync(self._kg.status())

    def load(self, path: str, *, mode: str | None = None) -> None:
        run_sync(self._kg.load(path, mode=mode))

    def clear_prefix(self, prefix: str) -> ClearResult:
        return run_sync(self._kg.clear_prefix(prefix))

    def execute(self, iql: str, *, timeout: float | None = None) -> ResultSet:
        return run_sync(self._kg.execute(iql, timeout=timeout))

    # ── Subscriptions ─────────────────────────────────────────────────

    def subscribe(self, *select: Any, **kwargs: Any) -> SubscriptionSync:
        """``KnowledgeGraph.subscribe`` as a blocking iterator of ``Change``
        events. The subscription runs on the background event loop, so events
        keep arriving (up to ``queue``) between reads; leaving the loop or
        ``close()`` ends it."""
        return SubscriptionSync(self._kg.subscribe(*select, **kwargs))

    def subscribe_group(
        self, members: Mapping[str, Any], **kwargs: Any
    ) -> GroupSubscriptionSync:
        """``KnowledgeGraph.subscribe_group`` as a blocking iterator of
        ``GroupChange`` events, run like ``subscribe``."""
        return GroupSubscriptionSync(self._kg.subscribe_group(members, **kwargs))

    def read(self, queries: Mapping[str, Any], *, timeout: float | None = None) -> ReadResult:
        return run_sync(self._kg.read(queries, timeout=timeout))

    def watch(self, *select: Any, **kwargs: Any) -> Iterator[Live[Any]]:
        """``KnowledgeGraph.watch`` as a blocking iterator of ``Live`` results."""
        levels = self._kg.watch(*select, **kwargs)
        try:
            while True:
                try:
                    yield run_sync(_next(levels))
                except StopAsyncIteration:
                    return
        finally:
            run_sync(levels.aclose())  # type: ignore[attr-defined]

    def on(self, *args: Any, **kwargs: Any) -> SubscriptionHandleSync:
        """``KnowledgeGraph.on``. The callback runs on the background event
        loop's thread, one change at a time; keep it short, or hand the change
        to your own thread."""

        async def start() -> SubscriptionHandle:
            return self._kg.on(*args, **kwargs)

        return SubscriptionHandleSync(run_sync(start()))


async def _next(iterator: AsyncIterator[T]) -> T:
    return await iterator.__anext__()


class _StandingSync(Generic[E]):
    """A subscription (or group) read by blocking."""

    def __init__(self, sub: _Standing[E]) -> None:
        self._standing = sub

    @property
    def id(self) -> str:
        return self._standing.id

    @property
    def stats(self) -> SubscriptionStats:
        return self._standing.stats

    def __iter__(self) -> Iterator[E]:
        # A generator, so that leaving the loop closes the subscription.
        try:
            while True:
                try:
                    change = self.next()
                except StopIteration:
                    return
                yield change
        finally:
            self.close()

    def next(self) -> E:
        """The next event, blocking until it arrives."""
        try:
            return run_sync(_next(self._standing))
        except StopAsyncIteration:
            raise StopIteration from None

    def close(self) -> None:
        run_sync(self._standing.close())

    def __exit__(self, *exc: Any) -> None:
        self.close()


class SubscriptionSync(_StandingSync[Change[Any]]):
    """A subscription read by blocking: ``for change in kg.subscribe(...)``."""

    def __init__(self, sub: Subscription[Any]) -> None:
        super().__init__(sub)
        self._sub = sub

    @property
    def query(self) -> str:
        return self._sub.query

    def __enter__(self) -> SubscriptionSync:
        return self


class GroupSubscriptionSync(_StandingSync[GroupChange]):
    """A subscription group read by blocking: ``for change in kg.subscribe_group(...)``."""

    def __init__(self, sub: GroupSubscription) -> None:
        super().__init__(sub)
        self._sub = sub

    @property
    def queries(self) -> dict[str, str]:
        return self._sub.queries

    def __enter__(self) -> GroupSubscriptionSync:
        return self


class SubscriptionHandleSync:
    """A callback subscription; see ``KnowledgeGraphSync.on``."""

    def __init__(self, handle: SubscriptionHandle) -> None:
        self._handle = handle

    @property
    def stats(self) -> SubscriptionStats:
        return self._handle.stats

    def close(self) -> None:
        run_sync(self._handle.close())


class InputLayerSync:
    """Synchronous wrapper around InputLayer."""

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
        self._client = InputLayer(
            url,
            username=username,
            password=password,
            api_key=api_key,
            auto_reconnect=auto_reconnect,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnect_attempts,
            initial_kg=initial_kg,
            last_seq=last_seq,
            epoch=epoch,
            default_timeout=default_timeout,
            keepalive=keepalive,
        )

    def connect(self) -> None:
        run_sync(self._client.connect())

    def close(self) -> None:
        run_sync(self._client.close())

    def __enter__(self) -> InputLayerSync:
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def connected(self) -> bool:
        return self._client.connected

    @property
    def session_id(self) -> str | None:
        return self._client.session_id

    @property
    def server_version(self) -> str | None:
        return self._client.server_version

    @property
    def role(self) -> str | None:
        return self._client.role

    def knowledge_graph(self, name: str, *, create: bool = True) -> KnowledgeGraphSync:
        kg = self._client.knowledge_graph(name, create=create)
        return KnowledgeGraphSync(kg)

    def list_knowledge_graphs(self) -> list[str]:
        return run_sync(self._client.list_knowledge_graphs())

    def drop_knowledge_graph(self, name: str) -> None:
        run_sync(self._client.drop_knowledge_graph(name))

    def create_user(self, username: str, password: str, role: str = "viewer") -> None:
        run_sync(self._client.create_user(username, password, role))

    def drop_user(self, username: str) -> None:
        run_sync(self._client.drop_user(username))

    def set_password(self, username: str, new_password: str) -> None:
        run_sync(self._client.set_password(username, new_password))

    def set_role(self, username: str, role: str) -> None:
        run_sync(self._client.set_role(username, role))

    def list_users(self) -> list[UserInfo]:
        return run_sync(self._client.list_users())

    def create_api_key(self, label: str, ttl: str | None = None) -> str:
        return run_sync(self._client.create_api_key(label, ttl))

    def list_api_keys(self) -> list[ApiKeyInfo]:
        return run_sync(self._client.list_api_keys())

    def expire_api_key(self, label: str, ttl: str) -> None:
        run_sync(self._client.expire_api_key(label, ttl))

    def revoke_api_key(self, label: str) -> None:
        run_sync(self._client.revoke_api_key(label))
