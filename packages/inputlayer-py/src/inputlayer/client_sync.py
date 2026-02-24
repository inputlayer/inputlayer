"""InputLayerSync / KnowledgeGraphSync - synchronous wrappers."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Iterator

from inputlayer.auth import AclEntry, ApiKeyInfo, UserInfo
from inputlayer.client import InputLayer
from inputlayer.index import HnswIndex
from inputlayer.knowledge_graph import (
    ClearResult,
    DeleteResult,
    ExplainResult,
    IndexInfo,
    IndexStats,
    InsertResult,
    RelationDescription,
    RelationInfo,
    RuleInfo,
    ServerStatus,
)
from inputlayer.relation import Relation
from inputlayer.result import ResultSet


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    """Get or create an event loop for sync wrappers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


class KnowledgeGraphSync:
    """Synchronous wrapper around KnowledgeGraph."""

    def __init__(self, kg: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._kg = kg
        self._loop = loop

    @property
    def name(self) -> str:
        return self._kg.name

    @property
    def session(self) -> Any:
        return self._kg.session  # TODO: SessionSync wrapper

    def define(self, *relations: type[Relation]) -> None:
        self._loop.run_until_complete(self._kg.define(*relations))

    def relations(self) -> list[RelationInfo]:
        return self._loop.run_until_complete(self._kg.relations())

    def describe(self, relation: type[Relation] | str) -> RelationDescription:
        return self._loop.run_until_complete(self._kg.describe(relation))

    def drop_relation(self, relation: type[Relation] | str) -> None:
        self._loop.run_until_complete(self._kg.drop_relation(relation))

    def insert(self, facts: Any, data: Any = None) -> InsertResult:
        return self._loop.run_until_complete(self._kg.insert(facts, data=data))

    def delete(self, facts: Any, *, where: Callable | None = None) -> DeleteResult:
        return self._loop.run_until_complete(self._kg.delete(facts, where=where))

    def query(self, *select: Any, **kwargs: Any) -> ResultSet:
        return self._loop.run_until_complete(self._kg.query(*select, **kwargs))

    def vector_search(self, relation: type[Relation], query_vec: list[float], **kwargs: Any) -> ResultSet:
        return self._loop.run_until_complete(self._kg.vector_search(relation, query_vec, **kwargs))

    def define_rules(self, *targets: Any) -> None:
        self._loop.run_until_complete(self._kg.define_rules(*targets))

    def list_rules(self) -> list[RuleInfo]:
        return self._loop.run_until_complete(self._kg.list_rules())

    def rule_definition(self, name: str | type) -> list[str]:
        return self._loop.run_until_complete(self._kg.rule_definition(name))

    def drop_rule(self, name: str | type) -> None:
        self._loop.run_until_complete(self._kg.drop_rule(name))

    def drop_rule_clause(self, name: str | type, index: int) -> None:
        self._loop.run_until_complete(self._kg.drop_rule_clause(name, index))

    def clear_rule(self, name: str | type) -> None:
        self._loop.run_until_complete(self._kg.clear_rule(name))

    def drop_rules_by_prefix(self, prefix: str) -> None:
        self._loop.run_until_complete(self._kg.drop_rules_by_prefix(prefix))

    def create_index(self, index: HnswIndex) -> None:
        self._loop.run_until_complete(self._kg.create_index(index))

    def list_indexes(self) -> list[IndexInfo]:
        return self._loop.run_until_complete(self._kg.list_indexes())

    def index_stats(self, name: str) -> IndexStats:
        return self._loop.run_until_complete(self._kg.index_stats(name))

    def drop_index(self, name: str) -> None:
        self._loop.run_until_complete(self._kg.drop_index(name))

    def rebuild_index(self, name: str) -> None:
        self._loop.run_until_complete(self._kg.rebuild_index(name))

    def grant_access(self, username: str, role: str) -> None:
        self._loop.run_until_complete(self._kg.grant_access(username, role))

    def revoke_access(self, username: str) -> None:
        self._loop.run_until_complete(self._kg.revoke_access(username))

    def list_acl(self) -> list[AclEntry]:
        return self._loop.run_until_complete(self._kg.list_acl())

    def explain(self, *select: Any, **kwargs: Any) -> ExplainResult:
        return self._loop.run_until_complete(self._kg.explain(*select, **kwargs))

    def compact(self) -> None:
        self._loop.run_until_complete(self._kg.compact())

    def status(self) -> ServerStatus:
        return self._loop.run_until_complete(self._kg.status())

    def load(self, path: str, *, mode: str | None = None) -> None:
        self._loop.run_until_complete(self._kg.load(path, mode=mode))

    def clear_prefix(self, prefix: str) -> ClearResult:
        return self._loop.run_until_complete(self._kg.clear_prefix(prefix))

    def execute(self, datalog: str) -> ResultSet:
        return self._loop.run_until_complete(self._kg.execute(datalog))


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
    ) -> None:
        self._loop = _get_or_create_loop()
        self._client = InputLayer(
            url,
            username=username,
            password=password,
            api_key=api_key,
            auto_reconnect=auto_reconnect,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnect_attempts,
            initial_kg=initial_kg,
        )

    def connect(self) -> None:
        self._loop.run_until_complete(self._client.connect())

    def close(self) -> None:
        self._loop.run_until_complete(self._client.close())

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
        return KnowledgeGraphSync(kg, self._loop)

    def list_knowledge_graphs(self) -> list[str]:
        return self._loop.run_until_complete(self._client.list_knowledge_graphs())

    def drop_knowledge_graph(self, name: str) -> None:
        self._loop.run_until_complete(self._client.drop_knowledge_graph(name))

    def create_user(self, username: str, password: str, role: str = "viewer") -> None:
        self._loop.run_until_complete(self._client.create_user(username, password, role))

    def drop_user(self, username: str) -> None:
        self._loop.run_until_complete(self._client.drop_user(username))

    def set_password(self, username: str, new_password: str) -> None:
        self._loop.run_until_complete(self._client.set_password(username, new_password))

    def set_role(self, username: str, role: str) -> None:
        self._loop.run_until_complete(self._client.set_role(username, role))

    def list_users(self) -> list[UserInfo]:
        return self._loop.run_until_complete(self._client.list_users())

    def create_api_key(self, label: str) -> str:
        return self._loop.run_until_complete(self._client.create_api_key(label))

    def list_api_keys(self) -> list[ApiKeyInfo]:
        return self._loop.run_until_complete(self._client.list_api_keys())

    def revoke_api_key(self, label: str) -> None:
        self._loop.run_until_complete(self._client.revoke_api_key(label))
