"""InputLayerSync / KnowledgeGraphSync - synchronous wrappers.

Uses a dedicated background event loop thread so these work safely from
any context: plain scripts, Jupyter notebooks, FastAPI, LangGraph, etc.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from inputlayer._sync import run_sync
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
    KnowledgeGraph,
    RelationDescription,
    RelationInfo,
    RuleInfo,
    ServerStatus,
    WhyNotResult,
    WhyResult,
)
from inputlayer.relation import Relation
from inputlayer.result import ResultSet


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

    def query(self, *select: Any, **kwargs: Any) -> ResultSet:
        return run_sync(self._kg.query(*select, **kwargs))

    def vector_search(
        self, relation: type[Relation], query_vec: list[float], **kwargs: Any
    ) -> ResultSet:
        return run_sync(self._kg.vector_search(relation, query_vec, **kwargs))

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

    def explain(self, *select: Any, **kwargs: Any) -> ExplainResult:
        return run_sync(self._kg.explain(*select, **kwargs))

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

    def execute(self, datalog: str) -> ResultSet:
        return run_sync(self._kg.execute(datalog))


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

    def create_api_key(self, label: str) -> str:
        return run_sync(self._client.create_api_key(label))

    def list_api_keys(self) -> list[ApiKeyInfo]:
        return run_sync(self._client.list_api_keys())

    def revoke_api_key(self, label: str) -> None:
        run_sync(self._client.revoke_api_key(label))
