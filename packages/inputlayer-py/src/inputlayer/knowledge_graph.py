"""KnowledgeGraph - the primary workspace for data, queries, and rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable

from inputlayer._ast import BoolExpr, Expr, OrderedColumn
from inputlayer._proxy import ColumnProxy, RelationProxy, RelationRef
from inputlayer.auth import AclEntry
from inputlayer.compiler import (
    compile_bulk_insert,
    compile_conditional_delete,
    compile_delete,
    compile_insert,
    compile_query,
    compile_rule,
    compile_schema,
)
from inputlayer.index import HnswIndex
from inputlayer.relation import Relation
from inputlayer.result import ResultSet
from inputlayer.session import Session

if TYPE_CHECKING:
    from inputlayer.connection import Connection
    from inputlayer.derived import Derived


# ── Data classes ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class RelationInfo:
    name: str
    row_count: int


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str


@dataclass(frozen=True)
class RelationDescription:
    name: str
    columns: list[ColumnInfo]
    row_count: int
    sample: list[dict]


@dataclass(frozen=True)
class RuleInfo:
    name: str
    clause_count: int


@dataclass(frozen=True)
class IndexInfo:
    name: str
    relation: str
    column: str
    metric: str
    row_count: int


@dataclass(frozen=True)
class IndexStats:
    name: str
    row_count: int
    layers: int
    memory_bytes: int


@dataclass(frozen=True)
class InsertResult:
    count: int


@dataclass(frozen=True)
class DeleteResult:
    count: int


@dataclass(frozen=True)
class ClearResult:
    relations_cleared: int
    facts_cleared: int
    details: list[tuple[str, int]]


@dataclass(frozen=True)
class ExplainResult:
    datalog: str
    plan: str


@dataclass(frozen=True)
class ServerStatus:
    version: str
    knowledge_graph: str


class KnowledgeGraph:
    """Primary workspace for interacting with a knowledge graph."""

    def __init__(self, name: str, connection: Connection) -> None:
        self._name = name
        self._conn = connection
        self._session = Session(connection)

    @property
    def name(self) -> str:
        return self._name

    @property
    def session(self) -> Session:
        return self._session

    # ── Schema ────────────────────────────────────────────────────────

    async def define(self, *relations: type[Relation]) -> None:
        """Deploy schema definitions. Idempotent."""
        for rel in relations:
            datalog = compile_schema(rel)
            await self._conn.execute(datalog)

    async def relations(self) -> list[RelationInfo]:
        """List all relations in this KG."""
        result = await self._conn.execute(".rel")
        return [
            RelationInfo(name=row[0], row_count=int(row[1]) if len(row) > 1 else 0)
            for row in result.rows
        ]

    async def describe(self, relation: type[Relation] | str) -> RelationDescription:
        """Describe a relation's schema."""
        name = relation if isinstance(relation, str) else Relation._resolve_name(relation)
        result = await self._conn.execute(f".rel {name}")
        columns = [ColumnInfo(name=row[0], type=row[1]) for row in result.rows]
        return RelationDescription(name=name, columns=columns, row_count=0, sample=[])

    async def drop_relation(self, relation: type[Relation] | str) -> None:
        """Drop a relation and all its data."""
        name = relation if isinstance(relation, str) else Relation._resolve_name(relation)
        await self._conn.execute(f".rel drop {name}")

    # ── Insert ────────────────────────────────────────────────────────

    async def insert(
        self,
        facts: Relation | list[Relation] | type[Relation],
        data: dict | list[dict] | Any | None = None,
    ) -> InsertResult:
        """Insert facts into the knowledge graph."""
        if isinstance(facts, type) and issubclass(facts, Relation):
            # Bulk mode: relation class + data
            if data is None:
                raise ValueError("Must provide data when passing a Relation class")
            rel_cls = facts
            if isinstance(data, dict):
                instances = [rel_cls(**data)]
            elif isinstance(data, list):
                instances = [rel_cls(**d) for d in data]
            else:
                # Try pandas DataFrame
                try:
                    instances = [rel_cls(**row) for row in data.to_dict("records")]
                except Exception:
                    raise TypeError(f"Unsupported data type: {type(data).__name__}")
            if len(instances) == 1:
                datalog = compile_insert(instances[0])
            else:
                datalog = compile_bulk_insert(rel_cls, instances)
        elif isinstance(facts, list):
            if not facts:
                return InsertResult(count=0)
            datalog = compile_bulk_insert(type(facts[0]), facts)
        elif isinstance(facts, Relation):
            datalog = compile_insert(facts)
        else:
            raise TypeError(f"Unsupported facts type: {type(facts).__name__}")

        result = await self._conn.execute(datalog)
        return InsertResult(count=len(result.rows) if result.rows else 0)

    # ── Delete ────────────────────────────────────────────────────────

    async def delete(
        self,
        facts: Relation | list[Relation] | type[Relation],
        *,
        where: Callable | None = None,
    ) -> DeleteResult:
        """Delete facts from the knowledge graph."""
        if isinstance(facts, type) and issubclass(facts, Relation) and where is not None:
            # Conditional delete
            rel_cls = facts
            proxy = RelationProxy(Relation._resolve_name(rel_cls))
            condition = where(proxy)
            datalog = compile_conditional_delete(rel_cls, condition)
        elif isinstance(facts, list):
            for fact in facts:
                datalog = compile_delete(fact)
                await self._conn.execute(datalog)
            return DeleteResult(count=len(facts))
        elif isinstance(facts, Relation):
            datalog = compile_delete(facts)
        else:
            raise TypeError(f"Unsupported facts type: {type(facts).__name__}")

        result = await self._conn.execute(datalog)
        return DeleteResult(count=len(result.rows) if result.rows else 0)

    # ── Query ─────────────────────────────────────────────────────────

    async def query(
        self,
        *select: type[Relation] | ColumnProxy | Expr,
        join: list[type[Relation] | RelationRef] | None = None,
        on: Callable | None = None,
        where: Callable | None = None,
        order_by: ColumnProxy | OrderedColumn | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **computed: Expr,
    ) -> ResultSet:
        """Query the knowledge graph."""
        # Convert ColumnProxy to AST
        ast_select = []
        relations = join or []
        for s in select:
            if isinstance(s, ColumnProxy):
                ast_select.append(s._to_ast())
                # Auto-add relation to join list if not present
                if not any(
                    (isinstance(r, type) and Relation._resolve_name(r) == s.relation)
                    or (isinstance(r, RelationRef) and r.relation_name == s.relation)
                    for r in relations
                ):
                    # We can't auto-add without the class, but the relation name is enough
                    pass
            elif isinstance(s, type) and issubclass(s, Relation):
                ast_select.append(s)
                if not any(
                    (isinstance(r, type) and r is s)
                    or (isinstance(r, RelationRef) and r.relation_cls is s)
                    for r in relations
                ):
                    relations.append(s)
            else:
                ast_select.append(s)

        # Convert computed columns
        ast_computed = {}
        for k, v in computed.items():
            if isinstance(v, ColumnProxy):
                ast_computed[k] = v._to_ast()
            else:
                ast_computed[k] = v

        # Build on condition
        on_condition = None
        if on and relations:
            proxies = [
                RelationProxy(
                    r.relation_name if isinstance(r, RelationRef) else Relation._resolve_name(r),
                    ref_alias=r.alias if isinstance(r, RelationRef) else None,
                )
                for r in relations
            ]
            on_condition = on(*proxies)

        # Build where condition
        where_condition = None
        if where and relations:
            proxies = [
                RelationProxy(
                    r.relation_name if isinstance(r, RelationRef) else Relation._resolve_name(r),
                    ref_alias=r.alias if isinstance(r, RelationRef) else None,
                )
                for r in relations
            ]
            where_condition = where(*proxies)

        # Convert order_by
        order_ast = None
        if order_by is not None:
            if isinstance(order_by, ColumnProxy):
                order_ast = order_by.asc()
            elif isinstance(order_by, OrderedColumn):
                order_ast = order_by
            else:
                order_ast = order_by

        datalog = compile_query(
            *ast_select,
            relations=relations,
            on_condition=on_condition,
            where_condition=where_condition,
            order_by=order_ast,
            limit=limit,
            offset=offset,
            computed=ast_computed or None,
        )

        if isinstance(datalog, list):
            # OR split → execute each and union
            all_rows: list[list] = []
            columns: list[str] = []
            for q in datalog:
                result = await self._conn.execute(q)
                if not columns:
                    columns = result.columns
                all_rows.extend(result.rows)
            return ResultSet(columns=columns, rows=all_rows)
        else:
            result = await self._conn.execute(datalog)
            rs = ResultSet(
                columns=result.columns,
                rows=result.rows,
                row_count=result.row_count,
                total_count=result.total_count,
                truncated=result.truncated,
                execution_time_ms=result.execution_time_ms,
                row_provenance=result.row_provenance,
            )
            if result.metadata:
                rs.has_ephemeral = result.metadata.get("has_ephemeral", False)
                rs.ephemeral_sources = result.metadata.get("ephemeral_sources", [])
                rs.warnings = result.metadata.get("warnings", [])
            return rs

    async def query_stream(
        self,
        *select: type[Relation] | ColumnProxy,
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> AsyncIterator[list]:
        """Stream query results in batches."""
        result = await self.query(*select, **kwargs)
        for i in range(0, len(result.rows), batch_size):
            yield result.rows[i : i + batch_size]

    # ── Vector search ─────────────────────────────────────────────────

    async def vector_search(
        self,
        relation: type[Relation],
        query_vec: list[float],
        *,
        column: str | None = None,
        k: int | None = None,
        radius: float | None = None,
        metric: str = "cosine",
        where: Callable | None = None,
    ) -> ResultSet:
        """Perform a vector similarity search."""
        from inputlayer.functions import cosine, euclidean, manhattan, dot

        rel_name = Relation._resolve_name(relation)
        cols = Relation._get_columns(relation)

        # Find vector column if not specified
        if column is None:
            col_types = Relation._get_column_types(relation)
            for c, tp in col_types.items():
                from inputlayer.types import Vector, _VectorMeta
                if tp is Vector or isinstance(tp, _VectorMeta):
                    column = c
                    break
            if column is None:
                raise ValueError(f"No vector column found in {rel_name}")

        # Build query using top_k or within_radius
        vec_str = "[" + ", ".join(str(v) for v in query_vec) + "]"
        dist_fn = {"cosine": "cosine", "euclidean": "euclidean", "manhattan": "manhattan", "dot_product": "dot"}
        fn_name = dist_fn.get(metric, "cosine")

        if k is not None:
            # top_k query
            col_vars = ", ".join(f"X{i}" for i in range(len(cols)))
            vec_var = f"X{cols.index(column)}"
            dist_assign = f"Dist = {fn_name}({vec_var}, {vec_str})"
            query = f"?top_k<{k}, {col_vars}, Dist:asc> <- {rel_name}({col_vars}), {dist_assign}"
        elif radius is not None:
            col_vars = ", ".join(f"X{i}" for i in range(len(cols)))
            vec_var = f"X{cols.index(column)}"
            dist_assign = f"Dist = {fn_name}({vec_var}, {vec_str})"
            query = f"?within_radius<{radius}, {col_vars}, Dist:asc> <- {rel_name}({col_vars}), {dist_assign}"
        else:
            raise ValueError("Must specify either k or radius")

        result = await self._conn.execute(query)
        return ResultSet(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            total_count=result.total_count,
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
        )

    # ── Rules ─────────────────────────────────────────────────────────

    async def define_rules(self, *targets: type[Derived]) -> None:
        """Deploy persistent rule definitions."""
        from inputlayer.derived import Derived

        for target in targets:
            head_name = Relation._resolve_name(target)
            head_columns = Relation._get_columns(target)
            for clause in target.rules:
                datalog = compile_rule(
                    head_name,
                    head_columns,
                    clause.select_map,
                    clause.relations,
                    clause.condition,
                    persistent=True,
                )
                await self._conn.execute(datalog)

    async def list_rules(self) -> list[RuleInfo]:
        """List all rules in this KG."""
        result = await self._conn.execute(".rule list")
        rules = []
        for row in result.rows:
            rules.append(RuleInfo(name=row[0], clause_count=int(row[1]) if len(row) > 1 else 1))
        return rules

    async def rule_definition(self, name: str | type) -> list[str]:
        """Get the Datalog definition of a rule."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        result = await self._conn.execute(f".rule show {name}")
        return [row[0] for row in result.rows]

    async def drop_rule(self, name: str | type) -> None:
        """Drop all clauses of a rule."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._conn.execute(f".rule drop {name}")

    async def drop_rule_clause(self, name: str | type, index: int) -> None:
        """Remove a specific clause from a rule (1-based index)."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._conn.execute(f".rule remove {name} {index}")

    async def edit_rule_clause(self, name: str | type, index: int, clause: Any) -> None:
        """Replace a specific rule clause (remove + re-add)."""
        await self.drop_rule_clause(name, index)
        # Re-add: compile the new clause
        if isinstance(name, type):
            head_name = Relation._resolve_name(name)
            head_columns = Relation._get_columns(name)
        else:
            head_name = name
            head_columns = list(clause.select_map.keys())
        datalog = compile_rule(
            head_name,
            head_columns,
            clause.select_map,
            clause.relations,
            clause.condition,
            persistent=True,
        )
        await self._conn.execute(datalog)

    async def clear_rule(self, name: str | type) -> None:
        """Clear a rule's materialized data."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._conn.execute(f".rule clear {name}")

    async def drop_rules_by_prefix(self, prefix: str) -> None:
        """Drop all rules whose names start with prefix."""
        await self._conn.execute(f".rule drop prefix {prefix}")

    # ── Indexes ───────────────────────────────────────────────────────

    async def create_index(self, index: HnswIndex) -> None:
        """Create an HNSW vector index."""
        await self._conn.execute(index.to_datalog())

    async def list_indexes(self) -> list[IndexInfo]:
        """List all indexes."""
        result = await self._conn.execute(".index list")
        indexes = []
        for row in result.rows:
            indexes.append(IndexInfo(
                name=row[0],
                relation=row[1] if len(row) > 1 else "",
                column=row[2] if len(row) > 2 else "",
                metric=row[3] if len(row) > 3 else "",
                row_count=int(row[4]) if len(row) > 4 else 0,
            ))
        return indexes

    async def index_stats(self, name: str) -> IndexStats:
        """Get statistics for an index."""
        result = await self._conn.execute(f".index stats {name}")
        row = result.rows[0] if result.rows else [name, 0, 0, 0]
        return IndexStats(
            name=str(row[0]),
            row_count=int(row[1]) if len(row) > 1 else 0,
            layers=int(row[2]) if len(row) > 2 else 0,
            memory_bytes=int(row[3]) if len(row) > 3 else 0,
        )

    async def drop_index(self, name: str) -> None:
        """Drop an index."""
        await self._conn.execute(f".index drop {name}")

    async def rebuild_index(self, name: str) -> None:
        """Rebuild an index."""
        await self._conn.execute(f".index rebuild {name}")

    # ── ACL ───────────────────────────────────────────────────────────

    async def grant_access(self, username: str, role: str) -> None:
        """Grant per-KG access."""
        await self._conn.execute(f".kg acl grant {self._name} {username} {role}")

    async def revoke_access(self, username: str) -> None:
        """Revoke per-KG access."""
        await self._conn.execute(f".kg acl revoke {self._name} {username}")

    async def list_acl(self) -> list[AclEntry]:
        """List ACL entries."""
        result = await self._conn.execute(f".kg acl list {self._name}")
        return [
            AclEntry(username=row[0], role=row[1])
            for row in result.rows
            if len(row) >= 2
        ]

    # ── Meta ──────────────────────────────────────────────────────────

    async def explain(self, *select: Any, **kwargs: Any) -> ExplainResult:
        """Show the query plan without executing."""
        datalog = compile_query(*select, **kwargs)
        if isinstance(datalog, list):
            datalog = datalog[0]
        result = await self._conn.execute(f".explain {datalog}")
        plan_text = "\n".join(row[0] for row in result.rows)
        return ExplainResult(datalog=datalog, plan=plan_text)

    async def compact(self) -> None:
        """Trigger storage compaction."""
        await self._conn.execute(".compact")

    async def status(self) -> ServerStatus:
        """Get server status."""
        result = await self._conn.execute(".status")
        row = result.rows[0] if result.rows else ["unknown", "unknown"]
        return ServerStatus(
            version=str(row[0]) if len(row) > 0 else "unknown",
            knowledge_graph=str(row[1]) if len(row) > 1 else self._name,
        )

    async def load(self, path: str, *, mode: str | None = None) -> None:
        """Load data from a file."""
        cmd = f".load {path}"
        if mode:
            cmd += f" {mode}"
        await self._conn.execute(cmd)

    async def clear_prefix(self, prefix: str) -> ClearResult:
        """Clear all relations matching a prefix."""
        result = await self._conn.execute(f".clear prefix {prefix}")
        return ClearResult(
            relations_cleared=len(result.rows),
            facts_cleared=sum(int(row[1]) for row in result.rows if len(row) > 1),
            details=[(row[0], int(row[1])) for row in result.rows if len(row) > 1],
        )

    async def execute(self, datalog: str) -> ResultSet:
        """Execute raw Datalog."""
        result = await self._conn.execute(datalog)
        return ResultSet(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            total_count=result.total_count,
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
        )
