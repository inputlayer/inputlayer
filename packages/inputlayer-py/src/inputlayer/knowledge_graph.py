"""KnowledgeGraph - the primary workspace for data, queries, and rules."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from inputlayer._ast import AggExpr, Column as AstColumn
from inputlayer._ast import Expr, OrderedColumn
from inputlayer._proxy import ColumnProxy, RelationProxy, RelationRef
from inputlayer.auth import AclEntry
from inputlayer.compiler import (
    AggCompiled,
    compile_bulk_insert,
    compile_conditional_delete,
    compile_delete,
    compile_insert,
    compile_query,
    compile_rule,
    compile_schema,
)
from inputlayer.exceptions import QueryError
from inputlayer.index import HnswIndex
from inputlayer.relation import Relation
from inputlayer.result import ResultSet
from inputlayer.session import Session

if TYPE_CHECKING:
    from inputlayer.connection import Connection
    from inputlayer.derived import Derived


# ── Helpers ───────────────────────────────────────────────────────────


def _column_relation_class(expr: Any) -> type | None:
    """Best-effort lookup of the originating Relation class for a column.

    Aggregations like ``count(Sale.region)`` end up holding an
    ``AstColumn`` (no class reference), so we walk the global subclass
    list of ``Relation`` and match by relation name. Returns ``None``
    when the column doesn't correspond to a known relation.
    """
    if not isinstance(expr, AstColumn):
        return None
    rel_name = expr.relation
    for sub in Relation.__subclasses__():
        if Relation._resolve_name(sub) == rel_name:
            return sub
    # Walk one level deeper for grandchildren of Relation.
    for sub in Relation.__subclasses__():
        for grand in sub.__subclasses__():
            if Relation._resolve_name(grand) == rel_name:
                return grand
    return None


def _resolve_sort_column(
    order_ast: Any, columns: list[str]
) -> tuple[str | None, bool]:
    """Resolve an ``order_by`` AST to a (result_column, descending) pair.

    Returns ``(None, False)`` if the column cannot be located in the
    result set, in which case the caller should leave the rows alone.
    """
    descending = False
    target: AstColumn | None = None
    if isinstance(order_ast, OrderedColumn):
        descending = order_ast.descending
        if isinstance(order_ast.column, AstColumn):
            target = order_ast.column
    elif isinstance(order_ast, AstColumn):
        target = order_ast
    if target is None:
        return None, descending
    # Engine returns either schema-column casing or the capitalized variable
    # form, depending on whether computed expressions are present. Try
    # both before giving up.
    candidates = [target.name, target.name[:1].upper() + target.name[1:]]
    for cand in candidates:
        if cand in columns:
            return cand, descending
    lower_lookup = {c.lower(): c for c in columns}
    if target.name.lower() in lower_lookup:
        return lower_lookup[target.name.lower()], descending
    return None, descending


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
class DebugResult:
    datalog: str
    plan: str


@dataclass(frozen=True)
class ServerStatus:
    version: str
    knowledge_graph: str


@dataclass(frozen=True)
class Conclusion:
    """The concluded predicate and its argument values."""

    pred: str
    args: list[Any]


@dataclass(frozen=True)
class ProofNode:
    """A single node in a proof tree (fact, rule, aggregate, etc.)."""

    kind: str
    conclusion: Conclusion
    children: list[str] = field(default_factory=list)
    source: str | None = None
    rule_id: str | None = None
    bindings: dict[str, Any] | None = None
    aggregate: dict[str, Any] | None = None
    negation: dict[str, Any] | None = None
    vector_search: dict[str, Any] | None = None
    truncated: dict[str, Any] | None = None
    why_not: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ProofNode:
        """Parse a node from the wire JSON format."""
        conc = d.get("conclusion", {})
        return cls(
            kind=d.get("kind", "unknown"),
            conclusion=Conclusion(pred=conc.get("pred", ""), args=conc.get("args", [])),
            children=d.get("children", []),
            source=d.get("source"),
            rule_id=d.get("rule_id"),
            bindings=d.get("bindings"),
            aggregate=d.get("aggregate"),
            negation=d.get("negation"),
            vector_search=d.get("vector_search"),
            truncated=d.get("truncated"),
            why_not=d.get("why_not"),
        )


@dataclass(frozen=True)
class ProofTree:
    """A proof tree explaining how/why a fact was derived (or not)."""

    version: int
    roots: list[str]
    nodes: dict[str, ProofNode]
    query: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ProofTree:
        """Parse a proof tree from the wire JSON format."""
        nodes = {k: ProofNode.from_dict(v) for k, v in d.get("nodes", {}).items()}
        return cls(
            version=d.get("version", 1),
            roots=d.get("roots", []),
            nodes=nodes,
            query=d.get("query"),
        )


@dataclass(frozen=True)
class WhyResult:
    """Result of a .why query with structured proof trees."""

    results: ResultSet
    proof_trees: list[ProofTree]
    result_count: int = 0


@dataclass(frozen=True)
class WhyNotResult:
    """Explanation of why a fact was NOT derived."""

    text: str
    explanation: ProofTree | None = None


class KnowledgeGraph:
    """Primary workspace for interacting with a knowledge graph."""

    def __init__(self, name: str, connection: Connection) -> None:
        self._name = name
        self._conn = connection
        self._session = Session(connection)

    async def _ensure_current(self) -> None:
        """Make sure the connection is bound to this KG before running ops.

        ``il.knowledge_graph(name)`` returns a handle without switching
        the server-side current KG, so every operation must verify the
        binding. This method is a no-op when the connection is already
        on the right KG.
        """
        if self._conn.current_kg != self._name:
            await self._conn.execute(f".kg use {self._name}")

    async def _execute(self, datalog: str) -> Any:
        """Execute a statement, switching to this KG first if needed."""
        await self._ensure_current()
        return await self._conn.execute(datalog)

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
            await self._execute(datalog)

    async def relations(self) -> list[RelationInfo]:
        """List all relations in this KG.

        The server's ``.rel`` command returns either a single info line
        when there are no relations, or a header row followed by one
        formatted line per relation::

            Relations:
              edge (arity: 2, columns: [src: int, dst: int], tuples: 12)

        We parse the formatted lines and skip everything else, so we
        return an empty list when no relations exist instead of mistakenly
        treating the header as a relation name.
        """
        import re

        result = await self._execute(".rel")
        out: list[RelationInfo] = []
        line_re = re.compile(r"^\s*([A-Za-z_][A-Za-z_0-9]*)\s*\(.*tuples:\s*(\d+)")
        for row in result.rows:
            if not row:
                continue
            text = str(row[0])
            m = line_re.match(text)
            if m:
                out.append(
                    RelationInfo(name=m.group(1), row_count=int(m.group(2)))
                )
        return out

    async def describe(self, relation: type[Relation] | str) -> RelationDescription:
        """Describe a relation's schema."""
        name = relation if isinstance(relation, str) else Relation._resolve_name(relation)
        result = await self._execute(f".rel {name}")
        columns = [ColumnInfo(name=row[0], type=row[1]) for row in result.rows]
        return RelationDescription(name=name, columns=columns, row_count=0, sample=[])

    async def drop_relation(self, relation: type[Relation] | str) -> None:
        """Drop a relation and all its data."""
        name = relation if isinstance(relation, str) else Relation._resolve_name(relation)
        await self._execute(f".rel drop {name}")

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

        result = await self._execute(datalog)
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
                await self._execute(datalog)
            return DeleteResult(count=len(facts))
        elif isinstance(facts, Relation):
            datalog = compile_delete(facts)
        else:
            raise TypeError(f"Unsupported facts type: {type(facts).__name__}")

        result = await self._execute(datalog)
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

        def _maybe_add_relation(cls: type | None) -> None:
            if cls is None:
                return
            if any(
                (isinstance(r, type) and r is cls)
                or (isinstance(r, RelationRef) and r.relation_cls is cls)
                for r in relations
            ):
                return
            relations.append(cls)

        for s in select:
            if isinstance(s, ColumnProxy):
                ast_select.append(s._to_ast())
                _maybe_add_relation(s.relation_cls)
            elif isinstance(s, type) and issubclass(s, Relation):
                ast_select.append(s)
                _maybe_add_relation(s)
            elif isinstance(s, AggExpr):
                ast_select.append(s)
                # Aggregates wrap a column - if the column came from a
                # Relation class proxy, auto-add that relation to the
                # join list so the body atom is included.
                if s.column is not None:
                    _agg_cls = _column_relation_class(s.column)
                    _maybe_add_relation(_agg_cls)
            else:
                ast_select.append(s)

        # Convert computed columns and harvest any embedded Relation refs.
        ast_computed = {}
        for k, v in computed.items():
            if isinstance(v, ColumnProxy):
                ast_computed[k] = v._to_ast()
                _maybe_add_relation(v.relation_cls)
            elif isinstance(v, AggExpr):
                ast_computed[k] = v
                if v.column is not None:
                    _maybe_add_relation(_column_relation_class(v.column))
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

        if isinstance(datalog, AggCompiled):
            # Aggregate query: register a temporary session rule, query
            # it, and best-effort drop it. The rule lives in the session
            # so a leak only persists for the lifetime of the connection.
            setup_result = await self._execute(datalog.setup)
            if setup_result.columns == ["error"]:
                raise QueryError(
                    setup_result.rows[0][0] if setup_result.rows else "unknown error",
                    query=datalog.setup,
                )
            try:
                result = await self._execute(datalog.query)
                if result.columns == ["error"]:
                    raise QueryError(
                        result.rows[0][0] if result.rows else "unknown error",
                        query=datalog.query,
                    )
                rs = ResultSet(
                    columns=result.columns,
                    rows=result.rows,
                    row_count=result.row_count,
                    total_count=result.total_count,
                    truncated=result.truncated,
                    execution_time_ms=result.execution_time_ms,
                )
            finally:
                import contextlib

                with contextlib.suppress(Exception):
                    await self._execute(f".rule drop {datalog.rule_name}")
        elif isinstance(datalog, list):
            # OR split → execute each and union
            all_rows: list[list] = []
            columns: list[str] = []
            for q in datalog:
                result = await self._execute(q)
                if result.columns == ["error"]:
                    raise QueryError(
                        result.rows[0][0] if result.rows else "unknown error",
                        query=q,
                    )
                if not columns:
                    columns = result.columns
                all_rows.extend(result.rows)
            rs = ResultSet(columns=columns, rows=all_rows)
        else:
            result = await self._execute(datalog)
            if result.columns == ["error"]:
                raise QueryError(
                    result.rows[0][0] if result.rows else "unknown error",
                    query=datalog,
                )
            rs = ResultSet(
                columns=result.columns,
                rows=result.rows,
                row_count=result.row_count,
                total_count=result.total_count,
                truncated=result.truncated,
                execution_time_ms=result.execution_time_ms,
                row_provenance=result.row_provenance,
                timing_breakdown=result.timing_breakdown,
            )
            if result.metadata:
                rs.has_ephemeral = result.metadata.get("has_ephemeral", False)
                rs.ephemeral_sources = result.metadata.get("ephemeral_sources", [])
                rs.warnings = result.metadata.get("warnings", [])

        # Apply client-side order_by + offset slicing. The compiler does
        # not include order_by in the query body because IQL only allows
        # ordering inside aggregate heads.
        if order_ast is not None and rs.rows:
            sort_col, descending = _resolve_sort_column(order_ast, rs.columns)
            if sort_col is not None:
                idx = rs.columns.index(sort_col)
                rs.rows.sort(
                    key=lambda r, _i=idx: (r[_i] is None, r[_i]),
                    reverse=descending,
                )
        if offset is not None and offset > 0:
            rs.rows = rs.rows[offset:]
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

        # Build query. Top-k aggregates are only valid inside a rule
        # head, so we register a temporary session rule and query it.
        # The rule name is unique per call to avoid colliding with
        # session state across concurrent calls on the same KG.
        import secrets

        vec_str = "[" + ", ".join(repr(float(v)) for v in query_vec) + "]"
        dist_fn = {
            "cosine": "cosine",
            "euclidean": "euclidean",
            "manhattan": "manhattan",
            "dot_product": "dot",
            "dot": "dot",
        }
        fn_name = dist_fn.get(metric, "cosine")

        # Capitalized variable names match IQL grammar (lowercase atoms
        # parse as constants).
        col_vars = [c[:1].upper() + c[1:] for c in cols]
        col_vars_str = ", ".join(col_vars)
        vec_var = col_vars[cols.index(column)]
        dist_assign = f"Dist = {fn_name}({vec_var}, {vec_str})"

        if k is not None:
            rule_name = f"il_vec_search_{secrets.token_hex(4)}"
            await self._execute(
                f"{rule_name}(top_k<{k}, {col_vars_str}, Dist:asc>) "
                f"<- {rel_name}({col_vars_str}), {dist_assign}"
            )
            query = f"?{rule_name}({col_vars_str}, Dist)"
        elif radius is not None:
            rule_name = f"il_vec_search_{secrets.token_hex(4)}"
            await self._execute(
                f"{rule_name}({col_vars_str}, Dist) "
                f"<- {rel_name}({col_vars_str}), {dist_assign}, Dist <= {radius}"
            )
            query = f"?{rule_name}({col_vars_str}, Dist)"
        else:
            raise ValueError("Must specify either k or radius")

        result = await self._execute(query)
        # Best-effort cleanup of the session rule. Failures are non-fatal
        # because the rule is namespaced per call and dies with the session.
        import contextlib

        with contextlib.suppress(Exception):
            await self._execute(f".rule drop {rule_name}")

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
                await self._execute(datalog)

    async def list_rules(self) -> list[RuleInfo]:
        """List all rules in this KG."""
        result = await self._execute(".rule list")
        rules = []
        for row in result.rows:
            rules.append(RuleInfo(name=row[0], clause_count=int(row[1]) if len(row) > 1 else 1))
        return rules

    async def rule_definition(self, name: str | type) -> list[str]:
        """Get the Datalog definition of a rule."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        result = await self._execute(f".rule show {name}")
        return [row[0] for row in result.rows]

    async def drop_rule(self, name: str | type) -> None:
        """Drop all clauses of a rule."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(f".rule drop {name}")

    async def drop_rule_clause(self, name: str | type, index: int) -> None:
        """Remove a specific clause from a rule (1-based index)."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(f".rule remove {name} {index}")

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
        await self._execute(datalog)

    async def clear_rule(self, name: str | type) -> None:
        """Clear a rule's materialized data."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(f".rule clear {name}")

    async def drop_rules_by_prefix(self, prefix: str) -> None:
        """Drop all rules whose names start with prefix."""
        await self._execute(f".rule drop prefix {prefix}")

    # ── Indexes ───────────────────────────────────────────────────────

    async def create_index(self, index: HnswIndex) -> None:
        """Create an HNSW vector index."""
        await self._execute(index.to_datalog())

    async def list_indexes(self) -> list[IndexInfo]:
        """List all indexes."""
        result = await self._execute(".index list")
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
        result = await self._execute(f".index stats {name}")
        row = result.rows[0] if result.rows else [name, 0, 0, 0]
        return IndexStats(
            name=str(row[0]),
            row_count=int(row[1]) if len(row) > 1 else 0,
            layers=int(row[2]) if len(row) > 2 else 0,
            memory_bytes=int(row[3]) if len(row) > 3 else 0,
        )

    async def drop_index(self, name: str) -> None:
        """Drop an index."""
        await self._execute(f".index drop {name}")

    async def rebuild_index(self, name: str) -> None:
        """Rebuild an index."""
        await self._execute(f".index rebuild {name}")

    # ── ACL ───────────────────────────────────────────────────────────

    async def grant_access(self, username: str, role: str) -> None:
        """Grant per-KG access."""
        await self._execute(f".kg acl grant {self._name} {username} {role}")

    async def revoke_access(self, username: str) -> None:
        """Revoke per-KG access."""
        await self._execute(f".kg acl revoke {self._name} {username}")

    async def list_acl(self) -> list[AclEntry]:
        """List ACL entries."""
        result = await self._execute(f".kg acl list {self._name}")
        return [
            AclEntry(username=row[0], role=row[1])
            for row in result.rows
            if len(row) >= 2
        ]

    # ── Meta ──────────────────────────────────────────────────────────

    async def debug(self, *select: Any, **kwargs: Any) -> DebugResult:
        """Show the query plan without executing."""
        datalog = compile_query(*select, **kwargs)
        if isinstance(datalog, list):
            datalog = datalog[0]
        result = await self._execute(f".debug {datalog}")
        plan_text = "\n".join(row[0] for row in result.rows)
        return DebugResult(datalog=datalog, plan=plan_text)

    async def why(self, *select: Any, full: bool = False, **kwargs: Any) -> WhyResult:
        """Show proof trees explaining why query results were derived.

        Returns structured proof trees alongside the result data.
        Each result row has a corresponding proof tree explaining its derivation.
        """
        datalog = compile_query(*select, **kwargs)
        if isinstance(datalog, list):
            datalog = datalog[0]
        cmd = f".why full {datalog}" if full else f".why {datalog}"
        result = await self._execute(cmd)
        result_set = ResultSet(
            columns=result.columns,
            rows=result.rows,
            row_count=len(result.rows),
            total_count=result.total_count,
            execution_time_ms=result.execution_time_ms,
        )
        raw_graphs = getattr(result, "proof_trees", None) or []
        graphs = [ProofTree.from_dict(g) if isinstance(g, dict) else g for g in raw_graphs]
        return WhyResult(
            results=result_set,
            proof_trees=graphs,
            result_count=len(result.rows),
        )

    async def why_not(self, relation: type, **values: Any) -> WhyNotResult:
        """Explain why a specific fact was NOT derived.

        Returns a structured explanation with the specific blocker for each rule.
        """
        from inputlayer.relation import Relation

        rel_name = Relation._resolve_name(relation)
        cols = Relation._get_columns(relation)
        parts = []
        for col in cols:
            v = values.get(col)
            if v is None:
                parts.append("null")
            elif isinstance(v, str):
                escaped = v.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'"{escaped}"')
            else:
                parts.append(str(v))
        vals_str = ", ".join(parts)
        result = await self._execute(f".why_not {rel_name}({vals_str})")
        text = "\n".join(str(row[0]) for row in result.rows)
        raw_graphs = getattr(result, "proof_trees", None) or []
        explanation = ProofTree.from_dict(raw_graphs[0]) if raw_graphs and isinstance(raw_graphs[0], dict) else (raw_graphs[0] if raw_graphs else None)
        return WhyNotResult(text=text, explanation=explanation)

    async def compact(self) -> None:
        """Trigger storage compaction."""
        await self._execute(".compact")

    async def status(self) -> ServerStatus:
        """Get server status."""
        result = await self._execute(".status")
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
        await self._execute(cmd)

    async def clear_prefix(self, prefix: str) -> ClearResult:
        """Clear all relations matching a prefix."""
        result = await self._execute(f".clear prefix {prefix}")
        return ClearResult(
            relations_cleared=len(result.rows),
            facts_cleared=sum(int(row[1]) for row in result.rows if len(row) > 1),
            details=[(row[0], int(row[1])) for row in result.rows if len(row) > 1],
        )

    async def execute(self, datalog: str) -> ResultSet:
        """Execute raw Datalog."""
        result = await self._execute(datalog)
        return ResultSet(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            total_count=result.total_count,
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
            timing_breakdown=result.timing_breakdown,
        )
