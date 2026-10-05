"""KnowledgeGraph - the primary workspace for data, queries, and rules."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from inputlayer import _meta
from inputlayer._ast import AggExpr, BoolExpr, Expr, OrderedColumn
from inputlayer._ast import Column as AstColumn
from inputlayer._literal import collect_params, ms_to_datetime
from inputlayer._literal import encode as encode_literal
from inputlayer._proxy import ColumnProxy, RelationProxy, RelationRef
from inputlayer.auth import AclEntry
from inputlayer.compiler import (
    QueryPlan,
    _is_datetime_type,
    compile_bulk_insert,
    compile_conditional_delete,
    compile_delete,
    compile_insert,
    compile_query_plan,
    compile_rule,
    compile_schema,
)
from inputlayer.exceptions import (
    CompileError,
    Conflict,
    InternalError,
    PreconditionFailed,
    QueryError,
    StatementFailedError,
    SubscriptionRejected,
)
from inputlayer.index import HnswIndex
from inputlayer.program import (
    GUARD_SCHEMAS,
    Claim,
    Program,
    ProgramResult,
    compile_claim,
    parse_write_message,
)
from inputlayer.relation import Relation
from inputlayer.result import ResultSet
from inputlayer.session import Session
from inputlayer.subscription import (
    DEFAULT_QUEUE,
    Change,
    Live,
    Subscription,
    SubscriptionHandle,
    iql_shape,
    plan_shape,
    run_callback,
    watch_changes,
)

R = TypeVar("R", bound=Relation)

if TYPE_CHECKING:
    from inputlayer._protocol import ResultResponse
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
    """Facts the engine newly stored; duplicates of stored facts do not count."""

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
    iql: str
    plan: str

    def __getattr__(self, name: str) -> Any:
        if name == "datalog":
            import warnings

            warnings.warn(
                "DebugResult.datalog is deprecated, use .iql instead",
                DeprecationWarning,
                stacklevel=2,
            )
            return self.iql
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")


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
        # Whether this handle declared the guard relations (il_txn,
        # il_txn_pending, il_assert).
        self._guard_relations_declared = False

    async def _execute(
        self,
        iql: str,
        *,
        timeout: float | None = None,
        params: dict[str, Any] | None = None,
    ) -> ResultResponse:
        """Execute a statement on this KG's own connection.

        The connection is bound to this KG when it opens (``?kg=``), so no
        statement ever switches it. Engine failures raise ``QueryError``
        naming *iql*. *params* are the values of its ``$name`` references.
        """
        return await _naming_query(
            iql, self._conn.execute(iql, timeout=timeout, params=params)
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def session(self) -> Session:
        return self._session

    # ── Schema ────────────────────────────────────────────────────────

    async def define(self, *relations: type[Relation]) -> None:
        """Deploy schema definitions, with the relations guarded programs use
        (``il_txn``, ``il_txn_pending``, ``il_assert``), in one program.
        Idempotent."""
        schemas = [compile_schema(rel) for rel in relations]
        await self._execute("\n".join([*schemas, *GUARD_SCHEMAS]))
        self._guard_relations_declared = True

    async def _ensure_guard_relations(self) -> None:
        """Declare the guard relations once per handle, for a graph defined elsewhere."""
        if not self._guard_relations_declared:
            await self._execute("\n".join(GUARD_SCHEMAS))
            self._guard_relations_declared = True

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
        # Values travel as parameters: the engine binds them without parsing.
        with collect_params() as params:
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
                    except Exception as err:
                        raise TypeError(
                            f"Unsupported data type: {type(data).__name__}"
                        ) from err
                if len(instances) == 1:
                    iql = compile_insert(instances[0])
                else:
                    iql = compile_bulk_insert(rel_cls, instances)
            elif isinstance(facts, list):
                if not facts:
                    return InsertResult(count=0)
                iql = compile_bulk_insert(type(facts[0]), facts)
            elif isinstance(facts, Relation):
                iql = compile_insert(facts)
            else:
                raise TypeError(f"Unsupported facts type: {type(facts).__name__}")

        result = await self._execute(iql, params=params)
        return InsertResult(count=_inserted_count(result))

    # ── Delete ────────────────────────────────────────────────────────

    async def delete(
        self,
        facts: Relation | list[Relation] | type[Relation],
        *,
        where: Callable | None = None,
    ) -> DeleteResult:
        """Delete facts from the knowledge graph."""
        with collect_params() as params:
            if isinstance(facts, type) and issubclass(facts, Relation) and where is not None:
                # Conditional delete
                rel_cls = facts
                proxy = RelationProxy(
                    Relation._resolve_name(rel_cls), columns=tuple(Relation._get_columns(rel_cls))
                )
                condition = where(proxy)
                iql = compile_conditional_delete(rel_cls, condition)
            elif isinstance(facts, list):
                if not facts:
                    return DeleteResult(count=0)
                iql = "\n".join(compile_delete(fact) for fact in facts)
            elif isinstance(facts, Relation):
                iql = compile_delete(facts)
            else:
                raise TypeError(f"Unsupported facts type: {type(facts).__name__}")

        result = await self._execute(iql, params=params)
        if isinstance(facts, list):
            return DeleteResult(count=len(facts))
        return DeleteResult(count=len(result.rows) if result.rows else 0)

    async def retract(
        self, row_or_relation: Relation | type[Relation], **key: Any
    ) -> DeleteResult:
        """Retract a row, or every row matching the given columns, as one program:
        ``retract(shipment_row)``, ``retract(Eta, shipment="S-77")``."""
        result = await self.program().retract(row_or_relation, **key).commit()
        return DeleteResult(count=result.deleted)

    # ── Programs and claims ───────────────────────────────────────────

    def program(self) -> Program:
        """Start a program: statements committed as one request and one
        transaction. ``.when()`` makes the whole program conditional::

            await (
                kg.program()
                .insert(AttemptDone(attempt="att-9f3", status="ok"))
                .when(Attempt.any(attempt="att-9f3"), ~AttemptDone.any(attempt="att-9f3"))
                .commit()
            )
        """
        return Program(self._commit_program)

    async def _commit_program(self, program: Program, strict: bool) -> ProgramResult:
        if program.guarded:
            await self._ensure_guard_relations()
        with collect_params() as params:
            compiled = program.compile(strict)
        iql = compiled.iql
        try:
            result = await self._execute(iql, params=params)
        except StatementFailedError as err:
            if compiled.assert_index is not None and err.errors[0].index == compiled.assert_index:
                raise PreconditionFailed(iql, err.result) from err
            raise _as_conflict(err, iql) from err
        except QueryError as err:
            raise _as_conflict(err, iql) from err

        def message(i: int) -> str:
            row = result.rows[i] if i < len(result.rows) else None
            return str(row[0]) if row else ""

        inserted = deleted = 0
        for i in compiled.write_indexes:
            counts = parse_write_message(message(i))
            inserted += counts.inserted
            deleted += counts.deleted
        applied = (
            compiled.token_index is None
            or parse_write_message(message(compiled.token_index)).inserted == 1
        )
        if strict and not applied:
            raise PreconditionFailed(iql, result)
        return ProgramResult(
            applied=applied, inserted=inserted, deleted=deleted, iql=iql, params=params
        )

    async def claim(
        self,
        row: R,
        *,
        when: BoolExpr | list[BoolExpr] | None = None,
        unless: BoolExpr | None = None,
        key: list[str | ColumnProxy] | None = None,
    ) -> Claim[R]:
        """Insert *row* only if the *when* conditions hold and no *unless* row
        exists, deciding at commit, and say who holds the key afterwards. One
        request: of many concurrent claims on one key, exactly one wins::

            c = await kg.claim(
                Attempt(order="ORD-1", tool="carrier_check", attempt=attempt_id),
                when=[CheckNeeded.any(order="ORD-1")],
                unless=Attempt.any(order="ORD-1", tool="carrier_check"),
            )
            if c.won: ...

        *unless* defaults to a row with the *key* columns of *row*; *key*
        defaults to the columns *unless* binds, else every column. ``holder``
        is *row* when won, the row already there when lost, and None when the
        *when* guard did not hold.
        """
        with collect_params() as params:
            iql = compile_claim(row, when=when, unless=unless, key=key).iql
        try:
            result = await self._execute(iql, params=params)
        except QueryError as err:
            raise _as_conflict(err, iql) from err
        rel = type(row)
        columns = Relation._get_columns(rel)
        ours = [encode_literal(getattr(row, c)) for c in columns]
        for r in result.rows:
            if len(r) != len(columns):
                raise InternalError(
                    f"Unexpected claim reply: {r!r} for columns {', '.join(columns)}"
                )
        if any([encode_literal(v) for v in r] == ours for r in result.rows):
            return Claim(won=True, holder=row)
        if not result.rows:
            return Claim(won=False, holder=None)
        types_ = Relation._get_column_types(rel)
        first = result.rows[0]
        values = {
            c: ms_to_datetime(v)
            if _is_datetime_type(types_[c]) and isinstance(v, int) and not isinstance(v, bool)
            else v
            for c, v in zip(columns, first, strict=True)
        }
        return Claim(won=False, holder=rel(**values))

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
        """Query the knowledge graph.

        One call sends one program. The result has the selected columns in
        select order, labelled by column name (computed columns by their
        keyword, aggregates as ``<func>_<column>``); a projection is a set.
        Ordering and pagination run in the engine.
        """
        plan, relation_cls = self._plan(
            *select, join=join, on=on, where=where, order_by=order_by,
            limit=limit, offset=offset, **computed,
        )
        result = await self._execute(plan.program)
        rows = plan.shape(result.rows)
        reshaped = len(rows) != len(result.rows)
        rs = ResultSet(
            columns=plan.labels,
            rows=rows,
            row_count=len(rows),
            total_count=(
                len(rows)
                if plan.dedupe and plan.limit is None and plan.offset is None
                else result.total_count
            ),
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
            row_provenance=None if reshaped else result.row_provenance,
            timing_breakdown=result.timing_breakdown,
            _relation_cls=relation_cls,
        )
        if result.metadata:
            rs.has_ephemeral = result.metadata.get("has_ephemeral", False)
            rs.ephemeral_sources = result.metadata.get("ephemeral_sources", [])
            rs.warnings = result.metadata.get("warnings", [])
        return rs

    def _plan(
        self,
        *select: Any,
        join: list[type[Relation] | RelationRef] | None = None,
        on: Callable | None = None,
        where: Callable | None = None,
        order_by: ColumnProxy | OrderedColumn | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **computed: Any,
    ) -> tuple[QueryPlan, type | None]:
        """Compile the arguments of ``query``, ``debug`` and ``why`` alike.

        Returns the plan and, when the query selects one whole relation and
        nothing else, that relation's class for typed rows.
        """
        relations: list[type[Relation] | RelationRef] = list(join or [])

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

        def _add_agg_relations(agg: AggExpr) -> None:
            # An aggregate's columns name their relation only; join it.
            for col in (agg.column, agg.order_column, *agg.passthrough):
                if col is not None:
                    _maybe_add_relation(_column_relation_class(col))

        ast_select: list[Any] = []
        for s in select:
            if isinstance(s, ColumnProxy):
                ast_select.append(s._to_ast())
                _maybe_add_relation(s.relation_cls)
            elif isinstance(s, type) and issubclass(s, Relation):
                ast_select.append(s)
                _maybe_add_relation(s)
            elif isinstance(s, AggExpr):
                ast_select.append(s)
                _add_agg_relations(s)
            else:
                ast_select.append(s)

        ast_computed: dict[str, Expr] = {}
        for k, v in computed.items():
            if isinstance(v, ColumnProxy):
                ast_computed[k] = v._to_ast()
                _maybe_add_relation(v.relation_cls)
            elif isinstance(v, AggExpr):
                ast_computed[k] = v
                _add_agg_relations(v)
            elif isinstance(v, OrderedColumn):
                raise CompileError(
                    f"{k}= is a sort direction, not a computed column",
                    hint=f"to sort, pass order_by= instead of {k}=",
                )
            elif isinstance(v, Expr):
                ast_computed[k] = v
            else:
                # A keyword names a computed column; a value is not one.
                is_column = any(
                    k in Relation._get_columns(r.relation_cls if isinstance(r, RelationRef) else r)
                    for r in relations
                )
                raise CompileError(
                    f"{k}={v!r} is not an expression: keyword arguments name computed columns",
                    hint=(
                        f"to filter on a value, pass where=lambda r: r.{k} == {v!r}"
                        if is_column
                        else "to filter, pass where=; to sort, pass order_by="
                    ),
                )

        proxies = [
            RelationProxy(
                r.relation_name if isinstance(r, RelationRef) else Relation._resolve_name(r),
                ref_alias=r.alias if isinstance(r, RelationRef) else None,
                columns=tuple(
                    Relation._get_columns(r.relation_cls if isinstance(r, RelationRef) else r)
                ),
            )
            for r in relations
        ]
        on_condition = on(*proxies) if on and relations else None
        where_condition = where(*proxies) if where and relations else None

        order_ast: Expr | None = None
        if isinstance(order_by, ColumnProxy):
            order_ast = order_by._to_ast()
        elif order_by is not None:
            order_ast = order_by

        plan = compile_query_plan(
            *ast_select,
            relations=relations,
            on_condition=on_condition,
            where_condition=where_condition,
            order_by=order_ast,
            limit=limit,
            offset=offset,
            computed=ast_computed or None,
        )
        whole = (
            ast_select[0]
            if len(ast_select) == 1 and not ast_computed and isinstance(ast_select[0], type)
            else None
        )
        return plan, whole

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

    # ── Subscriptions ─────────────────────────────────────────────────

    def subscribe(
        self,
        *select: type[Relation] | ColumnProxy | Expr,
        join: list[type[Relation] | RelationRef] | None = None,
        on: Callable[..., Any] | None = None,
        where: Callable[..., Any] | None = None,
        order_by: ColumnProxy | OrderedColumn | None = None,
        limit: int | None = None,
        offset: int | None = None,
        iql: str | None = None,
        queue: int = DEFAULT_QUEUE,
        timeout: float | None = None,
        **computed: Expr,
    ) -> Subscription[Any]:
        """Subscribe to a query (the arguments of ``query``) or to raw IQL
        (``iql="?..."``): an async iterator of ``Change`` events, a
        ``snapshot`` and then ``delta``s, with ``unverified`` and ``resync``
        around anything that broke the stream (see ``inputlayer.subscription``).
        It opens on the first event; leaving the loop or ``close()`` ends it.

        Rows are instances of the relation when one whole relation is selected,
        else ``Row`` records keyed by column. ``queue`` bounds the events held
        for a consumer that has not read them; one that falls further behind
        gets ``unverified`` (``slow_consumer``) and then a ``resync``.
        ``timeout`` (seconds) is the deadline of each ``.subscribe``, snapshot
        included. A result is a set, so ``order_by`` is ignored.

        Raises ``SubscriptionRejected`` for ``limit`` or ``offset``, an OR
        condition, an aggregate, a negated constant, or a session rule, which a
        standing query cannot track: declare a persistent rule instead::

            async for change in kg.subscribe(Late):
                for row in change.retracted:
                    cancel(row)
                for row in change.inserted:
                    start(row)
                if not change.verified:
                    pause()
        """
        if iql is not None:
            if select or join or on or where or computed:
                raise CompileError(
                    "subscribe takes a query or iql=, not both",
                    hint="put the whole query in iql=, or drop iql=",
                )
            shape = iql_shape(iql)
        else:
            if limit is not None or offset is not None:
                raise SubscriptionRejected(
                    "A subscription tracks the whole result: remove limit and offset "
                    "from the query.",
                    "limit_offset",
                )
            del order_by  # a result is a set: its order means nothing to deltas
            plan, relation_cls = self._plan(
                *select, join=join, on=on, where=where,
                order_by=None, limit=None, offset=None, **computed,
            )
            aggregate = any(isinstance(s, AggExpr) for s in (*select, *computed.values()))
            shape = plan_shape(plan, relation_cls, aggregate=aggregate)
        return Subscription(
            self._conn,
            shape,
            queue=queue,
            timeout=timeout,
            session_rules=self._session.list_rules,
        )

    def watch(self, *select: Any, **kwargs: Any) -> AsyncIterator[Live[Any]]:
        """The whole current result of a subscription (the arguments of
        ``subscribe``) each time it changes, with its revision.

        ``verified`` is false from a lost connection (or any other
        ``unverified`` event) until the fresh result arrives: act on nothing
        new meanwhile. Coalesced commits are seen as one change, so a row
        that appears and disappears between two evaluations is never seen;
        what must not be missed belongs in facts.
        """
        return watch_changes(self.subscribe(*select, **kwargs))

    def on(
        self,
        *args: Any,
        callback: Callable[[Change[Any]], Any] | None = None,
        on_error: Callable[[BaseException], Any] | None = None,
        **kwargs: Any,
    ) -> SubscriptionHandle:
        """Call a callback with every ``Change`` of a subscription, one at a
        time: ``kg.on(Late, callback)``, with the arguments of ``subscribe``
        before the callback (or the callback as ``callback=``). An async
        callback is awaited. Errors it raises, and the error that ends the
        subscription, go to *on_error* (default: logged). ``handle.close()``
        ends it. Must be called with an event loop running.
        """
        select = args
        if callback is None:
            if not args or isinstance(args[-1], type) or not callable(args[-1]):
                raise TypeError("on() needs a callback: kg.on(Relation, callback)")
            select, callback = args[:-1], args[-1]
        return run_callback(self.subscribe(*select, **kwargs), callback, on_error)

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
        extra_iql_clauses: list[str] | None = None,
    ) -> ResultSet:
        """Perform a vector similarity search.

        Composes a direct IQL query of the form
        ``?relation(...), Dist = metric(VecCol, [...]), <filters>``
        and applies k/radius filtering client-side.

        ``extra_iql_clauses`` appends raw IQL body clauses for metadata
        filtering. Clause strings are NOT escaped; callers must use
        ``iql_literal()`` for any user-supplied values. Example::

            from inputlayer.integrations.langchain.params import iql_literal
            await kg.vector_search(
                Doc, vec, k=10,
                extra_iql_clauses=[f"Source = {iql_literal(user_source)}"],
            )
        """
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

        if k is None and radius is None:
            raise ValueError("Must specify either k or radius")

        vec_str = encode_literal([float(v) for v in query_vec])
        _valid_metrics = {
            "cosine": "cosine",
            "euclidean": "euclidean",
            "manhattan": "manhattan",
            "dot_product": "dot",
            "dot": "dot",
        }
        fn_name = _valid_metrics.get(metric)
        if fn_name is None:
            raise ValueError(
                f"Unknown metric {metric!r}; "
                f"supported values: {sorted(_valid_metrics)}"
            )

        # IQL variables must be capitalized (lowercase atoms are constants).
        cap = {c: c[:1].upper() + c[1:] for c in cols}
        vec_var = cap[column]

        iql_parts = [
            f"?{rel_name}({', '.join(cap[c] for c in cols)})",
            f"Dist = {fn_name}({vec_var}, {vec_str})",
        ]

        # Radius filter is applied server-side in the query body.
        if radius is not None:
            iql_parts.append(f"Dist <= {encode_literal(radius)}")

        if extra_iql_clauses:
            iql_parts.extend(extra_iql_clauses)

        iql = ", ".join(iql_parts)
        result = await self._execute(iql)

        # Sort by distance ascending (closer = better) and apply k limit.
        rows = result.rows
        if "Dist" in result.columns:
            dist_idx = result.columns.index("Dist")
            rows = sorted(rows, key=lambda r: r[dist_idx])
        if k is not None:
            rows = rows[:k]

        return ResultSet(
            columns=result.columns,
            rows=rows,
            row_count=len(rows),
            total_count=result.total_count,
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
        )

    # ── Rules ─────────────────────────────────────────────────────────

    async def define_rules(self, *targets: type[Derived]) -> None:
        """Deploy persistent rule definitions in one program."""
        with collect_params() as params:
            clauses = [
                compile_rule(
                    Relation._resolve_name(target),
                    Relation._get_columns(target),
                    clause.select_map,
                    clause.relations,
                    clause.condition,
                    persistent=True,
                )
                for target in targets
                for clause in target.rules
            ]
        if clauses:
            await self._execute("\n".join(clauses), params=params)

    async def list_rules(self) -> list[RuleInfo]:
        """List all rules in this KG."""
        result = await self._execute(_meta.rule_list())
        return [RuleInfo(name, count) for name, count in _meta.rule_infos(result.rows)]

    async def rule_definition(self, name: str | type) -> list[str]:
        """Get the IQL clauses of a rule, in order."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        result = await self._execute(_meta.rule_def(name))
        return _meta.rule_clauses(result.rows)

    async def drop_rule(self, name: str | type) -> None:
        """Drop all clauses of a rule."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(_meta.rule_drop(name))

    async def drop_rule_clause(self, name: str | type, index: int) -> None:
        """Remove a specific clause from a rule (1-based index)."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(_meta.rule_remove(name, index))

    async def edit_rule_clause(self, name: str | type, index: int, clause: Any) -> None:
        """Replace a specific rule clause: remove and re-add in one program."""
        if isinstance(name, type):
            head_name = Relation._resolve_name(name)
            head_columns = Relation._get_columns(name)
        else:
            head_name = name
            head_columns = list(clause.select_map.keys())
        with collect_params() as params:
            iql = compile_rule(
                head_name,
                head_columns,
                clause.select_map,
                clause.relations,
                clause.condition,
                persistent=True,
            )
        await self._execute(f"{_meta.rule_remove(head_name, index)}\n{iql}", params=params)

    async def clear_rule(self, name: str | type) -> None:
        """Clear a rule's clauses."""
        if isinstance(name, type):
            name = Relation._resolve_name(name)
        await self._execute(_meta.rule_clear(name))

    async def drop_rules_by_prefix(self, prefix: str) -> None:
        """Drop all rules whose names start with prefix."""
        await self._execute(_meta.rule_drop_prefix(prefix))

    # ── Indexes ───────────────────────────────────────────────────────

    async def create_index(self, index: HnswIndex) -> None:
        """Create an HNSW vector index."""
        await self._execute(index.to_iql())

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
        """Show the query plan without executing.

        Takes the arguments of ``query``. ``.debug`` takes one statement:
        the query, an aggregate's rule, or the first branch of an OR split.
        """
        plan, _ = self._plan(*select, **kwargs)
        result = await self._execute(f".debug {plan.debug}")
        plan_text = "\n".join(row[0] for row in result.rows)
        return DebugResult(iql=plan.debug, plan=plan_text)

    async def why(self, *select: Any, full: bool = False, **kwargs: Any) -> WhyResult:
        """Show proof trees explaining why query results were derived.

        Takes the arguments of ``query`` and returns its rows (ordered and
        paginated the same way; an OR split explains its first branch),
        each with the proof tree of its derivation.
        """
        plan, _ = self._plan(*select, **kwargs)
        if plan.setup:
            # .why does not see a program's session facts.
            raise CompileError(
                "why() cannot explain a query whose negated atom is linked to the "
                "body by a constant only",
                hint="define the query as a rule with define_rules() and explain that rule",
            )
        result = await self._execute(_meta.why(plan.why, full))
        raw_graphs = getattr(result, "proof_trees", None) or []
        picked = plan.shape_why(result.rows)
        rows = [plan.project_why(result.rows[i]) for i in picked]
        graphs = [
            ProofTree.from_dict(raw_graphs[i]) if isinstance(raw_graphs[i], dict) else raw_graphs[i]
            for i in picked
            if i < len(raw_graphs)
        ]
        result_set = ResultSet(
            columns=plan.labels,
            rows=rows,
            row_count=len(rows),
            total_count=result.total_count,
            execution_time_ms=result.execution_time_ms,
        )
        return WhyResult(
            results=result_set,
            proof_trees=graphs,
            result_count=len(rows),
        )

    async def why_not(self, relation: type, **values: Any) -> WhyNotResult:
        """Explain why a specific fact was NOT derived.

        Returns a structured explanation with the specific blocker for each rule.
        """
        from inputlayer.relation import Relation

        rel_name = Relation._resolve_name(relation)
        cols = Relation._get_columns(relation)
        missing = [col for col in cols if values.get(col) is None]
        if missing:
            raise CompileError(
                f"why_not() needs a value for every column of {rel_name}",
                hint=f"pass {', '.join(missing)}",
            )
        vals_str = ", ".join(encode_literal(values[col]) for col in cols)
        result = await self._execute(_meta.why_not(f"{rel_name}({vals_str})"))
        text = "\n".join(str(row[0]) for row in result.rows)
        raw_graphs = getattr(result, "proof_trees", None) or []
        if raw_graphs and isinstance(raw_graphs[0], dict):
            explanation = ProofTree.from_dict(raw_graphs[0])
        elif raw_graphs:
            explanation = raw_graphs[0]
        else:
            explanation = None
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
        """Load an IQL file into this knowledge graph.

        The file is read locally and sent to the server as a single
        multi-statement program. The server parses every statement before
        running any, and commits the program's facts, schemas, rules and
        rule removals as one transaction: if any statement fails, none of
        them is applied and the error names the failed statement.

        The file must not contain commands that cannot join that
        transaction (``.kg``, ``.rel drop``, ``.clear``, ``.index``,
        ``.compact``, inspection commands and the like): the server rejects
        such a file before applying anything. Send those commands
        separately.

        (Sending ``.load`` over the wire does not work: the server treats
        it as a client-only REPL command and rejects it.)
        """
        if mode is not None:
            msg = "load(mode=...) is not supported; --replace/--merge are unimplemented server-side"
            raise NotImplementedError(msg)
        with open(path, encoding="utf-8") as f:
            program = f.read()
        await self._execute(program)

    async def clear_prefix(self, prefix: str) -> ClearResult:
        """Clear all relations matching a prefix."""
        result = await self._execute(f".clear prefix {prefix}")
        return ClearResult(
            relations_cleared=len(result.rows),
            facts_cleared=sum(int(row[1]) for row in result.rows if len(row) > 1),
            details=[(row[0], int(row[1])) for row in result.rows if len(row) > 1],
        )

    async def execute(
        self,
        iql: str,
        *,
        timeout: float | None = None,
        params: dict[str, Any] | None = None,
    ) -> ResultSet:
        """Execute raw IQL.

        ``timeout`` (seconds) is the request's deadline, default the client's
        ``default_timeout``; past it the engine stops the program before it
        commits and ``DeadlineExceeded`` is raised.

        ``params`` binds the program's ``$name`` references to values sent
        beside its text, never parsed as IQL: pass every value you did not
        write yourself this way::

            await kg.execute("+eta($shipment, $due)", params={"shipment": s, "due": d})

        A value is typed by its JSON form; ``{"float": 2}`` and
        ``{"int": "9007199254740993"}`` name the type explicitly.
        """
        result = await self._execute(iql, timeout=timeout, params=params)
        return ResultSet(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            total_count=result.total_count,
            truncated=result.truncated,
            execution_time_ms=result.execution_time_ms,
            timing_breakdown=result.timing_breakdown,
        )


def _inserted_count(result: ResultResponse) -> int:
    """Facts stored, summed over the engine's per-statement insert replies."""
    return sum(parse_write_message(str(row[0]) if row else "").inserted for row in result.rows)


def _as_conflict(err: QueryError, iql: str) -> QueryError:
    """A ``conflict`` failure of a conditional write as ``Conflict``; anything else unchanged."""
    if err.code == "conflict":
        return Conflict(err.message, iql=iql)
    return err


async def _naming_query(query: str, pending: Awaitable[ResultResponse]) -> ResultResponse:
    """Await *pending*, naming *query* in the engine error it raises."""
    try:
        return await pending
    except QueryError as err:
        err.query = query
        raise
