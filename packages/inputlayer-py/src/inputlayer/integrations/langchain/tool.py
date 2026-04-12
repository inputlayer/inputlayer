"""LangChain tool wrappers around InputLayer.

Two flavors:

- ``tools_from_relations(kg, [Relation, ...])`` - **preferred**. Generates
  one ``StructuredTool`` per relation with a typed Pydantic args schema
  derived from the relation's scalar columns. The LLM never writes
  IQL; it picks structured arguments and the tool builds the query.

- ``InputLayerIQLTool`` - escape hatch. Lets an agent send a raw
  InputLayer Query Language string (or fill a single ``:input``
  placeholder). Use only when the agent's task genuinely requires
  arbitrary query logic.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, ConfigDict, Field, create_model

from inputlayer._sync import run_sync
from inputlayer.integrations.langchain.params import bind_params, iql_literal
from inputlayer.relation import Relation

logger = logging.getLogger(__name__)

# ── Read-only guard ─────────────────────────────────────────────────

# Patterns that indicate a write or DDL operation in IQL.
_WRITE_RE = re.compile(
    r"^\s*[+\-]"           # fact assertion (+) or retraction (-)
    r"|^\s*\.(drop|create|load|save)\b",  # DDL dot-commands
    re.IGNORECASE | re.MULTILINE,
)


def _check_read_only(query: str) -> None:
    """Raise ``ValueError`` if *query* looks like a write/DDL statement."""
    if _WRITE_RE.search(query):
        raise ValueError(
            "Query rejected by read_only guard - it appears to contain a "
            "write or DDL operation. Set read_only=False on the tool if "
            "mutations are intentional."
        )


# ── Scalar type detection ────────────────────────────────────────────

# Only scalar types can be used as filter arguments. Everything else
# (Vector, list, dict, custom classes) is excluded from the generated
# args schema with a warning printed at generation time.
_SCALAR_TYPES: tuple[type, ...] = (str, int, float, bool)


def _is_scalar(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, _SCALAR_TYPES)


# ── Raw IQL tool ─────────────────────────────────────────────────────


class InputLayerIQLTool(BaseTool):
    """Tool that lets a LangChain agent run an IQL query against a KG.

    Two modes:

    1. **Open mode** (default) - the agent provides a full IQL query.
       Only suitable when the agent has been prompted with the schema
       and you trust it to write valid queries.

    2. **Templated mode** - provide ``query_template`` with a ``:input``
       placeholder; the agent's input is safely bound (escaped, quoted)
       rather than spliced as raw text.

    Prefer ``tools_from_relations`` for normal agent use.
    """

    name: str = "inputlayer_iql"
    description: str = (
        "Run an InputLayer Query Language query against a knowledge graph. "
        "Input must be a valid IQL query string."
    )
    kg: Any
    query_template: str | None = Field(
        default=None,
        description="Optional IQL template with :input placeholder",
    )
    max_rows: int = Field(default=50, description="Maximum rows to return")
    read_only: bool = Field(
        default=True,
        description=(
            "When True (default), reject queries that attempt writes or DDL. "
            "Set to False only if the agent is trusted to mutate the KG."
        ),
    )

    model_config = {"arbitrary_types_allowed": True}  # noqa: RUF012

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if not self.read_only and self.query_template is None:
            logger.warning(
                "InputLayerIQLTool instantiated in open mode with "
                "read_only=False - the agent can execute arbitrary "
                "writes and DDL against the knowledge graph."
            )

    def _run(
        self,
        query: str,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        return run_sync(self._arun(query, run_manager=run_manager))

    async def _arun(
        self,
        query: str,
        run_manager: AsyncCallbackManagerForToolRun | CallbackManagerForToolRun | None = None,
    ) -> str:
        if self.query_template is not None:
            compiled = bind_params(self.query_template, {"input": query})
        else:
            compiled = query

        if self.read_only:
            _check_read_only(compiled)

        logger.debug("IQL tool query: %s", compiled)
        result = await self.kg.execute(compiled)
        return _format_result(result, self.max_rows)


# ── Structured tools from relations ──────────────────────────────────


def tools_from_relations(
    kg: Any,
    relations: list[type[Relation]],
    *,
    max_rows: int = 50,
    name_prefix: str = "search_",
) -> list[StructuredTool]:
    """Generate one StructuredTool per relation.

    For each scalar column the tool exposes:
        - ``<col>``       equality filter (or ``IN`` if a list is passed)
        - ``min_<col>``   range lower bound (numeric columns only)
        - ``max_<col>``   range upper bound (numeric columns only)

    Non-scalar columns (``Vector``, ``list``, ``dict``, custom classes)
    are excluded from filters automatically.

    The LLM never sees IQL; the runner emits an ``?relation(...), ...``
    query directly via ``kg.execute`` based on the structured arguments.

    Example::

        from inputlayer.integrations.langchain import tools_from_relations

        tools = tools_from_relations(kg, [Employee, Article])
        agent = create_tool_calling_agent(llm, tools, prompt)

    Returned tools emit JSON arrays of row dicts so tool-calling LLMs
    can parse them directly.
    """
    return [
        _relation_to_tool(kg, r, max_rows=max_rows, name_prefix=name_prefix)
        for r in relations
    ]


def _relation_to_tool(
    kg: Any,
    relation: type[Relation],
    *,
    max_rows: int,
    name_prefix: str,
) -> StructuredTool:
    rel_name = Relation._resolve_name(relation)
    # Strip leading underscores so user-private relation names like
    # ``_Doc`` become ``search_doc`` rather than ``search__doc``.
    display_name = rel_name.lstrip("_") or rel_name
    col_types = Relation._get_column_types(relation)

    scalar_cols: dict[str, type] = {}
    skipped: list[str] = []
    for col, tp in col_types.items():
        if _is_scalar(tp):
            scalar_cols[col] = tp
        else:
            skipped.append(col)

    fields: dict[str, Any] = {}
    for col, tp in scalar_cols.items():
        # Equality / IN: accept either a single value or a list of them.
        fields[col] = (
            tp | list[tp] | None,  # type: ignore[valid-type]
            Field(
                default=None,
                description=(
                    f"Filter rows where {col} equals this value, "
                    f"or where {col} is in this list."
                ),
            ),
        )
        if tp in (int, float):
            fields[f"min_{col}"] = (
                tp | None,
                Field(default=None, description=f"Filter rows where {col} >= this value"),
            )
            fields[f"max_{col}"] = (
                tp | None,
                Field(default=None, description=f"Filter rows where {col} <= this value"),
            )
    args_model: type[BaseModel] = create_model(  # type: ignore[call-overload]
        f"{relation.__name__}SearchArgs",
        __config__=ConfigDict(arbitrary_types_allowed=True),
        **fields,
    )

    column_list = ", ".join(f"{c}: {t.__name__}" for c, t in scalar_cols.items())
    skipped_note = (
        f" Non-scalar columns excluded from filters: {', '.join(skipped)}."
        if skipped
        else ""
    )
    description = (
        f"Search the {rel_name} relation. Filterable columns: {column_list}.{skipped_note} "
        f"Pass any subset of fields. For numeric columns, min_<field>/max_<field> "
        f"express ranges. Returns up to {max_rows} matching rows as a JSON array."
    )

    runner = _make_relation_runner(kg, relation, scalar_cols, max_rows)

    return StructuredTool.from_function(
        coroutine=runner,
        name=f"{name_prefix}{display_name}",
        description=description,
        args_schema=args_model,
    )


def _make_relation_runner(
    kg: Any,
    relation: type[Relation],
    scalar_cols: dict[str, type],
    max_rows: int,
) -> Callable[..., Any]:
    """Build the async function StructuredTool will call.

    The runner parses the supplied kwargs into clauses, compiles them
    to an IQL query of the form ``?relation(Var1, Var2, ...), Filter, ...``,
    and dispatches via ``kg.execute``.

    We use ``kg.execute`` rather than ``kg.query`` because the tool
    needs IN-list expansion (one query per value, union client-side)
    and direct JSON output, which are concerns specific to the tool
    layer rather than the core query API.

    The runner exposes ``parse_clauses``, ``build_iql``, and
    ``build_iql_queries`` so unit tests can introspect what would be
    sent without touching a server.
    """
    rel_name = Relation._resolve_name(relation)
    all_cols = Relation._get_columns(relation)
    cap = {c: c[:1].upper() + c[1:] for c in all_cols}
    var_list = ", ".join(cap[c] for c in all_cols)

    def parse_clauses(kwargs: dict[str, Any]) -> list[tuple[str, str, Any]]:
        clauses: list[tuple[str, str, Any]] = []
        for k, v in kwargs.items():
            if v is None:
                continue
            if k in scalar_cols:
                if isinstance(v, list):
                    clauses.append((k, "in", v))
                else:
                    clauses.append((k, "==", v))
            elif k.startswith("min_") and k[4:] in scalar_cols:
                clauses.append((k[4:], ">=", v))
            elif k.startswith("max_") and k[4:] in scalar_cols:
                clauses.append((k[4:], "<=", v))
        return clauses

    def _filter_clause(col: str, op: str, val: Any) -> str:
        op_iql = {"==": "=", ">=": ">=", "<=": "<="}[op]
        return f"{cap[col]} {op_iql} {iql_literal(val)}"

    def build_iql(clauses: list[tuple[str, str, Any]]) -> str:
        head = f"?{rel_name}({var_list})"
        if not clauses:
            return head
        return head + ", " + ", ".join(
            _filter_clause(c, op, v) for c, op, v in clauses
        )

    def build_iql_queries(clauses: list[tuple[str, str, Any]]) -> list[str]:
        # Split out IN-list clauses; the rest are scalar filters.
        in_clauses = [c for c in clauses if c[1] == "in"]
        other = [c for c in clauses if c[1] != "in"]
        if not in_clauses:
            return [build_iql(other)]
        # Cartesian product of IN values, one query per combination.
        import itertools

        combos = itertools.product(*[c[2] for c in in_clauses])
        return [
            build_iql(
                other
                + [
                    (c[0], "==", val)
                    for c, val in zip(in_clauses, combo, strict=True)
                ]
            )
            for combo in combos
        ]

    async def run(**kwargs: Any) -> str:
        clauses = parse_clauses(kwargs)
        queries = build_iql_queries(clauses)

        merged_columns: list[str] = []
        merged_rows: list[list[Any]] = []
        seen: set[tuple[Any, ...]] = set()

        for q in queries:
            logger.debug("Structured tool query: %s", q)
            result = await kg.execute(q)
            if result.columns == ["error"]:
                return json.dumps(
                    {"error": result.rows[0][0] if result.rows else "unknown error"}
                )
            if not merged_columns:
                merged_columns = result.columns
            for row in result.rows:
                key = tuple(_hashable(v) for v in row)
                if key in seen:
                    continue
                seen.add(key)
                merged_rows.append(row)
                if len(merged_rows) >= max_rows + 1:
                    break
            if len(merged_rows) >= max_rows + 1:
                break

        # Wrap merged rows in a synthetic ResultSet-like object.
        return _format_result(_FakeResult(merged_columns, merged_rows), max_rows)

    run.__name__ = f"search_{rel_name}"
    run.parse_clauses = parse_clauses  # type: ignore[attr-defined]
    run.build_iql = build_iql  # type: ignore[attr-defined]
    run.build_iql_queries = build_iql_queries  # type: ignore[attr-defined]
    return run


class _FakeResult:
    def __init__(self, columns: list[str], rows: list[list[Any]]) -> None:
        self.columns = columns
        self.rows = rows
        self.row_count = len(rows)


def _hashable(v: Any) -> Any:
    if isinstance(v, list):
        return tuple(_hashable(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _hashable(x)) for k, x in v.items()))
    return v


# ── Result formatting ────────────────────────────────────────────────


def _format_result(result: Any, max_rows: int) -> str:
    """Format a ResultSet as a JSON array of row dicts.

    Tool-calling LLMs handle JSON better than tab-separated text and
    JSON is unambiguous about column names and types.
    """
    if not result.rows:
        return "[]"

    rows = result.rows[:max_rows]
    payload = [
        {col: _jsonify(val) for col, val in zip(result.columns, row, strict=False)}
        for row in rows
    ]

    total = getattr(result, "row_count", len(result.rows)) or len(result.rows)
    if total > max_rows:
        return json.dumps(
            {
                "rows": payload,
                "truncated": True,
                "shown": len(rows),
                "total": total,
            }
        )
    return json.dumps(payload)


def _jsonify(value: Any) -> Any:
    """Coerce a value into something json.dumps can handle."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)
