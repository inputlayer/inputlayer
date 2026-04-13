"""Tests for inputlayer.integrations.langchain.

Two tiers:

* **Unit tests** use mocked KGs to verify the Python-side wiring:
  parameter binding, document mapping, tool args generation, AST
  composition. Fast, deterministic, run on every commit.

* **Live integration tests** (``TestLive*``) talk to a real
  inputlayer-server over WebSocket. They're skipped unless
  ``INPUTLAYER_INTEGRATION=1`` and exercise the parts of the
  integration that mocks cannot prove: actual IQL accepted by the
  parser, real cosine math, embed-then-search round trips. Run them
  with::

      cd packages/inputlayer-py
      INPUTLAYER_INTEGRATION=1 uv run pytest tests/test_langchain.py -k Live -v
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# langchain-core is an optional dependency. Skip the whole module if it
# is not installed so the rest of the suite remains green when CI runs
# without the langchain extra.
pytest.importorskip(
    "langchain_core",
    reason="langchain-core not installed - install the [langchain] extra",
)

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from inputlayer.integrations.langchain import (
    InputLayerIQLTool,
    InputLayerRetriever,
    InputLayerVectorStore,
    bind_params,
    iql_literal,
    tools_from_relations,
)
from inputlayer.relation import Relation
from inputlayer.result import ResultSet
from inputlayer.types import Vector

# ── Helpers ──────────────────────────────────────────────────────────


def _mock_kg(
    columns: list[str] | None = None,
    rows: list[list[Any]] | None = None,
) -> MagicMock:
    columns = columns or []
    rows = rows or []
    result = ResultSet(columns=columns, rows=rows)
    kg = MagicMock()
    kg.execute = AsyncMock(return_value=result)
    kg.vector_search = AsyncMock(return_value=result)
    kg.query = AsyncMock(return_value=result)
    kg.insert = AsyncMock()
    kg.define = AsyncMock()
    return kg


class _StubEmbeddings(Embeddings):
    """Deterministic embedder for unit tests.

    Maps text to a 3-dim vector based on token count in three buckets.
    Distinct enough that different inputs produce different vectors.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    def embed_query(self, text: str) -> list[float]:
        self.calls.append(text)
        words = text.lower().split()
        a = sum(1 for w in words if "a" in w)
        b = sum(1 for w in words if "e" in w)
        c = sum(1 for w in words if "i" in w)
        n = max(a + b + c, 1)
        return [a / n, b / n, c / n]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self.embed_query(t) for t in texts]

    async def aembed_query(self, text: str) -> list[float]:
        return self.embed_query(text)

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        return self.embed_documents(texts)


# ═══════════════════════════════════════════════════════════════════════
#  1. iql_literal / bind_params
# ═══════════════════════════════════════════════════════════════════════


class TestIQLLiteral:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("hello", '"hello"'),
            ("", '""'),
            ('say "hi"', '"say \\"hi\\""'),
            ("a\\b", '"a\\\\b"'),
            (42, "42"),
            (-7, "-7"),
            (3.14, "3.14"),
            (True, "true"),
            (False, "false"),
            ([1.0, 2.0, 3.0], "[1.0, 2.0, 3.0]"),
            ([1, 2, 3], "[1.0, 2.0, 3.0]"),
            ((0.5, 0.5), "[0.5, 0.5]"),
        ],
    )
    def test_supported(self, value: Any, expected: str) -> None:
        assert iql_literal(value) == expected

    def test_none_rejected(self) -> None:
        with pytest.raises(ValueError, match="None"):
            iql_literal(None)

    def test_dict_rejected(self) -> None:
        with pytest.raises(TypeError):
            iql_literal({"a": 1})

    def test_list_with_strings_rejected(self) -> None:
        with pytest.raises(ValueError, match="numbers"):
            iql_literal(["a", "b"])


class TestBindParams:
    def test_basic(self) -> None:
        assert bind_params("?d(:input, X)", {"input": "alice"}) == '?d("alice", X)'

    def test_multiple(self) -> None:
        out = bind_params(
            "?d(T), s(:q, T), score(T) > :min", {"q": "ml", "min": 0.5}
        )
        assert out == '?d(T), s("ml", T), score(T) > 0.5'

    def test_quote_injection_escaped(self) -> None:
        evil = 'alice") | admin("'
        out = bind_params("?u(:input, X)", {"input": evil})
        assert out == '?u("alice\\") | admin(\\"", X)'

    def test_backslash_then_quote_escaped(self) -> None:
        evil = 'a\\"b'
        out = bind_params("?u(:input)", {"input": evil})
        assert out == '?u("a\\\\\\"b")'

    def test_placeholder_inside_string_literal_untouched(self) -> None:
        out = bind_params(
            '?d, s("literal :input here", :input)', {"input": "x"}
        )
        assert out == '?d, s("literal :input here", "x")'

    def test_placeholder_after_escaped_quote_in_literal(self) -> None:
        out = bind_params('?d, s("a\\"b :nope c"), X = :real', {"real": 1})
        assert out == '?d, s("a\\"b :nope c"), X = 1'

    def test_placeholder_inside_line_comment_untouched(self) -> None:
        # IQL comments are //; placeholders inside them must NOT substitute.
        out = bind_params(
            "// :nope\n?d(:input)", {"input": "x"}
        )
        assert out == '// :nope\n?d("x")'

    def test_placeholder_at_end_of_line_after_comment(self) -> None:
        out = bind_params("?d(X) // :nope\n, X = :v", {"v": 7})
        assert out == "?d(X) // :nope\n, X = 7"

    def test_missing_param(self) -> None:
        with pytest.raises(KeyError, match=":missing"):
            bind_params("?d(:missing)", {"input": "x"})

    def test_no_params_with_placeholder_raises(self) -> None:
        with pytest.raises(KeyError):
            bind_params("?d(:input)", None)

    def test_empty_dict_with_placeholder_raises(self) -> None:
        with pytest.raises(KeyError):
            bind_params("?d(:input)", {})

    def test_no_params_no_placeholders_ok(self) -> None:
        assert bind_params("?d(X)", None) == "?d(X)"

    def test_vector(self) -> None:
        out = bind_params("?d(X), D=cosine(X, :q)", {"q": [0.1, 0.2]})
        assert out == "?d(X), D=cosine(X, [0.1, 0.2])"

    def test_unused_params_ignored(self) -> None:
        assert bind_params("?d(:a)", {"a": 1, "b": 2, "c": 3}) == "?d(1)"


# ═══════════════════════════════════════════════════════════════════════
#  2. InputLayerRetriever (unit)
# ═══════════════════════════════════════════════════════════════════════


class _DocRel(Relation):
    title: str
    embedding: Vector


class TestRetrieverValidation:
    def test_requires_query_or_relation(self) -> None:
        with pytest.raises(ValueError, match="Must provide either"):
            InputLayerRetriever(kg=_mock_kg())

    def test_query_and_relation_mutually_exclusive(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            InputLayerRetriever(
                kg=_mock_kg(),
                query="?foo(X)",
                relation=_DocRel,
                embeddings=_StubEmbeddings(),
            )

    def test_vector_mode_requires_embeddings(self) -> None:
        with pytest.raises(ValueError, match="embeddings"):
            InputLayerRetriever(kg=_mock_kg(), relation=_DocRel)


class TestRetrieverIQLMode:
    def test_basic_query_passes_through(self) -> None:
        kg = _mock_kg(columns=["title", "content"], rows=[["Doc1", "Hello"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?docs(title, content)",
            page_content_columns=["content"],
        )
        docs = r.invoke("test")
        assert len(docs) == 1
        kg.execute.assert_awaited_once_with("?docs(title, content)")

    def test_input_param_substitution(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[["x"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?article(I, T, content, C), user_interest(:input, C)",
            page_content_columns=["content"],
        )
        r.invoke("alice")
        kg.execute.assert_awaited_once_with(
            '?article(I, T, content, C), user_interest("alice", C)'
        )

    def test_input_substitution_escapes_quotes(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[])
        r = InputLayerRetriever(
            kg=kg,
            query="?docs(content), user_interest(:input, X)",
            page_content_columns=["content"],
        )
        r.invoke('evil") attack("')
        called = kg.execute.await_args[0][0]
        assert called == '?docs(content), user_interest("evil\\") attack(\\"", X)'

    def test_callable_params(self) -> None:
        kg = _mock_kg(columns=["c"], rows=[["x"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?docs(c), s(:q, c), score(c) > :min",
            params=lambda q: {"q": q, "min": 0.5},
            page_content_columns=["c"],
        )
        r.invoke("ml")
        kg.execute.assert_awaited_once_with(
            '?docs(c), s("ml", c), score(c) > 0.5'
        )

    def test_dict_params_merged(self) -> None:
        kg = _mock_kg(columns=["c"], rows=[["x"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?docs(c), s(:input), score > :min",
            params={"min": 0.7},
            page_content_columns=["c"],
        )
        r.invoke("ml")
        kg.execute.assert_awaited_once_with(
            '?docs(c), s("ml"), score > 0.7'
        )

    def test_custom_input_param_name(self) -> None:
        kg = _mock_kg(columns=["c"], rows=[["x"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?docs(c), s(:user_query)",
            input_param="user_query",
            page_content_columns=["c"],
        )
        r.invoke("hello")
        kg.execute.assert_awaited_once_with('?docs(c), s("hello")')

    def test_engine_error_raised(self) -> None:
        kg = _mock_kg(columns=["error"], rows=[["bad parse"]])
        r = InputLayerRetriever(
            kg=kg, query="?bad", page_content_columns=["x"]
        )
        with pytest.raises(RuntimeError, match="bad parse"):
            r.invoke("q")

    async def test_async(self) -> None:
        kg = _mock_kg(columns=["c"], rows=[["async result"]])
        r = InputLayerRetriever(
            kg=kg, query="?docs(c)", page_content_columns=["c"]
        )
        docs = await r.ainvoke("q")
        assert docs[0].page_content == "async result"


class TestRetrieverDocumentMapping:
    def test_metadata_columns(self) -> None:
        kg = _mock_kg(
            columns=["content", "source", "ts"],
            rows=[["text", "wiki", "2024"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?d(content, source, ts)",
            page_content_columns=["content"],
            metadata_columns=["source", "ts"],
        )
        d = r.invoke("q")[0]
        assert d.metadata == {"source": "wiki", "ts": "2024"}

    def test_auto_metadata_excludes_explicit_content(self) -> None:
        kg = _mock_kg(columns=["content", "source"], rows=[["text", "wiki"]])
        r = InputLayerRetriever(
            kg=kg, query="?d(content, source)", page_content_columns=["content"]
        )
        d = r.invoke("q")[0]
        assert d.page_content == "text"
        assert d.metadata == {"source": "wiki"}

    def test_auto_metadata_excludes_fallback_content(self) -> None:
        # Regression: previously the fallback content column was duplicated.
        kg = _mock_kg(columns=["body", "tag"], rows=[["text", "wiki"]])
        r = InputLayerRetriever(kg=kg, query="?d(body, tag)")
        d = r.invoke("q")[0]
        assert d.page_content == "text"
        assert "body" not in d.metadata
        assert d.metadata == {"tag": "wiki"}

    def test_score_column(self) -> None:
        kg = _mock_kg(columns=["content", "dist"], rows=[["t", 0.12]])
        r = InputLayerRetriever(
            kg=kg,
            query="?d(content, dist)",
            page_content_columns=["content"],
            score_column="dist",
        )
        d = r.invoke("q")[0]
        assert d.metadata["score"] == 0.12
        assert "dist" not in d.metadata

    def test_unknown_metadata_column_raises(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[["t"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?d(content)",
            page_content_columns=["content"],
            metadata_columns=["nonexistent"],
        )
        with pytest.raises(KeyError, match="nonexistent"):
            r.invoke("q")

    def test_unknown_explicit_content_column_raises(self) -> None:
        kg = _mock_kg(columns=["body"], rows=[["t"]])
        r = InputLayerRetriever(
            kg=kg, query="?d(body)", page_content_columns=["nope"]
        )
        with pytest.raises(KeyError, match="nope"):
            r.invoke("q")

    def test_case_insensitive_match_warns(self) -> None:
        kg = _mock_kg(columns=["Content"], rows=[["t"]])
        r = InputLayerRetriever(
            kg=kg, query="?d(Content)", page_content_columns=["content"]
        )
        with pytest.warns(UserWarning, match="case-insensitively"):
            docs = r.invoke("q")
        assert docs[0].page_content == "t"

    def test_multi_column_content_concatenation(self) -> None:
        kg = _mock_kg(
            columns=["title", "summary", "body"],
            rows=[["T1", "Sum", "Body"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?d(title, summary, body)",
            page_content_columns=["title", "body"],
        )
        d = r.invoke("q")[0]
        assert d.page_content == "T1\nBody"
        assert d.metadata == {"summary": "Sum"}

    def test_none_values_skipped_from_content_join(self) -> None:
        kg = _mock_kg(columns=["title", "body"], rows=[[None, "text"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?d(title, body)",
            page_content_columns=["title", "body"],
        )
        assert r.invoke("q")[0].page_content == "text"


class TestRetrieverVectorMode:
    """Vector mode delegates to kg.vector_search()."""

    def test_delegates_to_vector_search(self) -> None:
        kg = _mock_kg(
            columns=["title", "embedding", "Dist"],
            rows=[["Doc1", [0.0, 0.0, 0.0], 0.05]],
        )
        emb = _StubEmbeddings()
        r = InputLayerRetriever(
            kg=kg,
            relation=_DocRel,
            embeddings=emb,
            k=5,
            metric="cosine",
            page_content_columns=["title"],
        )
        r.invoke("query text")
        kg.vector_search.assert_awaited_once()
        call_kwargs = kg.vector_search.await_args
        assert call_kwargs[0][0] is _DocRel
        assert call_kwargs[1]["k"] == 5
        assert call_kwargs[1]["metric"] == "cosine"

    def test_embedding_excluded_from_metadata(self) -> None:
        kg = _mock_kg(
            columns=["title", "embedding", "Dist"],
            rows=[["Doc1", [0.1, 0.2, 0.3], 0.05]],
        )
        r = InputLayerRetriever(
            kg=kg,
            relation=_DocRel,
            embeddings=_StubEmbeddings(),
            page_content_columns=["title"],
        )
        d = r.invoke("query")[0]
        assert "embedding" not in d.metadata
        assert d.metadata.get("score") == 0.05

    def test_results_passed_through(self) -> None:
        # vector_search returns sorted/trimmed results; retriever passes them through.
        kg = _mock_kg(
            columns=["title", "embedding", "Dist"],
            rows=[
                ["near", [0.0], 0.1],
                ["mid", [0.0], 0.5],
            ],
        )
        r = InputLayerRetriever(
            kg=kg,
            relation=_DocRel,
            embeddings=_StubEmbeddings(),
            k=2,
            page_content_columns=["title"],
        )
        docs = r.invoke("query")
        assert [d.page_content for d in docs] == ["near", "mid"]

    def test_engine_error_raised(self) -> None:
        kg = _mock_kg(columns=["error"], rows=[["bad vec"]])
        kg.vector_search = AsyncMock(side_effect=RuntimeError("bad vec"))
        r = InputLayerRetriever(
            kg=kg,
            relation=_DocRel,
            embeddings=_StubEmbeddings(),
            page_content_columns=["title"],
        )
        with pytest.raises(RuntimeError, match="bad vec"):
            r.invoke("q")


# ═══════════════════════════════════════════════════════════════════════
#  3. InputLayerIQLTool
# ═══════════════════════════════════════════════════════════════════════


class TestIQLTool:
    def test_default_metadata(self) -> None:
        t = InputLayerIQLTool(kg=_mock_kg())
        assert t.name == "inputlayer_iql"
        assert "InputLayer Query Language" in t.description

    def test_raw_query_passthrough(self) -> None:
        kg = _mock_kg(columns=["n"], rows=[["Alice"]])
        t = InputLayerIQLTool(kg=kg)
        out = t.invoke("?employee(n)")
        kg.execute.assert_awaited_once_with("?employee(n)")
        assert "Alice" in out

    def test_template_substitution_safe(self) -> None:
        kg = _mock_kg(columns=["c"], rows=[["x"]])
        t = InputLayerIQLTool(kg=kg, query_template="?docs(c), s(:input, c)")
        t.invoke('evil") attack("')
        called = kg.execute.await_args[0][0]
        assert called == '?docs(c), s("evil\\") attack(\\"", c)'

    def test_max_rows_truncation_emits_metadata(self) -> None:
        rows = [[f"row{i}"] for i in range(100)]
        kg = _mock_kg(columns=["v"], rows=rows)
        t = InputLayerIQLTool(kg=kg, max_rows=5)
        out = t.invoke("?big(v)")
        payload = json.loads(out)
        assert payload["truncated"] is True
        assert payload["shown"] == 5
        assert payload["total"] == 100

    def test_empty_results_returns_empty_array(self) -> None:
        kg = _mock_kg(columns=["x"], rows=[])
        t = InputLayerIQLTool(kg=kg)
        assert t.invoke("?empty(x)") == "[]"

    def test_result_is_json_array_of_row_dicts(self) -> None:
        kg = _mock_kg(
            columns=["name", "score"], rows=[["Alice", 95], ["Bob", 87]]
        )
        t = InputLayerIQLTool(kg=kg)
        payload = json.loads(t.invoke("?scores(name, score)"))
        assert payload == [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
        ]


# ═══════════════════════════════════════════════════════════════════════
#  4. tools_from_relations
# ═══════════════════════════════════════════════════════════════════════


class _Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class _Article(Relation):
    id: int
    title: str
    body: str
    embedding: Vector


class TestToolsFromRelationsSchema:
    def test_one_tool_per_relation(self) -> None:
        # Leading underscores in relation names are stripped from the
        # generated tool name so private test classes do not produce
        # double-underscore names like ``search__employee``.
        tools = tools_from_relations(_mock_kg(), [_Employee, _Article])
        assert [t.name for t in tools] == ["search_employee", "search_article"]

    def test_scalar_fields_get_equality_filter(self) -> None:
        tool = tools_from_relations(_mock_kg(), [_Employee])[0]
        fields = tool.args_schema.model_fields
        assert {"id", "name", "department", "salary", "active"} <= set(fields)

    def test_numeric_fields_get_min_max(self) -> None:
        tool = tools_from_relations(_mock_kg(), [_Employee])[0]
        fields = tool.args_schema.model_fields
        assert "min_salary" in fields and "max_salary" in fields
        assert "min_id" in fields and "max_id" in fields
        assert "min_name" not in fields

    def test_no_contains_filter(self) -> None:
        # contains_<field> was removed - no IQL backing for substring match.
        tool = tools_from_relations(_mock_kg(), [_Employee])[0]
        fields = tool.args_schema.model_fields
        assert not any(k.startswith("contains_") for k in fields)

    def test_vector_columns_excluded_from_filters(self) -> None:
        # Regression: create_model with Vector | None used to crash.
        tool = tools_from_relations(_mock_kg(), [_Article])[0]
        fields = tool.args_schema.model_fields
        assert "embedding" not in fields
        assert "id" in fields and "title" in fields and "body" in fields
        assert "embedding" in tool.description

    def test_vector_relation_constructs_without_error(self) -> None:
        tools_from_relations(_mock_kg(), [_Article])

    def test_description_is_informative(self) -> None:
        tool = tools_from_relations(_mock_kg(), [_Employee])[0]
        assert "_employee" in tool.description
        assert "salary" in tool.description
        assert "min_<field>" in tool.description
        assert "JSON" in tool.description


class TestToolsFromRelationsClauseParsing:
    def _runner(self, relation: type[Relation]) -> Any:
        tool = tools_from_relations(_mock_kg(), [relation])[0]
        return tool.coroutine

    def test_no_kwargs_yields_no_clauses(self) -> None:
        assert self._runner(_Employee).parse_clauses({}) == []

    def test_none_kwargs_skipped(self) -> None:
        assert self._runner(_Employee).parse_clauses(
            {"name": None, "salary": None}
        ) == []

    def test_equality_clause(self) -> None:
        assert self._runner(_Employee).parse_clauses({"department": "eng"}) == [
            ("department", "==", "eng")
        ]

    def test_in_list_clause(self) -> None:
        assert self._runner(_Employee).parse_clauses(
            {"department": ["eng", "sales"]}
        ) == [("department", "in", ["eng", "sales"])]

    def test_min_max_range(self) -> None:
        clauses = self._runner(_Employee).parse_clauses(
            {"min_salary": 100.0, "max_salary": 200.0}
        )
        assert ("salary", ">=", 100.0) in clauses
        assert ("salary", "<=", 200.0) in clauses


class TestToolsFromRelationsIQLBuilding:
    """Verify the runner emits the correct IQL strings before execution."""

    def _runner(self, relation: type[Relation]) -> Any:
        return tools_from_relations(_mock_kg(), [relation])[0].coroutine

    def test_no_clauses_emits_bare_query(self) -> None:
        run = self._runner(_Employee)
        assert run.build_iql([]) == (
            "?_employee(Id, Name, Department, Salary, Active)"
        )

    def test_equality_clause_emits_named_filter(self) -> None:
        run = self._runner(_Employee)
        iql = run.build_iql([("department", "==", "eng")])
        assert iql == (
            '?_employee(Id, Name, Department, Salary, Active), '
            'Department = "eng"'
        )

    def test_min_max_emits_two_filters(self) -> None:
        run = self._runner(_Employee)
        iql = run.build_iql(
            [("salary", ">=", 100.0), ("salary", "<=", 200.0)]
        )
        assert iql.endswith(", Salary >= 100.0, Salary <= 200.0")

    def test_in_list_explodes_to_multiple_queries(self) -> None:
        run = self._runner(_Employee)
        queries = run.build_iql_queries([("department", "in", ["eng", "sales"])])
        assert len(queries) == 2
        assert any('Department = "eng"' in q for q in queries)
        assert any('Department = "sales"' in q for q in queries)

    def test_in_list_with_other_filters_combines(self) -> None:
        run = self._runner(_Employee)
        queries = run.build_iql_queries(
            [
                ("department", "in", ["eng", "sales"]),
                ("salary", ">=", 100.0),
            ]
        )
        # Both queries include the salary filter
        assert all("Salary >= 100.0" in q for q in queries)
        assert sum('Department = "eng"' in q for q in queries) == 1
        assert sum('Department = "sales"' in q for q in queries) == 1

    def test_string_value_escaped(self) -> None:
        run = self._runner(_Employee)
        iql = run.build_iql([("name", "==", 'evil") attack("')])
        assert '"evil\\") attack(\\""' in iql


class TestToolsFromRelationsParity:
    """The integration's IQL emitter must agree with kg.query.

    ``tools_from_relations`` builds its own IQL strings to support
    IN-list expansion and JSON output without going through ``kg.query``.
    For the equality and range cases that *both* paths support, the two
    emitters must produce semantically identical queries: same head
    atom, same filter clauses, same order. This test catches drift when
    either path changes.
    """

    def _capture_kg_query_iql(self, **filter_kwargs: Any) -> str:
        """Compile what kg.query would send through ``compile_query``.

        Bypasses the WebSocket connection entirely by calling
        ``compile_query`` directly with the same AST kg.query would
        build. This avoids needing a working mock connection and lets
        the parity test stay deterministic.
        """
        from inputlayer._proxy import RelationProxy
        from inputlayer.compiler import compile_query

        clauses = [
            (k[4:], ">=", v) if k.startswith("min_")
            else (k[4:], "<=", v) if k.startswith("max_")
            else (k, "==", v)
            for k, v in filter_kwargs.items()
        ]

        proxy = RelationProxy(Relation._resolve_name(_Employee))

        cond: Any = None
        for col, op, val in clauses:
            col_proxy = getattr(proxy, col)
            if op == "==":
                expr = col_proxy == val
            elif op == ">=":
                expr = col_proxy >= val
            elif op == "<=":
                expr = col_proxy <= val
            else:
                raise ValueError(op)
            cond = expr if cond is None else (cond & expr)

        result = compile_query(
            _Employee,
            relations=[_Employee],
            where_condition=cond,
        )
        return result if isinstance(result, str) else result[0]

    def _tool_emitter_iql(self, **filter_kwargs: Any) -> str:
        runner = tools_from_relations(_mock_kg(), [_Employee])[0].coroutine
        clauses = runner.parse_clauses(filter_kwargs)
        return runner.build_iql(clauses)

    def _normalize(self, iql: str) -> tuple[str, frozenset[str]]:
        # Compare structurally: head atom + unordered set of filter clauses.
        head, _, body = iql.partition("),")
        head = head + ")"
        filters = frozenset(p.strip() for p in body.split(",") if p.strip())
        return head, filters

    def test_equality_filter_parity(self) -> None:
        kg_iql = self._capture_kg_query_iql(department="eng")
        tool_iql = self._tool_emitter_iql(department="eng")
        assert self._normalize(kg_iql) == self._normalize(tool_iql)

    def test_range_filter_parity(self) -> None:
        kg_iql = self._capture_kg_query_iql(min_salary=100.0, max_salary=200.0)
        tool_iql = self._tool_emitter_iql(min_salary=100.0, max_salary=200.0)
        assert self._normalize(kg_iql) == self._normalize(tool_iql)

    def test_combined_filter_parity(self) -> None:
        kg_iql = self._capture_kg_query_iql(
            department="eng", min_salary=100.0, max_salary=200.0
        )
        tool_iql = self._tool_emitter_iql(
            department="eng", min_salary=100.0, max_salary=200.0
        )
        assert self._normalize(kg_iql) == self._normalize(tool_iql)


class TestToolsFromRelationsExecution:
    async def test_no_filters_calls_execute_with_bare_query(self) -> None:
        kg = _mock_kg(
            columns=["id", "name", "department", "salary", "active"],
            rows=[[1, "Alice", "eng", 100.0, True]],
        )
        tool = tools_from_relations(kg, [_Employee])[0]
        out = await tool.ainvoke({})
        kg.execute.assert_awaited_once_with(
            "?_employee(Id, Name, Department, Salary, Active)"
        )
        kg.query.assert_not_called()
        payload = json.loads(out)
        assert payload[0]["name"] == "Alice"

    async def test_equality_filter_dispatches_correct_iql(self) -> None:
        kg = _mock_kg(columns=["id"], rows=[[1]])
        tool = tools_from_relations(kg, [_Employee])[0]
        await tool.ainvoke({"department": "eng"})
        called = kg.execute.await_args[0][0]
        assert called == (
            '?_employee(Id, Name, Department, Salary, Active), '
            'Department = "eng"'
        )

    async def test_in_list_executes_multiple_queries_and_merges(self) -> None:
        # Mock returns same rows for every call; we expect dedup.
        kg = _mock_kg(
            columns=["id", "name", "department", "salary", "active"],
            rows=[[1, "Alice", "eng", 100.0, True]],
        )
        tool = tools_from_relations(kg, [_Employee])[0]
        await tool.ainvoke({"department": ["eng", "sales"]})
        assert kg.execute.await_count == 2

    async def test_engine_error_returned_as_json(self) -> None:
        kg = _mock_kg(columns=["error"], rows=[["bad parse"]])
        tool = tools_from_relations(kg, [_Employee])[0]
        out = await tool.ainvoke({"department": "eng"})
        payload = json.loads(out)
        assert payload == {"error": "bad parse"}


# ═══════════════════════════════════════════════════════════════════════
#  5. InputLayerVectorStore (unit)
# ═══════════════════════════════════════════════════════════════════════


class _Chunk(Relation):
    id: str
    content: str
    source: str
    embedding: Vector


class _ChunkNoVector(Relation):
    id: str
    content: str


class _ChunkRequiredExtra(Relation):
    id: str
    content: str
    must_have: str
    embedding: Vector


class TestVectorStoreConstruction:
    def test_auto_detects_vector_column(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        assert vs._vector_field == "embedding"

    def test_ensure_schema_default_does_not_define(self) -> None:
        # Default is False to avoid sync-from-running-loop issues.
        kg = _mock_kg()
        InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        kg.define.assert_not_called()

    def test_ensure_schema_true_defines(self) -> None:
        kg = _mock_kg()
        InputLayerVectorStore(
            kg=kg,
            relation=_Chunk,
            embeddings=_StubEmbeddings(),
            ensure_schema=True,
        )
        kg.define.assert_called_once()

    def test_missing_content_field_raises(self) -> None:
        class C(Relation):
            id: str
            body: str
            embedding: Vector

        with pytest.raises(ValueError, match="content"):
            InputLayerVectorStore(
                kg=_mock_kg(), relation=C, embeddings=_StubEmbeddings()
            )

    def test_missing_vector_column_raises(self) -> None:
        with pytest.raises(ValueError, match="Vector"):
            InputLayerVectorStore(
                kg=_mock_kg(),
                relation=_ChunkNoVector,
                embeddings=_StubEmbeddings(),
            )


class TestVectorStoreAdd:
    def test_inserts_with_embeddings_and_metadata(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        ids = vs.add_texts(
            ["alpha beta", "gamma delta"],
            metadatas=[{"source": "a"}, {"source": "b"}],
        )
        assert len(ids) == 2
        kg.insert.assert_awaited_once()
        inserted = kg.insert.await_args[0][0]
        assert [r.content for r in inserted] == ["alpha beta", "gamma delta"]
        assert [r.source for r in inserted] == ["a", "b"]

    def test_auto_generated_ids_when_omitted(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        ids = vs.add_texts(["hello"], metadatas=[{"source": "a"}])
        assert len(ids[0]) > 0

    def test_unknown_metadata_key_warns(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        with pytest.warns(UserWarning, match="not present in relation"):
            vs.add_texts(["x"], metadatas=[{"source": "a", "bogus": 42}])

    def test_missing_required_metadata_raises(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_ChunkRequiredExtra, embeddings=_StubEmbeddings()
        )
        with pytest.raises(ValueError, match="must_have"):
            vs.add_texts(["x"], metadatas=[{}])

    def test_empty_input_no_op(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        assert vs.add_texts([]) == []
        kg.insert.assert_not_called()


class TestVectorStoreSearch:
    def test_similarity_search_delegates_to_vector_search(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.0, 0.0, 0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        docs = vs.similarity_search("query", k=3)
        assert len(docs) == 1
        assert docs[0].page_content == "hello"
        assert docs[0].metadata.get("source") == "a"
        assert "embedding" not in docs[0].metadata

        kg.vector_search.assert_awaited_once()
        call_kwargs = kg.vector_search.await_args[1]
        assert call_kwargs["k"] == 3
        assert call_kwargs["metric"] == "cosine"

    def test_similarity_search_with_score(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hi", "src", [0.0], 0.42]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        results = vs.similarity_search_with_score("q", k=1)
        doc, score = results[0]
        assert doc.page_content == "hi"
        assert score == pytest.approx(0.42)

    def test_results_from_vector_search_passed_through(self) -> None:
        # vector_search returns already sorted/trimmed results.
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[
                ["2", "near", "x", [0.0], 0.1],
                ["3", "mid", "x", [0.0], 0.5],
            ],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        docs = vs.similarity_search("q", k=2)
        assert [d.page_content for d in docs] == ["near", "mid"]

    def test_engine_error_raised(self) -> None:
        kg = _mock_kg(columns=["error"], rows=[["bad query"]])
        kg.vector_search = AsyncMock(side_effect=RuntimeError("bad query"))
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        with pytest.raises(RuntimeError, match="bad query"):
            vs.similarity_search("q", k=1)

    def test_as_retriever(self) -> None:
        vs = InputLayerVectorStore(
            kg=_mock_kg(), relation=_Chunk, embeddings=_StubEmbeddings()
        )
        retriever = vs.as_retriever(search_kwargs={"k": 7})
        assert retriever is not None

    def test_filter_passed_to_vector_search(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        vs.similarity_search("q", k=3, filter={"source": "a"})
        call_kwargs = kg.vector_search.await_args[1]
        extra = call_kwargs.get("extra_iql_clauses")
        assert extra is not None
        assert any('Source = "a"' in c for c in extra)

    def test_filter_unknown_key_warns(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        with pytest.warns(UserWarning, match="not a column"):
            vs.similarity_search("q", filter={"bogus": 1})


class TestVectorStoreDelete:
    def test_delete_emits_conditional_delete_per_id(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        result = vs.delete(ids=["x1", "x2"])
        assert result is True
        assert kg.execute.await_count == 2
        first = kg.execute.await_args_list[0][0][0]
        assert first.startswith("-_chunk(")
        assert 'Id = "x1"' in first

    def test_delete_empty_ids_is_noop(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        assert vs.delete(ids=[]) is None
        assert vs.delete(ids=None) is None
        kg.execute.assert_not_called()


class TestVectorStoreAddDocuments:
    def test_add_documents_routes_through_add_texts(self) -> None:
        # ``add_documents`` is inherited from the base ``VectorStore``
        # class. We rely on its default implementation, which extracts
        # ``page_content`` and ``metadata`` from each ``Document`` and
        # forwards to ``add_texts``. Verify the round trip lands the
        # right rows on the underlying KG.
        kg = _mock_kg()
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        vs.add_documents([
            Document(page_content="alpha", metadata={"source": "a"}),
            Document(page_content="beta", metadata={"source": "b"}),
        ])
        kg.insert.assert_awaited_once()
        inserted = kg.insert.await_args[0][0]
        assert [r.content for r in inserted] == ["alpha", "beta"]
        assert [r.source for r in inserted] == ["a", "b"]


class TestVectorStoreMMR:
    def test_mmr_returns_documents(self) -> None:
        # Three rows with diverging embeddings; lambda=1.0 selects pure
        # relevance order, lambda=0.0 maximizes diversity.
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[
                ["1", "doc1", "a", [1.0, 0.0, 0.0], 0.0],
                ["2", "doc2", "b", [0.95, 0.31, 0.0], 0.05],
                ["3", "doc3", "c", [0.0, 1.0, 0.0], 1.0],
            ],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        # lambda_mult=1.0 means pure relevance: top 2 by Dist asc
        relevance = vs.max_marginal_relevance_search(
            "q", k=2, fetch_k=10, lambda_mult=1.0
        )
        assert [d.page_content for d in relevance] == ["doc1", "doc2"]

        # lambda_mult=0.0 means maximize diversity: doc1 first, then the
        # furthest from doc1 which is doc3 (orthogonal), not doc2 (close).
        diversity = vs.max_marginal_relevance_search(
            "q", k=2, fetch_k=10, lambda_mult=0.0
        )
        assert [d.page_content for d in diversity] == ["doc1", "doc3"]


class TestVectorStoreFromTexts:
    def test_requires_kg_and_relation(self) -> None:
        # LSP escape: from_texts is documented to require these kwargs.
        with pytest.raises(ValueError, match="requires `kg=`"):
            InputLayerVectorStore.from_texts(
                ["a"], _StubEmbeddings()
            )

    def test_with_kwargs_constructs_and_inserts(self) -> None:
        kg = _mock_kg()
        vs = InputLayerVectorStore.from_texts(
            ["alpha", "beta"],
            _StubEmbeddings(),
            metadatas=[{"source": "x"}, {"source": "y"}],
            kg=kg,
            relation=_Chunk,
        )
        assert isinstance(vs, InputLayerVectorStore)
        kg.insert.assert_awaited_once()


# ═══════════════════════════════════════════════════════════════════════
#  6. Sync-from-running-loop interop
# ═══════════════════════════════════════════════════════════════════════


class TestSyncFromRunningLoop:
    def test_retriever_inside_running_loop(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[["data"]])
        r = InputLayerRetriever(
            kg=kg, query="?d(content)", page_content_columns=["content"]
        )
        result: list[Document] | None = None

        async def main() -> None:
            nonlocal result
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, r.invoke, "query")

        asyncio.run(main())
        assert result is not None and len(result) == 1

    def test_tool_inside_running_loop(self) -> None:
        kg = _mock_kg(columns=["x"], rows=[["val"]])
        t = InputLayerIQLTool(kg=kg)
        result: str | None = None

        async def main() -> None:
            nonlocal result
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, t.invoke, "?t(x)")

        asyncio.run(main())
        assert result is not None and "val" in result


# ═══════════════════════════════════════════════════════════════════════
#  7. Additional coverage: edge cases and gaps
# ═══════════════════════════════════════════════════════════════════════


class TestRetrieverMetricValidation:
    def test_metric_passed_to_vector_search(self) -> None:
        kg = _mock_kg(
            columns=["title", "embedding", "Dist"],
            rows=[["Doc1", [0.0, 0.0, 0.0], 0.05]],
        )
        r = InputLayerRetriever(
            kg=kg,
            relation=_DocRel,
            embeddings=_StubEmbeddings(),
            metric="dot_product",
            page_content_columns=["title"],
        )
        r.invoke("query")
        assert kg.vector_search.await_args[1]["metric"] == "dot_product"


class TestVectorStoreMetricValidation:
    def test_invalid_metric_raises(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        with pytest.raises(ValueError, match="Unknown metric"):
            vs.similarity_search_with_score("q", k=1, metric="bogus")


class TestVectorStoreSimilaritySearchByVector:
    def test_basic_search_by_vector(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[
                ["1", "hello", "a", [0.1, 0.2, 0.3], 0.05],
                ["2", "world", "b", [0.4, 0.5, 0.6], 0.15],
            ],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        docs = vs.similarity_search_by_vector([0.1, 0.2, 0.3], k=2)
        assert len(docs) == 2
        assert docs[0].page_content == "hello"
        assert docs[1].page_content == "world"
        kg.vector_search.assert_awaited_once()

    def test_search_by_vector_passes_k_to_vector_search(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "a", "x", [0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        vs.similarity_search_by_vector([0.1], k=1)
        assert kg.vector_search.await_args[1]["k"] == 1

    def test_search_by_vector_excludes_embedding_from_metadata(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.1, 0.2], 0.05]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        docs = vs.similarity_search_by_vector([0.1, 0.2], k=1)
        assert "embedding" not in docs[0].metadata

    def test_search_by_vector_with_filter(self) -> None:
        kg = _mock_kg(
            columns=["id", "content", "source", "embedding", "Dist"],
            rows=[["1", "hello", "a", [0.0], 0.1]],
        )
        vs = InputLayerVectorStore(
            kg=kg, relation=_Chunk, embeddings=_StubEmbeddings()
        )
        vs.similarity_search_by_vector([0.1], k=1, filter={"source": "a"})
        call_kwargs = kg.vector_search.await_args[1]
        extra = call_kwargs.get("extra_iql_clauses")
        assert extra is not None
        assert any('Source = "a"' in c for c in extra)


class TestRetrieverEngineErrorFallback:
    def test_engine_error_unknown_fallback(self) -> None:
        """Engine returns error column with empty rows."""
        kg = _mock_kg(columns=["error"], rows=[])
        r = InputLayerRetriever(
            kg=kg, query="?bad", page_content_columns=["x"]
        )
        with pytest.raises(RuntimeError, match="unknown error"):
            r.invoke("q")


# ═══════════════════════════════════════════════════════════════════════
#  8. LIVE integration tests against a real inputlayer-server
# ═══════════════════════════════════════════════════════════════════════
#
# These run only when INPUTLAYER_INTEGRATION=1. They define unique KG
# names per test, drop them at the end, and exercise the whole stack:
# WebSocket protocol, schema definition, fact insertion, IQL parsing,
# cosine math, and document mapping. This is the layer mocks cannot
# replicate.

requires_integration = pytest.mark.skipif(
    os.environ.get("INPUTLAYER_INTEGRATION") != "1",
    reason="set INPUTLAYER_INTEGRATION=1 to enable live-server tests",
)


def _live_url() -> str:
    return os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws")


def _live_user() -> str:
    return os.environ.get("INPUTLAYER_USER", "admin")


def _live_password() -> str:
    return os.environ.get("INPUTLAYER_PASSWORD", "admin")


# Module-level relations for live tests. Each test class gets its own
# uniquely-named relation because dropping a knowledge graph does not
# always evict the underlying schema/data when other graphs hold the
# same relation - safer to give every test its own namespace.


class LiveIqlDoc(Relation):
    __relation_name__ = "lc_iql_doc"
    id: int
    title: str
    content: str
    category: str
    embedding: Vector


class LiveIqlUserInterest(Relation):
    __relation_name__ = "lc_iql_user_interest"
    user: str
    category: str


class LiveVecDoc(Relation):
    __relation_name__ = "lc_vec_doc"
    id: int
    content: str
    title: str
    category: str
    embedding: Vector


class LiveToolUserInterest(Relation):
    __relation_name__ = "lc_tool_user_interest"
    user: str
    category: str


class LiveStructUserInterest(Relation):
    __relation_name__ = "lc_struct_user_interest"
    user: str
    category: str


class _DirEmb(Embeddings):
    """Live-test embedder mapping topical queries to fixed unit vectors."""

    def embed_query(self, text: str) -> list[float]:
        t = text.lower()
        if "ml" in t or "learn" in t:
            return [1.0, 0.0, 0.0]
        if "web" in t or "api" in t:
            return [0.0, 1.0, 0.0]
        return [0.0, 0.0, 1.0]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self.embed_query(t) for t in texts]

    async def aembed_query(self, text: str) -> list[float]:
        return self.embed_query(text)

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        return self.embed_documents(texts)


@requires_integration
class TestLiveIQLRetriever:
    """End-to-end IQL retriever round trip against a live server."""

    async def _setup_kg(self, il: Any, kg_name: str) -> Any:
        kg = il.knowledge_graph(kg_name)
        await kg.define(LiveIqlDoc, LiveIqlUserInterest)
        await kg.insert([
            LiveIqlDoc(
                id=1, title="ML Intro", content="machine learning basics",
                category="ml", embedding=[1.0, 0.0, 0.0],
            ),
            LiveIqlDoc(
                id=2, title="Web APIs", content="REST and HTTP",
                category="web", embedding=[0.0, 1.0, 0.0],
            ),
            LiveIqlDoc(
                id=3, title="Graph DBs", content="nodes and edges",
                category="db", embedding=[0.0, 0.0, 1.0],
            ),
        ])
        await kg.insert([
            LiveIqlUserInterest(user="alice", category="ml"),
            LiveIqlUserInterest(user="alice", category="db"),
            LiveIqlUserInterest(user="bob", category="web"),
        ])
        return kg

    async def test_iql_retriever_round_trip(self) -> None:
        from inputlayer import InputLayer

        kg_name = f"il_lc_iql_{uuid.uuid4().hex[:8]}"
        async with InputLayer(
            _live_url(), username=_live_user(), password=_live_password()
        ) as il:
            try:
                kg = await self._setup_kg(il, kg_name)
                retriever = InputLayerRetriever(
                    kg=kg,
                    query=(
                        "?lc_iql_doc(I, T, C, Cat, E), "
                        "lc_iql_user_interest(:input, Cat)"
                    ),
                    page_content_columns=["content"],
                    metadata_columns=["title", "category"],
                )
                docs = await retriever.ainvoke("alice")
                titles = sorted(d.metadata["title"] for d in docs)
                categories = sorted({d.metadata["category"] for d in docs})
                assert titles == ["Graph DBs", "ML Intro"]
                assert categories == ["db", "ml"]
                for d in docs:
                    assert "embedding" not in d.metadata
            finally:
                await il.drop_knowledge_graph(kg_name)


@requires_integration
class TestLiveVectorRetriever:
    async def test_vector_retriever_round_trip(self) -> None:
        from inputlayer import InputLayer

        kg_name = f"il_lc_vec_{uuid.uuid4().hex[:8]}"
        async with InputLayer(
            _live_url(), username=_live_user(), password=_live_password()
        ) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.define(LiveVecDoc)
                await kg.insert([
                    LiveVecDoc(
                        id=1, content="ML basics", title="ML",
                        category="ml", embedding=[1.0, 0.0, 0.0],
                    ),
                    LiveVecDoc(
                        id=2, content="HTTP APIs", title="Web",
                        category="web", embedding=[0.0, 1.0, 0.0],
                    ),
                    LiveVecDoc(
                        id=3, content="graph nodes", title="DB",
                        category="db", embedding=[0.0, 0.0, 1.0],
                    ),
                ])

                retriever = InputLayerRetriever(
                    kg=kg,
                    relation=LiveVecDoc,
                    embeddings=_DirEmb(),
                    k=2,
                    page_content_columns=["content"],
                    metadata_columns=["title", "category"],
                )
                docs = await retriever.ainvoke("learn ml")
                assert len(docs) == 2
                # Closest to [1,0,0] is the ML doc.
                assert docs[0].metadata["title"] == "ML"
                for d in docs:
                    assert "embedding" not in d.metadata
                    assert "score" in d.metadata
            finally:
                await il.drop_knowledge_graph(kg_name)


@requires_integration
class TestLiveVectorStore:
    """End-to-end VectorStore round trip with real cosine math."""

    async def test_vector_store_insert_and_search(self) -> None:
        from inputlayer import InputLayer

        kg_name = f"il_lc_vs_{uuid.uuid4().hex[:8]}"

        async with InputLayer(
            _live_url(), username=_live_user(), password=_live_password()
        ) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.define(LiveVecDoc)

                vs = InputLayerVectorStore(
                    kg=kg,
                    relation=LiveVecDoc,
                    embeddings=_DirEmb(),
                    content_field="content",
                    id_field="id",
                )
                await vs.aadd_texts(
                    texts=["ML basics", "HTTP APIs", "graph nodes"],
                    metadatas=[
                        {"title": "ML", "category": "ml"},
                        {"title": "Web", "category": "web"},
                        {"title": "DB", "category": "db"},
                    ],
                    ids=[100, 101, 102],
                )

                # "learn ml" maps to [1, 0, 0] which is closest to "ML basics".
                docs = await vs.asimilarity_search("learn ml", k=2)
                assert len(docs) == 2
                assert docs[0].page_content == "ML basics"
                for d in docs:
                    assert "embedding" not in d.metadata
                    assert d.metadata.get("title") in {"ML", "Web", "DB"}
            finally:
                await il.drop_knowledge_graph(kg_name)


@requires_integration
class TestLiveIQLTool:
    async def test_raw_iql_tool_against_server(self) -> None:
        from inputlayer import InputLayer

        kg_name = f"il_lc_tool_{uuid.uuid4().hex[:8]}"
        async with InputLayer(
            _live_url(), username=_live_user(), password=_live_password()
        ) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.define(LiveToolUserInterest)
                await kg.insert([
                    LiveToolUserInterest(user="alice", category="ml"),
                    LiveToolUserInterest(user="bob", category="web"),
                ])
                tool = InputLayerIQLTool(kg=kg)
                out = await tool.ainvoke("?lc_tool_user_interest(User, Category)")
                payload = json.loads(out)
                users = sorted({r["user"] for r in payload})
                assert users == ["alice", "bob"]
            finally:
                await il.drop_knowledge_graph(kg_name)


@requires_integration
class TestLiveStructuredTools:
    async def test_tools_from_relations_against_server(self) -> None:
        from inputlayer import InputLayer

        kg_name = f"il_lc_struct_{uuid.uuid4().hex[:8]}"
        async with InputLayer(
            _live_url(), username=_live_user(), password=_live_password()
        ) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.define(LiveStructUserInterest)
                await kg.insert([
                    LiveStructUserInterest(user="alice", category="ml"),
                    LiveStructUserInterest(user="alice", category="db"),
                    LiveStructUserInterest(user="bob", category="web"),
                ])
                tools = tools_from_relations(kg, [LiveStructUserInterest])
                assert len(tools) == 1
                tool = tools[0]

                out = await tool.ainvoke({"user": "alice"})
                payload = json.loads(out)
                cats = sorted(r["category"] for r in payload)
                assert cats == ["db", "ml"]
            finally:
                await il.drop_knowledge_graph(kg_name)


# ── 7. Tests for new safety features ──────────────────────────────────


class TestReadOnlyGuard:
    """Tests for the read_only guard on InputLayerIQLTool."""

    def test_check_read_only_blocks_assertion(self) -> None:
        from inputlayer.integrations.langchain.tool import _check_read_only

        with pytest.raises(ValueError, match="read_only guard"):
            _check_read_only("+fact(1, 2, 3)")

    def test_check_read_only_blocks_retraction(self) -> None:
        from inputlayer.integrations.langchain.tool import _check_read_only

        with pytest.raises(ValueError, match="read_only guard"):
            _check_read_only("-fact(1, 2, 3)")

    def test_check_read_only_blocks_drop(self) -> None:
        from inputlayer.integrations.langchain.tool import _check_read_only

        with pytest.raises(ValueError, match="read_only guard"):
            _check_read_only(".drop my_relation")

    def test_check_read_only_blocks_create(self) -> None:
        from inputlayer.integrations.langchain.tool import _check_read_only

        with pytest.raises(ValueError, match="read_only guard"):
            _check_read_only(".create my_relation")

    def test_check_read_only_allows_queries(self) -> None:
        from inputlayer.integrations.langchain.tool import _check_read_only

        _check_read_only("?fact(X, Y)")
        _check_read_only("?a(X), b(X, Y)")
        _check_read_only(".why ?fact(X)")

    @pytest.mark.asyncio
    async def test_tool_read_only_rejects_write(self) -> None:
        kg = _mock_kg(["x"], [[1]])
        tool = InputLayerIQLTool(kg=kg, read_only=True)
        with pytest.raises(ValueError, match="read_only guard"):
            await tool._arun("+fact(1, 2, 3)")

    @pytest.mark.asyncio
    async def test_tool_read_only_false_allows_write(self) -> None:
        kg = _mock_kg(["x"], [[1]])
        tool = InputLayerIQLTool(kg=kg, read_only=False)
        result = await tool._arun("+fact(1, 2, 3)")
        assert result  # should not raise

    @pytest.mark.asyncio
    async def test_tool_read_only_allows_queries(self) -> None:
        kg = _mock_kg(["x"], [[1]])
        tool = InputLayerIQLTool(kg=kg, read_only=True)
        result = await tool._arun("?fact(X, Y)")
        assert result


class TestDistanceToRelevance:
    """Tests for _distance_to_relevance metric conversion."""

    def test_cosine(self) -> None:
        from inputlayer.integrations.langchain.vector_store import (
            _distance_to_relevance,
        )

        assert _distance_to_relevance(0.0, "cosine") == 1.0
        assert _distance_to_relevance(0.3, "cosine") == pytest.approx(0.7)
        assert _distance_to_relevance(1.0, "cosine") == 0.0

    def test_euclidean(self) -> None:
        from inputlayer.integrations.langchain.vector_store import (
            _distance_to_relevance,
        )

        assert _distance_to_relevance(0.0, "euclidean") == 1.0
        assert _distance_to_relevance(1.0, "euclidean") == pytest.approx(0.5)
        assert _distance_to_relevance(9.0, "euclidean") == pytest.approx(0.1)

    def test_dot(self) -> None:
        from inputlayer.integrations.langchain.vector_store import (
            _distance_to_relevance,
        )

        assert _distance_to_relevance(0.8, "dot") == 0.8
        assert _distance_to_relevance(-0.5, "dot") == -0.5


class TestIqlLiteralEdgeCases:
    """Tests for inf/nan rejection in iql_literal."""

    def test_rejects_inf(self) -> None:
        with pytest.raises(ValueError, match="infinity or NaN"):
            iql_literal(float("inf"))

    def test_rejects_neg_inf(self) -> None:
        with pytest.raises(ValueError, match="infinity or NaN"):
            iql_literal(float("-inf"))

    def test_rejects_nan(self) -> None:
        with pytest.raises(ValueError, match="infinity or NaN"):
            iql_literal(float("nan"))

    def test_normal_floats_pass(self) -> None:
        assert iql_literal(3.14) == "3.14"
        assert iql_literal(0.0) == "0.0"
        assert iql_literal(-1.5) == "-1.5"

    def test_rejects_inf_in_list(self) -> None:
        with pytest.raises(ValueError, match="infinity or NaN"):
            iql_literal([1.0, float("inf")])

    def test_rejects_nan_in_list(self) -> None:
        with pytest.raises(ValueError, match="infinity or NaN"):
            iql_literal([float("nan"), 2.0])


class TestResolveParamsOrdering:
    """Tests that input_param is not silently overwritten by static params."""

    @pytest.mark.asyncio
    async def test_input_param_wins_over_static_params(self) -> None:
        kg = _mock_kg(["title"], [["ML Basics"]])
        retriever = InputLayerRetriever(
            kg=kg,
            query='?docs(T), topic(:input), min_score(:min)',
            params={"input": "should_be_overwritten", "min": 0.5},
        )
        params = retriever._resolve_params("actual_query")
        assert params["input"] == "actual_query"
        assert params["min"] == 0.5

    @pytest.mark.asyncio
    async def test_callable_params_not_affected(self) -> None:
        kg = _mock_kg(["title"], [["ML Basics"]])
        retriever = InputLayerRetriever(
            kg=kg,
            query='?docs(T), topic(:input)',
            params=lambda q: {"input": q, "extra": 42},
        )
        params = retriever._resolve_params("my_query")
        assert params["input"] == "my_query"
        assert params["extra"] == 42


class TestDebugResultDeprecation:
    """Tests for the DebugResult.datalog deprecation alias."""

    def test_datalog_alias_returns_iql(self) -> None:
        import warnings

        from inputlayer.knowledge_graph import DebugResult

        dr = DebugResult(iql="?test(X)", plan="plan text")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            val = dr.datalog
        assert val == "?test(X)"
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "deprecated" in str(w[0].message).lower()

    def test_unknown_attr_raises(self) -> None:
        from inputlayer.knowledge_graph import DebugResult

        dr = DebugResult(iql="?test(X)", plan="plan text")
        with pytest.raises(AttributeError, match="no_such_field"):
            dr.no_such_field
