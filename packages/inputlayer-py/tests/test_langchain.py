"""Tests for inputlayer.integrations.langchain - Retriever and Tool."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.documents import Document

from inputlayer.integrations.langchain import InputLayerRetriever, InputLayerTool
from inputlayer.integrations.langchain.retriever import _parse_vector
from inputlayer.result import ResultSet

# ── Helpers ──────────────────────────────────────────────────────────


def _mock_kg(
    columns: list[str] | None = None,
    rows: list[list] | None = None,
) -> MagicMock:
    """Create a mock KnowledgeGraph that returns the given result."""
    columns = columns or []
    rows = rows or []
    result = ResultSet(columns=columns, rows=rows)
    kg = MagicMock()
    kg.execute = AsyncMock(return_value=result)
    kg.vector_search = AsyncMock(return_value=result)
    return kg


# ═══════════════════════════════════════════════════════════════════════
#  InputLayerRetriever
# ═══════════════════════════════════════════════════════════════════════


class TestRetrieverValidation:
    def test_requires_query_or_relation(self) -> None:
        kg = _mock_kg()
        with pytest.raises(ValueError, match="Must provide either"):
            InputLayerRetriever(kg=kg)

    def test_accepts_query(self) -> None:
        kg = _mock_kg()
        r = InputLayerRetriever(kg=kg, query="?X <- foo(X)")
        assert r.query == "?X <- foo(X)"

    def test_accepts_relation(self) -> None:
        from inputlayer.relation import Relation

        class Doc(Relation):
            content: str

        kg = _mock_kg()
        r = InputLayerRetriever(kg=kg, relation=Doc)
        assert r.relation is Doc


class TestRetrieverDatalogMode:
    def test_basic_query(self) -> None:
        kg = _mock_kg(
            columns=["title", "content"],
            rows=[
                ["Doc1", "Hello world"],
                ["Doc2", "Goodbye world"],
            ],
        )
        r = InputLayerRetriever(kg=kg, query="?Title, Content <- docs(Title, Content)")
        docs = r.invoke("test query")

        assert len(docs) == 2
        assert isinstance(docs[0], Document)
        kg.execute.assert_awaited_once_with("?Title, Content <- docs(Title, Content)")

    def test_input_placeholder_substitution(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[["result"]])
        r = InputLayerRetriever(
            kg=kg,
            query='?Content <- search("{input}", Content)',
            page_content_columns=["content"],
        )
        r.invoke("machine learning")

        kg.execute.assert_awaited_once_with('?Content <- search("machine learning", Content)')

    def test_empty_results(self) -> None:
        kg = _mock_kg(columns=["content"], rows=[])
        r = InputLayerRetriever(kg=kg, query="?X <- empty(X)")
        docs = r.invoke("test")

        assert docs == []

    async def test_async_invoke(self) -> None:
        kg = _mock_kg(
            columns=["content"],
            rows=[["async result"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?Content <- docs(Content)",
            page_content_columns=["content"],
        )
        docs = await r.ainvoke("test")

        assert len(docs) == 1
        assert docs[0].page_content == "async result"


class TestRetrieverDocumentMapping:
    def test_page_content_from_single_column(self) -> None:
        kg = _mock_kg(
            columns=["title", "body"],
            rows=[["T1", "Body text"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?T, B <- docs(T, B)",
            page_content_columns=["body"],
        )
        docs = r.invoke("q")

        assert docs[0].page_content == "Body text"

    def test_page_content_from_multiple_columns(self) -> None:
        kg = _mock_kg(
            columns=["title", "summary", "body"],
            rows=[["T1", "Sum1", "Body1"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?T, S, B <- docs(T, S, B)",
            page_content_columns=["title", "body"],
        )
        docs = r.invoke("q")

        assert docs[0].page_content == "T1\nBody1"

    def test_metadata_from_specified_columns(self) -> None:
        kg = _mock_kg(
            columns=["content", "source", "timestamp"],
            rows=[["text", "wiki", "2024-01-01"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?C, S, T <- docs(C, S, T)",
            page_content_columns=["content"],
            metadata_columns=["source", "timestamp"],
        )
        docs = r.invoke("q")

        assert docs[0].metadata["source"] == "wiki"
        assert docs[0].metadata["timestamp"] == "2024-01-01"
        assert "content" not in docs[0].metadata

    def test_auto_metadata_when_none_specified(self) -> None:
        """When no metadata_columns specified, all non-content columns go to metadata."""
        kg = _mock_kg(
            columns=["content", "source", "score"],
            rows=[["text", "wiki", 0.95]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?C, S, Sc <- docs(C, S, Sc)",
            page_content_columns=["content"],
        )
        docs = r.invoke("q")

        assert docs[0].metadata["source"] == "wiki"
        assert docs[0].metadata["score"] == 0.95

    def test_score_column(self) -> None:
        kg = _mock_kg(
            columns=["content", "dist"],
            rows=[["text", 0.12]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?C, D <- docs(C, D)",
            page_content_columns=["content"],
            score_column="dist",
        )
        docs = r.invoke("q")

        assert docs[0].metadata["score"] == 0.12

    def test_none_values_excluded_from_content(self) -> None:
        kg = _mock_kg(
            columns=["title", "content"],
            rows=[[None, "text"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?T, C <- docs(T, C)",
            page_content_columns=["title", "content"],
        )
        docs = r.invoke("q")

        assert docs[0].page_content == "text"

    def test_multiple_rows(self) -> None:
        kg = _mock_kg(
            columns=["content"],
            rows=[["row1"], ["row2"], ["row3"]],
        )
        r = InputLayerRetriever(
            kg=kg,
            query="?C <- docs(C)",
            page_content_columns=["content"],
        )
        docs = r.invoke("q")

        assert len(docs) == 3
        assert [d.page_content for d in docs] == ["row1", "row2", "row3"]


class TestRetrieverVectorSearch:
    def test_vector_search_mode(self) -> None:
        from inputlayer.relation import Relation
        from inputlayer.types import Vector

        class MyDoc(Relation):
            title: str
            embedding: Vector

        kg = _mock_kg(
            columns=["title", "embedding", "dist"],
            rows=[["Doc1", [0.1, 0.2], 0.05]],
        )
        r = InputLayerRetriever(
            kg=kg,
            relation=MyDoc,
            k=5,
            metric="cosine",
            page_content_columns=["title"],
        )
        r.invoke("0.1 0.2 0.3")

        kg.vector_search.assert_awaited_once()
        call_kwargs = kg.vector_search.call_args
        assert call_kwargs[0][0] is MyDoc
        assert call_kwargs[1]["k"] == 5
        assert call_kwargs[1]["metric"] == "cosine"


class TestParseVector:
    def test_bracket_format(self) -> None:
        assert _parse_vector("[1.0, 2.0, 3.0]") == [1.0, 2.0, 3.0]

    def test_space_separated(self) -> None:
        assert _parse_vector("1.0 2.0 3.0") == [1.0, 2.0, 3.0]

    def test_mixed_format(self) -> None:
        assert _parse_vector("[1, 2.5, 3]") == [1.0, 2.5, 3.0]

    def test_single_value(self) -> None:
        assert _parse_vector("42.0") == [42.0]

    def test_with_whitespace(self) -> None:
        assert _parse_vector("  [1.0,  2.0,  3.0]  ") == [1.0, 2.0, 3.0]


# ═══════════════════════════════════════════════════════════════════════
#  InputLayerTool
# ═══════════════════════════════════════════════════════════════════════


class TestToolBasic:
    def test_default_name_and_description(self) -> None:
        kg = _mock_kg()
        tool = InputLayerTool(kg=kg)
        assert tool.name == "inputlayer_query"
        assert "Datalog" in tool.description

    def test_custom_name_and_description(self) -> None:
        kg = _mock_kg()
        tool = InputLayerTool(
            kg=kg,
            name="search_trips",
            description="Search for relevant trips",
        )
        assert tool.name == "search_trips"
        assert tool.description == "Search for relevant trips"


class TestToolExecution:
    def test_raw_datalog_query(self) -> None:
        kg = _mock_kg(
            columns=["name", "dept"],
            rows=[["Alice", "eng"], ["Bob", "sales"]],
        )
        tool = InputLayerTool(kg=kg)
        result = tool.invoke("?name, dept <- employee(name, dept)")

        kg.execute.assert_awaited_once_with("?name, dept <- employee(name, dept)")
        assert "Alice" in result
        assert "Bob" in result

    def test_template_mode(self) -> None:
        kg = _mock_kg(
            columns=["content"],
            rows=[["result text"]],
        )
        tool = InputLayerTool(
            kg=kg,
            query_template='?Content <- search("{input}", Content)',
        )
        tool.invoke("machine learning")

        kg.execute.assert_awaited_once_with('?Content <- search("machine learning", Content)')

    def test_empty_results(self) -> None:
        kg = _mock_kg(columns=["x"], rows=[])
        tool = InputLayerTool(kg=kg)
        result = tool.invoke("?X <- empty(X)")

        assert result == "No results found."

    async def test_async_invoke(self) -> None:
        kg = _mock_kg(
            columns=["value"],
            rows=[["async_result"]],
        )
        tool = InputLayerTool(kg=kg)
        result = await tool.ainvoke("?X <- test(X)")

        assert "async_result" in result

    def test_max_rows_truncation(self) -> None:
        rows = [[f"row{i}"] for i in range(100)]
        kg = _mock_kg(columns=["value"], rows=rows)
        tool = InputLayerTool(kg=kg, max_rows=5)
        result = tool.invoke("?X <- big_table(X)")

        # Should show 5 data rows + header + truncation message
        lines = result.strip().split("\n")
        assert lines[0] == "value"  # header
        assert len(lines) == 7  # header + 5 rows + truncation
        assert "95 more rows" in lines[-1]

    def test_result_format_tab_separated(self) -> None:
        kg = _mock_kg(
            columns=["name", "score"],
            rows=[["Alice", 95], ["Bob", 87]],
        )
        tool = InputLayerTool(kg=kg)
        result = tool.invoke("?N, S <- scores(N, S)")

        lines = result.strip().split("\n")
        assert lines[0] == "name\tscore"
        assert lines[1] == "Alice\t95"
        assert lines[2] == "Bob\t87"


class TestToolQueryBuilding:
    def test_raw_query_passthrough(self) -> None:
        kg = _mock_kg()
        tool = InputLayerTool(kg=kg)
        assert tool._build_query("?X <- foo(X)") == "?X <- foo(X)"

    def test_template_substitution(self) -> None:
        kg = _mock_kg()
        tool = InputLayerTool(
            kg=kg,
            query_template='?Doc <- search("{input}", Doc)',
        )
        assert tool._build_query("test") == '?Doc <- search("test", Doc)'

    def test_template_multiple_occurrences(self) -> None:
        kg = _mock_kg()
        tool = InputLayerTool(
            kg=kg,
            query_template="?X, Y <- foo({input}, X), bar({input}, Y)",
        )
        result = tool._build_query("val")
        assert result == "?X, Y <- foo(val, X), bar(val, Y)"


# ═══════════════════════════════════════════════════════════════════════
#  Sync/Async interop
# ═══════════════════════════════════════════════════════════════════════


class TestSyncFromRunningLoop:
    def test_retriever_sync_inside_running_loop(self) -> None:
        """Retriever.invoke() works when called from within a running event loop."""
        kg = _mock_kg(columns=["content"], rows=[["data"]])
        r = InputLayerRetriever(
            kg=kg,
            query="?C <- docs(C)",
            page_content_columns=["content"],
        )

        result = None

        async def main() -> None:
            nonlocal result
            # Simulates being inside FastAPI/LangGraph
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, r.invoke, "query")

        asyncio.run(main())
        assert result is not None
        assert len(result) == 1

    def test_tool_sync_inside_running_loop(self) -> None:
        """Tool.invoke() works when called from within a running event loop."""
        kg = _mock_kg(columns=["x"], rows=[["val"]])
        tool = InputLayerTool(kg=kg)

        result = None

        async def main() -> None:
            nonlocal result
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, tool.invoke, "?X <- t(X)")

        asyncio.run(main())
        assert result is not None
        assert "val" in result
