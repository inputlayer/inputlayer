"""Tests for inputlayer.integrations.langchain.vectorstore."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from inputlayer.integrations.langchain import InputLayerVectorStore
from inputlayer.result import ResultSet

# ── Mock embeddings ──────────────────────────────────────────────────


class FakeEmbeddings(Embeddings):
    """Deterministic toy embeddings: hash text length to a 3-d vector."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)

    def _embed(self, text: str) -> list[float]:
        # Use char count and word count for two dimensions, plus a constant
        return [
            float(len(text)) / 100.0,
            float(len(text.split())) / 10.0,
            0.5,
        ]


# ── Mock KG ──────────────────────────────────────────────────────────


class MockKG:
    """In-memory KG that handles the vectorstore relation operations."""

    def __init__(self) -> None:
        self.documents: list[tuple[str, str, str, list[float]]] = []
        self.executed: list[str] = []

    async def execute(self, datalog: str) -> ResultSet:
        self.executed.append(datalog)

        # Schema definition
        if datalog.startswith("+langchain_docs(id:") or "(id:" in datalog:
            return ResultSet(columns=[], rows=[])

        # Insert: +langchain_docs("id", "content", "metadata", [v1, v2, v3])
        if datalog.startswith("+langchain_docs("):
            args = self._parse_insert(datalog)
            if args:
                self.documents.append(args)
            return ResultSet(columns=[], rows=[])

        # Query by id: ?langchain_docs("id", Content, Metadata, Emb)
        if datalog.startswith('?langchain_docs("'):
            doc_id = datalog.split('"', 2)[1]
            for d in self.documents:
                if d[0] == doc_id:
                    return ResultSet(
                        columns=["content", "metadata", "embedding"],
                        rows=[[d[1], d[2], d[3]]],
                    )
            return ResultSet(columns=[], rows=[])

        # Search query: ?langchain_docs(Id, Content, Metadata, Emb), Dist = cosine(...)
        if datalog.startswith("?langchain_docs(Id"):
            # Extract the query vector from the cosine() call
            # Format: ... Dist = cosine(Emb, [v1, v2, v3])
            import re

            m = re.search(r"\(Emb, \[([^\]]+)\]\)", datalog)
            if not m:
                return ResultSet(columns=[], rows=[])
            query_vec = [float(v.strip()) for v in m.group(1).split(",")]

            # Compute cosine distance for each document
            rows = []
            for d in self.documents:
                doc_vec = d[3]
                dist = self._cosine_distance(query_vec, doc_vec)
                rows.append([d[0], d[1], d[2], doc_vec, dist])
            return ResultSet(
                columns=["id", "content", "metadata", "embedding", "dist"],
                rows=rows,
            )

        # Delete: -langchain_docs(...) <- ..., Id = "id"
        if datalog.startswith("-langchain_docs"):
            import re

            m = re.search(r'Id = "([^"]+)"', datalog)
            if m:
                doc_id = m.group(1)
                self.documents = [d for d in self.documents if d[0] != doc_id]
            return ResultSet(columns=[], rows=[])

        return ResultSet(columns=[], rows=[])

    def _parse_insert(self, datalog: str) -> tuple[str, str, str, list[float]] | None:
        """Parse +langchain_docs("id", "content", "meta", [vec])."""
        import re

        # Match: "id", "content", "meta", [vec]
        m = re.match(
            r'\+langchain_docs\(\s*"((?:[^"\\]|\\.)*)"\s*,\s*'
            r'"((?:[^"\\]|\\.)*)"\s*,\s*'
            r'"((?:[^"\\]|\\.)*)"\s*,\s*'
            r"\[([^\]]+)\]\s*\)",
            datalog,
        )
        if not m:
            return None
        doc_id = m.group(1).replace('\\"', '"').replace("\\\\", "\\")
        content = m.group(2).replace('\\"', '"').replace("\\\\", "\\")
        metadata = m.group(3).replace('\\"', '"').replace("\\\\", "\\")
        vec = [float(v.strip()) for v in m.group(4).split(",")]
        return (doc_id, content, metadata, vec)

    def _cosine_distance(self, a: list[float], b: list[float]) -> float:
        import math

        dot = sum(x * y for x, y in zip(a, b, strict=True))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0 or norm_b == 0:
            return 1.0
        cos_sim = dot / (norm_a * norm_b)
        return 1.0 - cos_sim  # distance = 1 - similarity


# ── Tests ────────────────────────────────────────────────────────────


class TestSetup:
    async def test_setup_creates_relation(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.asetup()
        assert any("+langchain_docs(id:" in s for s in kg.executed)

    async def test_setup_idempotent(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.asetup()
        count = len(kg.executed)
        await store.asetup()
        assert len(kg.executed) == count

    def test_default_collection_name(self) -> None:
        store = InputLayerVectorStore(kg=MagicMock(), embedding=FakeEmbeddings())
        assert store.collection_name == "langchain_docs"

    def test_custom_collection_name(self) -> None:
        store = InputLayerVectorStore(
            kg=MagicMock(),
            embedding=FakeEmbeddings(),
            collection_name="my_docs",
        )
        assert store.collection_name == "my_docs"


class TestAdd:
    async def test_add_texts_returns_ids(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        ids = await store.aadd_texts(["hello", "world"])
        assert len(ids) == 2
        assert all(isinstance(i, str) for i in ids)

    async def test_add_texts_with_explicit_ids(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        result_ids = await store.aadd_texts(["a", "b"], ids=["id1", "id2"])
        assert result_ids == ["id1", "id2"]
        assert len(kg.documents) == 2
        assert kg.documents[0][0] == "id1"

    async def test_add_texts_with_metadata(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(
            ["hello"],
            metadatas=[{"source": "test", "page": 1}],
            ids=["doc1"],
        )
        assert kg.documents[0][0] == "doc1"
        meta = json.loads(kg.documents[0][2])
        assert meta == {"source": "test", "page": 1}

    async def test_add_documents(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        docs = [
            Document(page_content="first", metadata={"i": 1}),
            Document(page_content="second", metadata={"i": 2}),
        ]
        ids = await store.aadd_documents(docs)
        assert len(ids) == 2
        assert len(kg.documents) == 2


class TestSearch:
    async def test_similarity_search_basic(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(
            ["short", "a much longer piece of text here"],
            ids=["a", "b"],
        )
        results = await store.asimilarity_search("short", k=2)
        assert len(results) == 2
        assert all(isinstance(r, Document) for r in results)

    async def test_similarity_search_returns_k_docs(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts([f"text number {i}" for i in range(10)])
        results = await store.asimilarity_search("query", k=3)
        assert len(results) == 3

    async def test_similarity_search_with_score(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["hello", "world"], ids=["a", "b"])
        results = await store.asimilarity_search_with_score("hello", k=2)
        assert len(results) == 2
        for doc, score in results:
            assert isinstance(doc, Document)
            assert isinstance(score, float)

    async def test_similarity_search_by_vector(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["hello", "world"])
        results = await store.asimilarity_search_by_vector([0.1, 0.2, 0.5], k=2)
        assert len(results) == 2

    async def test_similarity_search_returns_metadata(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["hello"], metadatas=[{"source": "wiki"}], ids=["doc1"])
        results = await store.asimilarity_search("hello", k=1)
        assert results[0].metadata.get("source") == "wiki"


class TestGetByIds:
    async def test_get_by_ids(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["alice", "bob", "carol"], ids=["a", "b", "c"])

        docs = await store.aget_by_ids(["a", "c"])
        assert len(docs) == 2
        contents = {d.page_content for d in docs}
        assert contents == {"alice", "carol"}

    async def test_get_by_ids_missing(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        docs = await store.aget_by_ids(["nonexistent"])
        assert docs == []


class TestDelete:
    async def test_delete_by_ids(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["a", "b", "c"], ids=["id1", "id2", "id3"])
        assert len(kg.documents) == 3

        result = await store.adelete(["id2"])
        assert result is True
        assert len(kg.documents) == 2
        remaining_ids = {d[0] for d in kg.documents}
        assert remaining_ids == {"id1", "id3"}

    async def test_delete_none_returns_none(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        result = await store.adelete(None)
        assert result is None


class TestFromTexts:
    async def test_afrom_texts(self) -> None:
        kg = MockKG()
        store = await InputLayerVectorStore.afrom_texts(
            texts=["hello", "world"],
            embedding=FakeEmbeddings(),
            kg=kg,
        )
        assert isinstance(store, InputLayerVectorStore)
        assert len(kg.documents) == 2

    def test_from_texts_sync(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore.from_texts(
            texts=["hello", "world"],
            embedding=FakeEmbeddings(),
            kg=kg,
        )
        assert isinstance(store, InputLayerVectorStore)
        assert len(kg.documents) == 2

    async def test_afrom_texts_requires_kg(self) -> None:
        with pytest.raises(ValueError, match="requires kg"):
            await InputLayerVectorStore.afrom_texts(texts=["x"], embedding=FakeEmbeddings())

    async def test_afrom_texts_with_metadatas(self) -> None:
        kg = MockKG()
        store = await InputLayerVectorStore.afrom_texts(
            texts=["a", "b"],
            embedding=FakeEmbeddings(),
            metadatas=[{"i": 1}, {"i": 2}],
            kg=kg,
        )
        docs = await store.aget_by_ids([d[0] for d in kg.documents])
        assert {d.metadata.get("i") for d in docs} == {1, 2}


class TestSyncBridge:
    def test_sync_add_texts(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        ids = store.add_texts(["hello", "world"])
        assert len(ids) == 2
        assert len(kg.documents) == 2

    def test_sync_similarity_search(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        store.add_texts(["hello", "world"])
        results = store.similarity_search("hello", k=2)
        assert len(results) == 2

    def test_sync_get_by_ids(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        store.add_texts(["hello"], ids=["doc1"])
        docs = store.get_by_ids(["doc1"])
        assert len(docs) == 1
        assert docs[0].page_content == "hello"

    def test_sync_delete(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        store.add_texts(["a", "b"], ids=["id1", "id2"])
        result = store.delete(["id1"])
        assert result is True
        assert len(kg.documents) == 1


class TestAsRetriever:
    """The base VectorStore provides as_retriever() that wraps similarity_search."""

    def test_as_retriever_returns_retriever(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        retriever = store.as_retriever(search_kwargs={"k": 5})
        assert retriever is not None
        # It's a VectorStoreRetriever
        from langchain_core.vectorstores import VectorStoreRetriever

        assert isinstance(retriever, VectorStoreRetriever)

    async def test_as_retriever_invokes_search(self) -> None:
        kg = MockKG()
        store = InputLayerVectorStore(kg=kg, embedding=FakeEmbeddings())
        await store.aadd_texts(["hello", "world", "foo"])

        retriever = store.as_retriever(search_kwargs={"k": 2})
        results = await retriever.ainvoke("hello")
        assert len(results) == 2
        assert all(isinstance(d, Document) for d in results)
