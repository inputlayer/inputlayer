"""InputLayerVectorStore — LangChain VectorStore backed by InputLayer.

Implements LangChain's standard ``VectorStore`` interface so InputLayer
can be used as a drop-in replacement for Chroma, Pinecone, Weaviate, etc.
in any existing LangChain RAG tutorial or chain.

The store creates a single relation per instance::

    +<collection>(id: string, content: string,
                  metadata: string, embedding: vector)

And uses InputLayer's vector_search for similarity queries.

Usage::

    from langchain_openai import OpenAIEmbeddings
    from inputlayer.integrations.langchain import InputLayerVectorStore

    embeddings = OpenAIEmbeddings()

    # From texts
    store = await InputLayerVectorStore.afrom_texts(
        texts=["Python is a language", "Rust is fast"],
        embedding=embeddings,
        kg=kg,
    )

    # Search
    docs = await store.asimilarity_search("programming", k=3)

    # As a LangChain retriever
    retriever = store.as_retriever(search_kwargs={"k": 5})
    chain = retriever | prompt | llm
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Sequence
from typing import Any

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from inputlayer._sync import run_sync


def _escape(s: str) -> str:
    """Escape a string for safe Datalog literal storage."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _vec_literal(vec: list[float]) -> str:
    """Format a vector as a Datalog literal: [v1, v2, v3]."""
    return "[" + ", ".join(str(v) for v in vec) + "]"


class InputLayerVectorStore(VectorStore):
    """LangChain VectorStore backed by an InputLayer KnowledgeGraph.

    Stores documents as facts in a single relation, with embeddings
    queried via InputLayer's vector_search.

    Args:
        kg: An InputLayer KnowledgeGraph handle.
        embedding: Embeddings model used to encode texts.
        collection_name: Name of the relation to store documents in
            (default: "langchain_docs"). Must be a valid Datalog
            relation name (lowercase, snake_case).
        metric: Distance metric for similarity search
            (cosine, euclidean, dot, manhattan).
    """

    def __init__(
        self,
        kg: Any,
        embedding: Embeddings,
        *,
        collection_name: str = "langchain_docs",
        metric: str = "cosine",
    ) -> None:
        self.kg = kg
        self._embedding = embedding
        self.collection_name = collection_name
        self.metric = metric
        self._setup_done = False

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    # ── Setup ────────────────────────────────────────────────────────

    async def asetup(self) -> None:
        """Create the underlying relation if it doesn't exist (idempotent)."""
        if self._setup_done:
            return
        await self.kg.execute(
            f"+{self.collection_name}(id: string, content: string, "
            f"metadata: string, embedding: vector)"
        )
        self._setup_done = True

    def setup(self) -> None:
        run_sync(self.asetup())

    # ── Async API (native) ───────────────────────────────────────────

    async def aadd_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        await self.asetup()

        text_list = list(texts)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in text_list]
        if metadatas is None:
            metadatas = [{} for _ in text_list]

        # Embed all texts in one call (most embedding APIs are batch-friendly)
        vectors = await self._aembed(text_list)

        for doc_id, text, meta, vec in zip(ids, text_list, metadatas, vectors, strict=True):
            meta_json = json.dumps(meta)
            await self.kg.execute(
                f"+{self.collection_name}("
                f'"{_escape(doc_id)}", '
                f'"{_escape(text)}", '
                f'"{_escape(meta_json)}", '
                f"{_vec_literal(vec)})"
            )

        return ids

    async def aadd_documents(self, documents: list[Document], **kwargs: Any) -> list[str]:
        texts = [d.page_content for d in documents]
        metadatas = [d.metadata for d in documents]
        ids = kwargs.pop("ids", None) or [d.id for d in documents if d.id]
        if len(ids) != len(texts):
            ids = None  # fall back to auto-generation
        return await self.aadd_texts(texts, metadatas=metadatas, ids=ids, **kwargs)

    async def asimilarity_search(self, query: str, k: int = 4, **kwargs: Any) -> list[Document]:
        results = await self.asimilarity_search_with_score(query, k=k, **kwargs)
        return [doc for doc, _ in results]

    async def asimilarity_search_by_vector(
        self, embedding: list[float], k: int = 4, **kwargs: Any
    ) -> list[Document]:
        results = await self._asearch_with_score_by_vector(embedding, k=k, **kwargs)
        return [doc for doc, _ in results]

    async def asimilarity_search_with_score(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> list[tuple[Document, float]]:
        await self.asetup()
        vec = (await self._aembed([query]))[0]
        return await self._asearch_with_score_by_vector(vec, k=k, **kwargs)

    async def _asearch_with_score_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        **kwargs: Any,
    ) -> list[tuple[Document, float]]:
        await self.asetup()

        # Use raw Datalog with cosine/euclidean/etc + ordering by distance.
        # We can't use top_k aggregation in a one-liner query, so we fetch
        # all rows with their distance and sort+limit in Python.
        # In production with large stores, this would use the HNSW index
        # via kg.vector_search() — but that requires the index to be
        # explicitly created. For now we use a simple scan.
        query = (
            f"?{self.collection_name}(Id, Content, Metadata, Emb), "
            f"Dist = {self.metric}(Emb, {_vec_literal(embedding)})"
        )
        result = await self.kg.execute(query)

        docs_with_scores: list[tuple[Document, float]] = []
        for row in result.rows:
            # Columns may include bound prefix or not — parse from end:
            # last col is Dist, then Emb, Metadata, Content, Id
            try:
                dist = float(row[-1])
                metadata_str = str(row[-3])
                content = str(row[-4])
                doc_id = str(row[-5])
            except (IndexError, ValueError, TypeError):
                continue

            try:
                metadata = json.loads(metadata_str)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

            doc = Document(id=doc_id, page_content=content, metadata=metadata)
            docs_with_scores.append((doc, dist))

        # Sort by distance ascending (lower is better for cosine/euclidean)
        docs_with_scores.sort(key=lambda x: x[1])
        return docs_with_scores[:k]

    async def aget_by_ids(self, ids: Sequence[str], /) -> list[Document]:
        await self.asetup()
        docs: list[Document] = []
        for doc_id in ids:
            r = await self.kg.execute(
                f'?{self.collection_name}("{_escape(doc_id)}", Content, Metadata, Emb)'
            )
            if r.rows:
                row = r.rows[0]
                content = str(row[-3])
                metadata_str = str(row[-2])
                try:
                    metadata = json.loads(metadata_str)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
                docs.append(Document(id=doc_id, page_content=content, metadata=metadata))
        return docs

    async def adelete(self, ids: list[str] | None = None, **kwargs: Any) -> bool | None:
        await self.asetup()
        if ids is None:
            return None
        for doc_id in ids:
            # Conditional delete by id
            await self.kg.execute(
                f"-{self.collection_name}(Id, Content, Metadata, Emb) <- "
                f"{self.collection_name}(Id, Content, Metadata, Emb), "
                f'Id = "{_escape(doc_id)}"'
            )
        return True

    # ── Sync API (via run_sync bridge) ───────────────────────────────

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        return run_sync(self.aadd_texts(texts, metadatas=metadatas, ids=ids, **kwargs))

    def add_documents(self, documents: list[Document], **kwargs: Any) -> list[str]:
        return run_sync(self.aadd_documents(documents, **kwargs))

    def similarity_search(self, query: str, k: int = 4, **kwargs: Any) -> list[Document]:
        return run_sync(self.asimilarity_search(query, k=k, **kwargs))

    def similarity_search_by_vector(
        self, embedding: list[float], k: int = 4, **kwargs: Any
    ) -> list[Document]:
        return run_sync(self.asimilarity_search_by_vector(embedding, k=k, **kwargs))

    def similarity_search_with_score(
        self, *args: Any, **kwargs: Any
    ) -> list[tuple[Document, float]]:
        return run_sync(self.asimilarity_search_with_score(*args, **kwargs))

    def get_by_ids(self, ids: Sequence[str], /) -> list[Document]:
        return run_sync(self.aget_by_ids(ids))

    def delete(self, ids: list[str] | None = None, **kwargs: Any) -> bool | None:
        return run_sync(self.adelete(ids, **kwargs))

    # ── Embedding helper ─────────────────────────────────────────────

    async def _aembed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts. Falls back to sync embed if async not available."""
        if hasattr(self._embedding, "aembed_documents"):
            return await self._embedding.aembed_documents(texts)
        return self._embedding.embed_documents(texts)

    # ── Class constructors ───────────────────────────────────────────

    @classmethod
    def from_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> InputLayerVectorStore:
        """Build a vector store from texts (sync).

        Required kwargs:
            kg: InputLayer KnowledgeGraph handle.

        Optional kwargs:
            collection_name: Relation name (default: langchain_docs).
            metric: Distance metric (default: cosine).
        """
        kg = kwargs.pop("kg", None)
        if kg is None:
            raise ValueError("InputLayerVectorStore.from_texts requires kg=<handle>")

        store = cls(
            kg=kg,
            embedding=embedding,
            collection_name=kwargs.pop("collection_name", "langchain_docs"),
            metric=kwargs.pop("metric", "cosine"),
        )
        store.add_texts(texts, metadatas=metadatas, ids=ids)
        return store

    @classmethod
    async def afrom_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict[str, Any]] | None = None,
        *,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> InputLayerVectorStore:
        """Build a vector store from texts (async)."""
        kg = kwargs.pop("kg", None)
        if kg is None:
            raise ValueError("InputLayerVectorStore.afrom_texts requires kg=<handle>")

        store = cls(
            kg=kg,
            embedding=embedding,
            collection_name=kwargs.pop("collection_name", "langchain_docs"),
            metric=kwargs.pop("metric", "cosine"),
        )
        await store.aadd_texts(texts, metadatas=metadatas, ids=ids)
        return store
