"""InputLayerRetriever - LangChain BaseRetriever backed by a KnowledgeGraph."""

from __future__ import annotations

from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForRetrieverRun,
    CallbackManagerForRetrieverRun,
)
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import Field, model_validator

from inputlayer._sync import run_sync


class InputLayerRetriever(BaseRetriever):
    """Retrieve documents from an InputLayer KnowledgeGraph.

    Supports two modes:

    1. **Raw Datalog** — full control over the query logic::

        retriever = InputLayerRetriever(
            kg=kg,
            query="?Title, Content <- relevant_docs(Title, Content)",
            page_content_columns=["Content"],
            metadata_columns=["Title"],
        )

    2. **Vector search** — similarity search on a Relation::

        retriever = InputLayerRetriever(
            kg=kg,
            relation=Document,
            k=10,
            metric="cosine",
        )

    The ``query`` parameter can include ``{input}`` as a placeholder for the
    user's search string::

        query="?Doc, Score <- search(\\"{input}\\", Doc, Score)"

    Both sync and async paths are natively supported via ``run_sync`` and
    direct ``await``.
    """

    kg: Any  # KnowledgeGraph — typed as Any for Pydantic compatibility
    query: str | None = None
    relation: Any | None = None
    k: int = Field(default=10, description="Number of results to return")
    metric: str = Field(default="cosine", description="Distance metric for vector search")
    page_content_columns: list[str] = Field(
        default_factory=lambda: ["content"],
        description="Column(s) to concatenate as page_content",
    )
    metadata_columns: list[str] = Field(
        default_factory=list,
        description="Columns to include in Document.metadata",
    )
    score_column: str | None = Field(
        default=None,
        description="Column containing relevance score (added to metadata)",
    )

    model_config = {"arbitrary_types_allowed": True}  # noqa: RUF012

    @model_validator(mode="after")
    def _validate_mode(self) -> InputLayerRetriever:
        if self.query is None and self.relation is None:
            raise ValueError("Must provide either 'query' (Datalog) or 'relation' (vector search)")
        return self

    # ── Sync path ────────────────────────────────────────────────────

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        return run_sync(self._aget_relevant_documents(query, run_manager=run_manager))

    # ── Async path (native) ──────────────────────────────────────────

    async def _aget_relevant_documents(
        self,
        query: str,
        *,
        run_manager: AsyncCallbackManagerForRetrieverRun | CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        result = await self._execute_query(query)
        return self._to_documents(result.columns, result.rows)

    # ── Query execution ──────────────────────────────────────────────

    async def _execute_query(self, user_query: str) -> Any:
        if self.query is not None:
            datalog = self.query.replace("{input}", user_query)
            return await self.kg.execute(datalog)

        # Vector search mode
        from inputlayer.relation import Relation

        if self.relation is None or not issubclass(self.relation, Relation):
            raise ValueError("relation must be a Relation subclass for vector search")

        # For vector search, user_query should be parseable as a vector
        # or the caller should use a Datalog query with embedding logic
        return await self.kg.vector_search(
            self.relation,
            _parse_vector(user_query),
            k=self.k,
            metric=self.metric,
        )

    # ── Result mapping ───────────────────────────────────────────────

    def _to_documents(self, columns: list[str], rows: list[list[Any]]) -> list[Document]:
        docs = []
        for row in rows:
            row_dict = dict(zip(columns, row, strict=False))

            # Build page_content from specified columns
            content_parts = []
            for col in self.page_content_columns:
                val = row_dict.get(col)
                if val is not None:
                    content_parts.append(str(val))
            page_content = "\n".join(content_parts) if content_parts else str(row)

            # Build metadata from specified columns + score
            metadata: dict[str, Any] = {}
            for col in self.metadata_columns:
                if col in row_dict:
                    metadata[col] = row_dict[col]

            if self.score_column and self.score_column in row_dict:
                metadata["score"] = row_dict[self.score_column]

            # Include all columns not used for content as metadata if none specified
            if not self.metadata_columns:
                for col in columns:
                    if col not in self.page_content_columns and col != self.score_column:
                        metadata[col] = row_dict[col]

            docs.append(Document(page_content=page_content, metadata=metadata))

        return docs


def _parse_vector(text: str) -> list[float]:
    """Parse a string as a vector of floats.

    Accepts formats like "[1.0, 2.0, 3.0]" or "1.0 2.0 3.0".
    """
    cleaned = text.strip().strip("[]")
    parts = cleaned.replace(",", " ").split()
    return [float(p) for p in parts]
