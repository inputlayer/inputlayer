"""InputLayerRetriever - LangChain BaseRetriever backed by a KnowledgeGraph."""

from __future__ import annotations

import logging
import warnings
from collections.abc import Callable
from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForRetrieverRun,
    CallbackManagerForRetrieverRun,
)
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.retrievers import BaseRetriever
from pydantic import Field, model_validator

from inputlayer._sync import run_sync
from inputlayer.integrations.langchain.params import bind_params

logger = logging.getLogger(__name__)

class InputLayerRetriever(BaseRetriever):
    """Retrieve documents from an InputLayer KnowledgeGraph.

    Two modes:

    1. **Vector search** - similarity search on a Relation. Requires an
       ``embeddings`` instance to convert the user's query into a vector::

            retriever = InputLayerRetriever(
                kg=kg,
                relation=Article,
                embeddings=OpenAIEmbeddings(),
                k=10,
                metric="cosine",
            )

    2. **InputLayer Query Language (IQL)** - full control with safe
       parameter binding via ``:placeholders``. The query is the raw
       IQL body that follows ``?``. The user's invoke argument is
       bound to the parameter named in ``input_param`` (default
       ``"input"``)::

            retriever = InputLayerRetriever(
                kg=kg,
                query="?article(I, T, C, Cat), user_interest(:input, Cat)",
                page_content_columns=["C"],
                metadata_columns=["T", "Cat"],
            )

       For multi-parameter queries, supply a ``params`` dict or callable::

            retriever = InputLayerRetriever(
                kg=kg,
                query="?docs(T, C), score(T) > :min, user_interest(:input, T)",
                params=lambda q: {"input": q, "min": 0.5},
            )

    Both sync and async paths are supported natively.
    """

    kg: Any  # KnowledgeGraph - typed as Any for Pydantic compatibility
    query: str | None = None
    relation: Any | None = None
    embeddings: Embeddings | None = None
    vector_column: str | None = Field(
        default=None,
        description="Vector column on the relation; auto-detected if omitted",
    )
    k: int = Field(default=10, description="Number of results to return")
    metric: str = Field(
        default="cosine",
        description="Distance function: cosine, euclidean, dot",
    )
    input_param: str = Field(
        default="input",
        description="Name of the placeholder filled with the invoke() argument",
    )
    params: dict[str, Any] | Callable[[str], dict[str, Any]] | None = None
    page_content_columns: list[str] | None = Field(
        default=None,
        description="Column(s) to concatenate as page_content (defaults to ['content'])",
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
            raise ValueError(
                "Must provide either 'query' (IQL) or 'relation' (vector search)"
            )
        if self.query is not None and self.relation is not None:
            raise ValueError("Provide either 'query' or 'relation', not both")
        if self.relation is not None and self.embeddings is None:
            raise ValueError(
                "Vector retriever requires an `embeddings` instance "
                "(e.g. OpenAIEmbeddings()). To pass a pre-computed vector, "
                "use IQL mode with a :placeholder."
            )
        return self

    # ── Sync path ────────────────────────────────────────────────────

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        return run_sync(self._do_retrieve(query))

    # ── Async path (native) ──────────────────────────────────────────

    async def _aget_relevant_documents(
        self,
        query: str,
        *,
        run_manager: AsyncCallbackManagerForRetrieverRun,
    ) -> list[Document]:
        return await self._do_retrieve(query)

    async def _do_retrieve(self, query: str) -> list[Document]:
        if self.relation is not None:
            return await self._vector_documents(query)
        return await self._iql_documents(query)

    # ── IQL mode ─────────────────────────────────────────────────────

    async def _iql_documents(self, user_query: str) -> list[Document]:
        if self.query is None:
            raise RuntimeError("Retriever has neither a relation nor a query")
        params = self._resolve_params(user_query)
        compiled = bind_params(self.query, params)
        logger.debug("IQL retriever query: %s", compiled)
        result = await self.kg.execute(compiled)
        if result.columns == ["error"]:
            msg = result.rows[0][0] if result.rows else "unknown error"
            raise RuntimeError(f"InputLayer rejected query: {msg}")
        return self._to_documents(result.columns, result.rows, hidden_columns=set())

    def _resolve_params(self, user_query: str) -> dict[str, Any]:
        if callable(self.params):
            return self.params(user_query)
        out: dict[str, Any] = {}
        if isinstance(self.params, dict):
            out.update(self.params)
        # input_param always wins over static params.
        out[self.input_param] = user_query
        return out

    # ── Vector mode ──────────────────────────────────────────────────

    async def _vector_documents(self, user_query: str) -> list[Document]:
        """Execute vector search via ``kg.vector_search()``."""
        if self.embeddings is None:
            raise RuntimeError("Vector retriever has no embeddings instance")

        from inputlayer.relation import Relation

        rel = self.relation
        cols = Relation._get_columns(rel)
        vec_col = self._resolve_vector_column(cols)

        vec = await self.embeddings.aembed_query(user_query)
        result = await self.kg.vector_search(
            rel,
            [float(v) for v in vec],
            column=vec_col,
            k=self.k,
            metric=self.metric,
        )

        # Hide the vector column from auto-metadata so we don't leak the
        # raw embedding into Document.metadata. Promote the synthetic
        # "Dist" column to the score (unless caller already set one).
        hidden = {vec_col}
        score_override = self.score_column or "Dist"
        # vector_search uses capitalized variable names in the IQL it
        # emits, so the engine returns capitalized columns. Suppress the
        # case warning since the case mismatch is expected and
        # user-controlled ``page_content_columns`` will normally be
        # lowercase.
        return self._to_documents(
            result.columns,
            result.rows,
            hidden_columns=hidden,
            score_override=score_override,
            suppress_case_warnings=True,
        )

    def _resolve_vector_column(self, cols: list[str]) -> str:
        if self.vector_column is not None:
            return self.vector_column
        from inputlayer.relation import Relation
        from inputlayer.types import Vector, _VectorMeta

        col_types = Relation._get_column_types(self.relation)
        for c in cols:
            tp = col_types.get(c)
            if tp is Vector or isinstance(tp, _VectorMeta):
                return c
        raise ValueError(
            f"Relation {Relation._resolve_name(self.relation)} has no Vector column; "
            f"set `vector_column` explicitly."
        )

    # ── Result mapping ───────────────────────────────────────────────

    def _to_documents(
        self,
        columns: list[str],
        rows: list[list[Any]],
        hidden_columns: set[str] | None = None,
        score_override: str | None = None,
        suppress_case_warnings: bool = False,
    ) -> list[Document]:
        hidden_lower = {h.lower() for h in (hidden_columns or set())}
        col_lookup = {c.lower(): c for c in columns}

        def resolve(name: str, kind: str) -> str | None:
            if name in columns:
                return name
            actual = col_lookup.get(name.lower())
            if actual is not None:
                if not suppress_case_warnings:
                    warnings.warn(
                        f"{kind} {name!r} matched column {actual!r} "
                        f"case-insensitively; prefer the exact name to avoid surprises.",
                        stacklevel=4,
                    )
                return actual
            return None

        resolved_content: list[str] = []
        content_cols = (
            self.page_content_columns if self.page_content_columns is not None else ["content"]
        )
        explicit_content = self.page_content_columns is not None
        for c in content_cols:
            actual = resolve(c, "page_content_columns")
            if actual is None:
                if explicit_content:
                    raise KeyError(
                        f"page_content_columns entry {c!r} not found in result "
                        f"columns {columns!r}"
                    )
            else:
                resolved_content.append(actual)

        # Map: user-supplied name → actual result column. Using the
        # user's name as the metadata key keeps the API stable when the
        # engine projects with capitalized variable names.
        resolved_metadata: list[tuple[str, str]] = []
        for c in self.metadata_columns:
            actual = resolve(c, "metadata_columns")
            if actual is None:
                raise KeyError(
                    f"metadata_columns entry {c!r} not found in result columns {columns!r}"
                )
            resolved_metadata.append((c, actual))

        score_name = score_override if score_override is not None else self.score_column
        resolved_score = resolve(score_name, "score_column") if score_name else None

        meta_actual_set = {actual for _, actual in resolved_metadata}

        fallback_content: str | None = None
        if not resolved_content:
            fallback_content = next(
                (
                    c
                    for c in columns
                    if c not in meta_actual_set
                    and c != resolved_score
                    and c.lower() not in hidden_lower
                ),
                None,
            )

        used_for_content = set(resolved_content)
        if fallback_content is not None:
            used_for_content.add(fallback_content)

        docs: list[Document] = []
        for row in rows:
            row_dict = dict(zip(columns, row, strict=True))

            content_parts: list[str] = []
            for col in resolved_content:
                val = row_dict.get(col)
                if val is not None:
                    content_parts.append(str(val))

            if content_parts:
                page_content = "\n".join(content_parts)
            elif fallback_content is not None:
                page_content = str(row_dict[fallback_content])
            else:
                page_content = ""

            metadata: dict[str, Any] = {}
            if resolved_metadata:
                for user_name, actual in resolved_metadata:
                    metadata[user_name] = row_dict[actual]
            else:
                for col in columns:
                    if (
                        col in used_for_content
                        or col == resolved_score
                        or col.lower() in hidden_lower
                    ):
                        continue
                    metadata[col] = row_dict[col]

            if resolved_score is not None:
                metadata["score"] = row_dict[resolved_score]

            docs.append(Document(page_content=page_content, metadata=metadata))

        return docs
