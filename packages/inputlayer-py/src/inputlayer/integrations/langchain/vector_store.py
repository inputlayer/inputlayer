"""InputLayerVectorStore - LangChain VectorStore backed by an InputLayer Relation."""

from __future__ import annotations

import logging
import uuid
import warnings
from collections.abc import Iterable
from typing import Any, get_args

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from inputlayer._sync import run_sync
from inputlayer.relation import Relation

logger = logging.getLogger(__name__)


_METRIC_MAP = {
    "cosine": "cosine",
    "euclidean": "euclidean",
    "dot": "dot",
    "dot_product": "dot",
}

_VALID_METRICS = set(_METRIC_MAP)


def _resolve_metric(metric: str) -> str:
    fn = _METRIC_MAP.get(metric)
    if fn is None:
        raise ValueError(
            f"Unknown metric {metric!r}; "
            f"supported values: {sorted(_VALID_METRICS)}"
        )
    return fn


def _distance_to_relevance(dist: float, metric: str) -> float:
    """Convert a distance/score value to a [0, 1]-ish relevance score.

    Each metric family has its own range, so a single ``1 - dist``
    only works for cosine.
    """
    if metric == "dot":
        # Dot-product is already a similarity (higher = more similar).
        return float(dist)
    if metric == "euclidean":
        # Euclidean distance is unbounded; squash into (0, 1].
        return 1.0 / (1.0 + dist)
    # cosine distance is in [0, 2]; similarity = 1 - distance.
    return 1.0 - dist


class InputLayerVectorStore(VectorStore):
    """LangChain VectorStore backed by an InputLayer Relation.

    The relation must define at minimum:
        - an ``id`` field (str or int)
        - a text content field (default name: ``content``)
        - a vector field (auto-detected from ``Vector`` type, or specified)

    Optional metadata fields are stored as plain columns.

    .. note::

        Embedding calls (``add_texts``, ``similarity_search``, etc.) delegate
        to the ``Embeddings`` instance you provide.  If the embedding provider
        hangs or is slow, those calls block indefinitely.  Configure timeouts
        on the provider itself, e.g.
        ``OpenAIEmbeddings(timeout=30, max_retries=2)``.

    Usage::

        class Chunk(Relation):
            id: str
            content: str
            source: str
            embedding: Vector[1536]

        vs = InputLayerVectorStore(
            kg=kg, relation=Chunk, embeddings=OpenAIEmbeddings(),
        )
        vs.add_documents([Document(page_content="...", metadata={"source": "foo"})])
        results = vs.similarity_search("query", k=5)

        # Or one-shot:
        vs = InputLayerVectorStore.from_documents(
            docs, OpenAIEmbeddings(), kg=kg, relation=Chunk,
        )

        # Plug into any LangChain chain:
        retriever = vs.as_retriever(search_kwargs={"k": 5})
    """

    def __init__(
        self,
        kg: Any,
        relation: type[Relation],
        embeddings: Embeddings,
        *,
        content_field: str = "content",
        vector_field: str | None = None,
        id_field: str = "id",
        ensure_schema: bool = False,
    ) -> None:
        self._kg = kg
        self._relation = relation
        self._embeddings = embeddings
        self._content_field = content_field
        self._id_field = id_field

        cols = Relation._get_column_types(relation)
        col_names = list(cols.keys())
        if any(not c or not c.isidentifier() for c in col_names):
            bad = [c for c in col_names if not c or not c.isidentifier()]
            raise ValueError(
                f"Relation {relation.__name__} has invalid column name(s) "
                f"{bad!r}; column names must be non-empty Python identifiers."
            )
        if content_field not in cols:
            raise ValueError(
                f"Relation {relation.__name__} has no field {content_field!r}; "
                f"pass `content_field=` to override."
            )
        if id_field not in cols:
            raise ValueError(
                f"Relation {relation.__name__} has no field {id_field!r}; "
                f"pass `id_field=` to override."
            )

        if vector_field is None:
            from inputlayer.types import Vector, _VectorMeta

            for c, tp in cols.items():
                if tp is Vector or isinstance(tp, _VectorMeta):
                    vector_field = c
                    break
            if vector_field is None:
                raise ValueError(
                    f"Relation {relation.__name__} has no Vector column; "
                    f"pass `vector_field=` explicitly."
                )
        self._vector_field = vector_field

        # Metadata = everything else. Validate that everything left over
        # is either optional or has a default - otherwise an `add_texts`
        # call will explode mid-batch with a confusing Pydantic error.
        self._metadata_fields = [
            c
            for c in Relation._get_columns(relation)
            if c not in (content_field, vector_field, id_field)
        ]
        self._required_metadata_fields = _required_metadata_fields(
            relation, self._metadata_fields
        )

        if ensure_schema:
            # Optional: callers in pure-sync contexts can opt in. In an
            # async context, prefer ``await kg.define(relation)`` before
            # constructing the store - mixing run_sync with an already-
            # running event loop attaches futures to the wrong loop.
            run_sync(kg.define(relation))

    # ── LangChain VectorStore interface ──────────────────────────────

    @property
    def embeddings(self) -> Embeddings:
        return self._embeddings

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        return run_sync(self.aadd_texts(texts, metadatas=metadatas, ids=ids, **kwargs))

    async def aadd_texts(
        self,
        texts: Iterable[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
        **kwargs: Any,
    ) -> list[str]:
        texts_list = list(texts)
        if not texts_list:
            return []

        vectors = await self._embeddings.aembed_documents(texts_list)
        ids_out = ids or [str(uuid.uuid4()) for _ in texts_list]
        metas = metadatas or [{} for _ in texts_list]

        rows: list[Relation] = []
        for i, (text, vec, meta, _id) in enumerate(
            zip(texts_list, vectors, metas, ids_out, strict=True)
        ):
            missing = [
                f for f in self._required_metadata_fields if f not in meta
            ]
            if missing:
                raise ValueError(
                    f"Document {i}: required relation field(s) {missing} not "
                    f"present in metadata. Provide them in the `metadatas` arg "
                    f"or make the field Optional in your Relation schema."
                )
            unknown = [
                k
                for k in meta
                if k not in self._metadata_fields and k != self._id_field
            ]
            if unknown:
                logger.warning(
                    "Document %d: metadata key(s) %s not present in "
                    "relation %s; values will be dropped. "
                    "Add columns to the Relation or remove the keys to "
                    "silence this warning.",
                    i,
                    unknown,
                    self._relation.__name__,
                )

            payload: dict[str, Any] = {
                self._id_field: _id,
                self._content_field: text,
                self._vector_field: vec,
            }
            for f in self._metadata_fields:
                if f in meta:
                    payload[f] = meta[f]
            rows.append(self._relation(**payload))

        await self._kg.insert(rows)
        return ids_out

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        return run_sync(
            self.asimilarity_search(query, k=k, filter=filter, **kwargs)
        )

    async def asimilarity_search(
        self,
        query: str,
        k: int = 4,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        docs_and_scores = await self.asimilarity_search_with_score(
            query, k=k, filter=filter, **kwargs
        )
        return [d for d, _ in docs_and_scores]

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Document, float]]:
        return run_sync(
            self.asimilarity_search_with_score(query, k=k, filter=filter, **kwargs)
        )

    async def asimilarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: dict[str, Any] | None = None,
        *,
        metric: str = "cosine",
        **kwargs: Any,
    ) -> list[tuple[Document, float]]:
        vec = await self._embeddings.aembed_query(query)
        return await self._search_by_vector(vec, k=k, metric=metric, filter=filter)

    def similarity_search_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        async def _go() -> list[Document]:
            results = await self._search_by_vector(
                embedding,
                k=k,
                metric=kwargs.get("metric", "cosine"),
                filter=filter,
            )
            return [d for d, _ in results]

        return run_sync(_go())

    def _build_filter_clauses(
        self,
        filter: dict[str, Any] | None,
        *,
        context: str = "similarity_search",
    ) -> list[str] | None:
        """Build IQL filter clauses from a metadata filter dict.

        Returns ``None`` (not ``[]``) when there are no clauses, matching
        the ``extra_iql_clauses`` API of ``kg.vector_search()``.
        """
        if not filter:
            return None
        from inputlayer.integrations.langchain.params import iql_literal

        cols = Relation._get_columns(self._relation)
        cap = {c: c[:1].upper() + c[1:] for c in cols}
        clauses: list[str] = []
        for key, value in filter.items():
            if key not in cap:
                warnings.warn(
                    f"{context} filter key {key!r} is not a column "
                    f"of {self._relation.__name__}; ignoring.",
                    stacklevel=5,
                )
                continue
            clauses.append(f"{cap[key]} = {iql_literal(value)}")
        return clauses or None

    async def _search_by_vector(
        self,
        embedding: list[float],
        *,
        k: int,
        metric: str,
        filter: dict[str, Any] | None = None,
    ) -> list[tuple[Document, float]]:
        """Run vector similarity via ``kg.vector_search()``."""
        metric = _resolve_metric(metric)
        result = await self._kg.vector_search(
            self._relation,
            [float(v) for v in embedding],
            column=self._vector_field,
            k=k,
            metric=metric,
            extra_iql_clauses=self._build_filter_clauses(filter),
        )
        return self._rows_to_documents(result.columns, result.rows)

    # ── Maximal Marginal Relevance ───────────────────────────────────

    def max_marginal_relevance_search(
        self,
        query: str,
        k: int = 4,
        fetch_k: int = 20,
        lambda_mult: float = 0.5,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        return run_sync(
            self.amax_marginal_relevance_search(
                query, k=k, fetch_k=fetch_k, lambda_mult=lambda_mult,
                filter=filter, **kwargs,
            )
        )

    async def amax_marginal_relevance_search(
        self,
        query: str,
        k: int = 4,
        fetch_k: int = 20,
        lambda_mult: float = 0.5,
        filter: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        """Maximal marginal relevance over the same vector path.

        Embeds the query, fetches the top ``fetch_k`` candidates by
        cosine, then iteratively picks the document that maximizes
        ``lambda_mult * relevance - (1 - lambda_mult) * max_similarity_to_selected``.
        Cosine math runs client-side because the engine returns the raw
        embedding column for each candidate row.
        """
        metric = _resolve_metric(kwargs.get("metric", "cosine"))
        query_vec = await self._embeddings.aembed_query(query)
        candidates = await self._fetch_candidates_with_vectors(
            query_vec,
            k=fetch_k,
            metric=metric,
            filter=filter,
        )
        if not candidates:
            return []

        # candidates: list[(Document, distance, embedding)]
        selected: list[int] = []
        remaining = list(range(len(candidates)))
        # Greedy selection.
        while remaining and len(selected) < k:
            best_idx = -1
            best_score = -float("inf")
            for idx in remaining:
                _, dist, vec = candidates[idx]
                relevance = _distance_to_relevance(dist, metric)
                if not selected:
                    diversity_pen = 0.0
                else:
                    sims = [
                        _cosine_similarity(vec, candidates[s][2])
                        for s in selected
                    ]
                    diversity_pen = max(sims)
                score = lambda_mult * relevance - (1 - lambda_mult) * diversity_pen
                if score > best_score:
                    best_score = score
                    best_idx = idx
            selected.append(best_idx)
            remaining.remove(best_idx)
        return [candidates[i][0] for i in selected]

    async def _fetch_candidates_with_vectors(
        self,
        embedding: list[float],
        *,
        k: int,
        metric: str,
        filter: dict[str, Any] | None = None,
    ) -> list[tuple[Document, float, list[float]]]:
        """Like ``_search_by_vector`` but also returns each row's embedding.

        MMR needs the actual embeddings, which the public document path
        deliberately strips. We use ``kg.vector_search()`` then extract
        the vector column from each row before mapping to Documents.

        Expects *metric* to be already resolved (canonical form).
        """
        result = await self._kg.vector_search(
            self._relation,
            [float(v) for v in embedding],
            column=self._vector_field,
            k=k,
            metric=metric,
            extra_iql_clauses=self._build_filter_clauses(
                filter, context="max_marginal_relevance_search"
            ),
        )

        # Find the vector column index in the result.
        vec_col_idx = None
        for i, c in enumerate(result.columns):
            if c.lower() == self._vector_field.lower():
                vec_col_idx = i
                break

        if vec_col_idx is None:
            raise ValueError(
                f"Could not find vector column {self._vector_field!r} in "
                f"result columns {result.columns!r}. MMR requires the raw "
                f"vectors to compute diversity. Check that your Relation "
                f"schema includes the vector column and that the engine "
                f"returns it in query results."
            )

        out: list[tuple[Document, float, list[float]]] = []
        docs_and_scores = self._rows_to_documents(result.columns, result.rows)
        for (doc, score), row in zip(docs_and_scores, result.rows, strict=True):
            raw = row[vec_col_idx] if vec_col_idx is not None else None
            vec = list(raw) if raw is not None else []
            out.append((doc, score, vec))
        return out

    def delete(
        self, ids: list[Any] | None = None, **kwargs: Any
    ) -> bool | None:
        """Delete rows by id.

        Returns ``True`` if the engine confirmed the deletion. ``ids=None``
        is treated as a no-op (LangChain's contract requires it to be a
        meaningful default; we refuse to drop the whole relation by
        accident - call ``kg.execute('-relation[(...)]')`` explicitly if
        you really want that).
        """
        if not ids:
            return None
        return run_sync(self.adelete(ids=ids, **kwargs))

    async def adelete(
        self, ids: list[Any] | None = None, **kwargs: Any
    ) -> bool | None:
        if not ids:
            return None
        from inputlayer.integrations.langchain.params import iql_literal

        rel_name = Relation._resolve_name(self._relation)
        # ``-relation[(id1,), (id2,)]`` only works when id is the sole
        # column. Use the conditional-delete form keyed on id_field, one
        # statement per id, which is universally supported.
        cap_id = self._id_field[:1].upper() + self._id_field[1:]
        cols = Relation._get_columns(self._relation)
        cap = {c: c[:1].upper() + c[1:] for c in cols}
        var_list = ", ".join(cap[c] for c in cols)
        for _id in ids:
            iql = (
                f"-{rel_name}({var_list}) <- {rel_name}({var_list}), "
                f"{cap_id} = {iql_literal(_id)}"
            )
            await self._kg.execute(iql)
        return True

    @classmethod
    def from_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: list[dict[str, Any]] | None = None,
        *,
        kg: Any | None = None,
        relation: type[Relation] | None = None,
        ids: list[Any] | None = None,
        content_field: str = "content",
        vector_field: str | None = None,
        id_field: str = "id",
        **kwargs: Any,
    ) -> InputLayerVectorStore:
        """Build a vector store from texts.

        Unlike most LangChain vector stores, ``InputLayerVectorStore``
        requires a knowledge graph and a target relation; there is no
        sensible default. We accept ``kg`` and ``relation`` as
        keyword-only parameters and raise a clear error when either is
        missing rather than letting Pydantic surface an opaque trace.
        """
        if kg is None or relation is None:
            raise ValueError(
                "InputLayerVectorStore.from_texts requires `kg=` and "
                "`relation=` keyword arguments. There is no default "
                "knowledge graph - construct one with "
                "`il.knowledge_graph(name)` and a Relation subclass that "
                "describes the target schema."
            )
        store = cls(
            kg=kg,
            relation=relation,
            embeddings=embedding,
            content_field=content_field,
            vector_field=vector_field,
            id_field=id_field,
        )
        store.add_texts(texts, metadatas=metadatas, ids=ids)
        return store

    # ── Helpers ──────────────────────────────────────────────────────

    def _rows_to_documents(
        self, columns: list[str], rows: list[list[Any]]
    ) -> list[tuple[Document, float]]:
        out: list[tuple[Document, float]] = []
        col_lookup = {c.lower(): c for c in columns}
        content_col = col_lookup.get(self._content_field.lower(), self._content_field)
        score_col = next(
            (c for c in columns if c.lower() in ("dist", "score")), None
        )

        # Build a map from result-column → canonical schema-column name so
        # that metadata keys are stable regardless of whether the engine
        # returned schema names (single-relation projection) or variable
        # names (computed-expression projection).
        schema_cols = Relation._get_columns(self._relation)
        schema_lookup = {c.lower(): c for c in schema_cols}

        def canonical(col: str) -> str:
            return schema_lookup.get(col.lower(), col)

        for row in rows:
            row_dict = dict(zip(columns, row, strict=True))
            content = str(row_dict.get(content_col, ""))
            metadata: dict[str, Any] = {}
            for k, v in row_dict.items():
                if k in (content_col, score_col):
                    continue
                if k.lower() == self._vector_field.lower():
                    continue
                metadata[canonical(k)] = v
            score = float(row_dict[score_col]) if score_col else 0.0
            out.append((Document(page_content=content, metadata=metadata), score))
        return out


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity in pure Python.

    Returns 0 when either vector is empty or has zero magnitude rather
    than raising a ZeroDivisionError; the caller is doing greedy MMR
    selection where falling back to "no penalty" is the right behavior.
    """
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b, strict=True):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return float(dot / ((na ** 0.5) * (nb ** 0.5)))


def _required_metadata_fields(
    relation: type[Relation], metadata_fields: list[str]
) -> list[str]:
    """Return metadata fields that the Relation requires (no default, not Optional)."""
    required: list[str] = []
    model_fields = getattr(relation, "model_fields", {})
    for f in metadata_fields:
        info = model_fields.get(f)
        if info is None:
            continue
        # Pydantic v2 marks required fields with PydanticUndefined defaults.
        if not getattr(info, "is_required", lambda: True)():
            continue
        annotation = getattr(info, "annotation", None)
        # Optional types (X | None) are not required even if no default.
        if annotation is not None and type(None) in get_args(annotation):
            continue
        required.append(f)
    return required
