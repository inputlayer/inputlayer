"""Exception hierarchy for the InputLayer OLM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from inputlayer._protocol import ErrorCode, ResultResponse, StatementError

# Longest program text an error message repeats.
_QUERY_PREVIEW_CHARS = 200


class InputLayerError(Exception):
    """Base exception for all InputLayer errors."""


class InputLayerConnectionError(InputLayerError):
    """Failed to connect or lost connection to the server."""


# Backward-compatible alias (shadows the builtin, kept for existing code).
ConnectionError = InputLayerConnectionError


class AuthenticationError(InputLayerError):
    """Authentication failed (bad credentials or API key)."""


class SchemaConflictError(InputLayerError):
    """Schema definition conflicts with an existing schema."""

    def __init__(
        self,
        message: str,
        *,
        existing_schema: dict | None = None,
        proposed_schema: dict | None = None,
        conflicts: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.existing_schema = existing_schema
        self.proposed_schema = proposed_schema
        self.conflicts = conflicts or []


class ValidationError(InputLayerError):
    """Data validation failed (type mismatch, constraint violation)."""

    def __init__(self, message: str, *, details: list[dict] | None = None) -> None:
        super().__init__(message)
        self.details = details or []


class QueryTimeoutError(InputLayerError):
    """Query exceeded the configured timeout."""


class QueryError(InputLayerError):
    """The engine rejected a program: it answered with an ``error`` frame.

    ``code`` says why (``validation``, ``not_found``, ``conflict``,
    ``unsupported`` or ``internal``). It is ``None`` when the failure has no
    statement cause, such as an overloaded or shutting-down server or a
    result too large to send. ``validation_errors`` lists parse errors.
    """

    def __init__(
        self,
        message: str,
        *,
        query: str | None = None,
        code: ErrorCode | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.query = query
        self.code = code
        self.validation_errors = validation_errors or []

    def __str__(self) -> str:
        base = super().__str__()
        if self.query:
            query = self.query
            if len(query) > _QUERY_PREVIEW_CHARS:
                query = query[:_QUERY_PREVIEW_CHARS] + "..."
            return f"{base}\n  query: {query}"
        return base


class StatementFailedError(QueryError):
    """Statements of a multi-statement program failed.

    The engine runs every statement of a program, so the statements not in
    ``errors`` took effect. ``result`` is the whole program's result. ``code``
    is the first failure's code.
    """

    def __init__(
        self,
        errors: list[StatementError],
        result: ResultResponse,
        *,
        query: str | None = None,
    ) -> None:
        first = errors[0]
        super().__init__(
            f"{len(errors)} statement(s) failed; "
            f"statement {first.index}: {first.message}",
            query=query,
            code=first.code,
        )
        self.errors = errors
        self.result = result


class InputLayerPermissionError(InputLayerError):
    """Insufficient permissions for the requested operation."""


# Backward-compatible alias (shadows the builtin, kept for existing code).
PermissionError = InputLayerPermissionError


class KnowledgeGraphNotFoundError(InputLayerError):
    """The specified knowledge graph does not exist."""


class KnowledgeGraphExistsError(InputLayerError):
    """The knowledge graph already exists."""


class CannotDropError(InputLayerError):
    """Cannot drop the target (e.g., default KG, currently bound KG)."""


class RelationNotFoundError(InputLayerError):
    """The specified relation does not exist."""


class RuleNotFoundError(InputLayerError):
    """The specified rule does not exist."""


class IndexNotFoundError(InputLayerError):
    """The specified index does not exist."""


class InternalError(InputLayerError):
    """An unexpected internal error occurred."""
