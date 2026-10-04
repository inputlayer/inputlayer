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


class ConnectionLost(InputLayerConnectionError):
    """The connection closed before a request was answered.

    ``code`` is the server's closing notice (``idle_timeout``,
    ``server_shutdown``, ...) when it sent one. ``may_have_committed`` is
    ``True`` for a request that could write and had been sent: read the state
    back before retrying it (fact writes are idempotent, so resending the same
    write is safe). Queries, pings and requests never sent are ``False``.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        may_have_committed: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.may_have_committed = may_have_committed


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

    ``code`` says why (an ``ErrorCode`` such as ``validation`` or
    ``not_found``). It is ``None`` when the failure has no
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


class OutcomeUnknownError(QueryError):
    """Write outcome unknown; the store may be read-only until restart.

    If so, a following write raises ``StoreReadOnlyError``.

    The transaction may or may not survive restart; read the recovered data
    before retrying it. ``result`` holds the server's response when it has one.
    """

    def __init__(self, message: str, result: ResultResponse | None = None) -> None:
        super().__init__(message, code="outcome_unknown")
        self.result = result


class StoreReadOnlyError(QueryError):
    """A write was refused because an earlier outcome is unknown.

    Every write fails until the server restarts and runs recovery.
    """

    def __init__(self, message: str, result: ResultResponse | None = None) -> None:
        super().__init__(message, code="store_read_only")
        self.result = result


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


class DeadlineExceeded(QueryError, QueryTimeoutError):
    """The request's deadline passed before it began committing; nothing it
    would have changed is applied. Retry if the answer is still wanted."""

    def __init__(self, message: str, *, query: str | None = None) -> None:
        super().__init__(message, query=query, code="deadline_exceeded")


class Cancelled(QueryError):
    """The request was cancelled before it began committing; nothing it would
    have changed is applied."""

    def __init__(self, message: str, *, query: str | None = None) -> None:
        super().__init__(message, query=query, code="cancelled")


class RateLimited(QueryError):
    """The server refused the request before running it: too many messages
    per second on this connection. The SDK has already retried it once."""

    def __init__(self, message: str, *, query: str | None = None) -> None:
        super().__init__(message, query=query, code="rate_limited")


class ProtocolError(QueryError):
    """The server could not read the request (``invalid_request``). Nothing ran.

    This is a bug in the SDK, not in the program: please report it."""

    def __init__(self, message: str, *, query: str | None = None) -> None:
        super().__init__(
            f"{message} (the server could not read the SDK's request; this is an SDK bug)",
            query=query,
            code="invalid_request",
        )


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
