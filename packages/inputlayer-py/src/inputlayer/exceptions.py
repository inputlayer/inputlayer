"""Exception hierarchy for the InputLayer OLM."""

from __future__ import annotations


class InputLayerError(Exception):
    """Base exception for all InputLayer errors."""


class ConnectionError(InputLayerError):
    """Failed to connect or lost connection to the server."""


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


class PermissionError(InputLayerError):
    """Insufficient permissions for the requested operation."""


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
