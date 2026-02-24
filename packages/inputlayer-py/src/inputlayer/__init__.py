"""inputlayer - Python Object-Logic Mapper for InputLayer knowledge graph engine."""

# Core types
from inputlayer.types import Timestamp, Vector, VectorInt8

# Relation system
from inputlayer.relation import Relation
from inputlayer.derived import Derived, From, RuleClause

# Aggregations
from inputlayer.aggregations import (
    avg,
    count,
    count_distinct,
    max_,
    min_,
    sum_,
    top_k,
    top_k_threshold,
    within_radius,
)

# Functions (re-export all)
from inputlayer import functions

# Index
from inputlayer.index import HnswIndex

# Exceptions
from inputlayer.exceptions import (
    AuthenticationError,
    CannotDropError,
    ConnectionError,
    IndexNotFoundError,
    InputLayerError,
    InternalError,
    KnowledgeGraphExistsError,
    KnowledgeGraphNotFoundError,
    PermissionError,
    QueryTimeoutError,
    RelationNotFoundError,
    RuleNotFoundError,
    SchemaConflictError,
    ValidationError,
)

# Result
from inputlayer.result import ResultSet

# Client
from inputlayer.client import InputLayer
from inputlayer.client_sync import InputLayerSync

# Knowledge Graph
from inputlayer.knowledge_graph import (
    ClearResult,
    ColumnInfo,
    DeleteResult,
    ExplainResult,
    IndexInfo,
    IndexStats,
    InsertResult,
    KnowledgeGraph,
    RelationDescription,
    RelationInfo,
    RuleInfo,
    ServerStatus,
)

# Auth
from inputlayer.auth import AclEntry, ApiKeyInfo, UserInfo

# Session
from inputlayer.session import Session

# Notifications
from inputlayer.notifications import NotificationEvent

# Migrations
from inputlayer.migrations import Migration

__version__ = "0.1.0"

__all__ = [
    # Types
    "Vector",
    "VectorInt8",
    "Timestamp",
    # Relation
    "Relation",
    "Derived",
    "From",
    "RuleClause",
    # Aggregations
    "count",
    "count_distinct",
    "sum_",
    "min_",
    "max_",
    "avg",
    "top_k",
    "top_k_threshold",
    "within_radius",
    # Functions
    "functions",
    # Index
    "HnswIndex",
    # Exceptions
    "InputLayerError",
    "ConnectionError",
    "AuthenticationError",
    "SchemaConflictError",
    "ValidationError",
    "QueryTimeoutError",
    "PermissionError",
    "KnowledgeGraphNotFoundError",
    "KnowledgeGraphExistsError",
    "CannotDropError",
    "RelationNotFoundError",
    "RuleNotFoundError",
    "IndexNotFoundError",
    "InternalError",
    # Result
    "ResultSet",
    # Client
    "InputLayer",
    "InputLayerSync",
    # KG
    "KnowledgeGraph",
    "RelationInfo",
    "RelationDescription",
    "ColumnInfo",
    "RuleInfo",
    "IndexInfo",
    "IndexStats",
    "InsertResult",
    "DeleteResult",
    "ClearResult",
    "ExplainResult",
    "ServerStatus",
    # Auth
    "UserInfo",
    "ApiKeyInfo",
    "AclEntry",
    # Session
    "Session",
    # Notifications
    "NotificationEvent",
    # Migrations
    "Migration",
]
