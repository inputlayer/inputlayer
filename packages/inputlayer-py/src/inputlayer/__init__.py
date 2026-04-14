"""inputlayer - Python Object-Logic Mapper for InputLayer knowledge graph engine."""

# Core types
# Functions (re-export all)
from inputlayer import functions

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

# Auth
from inputlayer.auth import AclEntry, ApiKeyInfo, UserInfo

# Client
from inputlayer.client import InputLayer
from inputlayer.client_sync import InputLayerSync
from inputlayer.derived import Derived, From, RuleClause

# Exceptions
from inputlayer.exceptions import (
    AuthenticationError,
    CannotDropError,
    ConnectionError,
    IndexNotFoundError,
    InputLayerConnectionError,
    InputLayerError,
    InputLayerPermissionError,
    InternalError,
    KnowledgeGraphExistsError,
    KnowledgeGraphNotFoundError,
    PermissionError,
    QueryError,
    QueryTimeoutError,
    RelationNotFoundError,
    RuleNotFoundError,
    SchemaConflictError,
    ValidationError,
)

# Index
from inputlayer.index import HnswIndex

# Knowledge Graph
from inputlayer.knowledge_graph import (
    ClearResult,
    ColumnInfo,
    DebugResult,
    DeleteResult,
    IndexInfo,
    IndexStats,
    InsertResult,
    KnowledgeGraph,
    ProofNode,
    ProofTree,
    RelationDescription,
    RelationInfo,
    RuleInfo,
    ServerStatus,
    WhyNotResult,
    WhyResult,
)

# Migrations
from inputlayer.migrations import Migration

# Notifications
from inputlayer.notifications import NotificationEvent

# Relation system
from inputlayer.relation import Relation

# Result
from inputlayer.result import ResultSet

# Session
from inputlayer.session import Session
from inputlayer.types import Timestamp, Vector, VectorInt8

__version__ = "0.1.0"

__all__ = [
    "AclEntry",
    "ApiKeyInfo",
    "AuthenticationError",
    "CannotDropError",
    "ClearResult",
    "ColumnInfo",
    "ConnectionError",
    "DebugResult",
    "DeleteResult",
    "Derived",

    "From",
    # Index
    "HnswIndex",
    "IndexInfo",
    "IndexNotFoundError",
    "IndexStats",
    # Client
    "InputLayer",
    # Exceptions
    "InputLayerConnectionError",
    "InputLayerError",
    "InputLayerPermissionError",
    "InputLayerSync",
    "InsertResult",
    "InternalError",
    # KG
    "KnowledgeGraph",
    "KnowledgeGraphExistsError",
    "KnowledgeGraphNotFoundError",
    # Migrations
    "Migration",
    # Notifications
    "NotificationEvent",
    "PermissionError",
    "ProofNode",
    "ProofTree",
    "QueryError",
    "QueryTimeoutError",
    # Relation
    "Relation",
    "RelationDescription",
    "RelationInfo",
    "RelationNotFoundError",
    # Result
    "ResultSet",
    "RuleClause",
    "RuleInfo",
    "RuleNotFoundError",
    "SchemaConflictError",
    "ServerStatus",
    # Session
    "Session",
    "Timestamp",
    # Auth
    "UserInfo",
    "ValidationError",
    # Types
    "Vector",
    "VectorInt8",
    "WhyNotResult",
    "WhyResult",
    "avg",
    # Aggregations
    "count",
    "count_distinct",
    # Functions
    "functions",
    "max_",
    "min_",
    "sum_",
    "top_k",
    "top_k_threshold",
    "within_radius",
]
