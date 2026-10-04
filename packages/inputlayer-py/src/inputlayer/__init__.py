"""inputlayer - Python Object-Logic Mapper for InputLayer knowledge graph engine."""

# Functions (re-export all)
from inputlayer import functions
from inputlayer._protocol import StatementError

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
    Cancelled,
    CannotDropError,
    CompileError,
    Conflict,
    ConnectionError,
    ConnectionLost,
    DeadlineExceeded,
    IndexNotFoundError,
    InputLayerConnectionError,
    InputLayerError,
    InputLayerPermissionError,
    InternalError,
    KnowledgeGraphExistsError,
    KnowledgeGraphNotFoundError,
    OutcomeUnknownError,
    PermissionError,
    PreconditionFailed,
    ProtocolError,
    QueryError,
    QueryTimeoutError,
    RateLimited,
    RelationNotFoundError,
    RuleNotFoundError,
    SchemaConflictError,
    StatementFailedError,
    StoreReadOnlyError,
    SubscriptionRejected,
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

# Notifications
from inputlayer.notifications import ConnectionEvent, NotificationEvent

# Programs, guards and claims
from inputlayer.program import Claim, Program, ProgramResult

# Relation system
from inputlayer.relation import Relation

# Result
from inputlayer.result import ResultSet

# Session
from inputlayer.session import Session

# Subscriptions
from inputlayer.subscription import (
    Change,
    GroupChange,
    GroupSubscription,
    Live,
    MemberChange,
    ReadResult,
    Row,
    Subscription,
    SubscriptionHandle,
    SubscriptionStats,
)
from inputlayer.types import Timestamp, Vector, VectorInt8

__version__ = "0.1.0"

__all__ = [
    "AclEntry",
    "ApiKeyInfo",
    "AuthenticationError",
    "Cancelled",
    "CannotDropError",
    "Change",
    "Claim",
    "ClearResult",
    "ColumnInfo",
    "CompileError",
    "Conflict",
    "ConnectionError",
    "ConnectionEvent",
    "ConnectionLost",
    "DeadlineExceeded",
    "DebugResult",
    "DeleteResult",
    "Derived",
    "From",
    "GroupChange",
    "GroupSubscription",
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
    "Live",
    "MemberChange",
    # Notifications
    "NotificationEvent",
    "OutcomeUnknownError",
    "PermissionError",
    "PreconditionFailed",
    "Program",
    "ProgramResult",
    "ProofNode",
    "ProofTree",
    "ProtocolError",
    "QueryError",
    "QueryTimeoutError",
    "RateLimited",
    "ReadResult",
    # Relation
    "Relation",
    "RelationDescription",
    "RelationInfo",
    "RelationNotFoundError",
    # Result
    "ResultSet",
    "Row",
    "RuleClause",
    "RuleInfo",
    "RuleNotFoundError",
    "SchemaConflictError",
    "ServerStatus",
    # Session
    "Session",
    "StatementError",
    "StatementFailedError",
    "StoreReadOnlyError",
    "Subscription",
    "SubscriptionHandle",
    "SubscriptionRejected",
    "SubscriptionStats",
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
