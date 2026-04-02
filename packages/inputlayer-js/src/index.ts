/**
 * inputlayer - TypeScript Object-Logic Mapper for InputLayer knowledge graph engine.
 */

// Core types
export { Timestamp } from './types.js';
export type { Vector, VectorInt8, IQLType, FieldValue, Fact, ColumnDef, RelationSchema } from './types.js';

// Relation system
export { relation, RelationDef, compileValue, resolveRelationName, getColumns, getColumnTypes } from './relation.js';
export type { ColumnTypes } from './relation.js';

// Derived relations / rule builder
export { from } from './derived.js';

// AST (for advanced usage)
export type {
  Expr,
  BoolExpr,
  Column,
  Literal,
  Arithmetic,
  FuncCall,
  AggExpr,
  OrderedColumn,
  Comparison,
  And,
  Or,
  Not,
  InExpr,
  NegatedIn,
  MatchExpr,
} from './ast.js';

// Proxy system
export { ColumnProxy, RelationProxy, RelationRef, AND, OR, NOT, wrap } from './proxy.js';

// Aggregations
export { count, countDistinct, sum, min, max, avg, topK, topKThreshold, withinRadius } from './aggregations.js';

// Index
export { HnswIndex } from './index-def.js';
export type { HnswIndexOptions } from './index-def.js';

// Exceptions
export {
  InputLayerError,
  ConnectionError,
  AuthenticationError,
  SchemaConflictError,
  ValidationError,
  QueryTimeoutError,
  PermissionError,
  KnowledgeGraphNotFoundError,
  KnowledgeGraphExistsError,
  CannotDropError,
  RelationNotFoundError,
  RuleNotFoundError,
  IndexNotFoundError,
  InternalError,
} from './errors.js';

// Result
export { ResultSet } from './result.js';
export type { ResultSetOptions } from './result.js';

// Client
export { InputLayer } from './client.js';
export type { InputLayerOptions } from './client.js';

// Knowledge Graph
export { KnowledgeGraph } from './knowledge-graph.js';
export type {
  RelationInfo,
  ColumnInfo,
  RelationDescription,
  RuleInfo,
  IndexInfo,
  IndexStats,
  InsertResult,
  DeleteResult,
  ClearResult,
  DebugResult,
  ServerStatus,
  WhyResult,
  WhyNotResult,
  WhyNotBlocker,
  DerivationGraph,
  DerivationNode,
} from './knowledge-graph.js';

// Auth
export type { UserInfo, ApiKeyInfo, AclEntry } from './auth.js';

// Session
export { Session } from './session.js';

// Notifications
export type { NotificationEvent, NotificationCallback } from './notifications.js';
export { NotificationDispatcher } from './notifications.js';

// Compiler (for advanced usage)
export {
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileQuery,
  compileRule,
  compileExpr,
  compileBoolExpr,
} from './compiler.js';
export type { QueryOptions, RuleClause } from './compiler.js';

// Naming utilities
export { camelToSnake, snakeToCamel, columnToVariable } from './naming.js';

// Protocol (for advanced usage)
export type {
  ClientMessage,
  ServerMessage,
  LoginMessage,
  AuthenticateMessage,
  ExecuteMessage,
  PingMessage,
  AuthenticatedResponse,
  AuthErrorResponse,
  ResultResponse,
  ErrorResponse,
  ResultStartResponse,
  ResultChunkResponse,
  ResultEndResponse,
  PongResponse,
  NotificationResponse,
  TimingBreakdown,
  RuleTiming,
} from './protocol.js';
export { serializeMessage, deserializeMessage } from './protocol.js';

// Connection (for advanced usage)
export { Connection } from './connection.js';
export type { ConnectionOptions } from './connection.js';
