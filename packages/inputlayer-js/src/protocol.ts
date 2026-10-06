/**
 * WebSocket wire protocol: message serialization and deserialization.
 * Matches the AsyncAPI spec at docs/spec/asyncapi.yaml (protocol version 5,
 * defined by the `inputlayer-ws-protocol` crate).
 *
 * Any request may carry an `id`; every reply to it echoes it. Pushes
 * (notifications, subscription deltas) and `notice` frames never carry one
 * and are never replies.
 */

/** The `/ws` protocol version this SDK speaks (`authenticated.protocol_version`). */
export const PROTOCOL_VERSION = 5;

/** The first protocol version whose `execute` takes `params`. */
export const PARAMS_PROTOCOL_VERSION = 4;

/** The first protocol version with `read` and `subscribe` (subscription groups). */
export const GROUPS_PROTOCOL_VERSION = 5;

// ── Client -> Server messages ───────────────────────────────────────

export interface LoginMessage {
  type: 'login';
  id?: string;
  username: string;
  password: string;
}

export interface AuthenticateMessage {
  type: 'authenticate';
  id?: string;
  api_key: string;
}

/**
 * One parameter's value. A bare value is typed by its JSON form: a string, a
 * boolean, an integer (int64), a number with a fraction or exponent (float)
 * or an array of numbers (vector). The one-key forms name the type: JSON
 * writes `2.0` as `2`, so send an integral float as `{ float: 2 }`, and an
 * integer past 2^53 as `{ int: "9007199254740993" }`.
 */
export type ParamValue =
  | string
  | boolean
  | number
  | number[]
  | { int: number | string }
  | { float: number }
  | { string: string }
  | { bool: boolean }
  | { vector: number[] };

/** Values of a program's `$name` references, by name (protocol version 4). */
export type Params = Record<string, ParamValue>;

export interface ExecuteMessage {
  type: 'execute';
  id?: string;
  program: string;
  /** Values bound to the program's `$name` references, never parsed as IQL. */
  params?: Params;
  /** Milliseconds the request may take from its arrival (queueing, admission, computation); capped by the engine's query timeout. */
  timeout_ms?: number;
}

/** One query of a `read` or `subscribe`, and the name its result goes by (unique within the request). */
export interface NamedQuery {
  name: string;
  /** One `?` query. */
  query: string;
}

/**
 * Run several queries on one pinned snapshot of the knowledge graph, so
 * every result is exact at one revision; answered by a `snapshot`. Reads
 * persistent data only, as a subscription does. `timeout_ms` and `cancel`
 * stop the whole read; it fails as a whole.
 */
export interface ReadMessage {
  type: 'read';
  id?: string;
  queries: NamedQuery[];
  timeout_ms?: number;
}

/**
 * Subscribe to a group of queries kept current together; answered by a
 * `snapshot` naming the subscription, then pushed
 * `subscription_group_delta`s. Ended by `.unsubscribe <subscription>`.
 */
export interface SubscribeMessage {
  type: 'subscribe';
  id?: string;
  subscription: string;
  queries: NamedQuery[];
}

/** Cancel the unanswered request `target`; answered by `cancel_ack` after the target's own reply. */
export interface CancelMessage {
  type: 'cancel';
  id?: string;
  target: string;
}

export interface PingMessage {
  type: 'ping';
  id?: string;
}

export type ClientMessage =
  | LoginMessage
  | AuthenticateMessage
  | ExecuteMessage
  | ReadMessage
  | SubscribeMessage
  | CancelMessage
  | PingMessage;

// ── Server -> Client messages ───────────────────────────────────────

export interface AuthenticatedResponse {
  type: 'authenticated';
  id?: string;
  session_id: string;
  knowledge_graph: string;
  version: string;
  role: string;
  protocol_version: number;
  /** This engine run's id; notification `seq` numbers belong to it. Pass it back with `last_seq` when reconnecting. */
  stream_epoch: string;
}

export interface AuthErrorResponse {
  type: 'auth_error';
  id?: string;
  message: string;
  /** `access_denied` when the credential may not use the knowledge graph. */
  code?: ErrorCode;
}

export interface RuleTiming {
  rule_head: string;
  execution_us: number;
  is_recursive: boolean;
  workers: number;
}

export interface TimingBreakdown {
  total_us: number;
  parse_us: number;
  sip_us: number;
  magic_sets_us: number;
  ir_build_us: number;
  optimize_us: number;
  shared_views_us: number;
  rules?: RuleTiming[];
}

/**
 * `invalid_request` and `rate_limited` reject a whole request before it runs.
 * `deadline_exceeded` and `cancelled` stop it before it began committing, so
 * nothing was applied; `outcome_unknown` means its commit failed in a way that
 * leaves the changes possibly applied: read the state back before retrying.
 * `resource_exhausted` refuses a query over the engine's per-query memory
 * limit or its server-wide query memory budget, or a write past its knowledge
 * graph's memory budget; nothing was applied. `replica_unconfirmed`: the write
 * committed on a primary shipping synchronously, but no replica confirmed it in
 * time; it is applied there, so do not retry it as a failed write.
 * `access_denied` refuses what the caller may not do (its role, write grants
 * or API key scope do not allow a statement, or its credential was revoked or
 * has expired); nothing ran. `overloaded` refuses a request the engine could
 * not admit to compute in time (its lane's queue was full, or no compute
 * permit came within the engine's longest admission wait); nothing ran, so
 * retry later with backoff.
 */
export type ErrorCode =
  | 'store_read_only'
  | 'validation'
  | 'not_found'
  | 'conflict'
  | 'unsupported'
  | 'internal'
  | 'invalid_request'
  | 'rate_limited'
  | 'deadline_exceeded'
  | 'cancelled'
  | 'outcome_unknown'
  | 'resource_exhausted'
  | 'replica_unconfirmed'
  | 'access_denied'
  | 'overloaded';

/** A failed statement of a multi-statement program (0-based `index`). */
export interface StatementError {
  index: number;
  code: ErrorCode;
  message: string;
}

/** The subscription a `.subscribe` or `subscribe` registered; pushes for it carry this generation. */
export interface Subscribed {
  subscription: string;
  generation: number;
  /** The knowledge graph revision the snapshot is the exact answer at; every later delta names a higher one. */
  revision: number;
}

export interface ResultResponse {
  type: 'result';
  id?: string;
  columns: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
  row_count: number;
  total_count: number;
  truncated: boolean;
  execution_time_ms: number;
  row_provenance?: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadata?: Record<string, any>;
  switched_kg?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  proof_trees?: any[];
  timing_breakdown?: TimingBreakdown;
  errors?: StatementError[];
  subscribed?: Subscribed;
}

export interface ErrorResponse {
  type: 'error';
  id?: string;
  message: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  validation_errors?: Array<Record<string, any>>;
  code?: ErrorCode;
}

export interface ResultStartResponse {
  type: 'result_start';
  id?: string;
  columns: string[];
  total_count: number;
  truncated: boolean;
  execution_time_ms: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadata?: Record<string, any>;
  switched_kg?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  proof_trees?: any[];
  timing_breakdown?: TimingBreakdown;
  errors?: StatementError[];
  /** Set on a streamed reply to `.subscribe`: the chunks hold the snapshot. */
  subscribed?: Subscribed;
}

export interface ResultChunkResponse {
  type: 'result_chunk';
  id?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
  chunk_index: number;
  row_provenance?: string[];
}

export interface ResultEndResponse {
  type: 'result_end';
  id?: string;
  row_count: number;
  chunk_count: number;
}

/** One query's result in a `snapshot`. */
export interface NamedResult {
  name: string;
  columns: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
  /** Rows before the result cap. */
  total_count: number;
  /** Whether the result cap cut the rows; never set in a subscription group's snapshot. */
  truncated: boolean;
}

/**
 * Results of several queries, all exact at `revision`, one per query in
 * request order: the reply to `read`, and to `subscribe` (then with
 * `subscribed`).
 */
export interface SnapshotResponse {
  type: 'snapshot';
  id?: string;
  knowledge_graph: string;
  revision: number;
  results: NamedResult[];
  execution_time_ms: number;
  subscribed?: Subscribed;
}

/** One query's result in a `snapshot_start`: `NamedResult` without its rows. */
export interface NamedResultHeader {
  name: string;
  columns: string[];
  /** Rows the result's chunks carry, in total. */
  row_count: number;
  total_count: number;
  truncated: boolean;
}

/**
 * Header of a snapshot streamed in chunks; complete only at its
 * `snapshot_end` (an `error` for the same request before then discards it).
 */
export interface SnapshotStartResponse {
  type: 'snapshot_start';
  id?: string;
  knowledge_graph: string;
  revision: number;
  results: NamedResultHeader[];
  execution_time_ms: number;
  subscribed?: Subscribed;
}

/**
 * Rows of result `result` (its index in the request) of a streamed snapshot.
 * `chunk_index` counts from 0 across all results; results stream in order,
 * and a result without rows has no chunk.
 */
export interface SnapshotChunkResponse {
  type: 'snapshot_chunk';
  id?: string;
  result: number;
  chunk_index: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
}

/** End of a streamed snapshot: the chunks it had. */
export interface SnapshotEndResponse {
  type: 'snapshot_end';
  id?: string;
  chunk_count: number;
}

export interface PongResponse {
  type: 'pong';
  id?: string;
}

/**
 * What a `cancel` did: `cancelled` (the target stopped before committing and
 * replies `cancelled`), `too_late` (it finished or began committing; its reply
 * reports what it did) or `not_found` (no unanswered request has that id).
 */
export interface CancelAckResponse {
  type: 'cancel_ack';
  id?: string;
  target: string;
  outcome: 'cancelled' | 'too_late' | 'not_found';
}

/** A connection event. The server closes the connection after every one but `notifications_missed` and `replay_gap`. */
export type NoticeCode =
  | 'notifications_missed'
  | 'replay_gap'
  | 'slow_consumer'
  | 'idle_timeout'
  | 'lifetime_exceeded'
  | 'auth_timeout'
  | 'credential_revoked'
  | 'credential_expired'
  | 'server_shutdown';

/** A connection event announced by the server; never a reply. */
export interface NoticeResponse {
  type: 'notice';
  code: NoticeCode;
  message: string;
}

/** Rows that entered and left a standing query's result. */
export interface SubscriptionDeltaResponse {
  type: 'subscription_delta';
  subscription: string;
  generation: number;
  knowledge_graph: string;
  /** Delta number within the generation, from 1, without gaps. */
  seq: number;
  /** The knowledge graph revision the result reaches with this delta. */
  revision: number;
  columns: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  inserted: any[][];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  retracted: any[][];
}

/**
 * Header of a delta streamed in chunks: a `subscription_delta` without its
 * rows. The delta applies only at its `subscription_delta_end`.
 */
export interface SubscriptionDeltaStartResponse {
  type: 'subscription_delta_start';
  subscription: string;
  generation: number;
  knowledge_graph: string;
  seq: number;
  revision: number;
  columns: string[];
}

/** Rows of a streamed delta, in order from `chunk_index` 0. */
export interface SubscriptionDeltaChunkResponse {
  type: 'subscription_delta_chunk';
  subscription: string;
  generation: number;
  seq: number;
  chunk_index: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  inserted: any[][];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  retracted: any[][];
}

/** End of a streamed delta: the counts its chunks must add up to. */
export interface SubscriptionDeltaEndResponse {
  type: 'subscription_delta_end';
  subscription: string;
  generation: number;
  seq: number;
  chunk_count: number;
  inserted_count: number;
  retracted_count: number;
}

/**
 * The server ended a subscription whose next change it could not deliver
 * whole: discard its rows and subscribe again.
 */
export interface SubscriptionResetResponse {
  type: 'subscription_reset';
  subscription: string;
  generation: number;
  message: string;
}

/** One member of a `subscription_group_delta`, in the group's order. */
export interface GroupMemberDelta {
  name: string;
  /** Whether the member's result did not change: `inserted` and `retracted` are then empty. */
  unchanged: boolean;
  columns: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  inserted: any[][];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  retracted: any[][];
}

/**
 * The results of a subscription group changed: every member listed, after
 * which each is its query's exact answer at `revision`. The group's pushes
 * share one gapless `seq`.
 */
export interface SubscriptionGroupDeltaResponse {
  type: 'subscription_group_delta';
  subscription: string;
  generation: number;
  knowledge_graph: string;
  seq: number;
  revision: number;
  members: GroupMemberDelta[];
}

/** One member of a `subscription_group_delta_start`: `GroupMemberDelta` with row counts for rows. */
export interface GroupMemberDeltaHeader {
  name: string;
  unchanged: boolean;
  columns: string[];
  inserted_count: number;
  retracted_count: number;
}

/** Header of a group delta streamed in chunks; it applies only at its end. */
export interface SubscriptionGroupDeltaStartResponse {
  type: 'subscription_group_delta_start';
  subscription: string;
  generation: number;
  knowledge_graph: string;
  seq: number;
  revision: number;
  members: GroupMemberDeltaHeader[];
}

/**
 * Rows of member `member` (its index in the group) of a streamed group
 * delta. `chunk_index` counts from 0 across all members; members stream in
 * order.
 */
export interface SubscriptionGroupDeltaChunkResponse {
  type: 'subscription_group_delta_chunk';
  subscription: string;
  generation: number;
  seq: number;
  chunk_index: number;
  member: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  inserted: any[][];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  retracted: any[][];
}

/** End of a streamed group delta: the chunks it had; each member's must add up to its header counts. */
export interface SubscriptionGroupDeltaEndResponse {
  type: 'subscription_group_delta_end';
  subscription: string;
  generation: number;
  seq: number;
  chunk_count: number;
}

/** A standing query failed to re-evaluate; it stays registered. */
export interface SubscriptionErrorResponse {
  type: 'subscription_error';
  subscription: string;
  generation: number;
  message: string;
  /** `access_denied` when the subscriber may no longer read the knowledge graph. */
  code?: ErrorCode;
}

export interface NotificationResponse {
  type: 'persistent_update' | 'rule_change' | 'kg_change' | 'schema_change';
  seq: number;
  timestamp_ms: number;
  session_id?: string;
  knowledge_graph?: string;
  // persistent_update fields
  relation?: string;
  operation?: string;
  count?: number;
  // rule_change fields
  rule_name?: string;
  // schema_change fields
  entity?: string;
}

export type ServerMessage =
  | AuthenticatedResponse
  | AuthErrorResponse
  | ResultResponse
  | ErrorResponse
  | ResultStartResponse
  | ResultChunkResponse
  | ResultEndResponse
  | SnapshotResponse
  | SnapshotStartResponse
  | SnapshotChunkResponse
  | SnapshotEndResponse
  | PongResponse
  | CancelAckResponse
  | NoticeResponse
  | NotificationResponse
  | SubscriptionDeltaResponse
  | SubscriptionDeltaStartResponse
  | SubscriptionDeltaChunkResponse
  | SubscriptionDeltaEndResponse
  | SubscriptionGroupDeltaResponse
  | SubscriptionGroupDeltaStartResponse
  | SubscriptionGroupDeltaChunkResponse
  | SubscriptionGroupDeltaEndResponse
  | SubscriptionErrorResponse
  | SubscriptionResetResponse;

/** Frames the server sends unprompted: never the reply to a request. */
export type PushMessage =
  | NoticeResponse
  | NotificationResponse
  | SubscriptionDeltaResponse
  | SubscriptionDeltaStartResponse
  | SubscriptionDeltaChunkResponse
  | SubscriptionDeltaEndResponse
  | SubscriptionGroupDeltaResponse
  | SubscriptionGroupDeltaStartResponse
  | SubscriptionGroupDeltaChunkResponse
  | SubscriptionGroupDeltaEndResponse
  | SubscriptionErrorResponse
  | SubscriptionResetResponse;

const PUSH_TYPES: ReadonlySet<string> = new Set([
  'notice',
  'persistent_update',
  'rule_change',
  'kg_change',
  'schema_change',
  'subscription_delta',
  'subscription_delta_start',
  'subscription_delta_chunk',
  'subscription_delta_end',
  'subscription_group_delta',
  'subscription_group_delta_start',
  'subscription_group_delta_chunk',
  'subscription_group_delta_end',
  'subscription_error',
  'subscription_reset',
]);

/** Whether `msg` was sent unprompted rather than in reply to a request. */
export function isPush(msg: ServerMessage): msg is PushMessage {
  return PUSH_TYPES.has(msg.type);
}

// ── Serialization ───────────────────────────────────────────────────

export function serializeMessage(msg: ClientMessage): string {
  return JSON.stringify(msg);
}

/** An integer token the engine sent past 2^53 decodes to a BigInt, keeping every digit. */
function exactIntegers(_key: string, value: unknown, context?: { source?: string }): unknown {
  if (typeof value === 'number' && !Number.isSafeInteger(value) && context?.source !== undefined && /^-?\d+$/.test(context.source)) {
    return BigInt(context.source);
  }
  return value;
}

/** A row's identity as text; an integer past 2^53 arrives as a BigInt. */
export function rowKey(values: unknown): string {
  return JSON.stringify(values, (_k, v: unknown) => (typeof v === 'bigint' ? `${v}n` : v));
}

/** An integer token of 16 or more digits: not part of a fraction or an exponent. */
const LARGE_INTEGER = /(?<![\d.eE+-])-?\d{16,}(?![\d.eE])/;

export function deserializeMessage(data: string): ServerMessage {
  const obj = LARGE_INTEGER.test(data) ? JSON.parse(data, exactIntegers) : JSON.parse(data);
  const type = obj.type;

  if (type === 'authenticated') {
    return obj as AuthenticatedResponse;
  }
  if (type === 'auth_error') {
    return obj as AuthErrorResponse;
  }
  if (type === 'result') {
    return obj as ResultResponse;
  }
  if (type === 'error') {
    return obj as ErrorResponse;
  }
  if (type === 'result_start') {
    return obj as ResultStartResponse;
  }
  if (type === 'result_chunk') {
    return obj as ResultChunkResponse;
  }
  if (type === 'result_end') {
    return obj as ResultEndResponse;
  }
  if (type === 'snapshot') {
    return obj as SnapshotResponse;
  }
  if (type === 'snapshot_start') {
    return obj as SnapshotStartResponse;
  }
  if (type === 'snapshot_chunk') {
    return obj as SnapshotChunkResponse;
  }
  if (type === 'snapshot_end') {
    return obj as SnapshotEndResponse;
  }
  if (type === 'pong') {
    return obj as PongResponse;
  }
  if (type === 'cancel_ack') {
    return obj as CancelAckResponse;
  }
  if (type === 'notice') {
    return obj as NoticeResponse;
  }
  if (type === 'subscription_delta') {
    return obj as SubscriptionDeltaResponse;
  }
  if (type === 'subscription_delta_start') {
    return obj as SubscriptionDeltaStartResponse;
  }
  if (type === 'subscription_delta_chunk') {
    return obj as SubscriptionDeltaChunkResponse;
  }
  if (type === 'subscription_delta_end') {
    return obj as SubscriptionDeltaEndResponse;
  }
  if (type === 'subscription_group_delta') {
    return obj as SubscriptionGroupDeltaResponse;
  }
  if (type === 'subscription_group_delta_start') {
    return obj as SubscriptionGroupDeltaStartResponse;
  }
  if (type === 'subscription_group_delta_chunk') {
    return obj as SubscriptionGroupDeltaChunkResponse;
  }
  if (type === 'subscription_group_delta_end') {
    return obj as SubscriptionGroupDeltaEndResponse;
  }
  if (type === 'subscription_error') {
    return obj as SubscriptionErrorResponse;
  }
  if (type === 'subscription_reset') {
    return obj as SubscriptionResetResponse;
  }
  if (
    type === 'persistent_update' ||
    type === 'rule_change' ||
    type === 'kg_change' ||
    type === 'schema_change'
  ) {
    return obj as NotificationResponse;
  }

  throw new Error(`Unknown message type: ${type}`);
}
