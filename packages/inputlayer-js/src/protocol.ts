/**
 * WebSocket wire protocol: message serialization and deserialization.
 * Matches the AsyncAPI spec at docs/spec/asyncapi.yaml (protocol version 3,
 * defined by the `inputlayer-ws-protocol` crate).
 *
 * Any request may carry an `id`; every reply to it echoes it. Pushes
 * (notifications, subscription deltas) and `notice` frames never carry one
 * and are never replies.
 */

/** The `/ws` protocol version this SDK speaks (`authenticated.protocol_version`). */
export const PROTOCOL_VERSION = 3;

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

export interface ExecuteMessage {
  type: 'execute';
  id?: string;
  program: string;
  /** Milliseconds the request may take from its arrival (queueing, admission, computation); capped by the engine's query timeout. */
  timeout_ms?: number;
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
 * limit or its server-wide query memory budget, or a write past its knowledge graph's memory budget; nothing was
 * applied.
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
  | 'resource_exhausted';

/** A failed statement of a multi-statement program (0-based `index`). */
export interface StatementError {
  index: number;
  code: ErrorCode;
  message: string;
}

/** The subscription a `.subscribe` registered; pushes for it carry this generation. */
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

/** A standing query failed to re-evaluate; it stays registered. */
export interface SubscriptionErrorResponse {
  type: 'subscription_error';
  subscription: string;
  generation: number;
  message: string;
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
  | PongResponse
  | CancelAckResponse
  | NoticeResponse
  | NotificationResponse
  | SubscriptionDeltaResponse
  | SubscriptionDeltaStartResponse
  | SubscriptionDeltaChunkResponse
  | SubscriptionDeltaEndResponse
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
