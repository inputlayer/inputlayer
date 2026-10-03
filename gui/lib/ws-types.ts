// WebSocket protocol types (/ws protocol version 2), mirroring the
// `inputlayer-ws-protocol` crate and docs/spec/asyncapi.yaml. Any request may
// carry an `id`, echoed on every reply to it; `notice` and push frames never
// carry one.

// ── Client → Server ─────────────────────────────────────────────────────────

export interface WsExecuteRequest {
  type: "execute"
  id?: string
  program: string
}

export interface WsPingRequest {
  type: "ping"
  id?: string
}

export interface WsLoginRequest {
  type: "login"
  id?: string
  username: string
  password: string
}

export interface WsAuthenticateRequest {
  type: "authenticate"
  id?: string
  api_key: string
}

export type WsClientMessage = WsExecuteRequest | WsPingRequest | WsLoginRequest | WsAuthenticateRequest

// ── Server → Client ─────────────────────────────────────────────────────────

export interface WsConnectedMessage {
  type: "connected"
  session_id: number
  knowledge_graph: string
}

export interface WsAuthenticatedMessage {
  type: "authenticated"
  id?: string
  protocol_version: number
  session_id: string
  knowledge_graph: string
  version: string
  role: string
  /** This engine run's id; notification `seq` numbers belong to it. */
  stream_epoch: string
}

export interface WsAuthErrorMessage {
  type: "auth_error"
  id?: string
  message: string
}

// --- Proof Tree Types ---

export type JsonValue = string | number | boolean | null

export interface WsProofTree {
  version: number
  query?: string
  roots: string[]
  nodes: Record<string, WsProofNode>
}

export interface WsProofNode {
  kind: "fact" | "rule" | "negation" | "vector_search" | "aggregate" | "truncated" | "why_not"
  conclusion: { pred: string; args: JsonValue[] }
  source?: "edb" | "derived"
  rule_id?: string
  bindings?: Record<string, JsonValue>
  aggregate?: {
    fn: string
    value_var: string
    result: JsonValue
    contributing_count: number
    sample_inputs?: JsonValue[][]
    full_inputs?: JsonValue[][] | null
  }
  negation?: { pattern: string }
  vector_search?: {
    index_name: string
    metric: string
    query_vector: number[]
    result_id: number
    distance: number
    k: number
    ef_search?: number
  }
  truncated?: { depth_limit: number }
  why_not?: {
    rule_name: string
    clause_index: number
    clause_text: string
    blocker: {
      type: string
      reason?: string
      predicate_index?: number
      predicate_text?: string
      comparison_text?: string
      lhs_value?: string
      rhs_value?: string
      relation?: string
      matching_tuple?: JsonValue[]
      index_name?: string
      k?: number
    }
  }
  children: string[]
}

export interface WsTimingBreakdown {
  total_us: number
  parse_us: number
  sip_us: number
  magic_sets_us: number
  ir_build_us: number
  optimize_us: number
  shared_views_us: number
  rules?: Array<{
    rule_head: string
    execution_us: number
    is_recursive: boolean
    workers: number
  }>
}

export type WsErrorCode =
  | "validation"
  | "not_found"
  | "conflict"
  | "unsupported"
  | "internal"
  | "invalid_request"
  | "rate_limited"
  | "deadline_exceeded"
  | "cancelled"
  | "outcome_unknown"
  | "store_read_only"

/** A failed statement of a multi-statement program (0-based `index`). */
export interface WsStatementError {
  index: number
  code: WsErrorCode
  message: string
}

export interface WsResultMessage {
  type: "result"
  id?: string
  columns: string[]
  rows: (string | number | boolean | null)[][]
  row_count: number
  total_count: number
  truncated: boolean
  execution_time_ms: number
  row_provenance?: string[]
  metadata?: WsResultMetadata
  switched_kg?: string
  proof_trees?: WsProofTree[]
  timing_breakdown?: WsTimingBreakdown
  errors?: WsStatementError[]
}

export interface WsResultMetadata {
  has_ephemeral: boolean
  ephemeral_sources?: string[]
  warnings?: string[]
}

export interface WsValidationError {
  line: number
  statement_index: number
  error: string
}

export interface WsErrorMessage {
  type: "error"
  id?: string
  message: string
  validation_errors?: WsValidationError[]
  code?: WsErrorCode
}

export interface WsPongMessage {
  type: "pong"
  id?: string
}

export interface WsNotificationMessage {
  type: "notification"
  event: string
  knowledge_graph: string
  relation: string
  operation: string
  count: number
}

// ── Streaming result types ──────────────────────────────────────────────────

export interface WsResultStartMessage {
  type: "result_start"
  id?: string
  columns: string[]
  total_count: number
  truncated: boolean
  execution_time_ms: number
  metadata?: WsResultMetadata
  switched_kg?: string
  proof_trees?: WsProofTree[]
  timing_breakdown?: WsTimingBreakdown
  errors?: WsStatementError[]
}

export interface WsResultChunkMessage {
  type: "result_chunk"
  id?: string
  rows: (string | number | boolean | null)[][]
  row_provenance?: string[]
  chunk_index: number
}

export interface WsResultEndMessage {
  type: "result_end"
  id?: string
  row_count: number
  chunk_count: number
}

/** A connection event; all but `notifications_missed` and `replay_gap` precede the server closing the connection. */
export interface WsNoticeMessage {
  type: "notice"
  code:
    | "notifications_missed"
    | "replay_gap"
    | "slow_consumer"
    | "idle_timeout"
    | "lifetime_exceeded"
    | "auth_timeout"
    | "credential_revoked"
    | "server_shutdown"
  message: string
}

export interface WsSubscriptionDeltaMessage {
  type: "subscription_delta"
  subscription: string
  generation: number
  knowledge_graph: string
  seq: number
  /** The knowledge graph revision the result reaches with this delta. */
  revision: number
  columns: string[]
  inserted: (string | number | boolean | null)[][]
  retracted: (string | number | boolean | null)[][]
}

export interface WsSubscriptionErrorMessage {
  type: "subscription_error"
  subscription: string
  generation: number
  message: string
}

export type WsServerMessage =
  | WsConnectedMessage
  | WsAuthenticatedMessage
  | WsAuthErrorMessage
  | WsResultMessage
  | WsErrorMessage
  | WsPongMessage
  | WsNotificationMessage
  | WsResultStartMessage
  | WsResultChunkMessage
  | WsResultEndMessage
  | WsNoticeMessage
  | WsSubscriptionDeltaMessage
  | WsSubscriptionErrorMessage

// ── Connection state ────────────────────────────────────────────────────────

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting"
