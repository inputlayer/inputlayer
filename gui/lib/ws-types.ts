// WebSocket protocol types matching the server's GlobalWsRequest/GlobalWsResponse

// ── Client → Server ─────────────────────────────────────────────────────────

export interface WsExecuteRequest {
  type: "execute"
  program: string
}

export interface WsPingRequest {
  type: "ping"
}

export interface WsLoginRequest {
  type: "login"
  username: string
  password: string
}

export interface WsAuthenticateRequest {
  type: "authenticate"
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
  session_id: string
  knowledge_graph: string
  version: string
  role: string
}

export interface WsAuthErrorMessage {
  type: "auth_error"
  message: string
}

export interface WsProofTree {
  node_type: string
  relation?: string
  values?: (string | number | boolean | null)[]
  rule_name?: string
  clause_index?: number
  clause_text?: string
  bindings?: Array<{ variable: string; value: string | number | boolean | null }>
  children?: WsProofTree[]
  pattern?: string
  index_name?: string
  metric?: string
  query_vector?: number[]
  result_id?: number
  distance?: number
  k?: number
  ef_search?: number
  aggregate_fn?: string
  contributing_count?: number
  sample_inputs?: (string | number | boolean | null)[][]
  full_inputs?: (string | number | boolean | null)[][] | null
  iteration?: number
  inner?: WsProofTree
  depth_limit?: number
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

export interface WsResultMessage {
  type: "result"
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
  message: string
  validation_errors?: WsValidationError[]
}

export interface WsPongMessage {
  type: "pong"
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
  columns: string[]
  total_count: number
  truncated: boolean
  execution_time_ms: number
  metadata?: WsResultMetadata
  switched_kg?: string
  proof_trees?: WsProofTree[]
  timing_breakdown?: WsTimingBreakdown
}

export interface WsResultChunkMessage {
  type: "result_chunk"
  rows: (string | number | boolean | null)[][]
  row_provenance?: string[]
  chunk_index: number
}

export interface WsResultEndMessage {
  type: "result_end"
  row_count: number
  chunk_count: number
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

// ── Connection state ────────────────────────────────────────────────────────

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting"
