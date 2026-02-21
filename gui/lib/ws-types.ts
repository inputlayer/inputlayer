// WebSocket protocol types matching the server's GlobalWsRequest/GlobalWsResponse

// ── Client → Server ─────────────────────────────────────────────────────────

export interface WsExecuteRequest {
  type: "execute"
  program: string
}

export interface WsPingRequest {
  type: "ping"
}

export type WsClientMessage = WsExecuteRequest | WsPingRequest

// ── Server → Client ─────────────────────────────────────────────────────────

export interface WsConnectedMessage {
  type: "connected"
  session_id: number
  knowledge_graph: string
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

export type WsServerMessage =
  | WsConnectedMessage
  | WsResultMessage
  | WsErrorMessage
  | WsPongMessage
  | WsNotificationMessage

// ── Connection state ────────────────────────────────────────────────────────

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting"
