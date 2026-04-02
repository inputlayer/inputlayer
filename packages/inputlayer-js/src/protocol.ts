/**
 * WebSocket wire protocol: message serialization and deserialization.
 * Matches the AsyncAPI spec at docs/spec/asyncapi.yaml.
 */

// ── Client -> Server messages ───────────────────────────────────────

export interface LoginMessage {
  type: 'login';
  username: string;
  password: string;
}

export interface AuthenticateMessage {
  type: 'authenticate';
  api_key: string;
}

export interface ExecuteMessage {
  type: 'execute';
  program: string;
}

export interface PingMessage {
  type: 'ping';
}

export type ClientMessage =
  | LoginMessage
  | AuthenticateMessage
  | ExecuteMessage
  | PingMessage;

// ── Server -> Client messages ───────────────────────────────────────

export interface AuthenticatedResponse {
  type: 'authenticated';
  session_id: string;
  knowledge_graph: string;
  version: string;
  role: string;
}

export interface AuthErrorResponse {
  type: 'auth_error';
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

export interface ResultResponse {
  type: 'result';
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
}

export interface ErrorResponse {
  type: 'error';
  message: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  validation_errors?: Array<Record<string, any>>;
}

export interface ResultStartResponse {
  type: 'result_start';
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
}

export interface ResultChunkResponse {
  type: 'result_chunk';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
  chunk_index: number;
  row_provenance?: string[];
}

export interface ResultEndResponse {
  type: 'result_end';
  row_count: number;
  chunk_count: number;
}

export interface PongResponse {
  type: 'pong';
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
  | NotificationResponse;

// ── Serialization ───────────────────────────────────────────────────

export function serializeMessage(msg: ClientMessage): string {
  return JSON.stringify(msg);
}

export function deserializeMessage(data: string): ServerMessage {
  const obj = JSON.parse(data);
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
