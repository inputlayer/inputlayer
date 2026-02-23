import type {
  WsAuthenticatedMessage,
  WsResultMessage,
  WsResultStartMessage,
  WsResultChunkMessage,
  WsErrorMessage,
  WsNotificationMessage,
  WsServerMessage,
  ConnectionState,
} from "./ws-types"

export interface WsClientConfig {
  /** Full WebSocket URL, e.g. "ws://localhost:8080/ws" */
  url: string
  /** Knowledge graph to bind to (default: "default") */
  kg?: string
  /** Username for login authentication */
  username?: string
  /** Password for login authentication */
  password?: string
  /** Auto-reconnect on unexpected close (default: true) */
  autoReconnect?: boolean
  /** Max reconnect attempts (default: 10) */
  maxReconnectAttempts?: number
  /** Base reconnect delay in ms (default: 1000) */
  reconnectDelayMs?: number
}

export class WsError extends Error {
  validationErrors?: { line: number; statement_index: number; error: string }[]

  constructor(msg: WsErrorMessage) {
    super(msg.message)
    this.name = "WsError"
    this.validationErrors = msg.validation_errors
  }
}

type PendingRequest = {
  resolve: (msg: WsResultMessage) => void
  reject: (err: Error) => void
}

export type NotificationHandler = (notification: WsNotificationMessage) => void
export type ConnectionStateHandler = (state: ConnectionState, kg?: string) => void

/** State for accumulating a streamed result */
interface StreamingState {
  columns: string[]
  totalCount: number
  truncated: boolean
  executionTimeMs: number
  metadata?: WsResultStartMessage["metadata"]
  switchedKg?: string
  rows: (string | number | boolean | null)[][]
  rowProvenance: string[]
}

export class WsClient {
  private ws: WebSocket | null = null
  private sessionId: string | null = null
  private knowledgeGraph = "default"
  private userRole = ""
  private state: ConnectionState = "disconnected"
  private pendingQueue: PendingRequest[] = []
  private notificationHandlers = new Set<NotificationHandler>()
  private stateHandlers = new Set<ConnectionStateHandler>()
  private reconnectAttempts = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private intentionalClose = false
  private pingInterval: ReturnType<typeof setInterval> | null = null
  /** Active streaming accumulation (null when not streaming) */
  private streamingState: StreamingState | null = null

  private readonly url: string
  private readonly kg: string
  private readonly username: string
  private readonly password: string
  private readonly autoReconnect: boolean
  private readonly maxReconnectAttempts: number
  private readonly reconnectDelayMs: number

  constructor(config: WsClientConfig) {
    this.url = config.url
    this.kg = config.kg ?? "default"
    this.username = config.username ?? ""
    this.password = config.password ?? ""
    this.autoReconnect = config.autoReconnect ?? true
    this.maxReconnectAttempts = config.maxReconnectAttempts ?? 10
    this.reconnectDelayMs = config.reconnectDelayMs ?? 1000
  }

  /** Connect to the WebSocket server, authenticate, and resolve when ready. */
  connect(): Promise<WsAuthenticatedMessage> {
    return new Promise((resolve, reject) => {
      if (this.ws) {
        this.ws.close()
        this.ws = null
      }

      this.intentionalClose = false
      this.setState("connecting")

      const wsUrl = `${this.url}?kg=${encodeURIComponent(this.kg)}`
      const ws = new WebSocket(wsUrl)

      let authenticated = false

      ws.onopen = () => {
        // Send login message immediately after connection
        ws.send(JSON.stringify({
          type: "login",
          username: this.username,
          password: this.password,
        }))
      }

      ws.onmessage = (event) => {
        let msg: WsServerMessage
        try {
          msg = JSON.parse(event.data)
        } catch {
          return
        }

        if (!authenticated) {
          if (msg.type === "authenticated") {
            authenticated = true
            this.sessionId = msg.session_id
            this.knowledgeGraph = msg.knowledge_graph
            this.userRole = msg.role
            this.reconnectAttempts = 0
            this.setState("connected", this.knowledgeGraph)
            this.startPing()
            resolve(msg)
            return
          }
          if (msg.type === "auth_error") {
            this.setState("disconnected")
            ws.close()
            reject(new Error(msg.message))
            return
          }
          // Ignore other messages before auth
          return
        }

        this.handleMessage(msg)
      }

      ws.onerror = () => {
        if (!authenticated) {
          this.setState("disconnected")
          reject(new Error("WebSocket connection failed"))
        }
      }

      ws.onclose = () => {
        this.stopPing()
        this.ws = null

        // Reject all pending requests
        for (const pending of this.pendingQueue) {
          pending.reject(new Error("WebSocket connection closed"))
        }
        this.pendingQueue = []

        if (!authenticated) {
          this.setState("disconnected")
          reject(new Error("WebSocket connection closed before authentication"))
          return
        }

        if (!this.intentionalClose && this.autoReconnect) {
          this.attemptReconnect()
        } else {
          this.setState("disconnected")
        }
      }

      this.ws = ws
    })
  }

  /** Execute a Datalog program or meta command. Returns the result. */
  execute(program: string, timeoutMs = 30000): Promise<WsResultMessage> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("WebSocket not connected"))
        return
      }

      let settled = false
      const timer = timeoutMs > 0 ? setTimeout(() => {
        if (!settled) {
          settled = true
          // Remove from pending queue
          const idx = this.pendingQueue.findIndex((p) => p.resolve === wrappedResolve)
          if (idx !== -1) this.pendingQueue.splice(idx, 1)
          reject(new Error(`Query timed out after ${timeoutMs / 1000}s`))
        }
      }, timeoutMs) : null

      const wrappedResolve = (msg: WsResultMessage) => {
        if (settled) return
        settled = true
        if (timer) clearTimeout(timer)
        resolve(msg)
      }
      const wrappedReject = (err: Error) => {
        if (settled) return
        settled = true
        if (timer) clearTimeout(timer)
        reject(err)
      }

      this.pendingQueue.push({ resolve: wrappedResolve, reject: wrappedReject })
      this.ws.send(JSON.stringify({ type: "execute", program }))
    })
  }

  /** Disconnect gracefully. */
  disconnect(): void {
    this.intentionalClose = true
    this.clearReconnectTimer()
    this.stopPing()

    if (this.ws) {
      this.ws.close()
      this.ws = null
    }

    for (const pending of this.pendingQueue) {
      pending.reject(new Error("Client disconnected"))
    }
    this.pendingQueue = []
    this.sessionId = null
    this.setState("disconnected")
  }

  /** Subscribe to push notifications. Returns unsubscribe function. */
  onNotification(handler: NotificationHandler): () => void {
    this.notificationHandlers.add(handler)
    return () => this.notificationHandlers.delete(handler)
  }

  /** Subscribe to connection state changes. Returns unsubscribe function. */
  onStateChange(handler: ConnectionStateHandler): () => void {
    this.stateHandlers.add(handler)
    return () => this.stateHandlers.delete(handler)
  }

  getState(): ConnectionState {
    return this.state
  }

  getSessionId(): string | null {
    return this.sessionId
  }

  getKnowledgeGraph(): string {
    return this.knowledgeGraph
  }

  getRole(): string {
    return this.userRole
  }

  // ── Private ─────────────────────────────────────────────────────────────

  private handleMessage(msg: WsServerMessage): void {
    switch (msg.type) {
      case "result": {
        const pending = this.pendingQueue.shift()
        if (pending) {
          // Track KG switches
          if (msg.switched_kg) {
            this.knowledgeGraph = msg.switched_kg
          }
          pending.resolve(msg)
        }
        break
      }
      // ── Streaming result protocol ──────────────────────────────
      case "result_start": {
        this.streamingState = {
          columns: msg.columns,
          totalCount: msg.total_count,
          truncated: msg.truncated,
          executionTimeMs: msg.execution_time_ms,
          metadata: msg.metadata,
          switchedKg: msg.switched_kg,
          rows: [],
          rowProvenance: [],
        }
        break
      }
      case "result_chunk": {
        if (this.streamingState) {
          this.streamingState.rows.push(...msg.rows)
          if (msg.row_provenance) {
            this.streamingState.rowProvenance.push(...msg.row_provenance)
          }
        }
        break
      }
      case "result_end": {
        if (this.streamingState) {
          const s = this.streamingState
          this.streamingState = null

          // Track KG switches
          if (s.switchedKg) {
            this.knowledgeGraph = s.switchedKg
          }

          // Assemble synthetic WsResultMessage and resolve pending request
          const assembled: WsResultMessage = {
            type: "result",
            columns: s.columns,
            rows: s.rows,
            row_count: s.rows.length,
            total_count: s.totalCount,
            truncated: s.truncated,
            execution_time_ms: s.executionTimeMs,
            row_provenance: s.rowProvenance.length > 0 ? s.rowProvenance : undefined,
            metadata: s.metadata,
            switched_kg: s.switchedKg,
          }
          const pending = this.pendingQueue.shift()
          if (pending) {
            pending.resolve(assembled)
          }
        }
        break
      }
      // ── End streaming ──────────────────────────────────────────
      case "error": {
        // If we were streaming, abort and reject
        this.streamingState = null
        const pending = this.pendingQueue.shift()
        if (pending) {
          pending.reject(new WsError(msg))
        }
        break
      }
      case "notification": {
        for (const handler of this.notificationHandlers) {
          handler(msg)
        }
        break
      }
      case "pong":
        // Keep-alive acknowledged
        break
    }
  }

  private setState(state: ConnectionState, kg?: string): void {
    this.state = state
    for (const handler of this.stateHandlers) {
      handler(state, kg)
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.setState("disconnected")
      return
    }

    this.setState("reconnecting")
    this.reconnectAttempts++

    // Exponential backoff with jitter: delay * 2^attempt * (0.8..1.2)
    const base = this.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1)
    const jitter = 0.8 + Math.random() * 0.4
    const delay = Math.min(base * jitter, 30000)

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this.connect()
      } catch {
        // connect() failure will trigger onclose which calls attemptReconnect again
      }
    }, delay)
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  private startPing(): void {
    this.stopPing()
    this.pingInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: "ping" }))
      }
    }, 30000)
  }

  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval)
      this.pingInterval = null
    }
  }
}
