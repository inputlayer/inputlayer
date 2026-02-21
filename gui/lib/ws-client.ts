import type {
  WsConnectedMessage,
  WsResultMessage,
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

export class WsClient {
  private ws: WebSocket | null = null
  private sessionId: number | null = null
  private knowledgeGraph = "default"
  private state: ConnectionState = "disconnected"
  private pendingQueue: PendingRequest[] = []
  private notificationHandlers = new Set<NotificationHandler>()
  private stateHandlers = new Set<ConnectionStateHandler>()
  private reconnectAttempts = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private intentionalClose = false
  private pingInterval: ReturnType<typeof setInterval> | null = null

  private readonly url: string
  private readonly kg: string
  private readonly autoReconnect: boolean
  private readonly maxReconnectAttempts: number
  private readonly reconnectDelayMs: number

  constructor(config: WsClientConfig) {
    this.url = config.url
    this.kg = config.kg ?? "default"
    this.autoReconnect = config.autoReconnect ?? true
    this.maxReconnectAttempts = config.maxReconnectAttempts ?? 10
    this.reconnectDelayMs = config.reconnectDelayMs ?? 1000
  }

  /** Connect to the WebSocket server. Resolves when `connected` message received. */
  connect(): Promise<WsConnectedMessage> {
    return new Promise((resolve, reject) => {
      if (this.ws) {
        this.ws.close()
        this.ws = null
      }

      this.intentionalClose = false
      this.setState("connecting")

      const wsUrl = `${this.url}?kg=${encodeURIComponent(this.kg)}`
      const ws = new WebSocket(wsUrl)

      let connected = false

      ws.onopen = () => {
        // Wait for the `connected` message
      }

      ws.onmessage = (event) => {
        let msg: WsServerMessage
        try {
          msg = JSON.parse(event.data)
        } catch {
          return
        }

        if (!connected && msg.type === "connected") {
          connected = true
          this.sessionId = msg.session_id
          this.knowledgeGraph = msg.knowledge_graph
          this.reconnectAttempts = 0
          this.setState("connected", this.knowledgeGraph)
          this.startPing()
          resolve(msg)
          return
        }

        this.handleMessage(msg)
      }

      ws.onerror = () => {
        if (!connected) {
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

        if (!connected) {
          this.setState("disconnected")
          reject(new Error("WebSocket connection closed before connected"))
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
  execute(program: string): Promise<WsResultMessage> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("WebSocket not connected"))
        return
      }

      this.pendingQueue.push({ resolve, reject })
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

  getSessionId(): number | null {
    return this.sessionId
  }

  getKnowledgeGraph(): string {
    return this.knowledgeGraph
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
      case "error": {
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
