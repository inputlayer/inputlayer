import WebSocket from "ws"
import type { Config } from "./config.js"

interface PendingRequest {
  resolve: (rows: unknown[][]) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/** Streaming result accumulator */
interface StreamingState {
  rows: unknown[][]
}

/**
 * WebSocket client that connects to InputLayer as admin
 * and executes commands for user provisioning.
 */
export class AdminClient {
  private ws: WebSocket | null = null
  private authenticated = false
  private pendingQueue: PendingRequest[] = []
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempts = 0
  private readonly maxReconnectAttempts = 20
  private readonly reconnectDelayMs = 1000
  private connectPromise: Promise<void> | null = null
  private streamingState: StreamingState | null = null

  constructor(private config: Config) {}

  async connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.doConnect()
    try {
      await this.connectPromise
    } finally {
      this.connectPromise = null
    }
  }

  private doConnect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const { host, port, adminUser, adminPassword } = this.config.inputlayer
      const url = `ws://${host}:${port}/ws?kg=_internal`

      if (this.ws) {
        this.ws.close()
        this.ws = null
      }

      this.authenticated = false
      const ws = new WebSocket(url)

      ws.on("open", () => {
        ws.send(
          JSON.stringify({
            type: "login",
            username: adminUser,
            password: adminPassword,
          })
        )
      })

      ws.on("message", (data) => {
        let msg: Record<string, unknown>
        try {
          msg = JSON.parse(data.toString())
        } catch {
          return
        }

        if (!this.authenticated) {
          if (msg.type === "authenticated") {
            this.authenticated = true
            this.reconnectAttempts = 0
            console.log("[admin-client] authenticated to InputLayer")
            resolve()
            return
          }
          if (msg.type === "auth_error") {
            const err = new Error(`Admin auth failed: ${msg.message}`)
            console.error("[admin-client]", err.message)
            ws.close()
            reject(err)
            return
          }
          return
        }

        this.handleMessage(msg)
      })

      ws.on("error", (err) => {
        if (!this.authenticated) {
          reject(new Error(`WebSocket error: ${err.message}`))
        }
      })

      ws.on("close", () => {
        this.ws = null
        this.authenticated = false

        // Reject pending requests
        for (const pending of this.pendingQueue) {
          clearTimeout(pending.timer)
          pending.reject(new Error("Connection closed"))
        }
        this.pendingQueue = []

        this.scheduleReconnect()
      })

      this.ws = ws
    })
  }

  private handleMessage(msg: Record<string, unknown>) {
    switch (msg.type) {
      case "result": {
        const pending = this.pendingQueue.shift()
        if (pending) {
          clearTimeout(pending.timer)
          pending.resolve((msg.rows as unknown[][]) ?? [])
        }
        break
      }
      case "result_start": {
        this.streamingState = { rows: [] }
        break
      }
      case "result_chunk": {
        if (this.streamingState && Array.isArray(msg.rows)) {
          this.streamingState.rows.push(...(msg.rows as unknown[][]))
        }
        break
      }
      case "result_end": {
        if (this.streamingState) {
          const rows = this.streamingState.rows
          this.streamingState = null
          const pending = this.pendingQueue.shift()
          if (pending) {
            clearTimeout(pending.timer)
            pending.resolve(rows)
          }
        }
        break
      }
      case "error": {
        this.streamingState = null
        const pending = this.pendingQueue.shift()
        if (pending) {
          clearTimeout(pending.timer)
          pending.reject(new Error(String(msg.message || "Unknown error")))
        }
        break
      }
    }
  }

  private scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error("[admin-client] max reconnect attempts reached")
      return
    }

    this.reconnectAttempts++
    const delay = Math.min(
      this.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1),
      30000
    )

    console.log(`[admin-client] reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`)

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this.connect()
      } catch (err) {
        console.error("[admin-client] reconnect failed:", err)
      }
    }, delay)
  }

  /** Execute a Datalog program or meta command, return result rows. */
  async execute(program: string, timeoutMs = 15000): Promise<unknown[][]> {
    if (!this.ws || !this.authenticated) {
      await this.connect()
    }

    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("Not connected"))
        return
      }

      const timer = setTimeout(() => {
        const idx = this.pendingQueue.findIndex((p) => p.timer === timer)
        if (idx !== -1) this.pendingQueue.splice(idx, 1)
        reject(new Error(`Command timed out after ${timeoutMs}ms`))
      }, timeoutMs)

      this.pendingQueue.push({ resolve, reject, timer })
      this.ws.send(JSON.stringify({ type: "execute", program }))
    })
  }

  /**
   * Create a demo viewer user and grant access to a knowledge graph.
   * Returns { username, password } on success.
   */
  /** All demo knowledge graphs that users should be able to access */
  private static readonly DEMO_KGS = [
    "default", "flights", "rules_vectors", "retraction", "incremental", "provenance",
  ]

  async createDemoUser(
    username: string,
    password: string,
    kg: string
  ): Promise<{ username: string; password: string }> {
    // Create user with editor role so they can create their own KGs (e.g. tutorials)
    await this.execute(`.user create ${username} ${password} editor`)

    // Grant viewer access to all demo KGs so users can browse but not modify them
    for (const kgName of AdminClient.DEMO_KGS) {
      try {
        await this.execute(`.kg acl grant ${kgName} ${username} viewer`)
      } catch {
        // KG may not exist yet - ignore
      }
    }

    console.log(`[admin-client] created demo user ${username} with editor role and viewer access to demo KGs`)
    return { username, password }
  }

  disconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
    this.authenticated = false
  }
}
