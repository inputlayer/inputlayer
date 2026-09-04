/**
 * Model gateway client.
 *
 * The gateway is a SEPARATE service from the engine: it holds the model
 * provider key, serves the OpenAI-compatible chat API, and streams
 * evaluation events. The Studio talks to it over plain HTTP/WebSocket with
 * its own bearer token - the engine WebSocket connection is unrelated and
 * stays in `iql-store`.
 *
 * Selection is a list of `<kg>/<ontology>` pairs: the named knowledge graph
 * must already have that ontology installed (Ontologies tab). Omit the
 * selection and the gateway is a plain OpenAI proxy.
 */

export interface ChatMessage {
  role: "system" | "user" | "assistant"
  content: string
}

export interface FindingSpan {
  message: string
  surface: string
}

export interface Finding {
  view: string
  title: string | null
  blocking: boolean
  spans: FindingSpan[]
  row: string[]
  proof: unknown
}

export interface Report {
  kg: string
  ontology: string
  digest: string
  status: "complete" | "incomplete"
  reason?: string
  findings: Finding[]
  dropped: string[]
  notes?: string[]
  trace?: {
    model?: string
    extract_ms?: number
    engine_ms?: number
    kg?: string
    conversation?: string
    statements?: string[]
    extraction?: unknown
  }
}

export interface ChatResult {
  content: string
  model: string
  reports: Report[]
  usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number }
  /** Set when the gateway refused to complete (enforce mode). */
  refusal?: { type: string; message: string }
}

export interface GatewayOntology {
  name: string
  version: string
  digest: string
  title: string
}

export interface GatewayConfig {
  url: string
  apiKey: string
}

/** Same-origin default works behind the reverse proxy with zero config. */
export function defaultGatewayUrl(): string {
  if (typeof window === "undefined") return "http://127.0.0.1:8081"
  return window.location.origin
}

function base(config: GatewayConfig): string {
  return config.url.replace(/\/+$/, "")
}

function headers(config: GatewayConfig, extra: Record<string, string> = {}): HeadersInit {
  const result: Record<string, string> = { "content-type": "application/json", ...extra }
  if (config.apiKey) result.authorization = `Bearer ${config.apiKey}`
  return result
}

async function readError(response: Response): Promise<string> {
  try {
    const body = await response.json()
    return body?.error?.message ?? `${response.status} ${response.statusText}`
    } catch {
    return `${response.status} ${response.statusText}`
  }
}

/** Gateway health: what it serves and whether a model key is configured. */
export async function gatewayHealth(
  config: GatewayConfig,
): Promise<{ ontologies: string[]; modelKeyConfigured: boolean; version: string }> {
  const response = await fetch(`${base(config)}/health`, { cache: "no-store" })
  if (!response.ok) throw new Error(await readError(response))
  const body = await response.json()
  return {
    ontologies: body.ontologies ?? [],
    modelKeyConfigured: Boolean(body.model_key_configured),
    version: body.version ?? "",
  }
}

/** Ontologies this gateway can evaluate against (requires the bearer key). */
export async function gatewayOntologies(config: GatewayConfig): Promise<GatewayOntology[]> {
  const response = await fetch(`${base(config)}/v1/ontologies`, {
    headers: headers(config),
    cache: "no-store",
  })
  if (!response.ok) throw new Error(await readError(response))
  const body = await response.json()
  return body.ontologies ?? []
}

export interface ChatRequestOptions {
  messages: ChatMessage[]
  model?: string
  /** `<kg>/<ontology>` pairs; empty means a plain proxy completion. */
  selections: string[]
  conversation?: string
  mode?: "annotate" | "enforce"
  trace?: boolean
}

/**
 * One chat turn. A refusal (enforce mode) is returned as data, not thrown:
 * the caller needs the reports that explain WHY it was refused.
 */
export async function chatCompletion(
  config: GatewayConfig,
  options: ChatRequestOptions,
): Promise<ChatResult> {
  const extra: Record<string, string> = {}
  if (options.selections.length > 0) extra["x-il-ontology"] = options.selections.join(", ")
  if (options.conversation) extra["x-il-conversation"] = options.conversation
  if (options.mode) extra["x-il-mode"] = options.mode
  if (options.trace) extra["x-il-trace"] = "1"

  const response = await fetch(`${base(config)}/v1/chat/completions`, {
    method: "POST",
    headers: headers(config, extra),
    body: JSON.stringify({
      model: options.model ?? "claude-haiku-4-5",
      messages: options.messages,
    }),
  })

  const body = await response.json().catch(() => null)
  if (!response.ok) {
    const reports: Report[] = body?.inputlayer?.reports ?? []
    // 422/503 from enforce mode carry the reports that justify the refusal.
    if (reports.length > 0 || body?.error) {
      return {
        content: "",
        model: options.model ?? "",
        reports,
        refusal: {
          type: body?.error?.type ?? "error",
          message: body?.error?.message ?? `${response.status} ${response.statusText}`,
        },
      }
    }
    throw new Error(`${response.status} ${response.statusText}`)
  }

  return {
    content: body?.choices?.[0]?.message?.content ?? "",
    model: body?.model ?? "",
    reports: body?.inputlayer?.reports ?? [],
    usage: body?.usage,
  }
}

export interface GatewayEvent {
  type: "translation" | "finding" | "report" | "lagged"
  conversation?: string
  request?: string
  kg?: string
  ontology?: string
  [key: string]: unknown
}

/**
 * Subscribe to a conversation's evaluation events. Returns a closer.
 *
 * Browsers cannot set headers on a WebSocket, so the bearer token rides in
 * the query string (the gateway accepts `?token=` for exactly this reason).
 */
export function subscribeEvents(
  config: GatewayConfig,
  conversation: string,
  onEvent: (event: GatewayEvent) => void,
  onError?: (message: string) => void,
): () => void {
  const url = new URL(`${base(config)}/v1/events`)
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
  url.searchParams.set("conversation", conversation)
  if (config.apiKey) url.searchParams.set("token", config.apiKey)

  let socket: WebSocket | null = null
  try {
    socket = new WebSocket(url.toString())
  } catch (error) {
    onError?.(error instanceof Error ? error.message : String(error))
    return () => {}
  }
  socket.onmessage = (message) => {
    try {
      onEvent(JSON.parse(message.data as string) as GatewayEvent)
    } catch {
      // A frame we cannot parse is not worth tearing the stream down for.
    }
  }
  socket.onerror = () => onError?.("event stream error")
  return () => socket?.close()
}
