"use client"

import { create } from "zustand"
import { toast } from "sonner"
import { WsClient, WsError } from "./ws-client"
import type { WsNotificationMessage } from "./ws-types"
import { parseKgList, parseRelList, parseRuleList, parseRuleDefinition, parseDependenciesFromDefinition, parseRuleClauses, parseSessionNames, generateVariables } from "./ws-parsers"

// LocalStorage keys
const STORAGE_KEY_CONNECTION = "inputlayer_connection"
const STORAGE_KEY_SELECTED_KG = "inputlayer_selected_kg"
const STORAGE_KEY_EDITOR = "inputlayer_editor_content"
const STORAGE_KEY_HISTORY = "inputlayer_query_history"
// sessionStorage key - survives page refresh but cleared on tab close (security)
const SESSION_KEY_PASSWORD = "inputlayer_session_pw"

export interface IQLConnection {
  id: string
  name: string
  host: string
  port: number
  status: "connected" | "disconnected" | "connecting" | "reconnecting"
}

export interface KnowledgeGraph {
  id: string
  name: string
  description?: string
  relationsCount: number
  viewsCount: number
}

export interface Relation {
  id: string
  name: string
  arity: number
  tupleCount: number
  columns: string[]
  columnTypes: string[]
  data: (string | number | boolean | null)[][]
  isView: boolean
  isSession: boolean
}

export interface View {
  id: string
  name: string
  definition: string
  arity: number
  dependencies: string[]
  computationSteps: ComputationStep[]
  debugPlan: string
  isSession: boolean
  benchmark?: ViewBenchmark
}

export interface ComputationStep {
  id: string
  operation: string
  inputs: string[]
  output: string
  description: string
}

export interface ValidationError {
  line: number
  statement_index: number
  error: string
}

export interface QueryResult {
  id: string
  query: string
  data: (string | number | boolean | null)[][]
  columns: string[]
  executionTime: number
  timestamp: Date
  status: "success" | "error"
  error?: string
  validationErrors?: ValidationError[]
  truncated?: boolean
  totalCount?: number
  warnings?: string[]
  rowProvenance?: string[]
  hasEphemeral?: boolean
  ephemeralSources?: string[]
  proofTrees?: import("./ws-types").WsProofTree[]
  timingBreakdown?: import("./ws-types").WsTimingBreakdown
}

/** Profiling result for a view benchmark */
export interface ViewBenchmark {
  executionTimeMs: number
  timingBreakdown: import("./ws-types").WsTimingBreakdown
  rowCount: number
  benchmarkedAt: Date
}

/** Tracks the most recent mutation for a relation */
export interface RelationMutationEvent {
  relation: string
  operation: "insert" | "delete"
  count: number
  timestamp: Date
}

interface StoredConnection {
  host: string
  port: number
  name: string
  username: string
  // Password stored in sessionStorage (not localStorage) - survives page refresh
  // but is cleared when the tab/browser closes for security.
}

interface IQLStore {
  connection: IQLConnection | null
  username: string | null
  knowledgeGraphs: KnowledgeGraph[]
  selectedKnowledgeGraph: KnowledgeGraph | null
  relations: Relation[]
  views: View[]
  queryHistory: QueryResult[]
  editorContent: string
  isInitialized: boolean
  isRestoringSession: boolean
  isRefreshing: boolean
  queryCancelRef: (() => void) | null
  /** Most recent mutation per relation (for performance tab context) */
  recentMutations: Map<string, RelationMutationEvent>

  setEditorContent: (content: string) => void
  setConnection: (connection: IQLConnection | null) => void
  setKnowledgeGraphs: (knowledgeGraphs: KnowledgeGraph[]) => void
  selectKnowledgeGraph: (knowledgeGraph: KnowledgeGraph | null) => void
  setRelations: (relations: Relation[]) => void
  setViews: (views: View[]) => void
  addQueryToHistory: (queryResult: QueryResult) => void

  // API actions
  connect: (host: string, port: number, name: string, username: string, password: string) => Promise<void>
  disconnect: () => void
  loadKnowledgeGraph: (kgName: string) => Promise<void>
  executeQuery: (query: string) => Promise<QueryResult>
  executeInternalQuery: (query: string) => Promise<QueryResult>
  cancelCurrentQuery: () => void
  loadRelationData: (relationName: string) => Promise<Relation | null>
  loadViewData: (viewName: string) => Promise<View | null>
  debugQuery: (query: string) => Promise<string>
  whyQuery: (query: string) => Promise<string>
  whyNotQuery: (input: string) => Promise<string>
  createKnowledgeGraph: (name: string) => Promise<void>
  deleteKnowledgeGraph: (name: string) => Promise<void>
  deleteRelation: (name: string) => Promise<void>
  dropRule: (name: string, isSession: boolean) => Promise<void>
  loadExample: (categoryId: string, example: { name: string; description: string; code: string }) => Promise<void>

  // Persistence and refresh
  initFromStorage: () => Promise<void>
  refreshCurrentKnowledgeGraph: () => Promise<void>
}

// ── localStorage helpers ────────────────────────────────────────────────────

function safeLsSet(key: string, value: string) {
  try {
    localStorage.setItem(key, value)
  } catch {
    // Quota or security error - ignore
  }
}

function saveConnectionToStorage(host: string, port: number, name: string, username: string, password: string) {
  if (typeof window === "undefined") return
  const stored: StoredConnection = { host, port, name, username }
  safeLsSet(STORAGE_KEY_CONNECTION, JSON.stringify(stored))
  // Password in sessionStorage - survives page refresh, cleared on tab close
  try { sessionStorage.setItem(SESSION_KEY_PASSWORD, password) } catch {}
}

function isStoredConnection(v: unknown): v is StoredConnection {
  return (
    typeof v === "object" && v !== null &&
    typeof (v as StoredConnection).host === "string" &&
    typeof (v as StoredConnection).port === "number" &&
    typeof (v as StoredConnection).name === "string" &&
    typeof (v as StoredConnection).username === "string"
  )
}

function getConnectionFromStorage(): (StoredConnection & { password: string }) | null {
  if (typeof window === "undefined") return null
  const raw = localStorage.getItem(STORAGE_KEY_CONNECTION)
  if (!raw) return null
  try {
    const parsed: unknown = JSON.parse(raw)
    if (!isStoredConnection(parsed)) return null
    const password = sessionStorage.getItem(SESSION_KEY_PASSWORD) ?? ""
    return { ...parsed, password }
  } catch {
    return null
  }
}

function saveSelectedKgToStorage(kgName: string) {
  if (typeof window === "undefined") return
  safeLsSet(STORAGE_KEY_SELECTED_KG, kgName)
}

function getSelectedKgFromStorage(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(STORAGE_KEY_SELECTED_KG)
}

function clearStorage() {
  if (typeof window === "undefined") return
  localStorage.removeItem(STORAGE_KEY_CONNECTION)
  localStorage.removeItem(STORAGE_KEY_SELECTED_KG)
  localStorage.removeItem(STORAGE_KEY_EDITOR)
  try { sessionStorage.removeItem(SESSION_KEY_PASSWORD) } catch {}
}

const KG_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

function validateKgName(name: string): void {
  if (!KG_NAME_RE.test(name)) {
    throw new Error(`Invalid knowledge graph name "${name}". Names must start with a letter or underscore and contain only alphanumeric characters and underscores.`)
  }
}

// ── WS client singleton ────────────────────────────────────────────────────

let wsClient: WsClient | null = null
let notificationUnsubscribe: (() => void) | null = null
let stateUnsubscribe: (() => void) | null = null
let refreshDebounceTimer: ReturnType<typeof setTimeout> | null = null

// ── Helpers ─────────────────────────────────────────────────────────────────

/** Fetch KG list via `.kg list` and return parsed KnowledgeGraph objects */
async function fetchKnowledgeGraphs(ws: WsClient): Promise<KnowledgeGraph[]> {
  const result = await ws.execute(".kg list")
  const parsed = parseKgList(result)
  return parsed.map((kg, i) => ({
    id: String(i + 1),
    name: kg.name,
    relationsCount: 0,
    viewsCount: 0,
  }))
}

/** Fetch relations via `.rel` */
async function fetchRelations(ws: WsClient): Promise<Relation[]> {
  const result = await ws.execute(".rel")
  const parsed = parseRelList(result)
  return parsed.map((r, i) => ({
    id: `r${i + 1}`,
    name: r.name,
    arity: r.arity,
    tupleCount: r.tupleCount,
    columns: r.columns,
    columnTypes: r.columnTypes,
    data: [],
    isView: false,
    isSession: false,
  }))
}

/** Fetch views/rules via `.rule list` */
async function fetchViews(ws: WsClient): Promise<View[]> {
  const result = await ws.execute(".rule list")
  const parsed = parseRuleList(result)
  return parsed.map((r, i) => ({
    id: `v${i + 1}`,
    name: r.name,
    definition: "",
    arity: 0, // will be set from parsed definition in loadViewData()
    dependencies: [],
    computationSteps: [],
    debugPlan: "",
    isSession: false,
  }))
}

/** Build a WebSocket URL from host/port */
function buildWsUrl(host: string, port: number): string {
  // Use the page's own host when connecting to the same server that serves the GUI
  // to avoid cross-origin issues (e.g., 127.0.0.1 vs localhost)
  if (typeof window !== "undefined" && window.location.port === String(port)) {
    const protocol = window.location.protocol === "https:" ? "wss" : "ws"
    return `${protocol}://${window.location.hostname}:${port}/ws`
  }
  const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "wss" : "ws"
  return `${protocol}://${host}:${port}/ws`
}

/** Health check via REST (quick server reachability test) */
async function checkHealth(host: string, port: number): Promise<void> {
  // Use relative URL when connecting to the same server that serves the GUI
  // to avoid cross-origin issues (e.g., 127.0.0.1 vs localhost)
  let url: string
  if (typeof window !== "undefined" && window.location.port === String(port)) {
    url = "/health"
  } else {
    const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "https" : "http"
    url = `${protocol}://${host}:${port}/health`
  }
  const resp = await fetch(url)
  if (!resp.ok) {
    const body = await resp.text().catch(() => "")
    throw new Error(`Health check failed: ${resp.status}${body ? ` - ${body}` : ""}`)
  }
}

// ── Store ───────────────────────────────────────────────────────────────────

export const useIQLStore = create<IQLStore>((set, get) => ({
  connection: null,
  username: null,
  knowledgeGraphs: [],
  selectedKnowledgeGraph: null,
  relations: [],
  views: [],
  queryHistory: [],
  editorContent: "",
  isInitialized: false,
  isRestoringSession: false,
  isRefreshing: false,
  queryCancelRef: null,
  recentMutations: new Map(),

  setEditorContent: (content) => {
    set({ editorContent: content })
    if (typeof window !== "undefined") safeLsSet(STORAGE_KEY_EDITOR, content)
  },
  setConnection: (connection) => set({ connection }),
  setKnowledgeGraphs: (knowledgeGraphs) => set({ knowledgeGraphs }),
  selectKnowledgeGraph: (knowledgeGraph) => set({ selectedKnowledgeGraph: knowledgeGraph }),
  setRelations: (relations) => set({ relations }),
  setViews: (views) => set({ views }),
  addQueryToHistory: (queryResult) =>
    set((state) => {
      // Remove previous consecutive duplicate (same query text) to avoid clutter
      const prev = state.queryHistory
      const rest = prev.length > 0 && prev[0].query === queryResult.query ? prev.slice(1) : prev
      const newHistory = [queryResult, ...rest.slice(0, 49)]
      // Persist lightweight history to localStorage
      try {
        const serialized = newHistory.slice(0, 50).map((h) => ({
          id: h.id,
          query: h.query,
          status: h.status,
          executionTime: h.executionTime,
          timestamp: h.timestamp instanceof Date ? h.timestamp.toISOString() : h.timestamp,
          error: h.error,
        }))
        safeLsSet(STORAGE_KEY_HISTORY, JSON.stringify(serialized))
      } catch { /* ignore quota errors */ }
      return { queryHistory: newHistory }
    }),

  initFromStorage: async () => {
    if (get().isInitialized) return

    // Read ALL localStorage data BEFORE any set() calls to avoid intermediate renders
    let savedEditor: string | null = null
    let savedHistory: QueryResult[] = []
    let stored: (StoredConnection & { password: string }) | null = null

    if (typeof window !== "undefined") {
      savedEditor = localStorage.getItem(STORAGE_KEY_EDITOR)

      try {
        const rawHistory = localStorage.getItem(STORAGE_KEY_HISTORY)
        if (rawHistory) {
          const parsed = JSON.parse(rawHistory) as Array<{
            id: string; query: string; status: string; executionTime: number; timestamp: string; error?: string
          }>
          savedHistory = parsed.map((h) => ({
            id: h.id,
            query: h.query,
            data: [],
            columns: [],
            executionTime: h.executionTime,
            timestamp: new Date(h.timestamp),
            status: h.status as "success" | "error",
            error: h.error,
          }))
        }
      } catch { /* ignore corrupted history */ }

      stored = getConnectionFromStorage()
    }

    // Single atomic set: isInitialized + isRestoringSession + connection + editor + history
    // This prevents intermediate renders that would flash the ConnectionScreen.
    set({
      isInitialized: true,
      ...(savedEditor ? { editorContent: savedEditor } : {}),
      ...(savedHistory.length > 0 ? { queryHistory: savedHistory } : {}),
      ...(stored ? {
        isRestoringSession: true,
        connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connecting" as const },
      } : {}),
    })

    // Auto-connect: if no stored session but we're served by an InputLayer server
    // (same origin), skip the connection screen and connect automatically.
    if (!stored && typeof window !== "undefined") {
      const port = parseInt(window.location.port || (window.location.protocol === "https:" ? "443" : "80"), 10)
      try {
        await checkHealth(window.location.hostname, port)
        // Server is alive on same origin - auto-connect with dev API key
        stored = { host: window.location.hostname, port, name: "Local Server", username: "admin", password: "dev-api-key" }
        ;(stored as unknown as Record<string, unknown>).apiKey = "dev-api-key"
        set({
          isRestoringSession: true,
          connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connecting" as const },
        })
      } catch {
        // No same-origin server - show connection screen
      }
    }

    if (!stored) return

    try {
      await checkHealth(stored.host, stored.port)

      const savedKgName = getSelectedKgFromStorage()
      const kg = savedKgName || "default"

      const apiKey = (stored as unknown as Record<string, unknown>).apiKey as string | undefined
      const ws = new WsClient({ url: buildWsUrl(stored.host, stored.port), kg, username: stored.username, password: stored.password, apiKey })
      await ws.connect()
      wsClient = ws

      // Subscribe to state changes for reconnection feedback
      stateUnsubscribe = ws.onStateChange((state) => {
        const conn = get().connection
        if (conn) {
          set({ connection: { ...conn, status: state } })
        }
      })

      const knowledgeGraphs = await fetchKnowledgeGraphs(ws)

      set({
        connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connected" },
        username: stored.username,
        knowledgeGraphs,
      })

      // Determine which KG to load
      const targetKgName = savedKgName || (knowledgeGraphs.length > 0 ? knowledgeGraphs[0].name : null)
      if (targetKgName) {
        const targetKg = knowledgeGraphs.find((k) => k.name === targetKgName)
        if (targetKg) {
          await get().loadKnowledgeGraph(targetKg.name)
          toast.success("Session restored", { description: `Reconnected to "${targetKg.name}"` })
        } else if (knowledgeGraphs.length > 0) {
          await get().loadKnowledgeGraph(knowledgeGraphs[0].name)
        }
      }

      // Subscribe to notifications for auto-refresh
      notificationUnsubscribe = ws.onNotification((notification: WsNotificationMessage) => {
        // Optimistic tuple count update + track mutation events
        if (notification.event === "persistent_update") {
          const mutationEvent: RelationMutationEvent = {
            relation: notification.relation,
            operation: notification.operation as "insert" | "delete",
            count: notification.count,
            timestamp: new Date(),
          }
          set((state) => {
            const newMutations = new Map(state.recentMutations)
            newMutations.set(notification.relation, mutationEvent)
            return {
              recentMutations: newMutations,
              relations: state.relations.map((r) =>
                r.name === notification.relation
                  ? { ...r, tupleCount: Math.max(0, r.tupleCount + (notification.operation === "insert" ? notification.count : -notification.count)) }
                  : r
              ),
            }
          })
        }
        // Debounced full refresh
        if (refreshDebounceTimer) clearTimeout(refreshDebounceTimer)
        refreshDebounceTimer = setTimeout(() => {
          refreshDebounceTimer = null
          if (wsClient) get().refreshCurrentKnowledgeGraph()
        }, 500)
      })

      set({ isRestoringSession: false })
    } catch (error) {
      console.error("Failed to restore session:", error)
      if (stateUnsubscribe) { stateUnsubscribe(); stateUnsubscribe = null }
      if (notificationUnsubscribe) { notificationUnsubscribe(); notificationUnsubscribe = null }
      if (wsClient) { wsClient.disconnect(); wsClient = null }

      // Session restore failed - try auto-connect with dev API key before showing login
      if (typeof window !== "undefined") {
        const port = parseInt(window.location.port || (window.location.protocol === "https:" ? "443" : "80"), 10)
        try {
          await checkHealth(window.location.hostname, port)
          // Server is up - try connecting with dev API key
          const ws = new WsClient({ url: buildWsUrl(window.location.hostname, port), kg: "default", apiKey: "dev-api-key" })
          await ws.connect()
          wsClient = ws
          const knowledgeGraphs = await fetchKnowledgeGraphs(ws)
          saveConnectionToStorage(window.location.hostname, port, "Local Server", "admin", "dev-api-key")
          set({
            isRestoringSession: false,
            connection: { id: "1", name: "Local Server", host: window.location.hostname, port, status: "connected" },
            username: "admin",
            knowledgeGraphs,
          })
          if (knowledgeGraphs.length > 0) {
            await get().loadKnowledgeGraph(knowledgeGraphs[0].name)
          }
          return
        } catch {
          // Auto-connect also failed - show login screen
        }
      }

      set({ isRestoringSession: false, connection: null, knowledgeGraphs: [], selectedKnowledgeGraph: null, relations: [], views: [] })
    }
  },

  connect: async (host, port, name, username, password) => {
    set({ connection: { id: "1", name, host, port, status: "connecting" } })

    try {
      await checkHealth(host, port)

      const ws = new WsClient({ url: buildWsUrl(host, port), kg: "default", username, password })
      await ws.connect()
      wsClient = ws

      // Subscribe to state changes
      stateUnsubscribe = ws.onStateChange((state) => {
        const conn = get().connection
        if (conn) {
          set({ connection: { ...conn, status: state } })
        }
      })

      saveConnectionToStorage(host, port, name, username, password)

      const knowledgeGraphs = await fetchKnowledgeGraphs(ws)

      if (knowledgeGraphs.length === 0) {
        toast.warning("No knowledge graphs found", { description: "Create a knowledge graph to get started." })
        set({ connection: { id: "1", name, host, port, status: "connected" }, username, knowledgeGraphs })
        return
      }

      // Find current KG (the one the WS session is already bound to)
      const currentKg = knowledgeGraphs[0]

      set({ connection: { id: "1", name, host, port, status: "connected" }, username, knowledgeGraphs })

      if (currentKg) {
        await get().loadKnowledgeGraph(currentKg.name)
        toast.success("Connected", { description: `Using knowledge graph "${currentKg.name}"` })
      }

      // Subscribe to notifications
      notificationUnsubscribe = ws.onNotification((notification: WsNotificationMessage) => {
        if (notification.event === "persistent_update") {
          set((state) => ({
            relations: state.relations.map((r) =>
              r.name === notification.relation
                ? { ...r, tupleCount: Math.max(0, r.tupleCount + (notification.operation === "insert" ? notification.count : -notification.count)) }
                : r
            ),
          }))
        }
        if (refreshDebounceTimer) clearTimeout(refreshDebounceTimer)
        refreshDebounceTimer = setTimeout(() => {
          refreshDebounceTimer = null
          if (wsClient) get().refreshCurrentKnowledgeGraph()
        }, 500)
      })
    } catch (error) {
      console.error("Connection failed:", error)
      const errorMessage = error instanceof WsError ? error.message : error instanceof Error ? error.message : "Could not connect to server"
      toast.error("Connection failed", { description: errorMessage })
      if (wsClient) { wsClient.disconnect(); wsClient = null }
      set({ connection: { id: "1", name, host, port, status: "disconnected" }, knowledgeGraphs: [] })
    }
  },

  disconnect: () => {
    if (notificationUnsubscribe) { notificationUnsubscribe(); notificationUnsubscribe = null }
    if (stateUnsubscribe) { stateUnsubscribe(); stateUnsubscribe = null }
    if (refreshDebounceTimer) { clearTimeout(refreshDebounceTimer); refreshDebounceTimer = null }
    if (wsClient) { wsClient.disconnect(); wsClient = null }
    clearStorage()
    set({ connection: null, username: null, knowledgeGraphs: [], selectedKnowledgeGraph: null, relations: [], views: [] })
  },

  loadKnowledgeGraph: async (kgName: string) => {
    if (!wsClient) return
    const kg = get().knowledgeGraphs.find((k) => k.name === kgName)
    if (!kg) return

    try {
      // Switch KG on the server session
      await wsClient.execute(`.kg use ${kgName}`)

      // Fetch relations, views, and session info
      const [relations, views, sessionResult] = await Promise.all([
        fetchRelations(wsClient),
        fetchViews(wsClient),
        wsClient.execute(".session").catch(() => null),
      ])

      // Mark relations that are also views or session-derived
      const viewNames = new Set(views.map((v) => v.name))
      const sessionNames = sessionResult ? new Set(parseSessionNames(sessionResult)) : new Set<string>()
      const relationsWithFlags = relations.map((r) => ({
        ...r,
        isView: viewNames.has(r.name),
        isSession: sessionNames.has(r.name),
      }))
      const viewsWithFlags = views.map((v) => ({
        ...v,
        isSession: sessionNames.has(v.name),
      }))

      saveSelectedKgToStorage(kgName)

      const updatedKg = { ...kg, relationsCount: relations.length, viewsCount: views.length }
      set({
        selectedKnowledgeGraph: updatedKg,
        relations: relationsWithFlags,
        views: viewsWithFlags,
        knowledgeGraphs: get().knowledgeGraphs.map((k) =>
          k.name === kgName ? updatedKg : k
        ),
      })
    } catch (error) {
      console.error("Failed to load knowledge graph:", error)
    }
  },

  refreshCurrentKnowledgeGraph: async () => {
    if (!wsClient) return
    const kg = get().selectedKnowledgeGraph
    if (!kg) return

    set({ isRefreshing: true })

    try {
      const [knowledgeGraphs, relations, views, sessionResult] = await Promise.all([
        fetchKnowledgeGraphs(wsClient),
        fetchRelations(wsClient),
        fetchViews(wsClient),
        wsClient.execute(".session").catch(() => null),
      ])

      const updatedKg = knowledgeGraphs.find((k) => k.name === kg.name)
      const viewNames = new Set(views.map((v) => v.name))
      const sessionNames = sessionResult ? new Set(parseSessionNames(sessionResult)) : new Set<string>()
      const relationsWithFlags = relations.map((r) => ({
        ...r,
        isView: viewNames.has(r.name),
        isSession: sessionNames.has(r.name),
      }))
      // Preserve existing benchmark data across refreshes
      const existingViews = get().views
      const viewsWithFlags = views.map((v) => {
        const existing = existingViews.find((ev) => ev.name === v.name)
        return {
          ...v,
          isSession: sessionNames.has(v.name),
          benchmark: existing?.benchmark,
          definition: existing?.definition || v.definition,
          dependencies: existing?.dependencies?.length ? existing.dependencies : v.dependencies,
          computationSteps: existing?.computationSteps?.length ? existing.computationSteps : v.computationSteps,
          debugPlan: existing?.debugPlan || v.debugPlan,
          arity: existing?.arity || v.arity,
        }
      })

      set({
        knowledgeGraphs,
        selectedKnowledgeGraph: updatedKg
          ? { ...updatedKg, relationsCount: relations.length, viewsCount: views.length }
          : kg,
        relations: relationsWithFlags,
        views: viewsWithFlags,
        isRefreshing: false,
      })
    } catch (error) {
      console.error("Failed to refresh knowledge graph:", error)
      set({ isRefreshing: false })
    }
  },

  cancelCurrentQuery: () => {
    const cancel = get().queryCancelRef
    if (cancel) {
      cancel()
      set({ queryCancelRef: null })
    }
  },

  executeQuery: async (query: string): Promise<QueryResult> => {
    if (!wsClient) {
      const errorResult: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: [],
        columns: [],
        executionTime: 0,
        timestamp: new Date(),
        status: "error",
        error: "Not connected",
      }
      get().addQueryToHistory(errorResult)
      return errorResult
    }

    const start = Date.now()

    // Set up cancellation
    let cancelled = false
    set({ queryCancelRef: () => { cancelled = true } })

    try {
      const response = await wsClient.execute(query)

      if (cancelled) {
        return {
          id: crypto.randomUUID(),
          query,
          data: [],
          columns: [],
          executionTime: Date.now() - start,
          timestamp: new Date(),
          status: "error",
          error: "Query cancelled",
        }
      }

      const result: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: response.rows as (string | number | boolean | null)[][],
        columns: response.columns,
        executionTime: response.execution_time_ms,
        timestamp: new Date(),
        status: "success",
        truncated: response.truncated || undefined,
        totalCount: response.total_count,
        warnings: response.metadata?.warnings,
        rowProvenance: response.row_provenance,
        hasEphemeral: response.metadata?.has_ephemeral,
        ephemeralSources: response.metadata?.ephemeral_sources,
        proofTrees: response.proof_trees,
        timingBreakdown: response.timing_breakdown,
      }
      get().addQueryToHistory(result)
      set({ queryCancelRef: null })

      // Immediate refresh after mutations so navigating away shows fresh data
      const trimmed = query.trim()
      if (trimmed.startsWith("+") || trimmed.startsWith("-") ||
          trimmed.includes("<-") || trimmed.startsWith(".")) {
        get().refreshCurrentKnowledgeGraph()  // fire-and-forget
      }

      return result
    } catch (error) {
      set({ queryCancelRef: null })
      const executionTime = Date.now() - start
      const errorMessage = error instanceof WsError ? error.message : String(error)
      const validationErrors = error instanceof WsError ? error.validationErrors : undefined

      const errorResult: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: [],
        columns: [],
        executionTime,
        timestamp: new Date(),
        status: "error",
        error: errorMessage,
        validationErrors,
      }
      get().addQueryToHistory(errorResult)
      return errorResult
    }
  },

  executeInternalQuery: async (query: string): Promise<QueryResult> => {
    if (!wsClient) {
      return {
        id: crypto.randomUUID(),
        query,
        data: [],
        columns: [],
        executionTime: 0,
        timestamp: new Date(),
        status: "error",
        error: "Not connected",
      }
    }

    const start = Date.now()
    try {
      const response = await wsClient.execute(query)
      return {
        id: crypto.randomUUID(),
        query,
        data: response.rows as (string | number | boolean | null)[][],
        columns: response.columns,
        executionTime: response.execution_time_ms,
        timestamp: new Date(),
        status: "success",
        rowProvenance: response.row_provenance,
        hasEphemeral: response.metadata?.has_ephemeral,
        ephemeralSources: response.metadata?.ephemeral_sources,
        proofTrees: response.proof_trees,
        timingBreakdown: response.timing_breakdown,
      }
    } catch (error) {
      return {
        id: crypto.randomUUID(),
        query,
        data: [],
        columns: [],
        executionTime: Date.now() - start,
        timestamp: new Date(),
        status: "error",
        error: error instanceof WsError ? error.message : String(error),
      }
    }
  },

  loadRelationData: async (relationName: string): Promise<Relation | null> => {
    if (!wsClient) return null
    const relation = get().relations.find((r) => r.name === relationName)
    if (!relation) return null

    try {
      // Query the relation data using generated variable names
      const vars = generateVariables(relation.arity)
      const query = `?${relationName}(${vars.join(", ")})`
      const response = await wsClient.execute(query)

      // Prefer schema column names over generic query-result columns (col0, col1, ...)
      const responseArity = response.rows.length > 0 ? (response.rows[0] as unknown[]).length : relation.arity
      const hasSchemaColumns = relation.columns.length > 0
        && relation.columns.length === responseArity
        && !relation.columns.every((c, i) => c === `col${i}`)
      const updated: Relation = {
        ...relation,
        columns: hasSchemaColumns ? relation.columns : response.columns,
        data: response.rows as (string | number | boolean | null)[][],
        tupleCount: response.total_count,
      }

      set((state) => ({
        relations: state.relations.map((r) => (r.name === relationName ? updated : r)),
      }))
      return updated
    } catch (error) {
      console.error("Failed to load relation data:", error)
      return null
    }
  },

  loadViewData: async (viewName: string): Promise<View | null> => {
    if (!wsClient) return null
    const view = get().views.find((v) => v.name === viewName)
    if (!view) return null

    try {
      // Fetch rule definition
      const defResult = await wsClient.execute(`.rule def ${viewName}`)
      const definition = parseRuleDefinition(defResult)
      const dependencies = parseDependenciesFromDefinition(definition, viewName)

      // Build computation steps from parsed clauses
      const clauses = parseRuleClauses(definition)
      const computationSteps: ComputationStep[] = clauses.map((clause, i) => ({
        id: `step_${i}`,
        operation: `Clause ${i + 1}`,
        inputs: clause.body,
        output: clause.head,
        description: `${clause.head}(...) <- ${clause.body.join(", ")}(...)`,
      }))

      // Derive arity from the first clause head (more reliable than clauseCount)
      const arity = clauses.length > 0 ? clauses[0].headArity : view.arity

      // Fetch debug plan (best-effort)
      let debugPlan = ""
      try {
        const vars = generateVariables(arity > 0 ? arity : 2)
        debugPlan = await get().debugQuery(`?${viewName}(${vars.join(", ")})`)
      } catch {
        // debug may fail for views with no data or complex recursive views
      }

      // Auto-benchmark: run the view query to get timing breakdown (best-effort)
      let benchmark: ViewBenchmark | undefined
      try {
        const vars = generateVariables(arity > 0 ? arity : 2)
        const benchResult = await wsClient.execute(`?${viewName}(${vars.join(", ")})`)
        if (benchResult.timing_breakdown) {
          benchmark = {
            executionTimeMs: benchResult.execution_time_ms,
            timingBreakdown: benchResult.timing_breakdown,
            rowCount: benchResult.row_count,
            benchmarkedAt: new Date(),
          }
        }
      } catch {
        // benchmark may fail for views with no data or complex recursive views
      }

      const updated: View = {
        ...view,
        definition,
        dependencies,
        computationSteps,
        debugPlan,
        arity,
        benchmark,
      }

      set((state) => ({
        views: state.views.map((v) => (v.name === viewName ? updated : v)),
      }))
      return updated
    } catch (error) {
      console.error("Failed to load view data:", error)
      return null
    }
  },


  debugQuery: async (query: string): Promise<string> => {
    if (!wsClient) throw new Error("Not connected")
    const result = await wsClient.execute(`.debug ${query}`)
    return result.rows.map((row) => String(row[0])).join("\n")
  },

  whyQuery: async (query: string): Promise<string> => {
    if (!wsClient) throw new Error("Not connected")
    const result = await wsClient.execute(`.why ${query}`)
    return result.rows.map((row) => String(row[0])).join("\n")
  },

  whyNotQuery: async (input: string): Promise<string> => {
    if (!wsClient) throw new Error("Not connected")
    const result = await wsClient.execute(`.why_not ${input}`)
    return result.rows.map((row) => String(row[0])).join("\n")
  },

  createKnowledgeGraph: async (name: string): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
    validateKgName(name)
    await wsClient.execute(`.kg create ${name}`)
    // Refresh KG list
    const knowledgeGraphs = await fetchKnowledgeGraphs(wsClient)
    set({ knowledgeGraphs })
  },

  deleteKnowledgeGraph: async (name: string): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
    validateKgName(name)
    await wsClient.execute(`.kg drop ${name}`)
    // Refresh KG list
    const knowledgeGraphs = await fetchKnowledgeGraphs(wsClient)
    const selected = get().selectedKnowledgeGraph
    if (selected?.name === name) {
      set({ knowledgeGraphs, selectedKnowledgeGraph: null, relations: [], views: [] })
    } else {
      set({ knowledgeGraphs })
    }
  },

  deleteRelation: async (name: string): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
    await wsClient.execute(`.rel drop ${name}`)
    await get().refreshCurrentKnowledgeGraph()
  },

  dropRule: async (name: string, isSession: boolean): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
    const command = isSession ? `.session drop ${name}` : `.rule drop ${name}`
    await wsClient.execute(command)
    await get().refreshCurrentKnowledgeGraph()
  },

  loadExample: async (categoryId: string, example: { name: string; description: string; code: string }): Promise<void> => {
    if (!wsClient) return
    const { splitExampleCode } = await import("@/lib/examples")
    const { setup, queries } = splitExampleCode(example.code)

    // Meta examples (.kg commands, .rule management) just paste the code directly
    const hasMeta = example.code.includes(".kg ")
    const hasSetup = setup.trim().length > 0
    if (hasMeta || (!hasSetup && !queries.trim())) {
      const content = `// ${example.name}\n// ${example.description}\n\n${example.code}`
      set({ editorContent: content })
      if (typeof window !== "undefined") safeLsSet(STORAGE_KEY_EDITOR, content)
      return
    }

    // Use the example name as the KG name (slugified)
    const kgName = example.name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_|_$/g, "") || "_playground"

    try {
      // Wipe and recreate the KG, seed all data in one batch
      try { await wsClient.execute(`.kg drop ${kgName}`) } catch { /* ignore */ }
      try { await wsClient.execute(`.kg create ${kgName}`) } catch { /* ignore */ }
      await wsClient.execute(`.kg use ${kgName}`)
      if (setup) {
        // Send entire setup as one program (single round trip)
        try { await wsClient.execute(setup) } catch { /* ignore partial failures */ }
      }

      // Build guide content for the editor
      const guide = `// ${example.name}
// ${example.description}
//
// Knowledge graph "${kgName}" is ready with data and rules loaded.
// Follow the Learn panel on the right for a guided walkthrough.
// Paste suggested queries here and press Cmd+Enter to run them.
`

      // Load the KG (fetches relations, updates dropdown) - skip editor override
      await get().loadKnowledgeGraph(kgName)

      // Set the editor content to the guide
      set({ editorContent: guide })
      if (typeof window !== "undefined") safeLsSet(STORAGE_KEY_EDITOR, guide)

      // Auto-execute the first query so the user sees results immediately
      const firstQuery = queries.split("\n").find((l) => l.trim().startsWith("?") || l.trim().startsWith(".why") || l.trim().startsWith(".rel"))
      if (firstQuery) {
        await get().executeQuery(firstQuery.trim())
      }
    } catch (error) {
      console.error("Failed to load example:", error)
      const content = `// ${example.name}\n// ${example.description}\n\n${example.code}`
      set({ editorContent: content })
      if (typeof window !== "undefined") safeLsSet(STORAGE_KEY_EDITOR, content)
    }
  },
}))
