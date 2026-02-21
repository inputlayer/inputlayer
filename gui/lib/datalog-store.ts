"use client"

import { create } from "zustand"
import { toast } from "sonner"
import { WsClient, WsError } from "./ws-client"
import type { WsNotificationMessage } from "./ws-types"
import { parseKgList, parseRelList, parseRuleList, parseRuleDefinition, generateVariables } from "./ws-parsers"

// LocalStorage keys
const STORAGE_KEY_CONNECTION = "inputlayer_connection"
const STORAGE_KEY_SELECTED_KG = "inputlayer_selected_kg"

export interface DatalogConnection {
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
  data: (string | number | boolean | null)[][]
  isView: boolean
}

export interface View {
  id: string
  name: string
  definition: string
  arity: number
  dependencies: string[]
  computationSteps: ComputationStep[]
}

export interface ComputationStep {
  id: string
  operation: string
  inputs: string[]
  output: string
  description: string
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
}

interface StoredConnection {
  host: string
  port: number
  name: string
}

interface DatalogStore {
  connection: DatalogConnection | null
  knowledgeGraphs: KnowledgeGraph[]
  selectedKnowledgeGraph: KnowledgeGraph | null
  relations: Relation[]
  views: View[]
  queryHistory: QueryResult[]
  isInitialized: boolean
  isRefreshing: boolean

  setConnection: (connection: DatalogConnection | null) => void
  setKnowledgeGraphs: (knowledgeGraphs: KnowledgeGraph[]) => void
  selectKnowledgeGraph: (knowledgeGraph: KnowledgeGraph | null) => void
  setRelations: (relations: Relation[]) => void
  setViews: (views: View[]) => void
  addQueryToHistory: (queryResult: QueryResult) => void

  // API actions
  connect: (host: string, port: number, name: string) => Promise<void>
  disconnect: () => void
  loadKnowledgeGraph: (kgName: string) => Promise<void>
  executeQuery: (query: string) => Promise<QueryResult>
  loadRelationData: (relationName: string) => Promise<Relation | null>
  loadViewData: (viewName: string) => Promise<View | null>
  explainQuery: (query: string) => Promise<string>
  createKnowledgeGraph: (name: string) => Promise<void>
  deleteKnowledgeGraph: (name: string) => Promise<void>

  // Persistence and refresh
  initFromStorage: () => Promise<void>
  refreshCurrentKnowledgeGraph: () => Promise<void>
}

// ── localStorage helpers ────────────────────────────────────────────────────

function saveConnectionToStorage(host: string, port: number, name: string) {
  if (typeof window === "undefined") return
  const stored: StoredConnection = { host, port, name }
  localStorage.setItem(STORAGE_KEY_CONNECTION, JSON.stringify(stored))
}

function getConnectionFromStorage(): StoredConnection | null {
  if (typeof window === "undefined") return null
  const stored = localStorage.getItem(STORAGE_KEY_CONNECTION)
  if (!stored) return null
  try {
    return JSON.parse(stored) as StoredConnection
  } catch {
    return null
  }
}

function saveSelectedKgToStorage(kgName: string) {
  if (typeof window === "undefined") return
  localStorage.setItem(STORAGE_KEY_SELECTED_KG, kgName)
}

function getSelectedKgFromStorage(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(STORAGE_KEY_SELECTED_KG)
}

function clearStorage() {
  if (typeof window === "undefined") return
  localStorage.removeItem(STORAGE_KEY_CONNECTION)
  localStorage.removeItem(STORAGE_KEY_SELECTED_KG)
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
    data: [],
    isView: false,
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
    arity: r.clauseCount, // will be refined when loading view data
    dependencies: [],
    computationSteps: [],
  }))
}

/** Build a WebSocket URL from host/port */
function buildWsUrl(host: string, port: number): string {
  const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "wss" : "ws"
  return `${protocol}://${host}:${port}/ws`
}

/** Health check via REST (quick server reachability test) */
async function checkHealth(host: string, port: number): Promise<void> {
  const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "https" : "http"
  const resp = await fetch(`${protocol}://${host}:${port}/health`)
  if (!resp.ok) throw new Error(`Health check failed: ${resp.status}`)
}

// ── Store ───────────────────────────────────────────────────────────────────

export const useDatalogStore = create<DatalogStore>((set, get) => ({
  connection: null,
  knowledgeGraphs: [],
  selectedKnowledgeGraph: null,
  relations: [],
  views: [],
  queryHistory: [],
  isInitialized: false,
  isRefreshing: false,

  setConnection: (connection) => set({ connection }),
  setKnowledgeGraphs: (knowledgeGraphs) => set({ knowledgeGraphs }),
  selectKnowledgeGraph: (knowledgeGraph) => set({ selectedKnowledgeGraph: knowledgeGraph }),
  setRelations: (relations) => set({ relations }),
  setViews: (views) => set({ views }),
  addQueryToHistory: (queryResult) =>
    set((state) => ({
      queryHistory: [queryResult, ...state.queryHistory.slice(0, 49)],
    })),

  initFromStorage: async () => {
    if (get().isInitialized) return
    set({ isInitialized: true })

    const stored = getConnectionFromStorage()
    if (!stored) return

    set({ connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connecting" } })

    try {
      await checkHealth(stored.host, stored.port)

      const savedKgName = getSelectedKgFromStorage()
      const kg = savedKgName || "default"

      const ws = new WsClient({ url: buildWsUrl(stored.host, stored.port), kg })
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
        // Optimistic tuple count update
        if (notification.event === "persistent_update") {
          set((state) => ({
            relations: state.relations.map((r) =>
              r.name === notification.relation
                ? { ...r, tupleCount: Math.max(0, r.tupleCount + (notification.operation === "insert" ? notification.count : -notification.count)) }
                : r
            ),
          }))
        }
        // Debounced full refresh
        if (refreshDebounceTimer) clearTimeout(refreshDebounceTimer)
        refreshDebounceTimer = setTimeout(() => get().refreshCurrentKnowledgeGraph(), 500)
      })
    } catch (error) {
      console.error("Failed to restore session:", error)
      clearStorage()
      if (wsClient) { wsClient.disconnect(); wsClient = null }
      set({ connection: null, knowledgeGraphs: [], selectedKnowledgeGraph: null, relations: [], views: [] })
    }
  },

  connect: async (host, port, name) => {
    set({ connection: { id: "1", name, host, port, status: "connecting" } })

    try {
      await checkHealth(host, port)

      const ws = new WsClient({ url: buildWsUrl(host, port), kg: "default" })
      await ws.connect()
      wsClient = ws

      // Subscribe to state changes
      stateUnsubscribe = ws.onStateChange((state) => {
        const conn = get().connection
        if (conn) {
          set({ connection: { ...conn, status: state } })
        }
      })

      saveConnectionToStorage(host, port, name)

      const knowledgeGraphs = await fetchKnowledgeGraphs(ws)

      if (knowledgeGraphs.length === 0) {
        toast.warning("No knowledge graphs found", { description: "Create a knowledge graph to get started." })
        set({ connection: { id: "1", name, host, port, status: "connected" }, knowledgeGraphs })
        return
      }

      // Find current KG (the one the WS session is already bound to)
      const currentKg = knowledgeGraphs[0]

      set({ connection: { id: "1", name, host, port, status: "connected" }, knowledgeGraphs })

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
        refreshDebounceTimer = setTimeout(() => get().refreshCurrentKnowledgeGraph(), 500)
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
    set({ connection: null, knowledgeGraphs: [], selectedKnowledgeGraph: null, relations: [], views: [] })
  },

  loadKnowledgeGraph: async (kgName: string) => {
    if (!wsClient) return
    const kg = get().knowledgeGraphs.find((k) => k.name === kgName)
    if (!kg) return

    try {
      // Switch KG on the server session
      await wsClient.execute(`.kg use ${kgName}`)

      // Fetch relations and views
      const [relations, views] = await Promise.all([
        fetchRelations(wsClient),
        fetchViews(wsClient),
      ])

      // Mark relations that are also views
      const viewNames = new Set(views.map((v) => v.name))
      const relationsWithViewFlag = relations.map((r) => ({
        ...r,
        isView: viewNames.has(r.name),
      }))

      saveSelectedKgToStorage(kgName)

      set({
        selectedKnowledgeGraph: { ...kg, relationsCount: relations.length, viewsCount: views.length },
        relations: relationsWithViewFlag,
        views,
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
      const [knowledgeGraphs, relations, views] = await Promise.all([
        fetchKnowledgeGraphs(wsClient),
        fetchRelations(wsClient),
        fetchViews(wsClient),
      ])

      const updatedKg = knowledgeGraphs.find((k) => k.name === kg.name)
      const viewNames = new Set(views.map((v) => v.name))
      const relationsWithViewFlag = relations.map((r) => ({
        ...r,
        isView: viewNames.has(r.name),
      }))

      set({
        knowledgeGraphs,
        selectedKnowledgeGraph: updatedKg
          ? { ...updatedKg, relationsCount: relations.length, viewsCount: views.length }
          : kg,
        relations: relationsWithViewFlag,
        views,
        isRefreshing: false,
      })
    } catch (error) {
      console.error("Failed to refresh knowledge graph:", error)
      set({ isRefreshing: false })
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

    try {
      const response = await wsClient.execute(query)

      const result: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: response.rows as (string | number | boolean | null)[][],
        columns: response.columns,
        executionTime: response.execution_time_ms,
        timestamp: new Date(),
        status: "success",
      }
      get().addQueryToHistory(result)
      return result
    } catch (error) {
      const executionTime = Date.now() - start
      const errorMessage = error instanceof WsError ? error.message : String(error)

      const errorResult: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: [],
        columns: [],
        executionTime,
        timestamp: new Date(),
        status: "error",
        error: errorMessage,
      }
      get().addQueryToHistory(errorResult)
      return errorResult
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
      const hasSchemaColumns = relation.columns.length > 0
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
      const defResult = await wsClient.execute(`.rule show ${viewName}`)
      const definition = parseRuleDefinition(defResult)

      const updated: View = {
        ...view,
        definition,
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

  explainQuery: async (query: string): Promise<string> => {
    if (!wsClient) throw new Error("Not connected")
    const result = await wsClient.execute(`.explain ${query}`)
    return result.rows.map((row) => String(row[0])).join("\n")
  },

  createKnowledgeGraph: async (name: string): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
    await wsClient.execute(`.kg create ${name}`)
    // Refresh KG list
    const knowledgeGraphs = await fetchKnowledgeGraphs(wsClient)
    set({ knowledgeGraphs })
  },

  deleteKnowledgeGraph: async (name: string): Promise<void> => {
    if (!wsClient) throw new Error("Not connected")
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
}))
