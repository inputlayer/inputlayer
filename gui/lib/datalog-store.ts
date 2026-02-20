"use client"

import { create } from "zustand"
import { toast } from "sonner"
import {
  InputLayerClient,
  ApiError,
  type KnowledgeGraph as ApiKnowledgeGraph,
  type Relation as ApiRelation,
  type View as ApiView,
} from "@inputlayer/api-client"

// Create a singleton client instance
const client = new InputLayerClient({ baseUrl: "/api/v1" })

// LocalStorage keys
const STORAGE_KEY_CONNECTION = "inputlayer_connection"
const STORAGE_KEY_SELECTED_KG = "inputlayer_selected_kg"

export interface DatalogConnection {
  id: string
  name: string
  host: string
  port: number
  status: "connected" | "disconnected" | "connecting"
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
  data: (string | number | boolean)[][]
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
  data: (string | number | boolean)[][]
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

  // New actions for persistence and refresh
  initFromStorage: () => Promise<void>
  refreshCurrentKnowledgeGraph: () => Promise<void>
}

// Helper to convert API KnowledgeGraph to store KnowledgeGraph
function toKnowledgeGraph(kg: ApiKnowledgeGraph, index: number): KnowledgeGraph {
  return {
    id: String(index + 1),
    name: kg.name,
    description: kg.description,
    relationsCount: kg.relationsCount,
    viewsCount: kg.viewsCount,
  }
}

// Helper to convert API Relation to store Relation
function toRelation(r: ApiRelation, index: number): Relation {
  return {
    id: `r${index + 1}`,
    name: r.name,
    arity: r.arity,
    tupleCount: r.tupleCount,
    columns: r.columns,
    data: [],
    isView: r.isView,
  }
}

// Helper to convert API View to store View
function toView(v: ApiView, index: number): View {
  return {
    id: `v${index + 1}`,
    name: v.name,
    definition: v.definition,
    arity: v.arity,
    dependencies: v.dependencies,
    computationSteps: [],
  }
}

// Helper to save connection to localStorage
function saveConnectionToStorage(host: string, port: number, name: string) {
  if (typeof window === "undefined") return
  const stored: StoredConnection = { host, port, name }
  localStorage.setItem(STORAGE_KEY_CONNECTION, JSON.stringify(stored))
}

// Helper to get connection from localStorage
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

// Helper to save selected KG to localStorage
function saveSelectedKgToStorage(kgName: string) {
  if (typeof window === "undefined") return
  localStorage.setItem(STORAGE_KEY_SELECTED_KG, kgName)
}

// Helper to get selected KG from localStorage
function getSelectedKgFromStorage(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(STORAGE_KEY_SELECTED_KG)
}

// Helper to clear storage
function clearStorage() {
  if (typeof window === "undefined") return
  localStorage.removeItem(STORAGE_KEY_CONNECTION)
  localStorage.removeItem(STORAGE_KEY_SELECTED_KG)
}

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
    // Prevent multiple initializations
    if (get().isInitialized) return
    set({ isInitialized: true })

    const stored = getConnectionFromStorage()
    if (!stored) return

    // Try to reconnect silently
    set({ connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connecting" } })

    try {
      // Check health endpoint
      await client.admin.health()

      // Fetch knowledge graphs
      const kgData = await client.knowledgeGraphs.list()
      const knowledgeGraphs = kgData.knowledgeGraphs.map(toKnowledgeGraph)

      set({
        connection: { id: "1", name: stored.name, host: stored.host, port: stored.port, status: "connected" },
        knowledgeGraphs,
      })

      // Try to restore the previously selected knowledge graph
      const savedKgName = getSelectedKgFromStorage()
      const targetKgName = savedKgName || kgData.current

      if (targetKgName) {
        const targetKg = knowledgeGraphs.find((kg) => kg.name === targetKgName)
        if (targetKg) {
          await get().loadKnowledgeGraph(targetKg.name)
          toast.success("Session restored", {
            description: `Reconnected to "${targetKg.name}"`,
          })
        } else if (knowledgeGraphs.length > 0) {
          // Fallback to first KG if saved one doesn't exist
          await get().loadKnowledgeGraph(knowledgeGraphs[0].name)
        }
      } else if (knowledgeGraphs.length > 0) {
        await get().loadKnowledgeGraph(knowledgeGraphs[0].name)
      }
    } catch (error) {
      console.error("Failed to restore session:", error)
      // Clear invalid stored connection
      clearStorage()
      set({
        connection: null,
        knowledgeGraphs: [],
        selectedKnowledgeGraph: null,
        relations: [],
        views: [],
      })
    }
  },

  connect: async (host, port, name) => {
    set({ connection: { id: "1", name, host, port, status: "connecting" } })

    try {
      // Check health endpoint using the client
      await client.admin.health()

      // Fetch knowledge graphs using the client
      const kgData = await client.knowledgeGraphs.list()
      const knowledgeGraphs = kgData.knowledgeGraphs.map(toKnowledgeGraph)

      // Save connection to localStorage for persistence
      saveConnectionToStorage(host, port, name)

      if (knowledgeGraphs.length === 0) {
        toast.warning("No knowledge graphs found", {
          description: "Create a knowledge graph to get started.",
        })
        set({
          connection: { id: "1", name, host, port, status: "connected" },
          knowledgeGraphs,
        })
        return
      }

      // Check if there's a warning from the backend about KG not found
      if (kgData.warning) {
        toast.warning("Knowledge graph not found", {
          description: kgData.warning,
        })
      }

      // Find the current knowledge graph to auto-select it
      const currentKgName = kgData.current
      let currentKg = currentKgName ? knowledgeGraphs.find((kg) => kg.name === currentKgName) : null

      // If the current KG doesn't exist, fall back to the first one
      if (!currentKg) {
        currentKg = knowledgeGraphs[0]
      }

      set({
        connection: { id: "1", name, host, port, status: "connected" },
        knowledgeGraphs,
      })

      // Auto-load the selected knowledge graph
      if (currentKg) {
        await get().loadKnowledgeGraph(currentKg.name)
        toast.success("Connected", {
          description: `Using knowledge graph "${currentKg.name}"`,
        })
      }
    } catch (error) {
      console.error("Connection failed:", error)
      const errorMessage = error instanceof ApiError ? error.message : "Could not connect to server"
      toast.error("Connection failed", {
        description: errorMessage,
      })
      set({
        connection: { id: "1", name, host, port, status: "disconnected" },
        knowledgeGraphs: [],
      })
    }
  },

  disconnect: () => {
    // Clear localStorage on disconnect
    clearStorage()
    set({
      connection: null,
      knowledgeGraphs: [],
      selectedKnowledgeGraph: null,
      relations: [],
      views: [],
    })
  },

  loadKnowledgeGraph: async (kgName: string) => {
    const kg = get().knowledgeGraphs.find((k) => k.name === kgName)
    if (!kg) return

    try {
      // Fetch relations using the client
      const relData = await client.relations.list(kgName)
      const relations = relData.relations.map(toRelation)

      // Fetch views using the client
      const viewData = await client.views.list(kgName)
      const views = viewData.views.map(toView)

      // Save selected KG to localStorage
      saveSelectedKgToStorage(kgName)

      set({
        selectedKnowledgeGraph: kg,
        relations,
        views,
      })
    } catch (error) {
      console.error("Failed to load knowledge graph:", error)
    }
  },

  refreshCurrentKnowledgeGraph: async () => {
    const kg = get().selectedKnowledgeGraph
    if (!kg) return

    set({ isRefreshing: true })

    try {
      // Re-fetch the KG list to get updated counts
      const kgData = await client.knowledgeGraphs.list()
      const knowledgeGraphs = kgData.knowledgeGraphs.map(toKnowledgeGraph)

      // Find the updated KG info
      const updatedKg = knowledgeGraphs.find((k) => k.name === kg.name)

      // Fetch relations
      const relData = await client.relations.list(kg.name)
      const relations = relData.relations.map(toRelation)

      // Fetch views
      const viewData = await client.views.list(kg.name)
      const views = viewData.views.map(toView)

      set({
        knowledgeGraphs,
        selectedKnowledgeGraph: updatedKg || kg,
        relations,
        views,
        isRefreshing: false,
      })
    } catch (error) {
      console.error("Failed to refresh knowledge graph:", error)
      set({ isRefreshing: false })
    }
  },

  executeQuery: async (query: string): Promise<QueryResult> => {
    const kg = get().selectedKnowledgeGraph
    const start = Date.now()

    try {
      const response = await client.query.execute({
        query,
        knowledgeGraph: kg?.name || "default",
        timeoutMs: 30000,
      })

      const result: QueryResult = {
        id: crypto.randomUUID(),
        query,
        data: response.rows as (string | number | boolean)[][],
        columns: response.columns,
        executionTime: response.executionTimeMs,
        timestamp: new Date(),
        status: response.status === "success" ? "success" : "error",
        error: response.error,
      }
      get().addQueryToHistory(result)
      return result
    } catch (error) {
      const executionTime = Date.now() - start
      const errorMessage = error instanceof ApiError
        ? `[${error.code}] ${error.message}`
        : String(error)

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
    const kg = get().selectedKnowledgeGraph
    if (!kg) return null

    try {
      const data = await client.relations.getData(kg.name, relationName)

      const relation = get().relations.find((r) => r.name === relationName)
      if (relation) {
        const updated: Relation = {
          ...relation,
          columns: data.columns,
          data: data.rows as (string | number | boolean)[][],
          tupleCount: data.totalCount,
        }
        // Update the relation in the store
        set((state) => ({
          relations: state.relations.map((r) =>
            r.name === relationName ? updated : r
          ),
        }))
        return updated
      }
      return null
    } catch (error) {
      console.error("Failed to load relation data:", error)
      return null
    }
  },

  loadViewData: async (viewName: string): Promise<View | null> => {
    const kg = get().selectedKnowledgeGraph
    if (!kg) return null

    try {
      const data = await client.views.get(kg.name, viewName)

      const view = get().views.find((v) => v.name === viewName)
      if (view) {
        const updated: View = {
          ...view,
          definition: data.definition,
          dependencies: data.dependencies,
        }
        set((state) => ({
          views: state.views.map((v) =>
            v.name === viewName ? updated : v
          ),
        }))
        return updated
      }
      return null
    } catch (error) {
      console.error("Failed to load view data:", error)
      return null
    }
  },
}))
