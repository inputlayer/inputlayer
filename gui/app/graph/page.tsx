"use client"

import { Suspense, useState, useEffect, useMemo, useCallback, useRef } from "react"
import { useSearchParams } from "next/navigation"
import { AppShell } from "@/components/app-shell"
import { GraphSidebar } from "@/components/graph-sidebar"
import { GraphCanvas } from "@/components/graph-canvas"
import { useDatalogStore, type Relation } from "@/lib/datalog-store"
import { buildGraphElements } from "@/lib/graph-utils"
import { generateVariables } from "@/lib/ws-parsers"
import { AlertCircle, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"

function GraphPageInner() {
  const {
    selectedKnowledgeGraph,
    relations,
    views,
    isRefreshing,
    refreshCurrentKnowledgeGraph,
    loadRelationData,
    executeInternalQuery,
  } = useDatalogStore()

  const searchParams = useSearchParams()
  const [selectedNames, setSelectedNames] = useState<Set<string>>(new Set())
  const [loadingRelations, setLoadingRelations] = useState<Set<string>>(new Set())
  const [viewRelations, setViewRelations] = useState<Map<string, Relation>>(new Map())
  const [grouped, setGrouped] = useState(false)
  const initialSelectHandled = useRef(false)
  const loadGenRef = useRef(0)
  const inFlightLoads = useRef(new Set<string>())

  // Merge base relations with view-derived relations for the graph
  const allRelations = useMemo(() => {
    const baseRelNames = new Set(relations.map((r) => r.name))
    // Include views not already in base relations (arity 0 means not yet loaded)
    const viewEntries: Relation[] = views
      .filter((v) => !baseRelNames.has(v.name))
      .map((v) => viewRelations.get(v.name) ?? {
        id: `view_${v.name}`,
        name: v.name,
        arity: v.arity || 2,
        tupleCount: 0,
        columns: [],
        columnTypes: [],
        data: [],
        isView: true,
        isSession: v.isSession,
      })
    return [...relations, ...viewEntries]
  }, [relations, views, viewRelations])

  const graphRelations = useMemo(
    () => allRelations.filter((r) => r.arity >= 1),
    [allRelations]
  )

  useEffect(() => {
    if (selectedKnowledgeGraph) {
      refreshCurrentKnowledgeGraph()
    }
  }, [selectedKnowledgeGraph?.name])

  // Guard against concurrent loads of the same relation
  const deduplicatedLoad = useCallback(async (name: string, loadFn: () => Promise<unknown>) => {
    if (inFlightLoads.current.has(name)) return
    inFlightLoads.current.add(name)
    try { await loadFn() } finally { inFlightLoads.current.delete(name) }
  }, [])

  const loadViewAsRelation = useCallback(async (name: string) => {
    setLoadingRelations((prev) => new Set(prev).add(name))
    try {
      const view = views.find((v) => v.name === name)
      // If arity is unknown (0), try with 2 variables (binary)
      const arity = (view?.arity && view.arity > 0) ? view.arity : 2
      const vars = generateVariables(arity)
      const result = await executeInternalQuery(`?${name}(${vars.join(", ")})`)
      if (result.status === "success") {
        const actualArity = result.columns.length || arity
        const rel: Relation = {
          id: `view_${name}`,
          name,
          arity: actualArity,
          tupleCount: result.data.length,
          columns: result.columns,
          columnTypes: [],
          data: result.data,
          isView: true,
          isSession: view?.isSession ?? false,
        }
        setViewRelations((prev) => new Map(prev).set(name, rel))
      } else {
        console.warn(`Failed to load view "${name}":`, result.error)
      }
    } catch (err) {
      console.warn(`Failed to load view "${name}":`, err)
    } finally {
      setLoadingRelations((prev) => {
        const s = new Set(prev)
        s.delete(name)
        return s
      })
    }
  }, [views, executeInternalQuery])

  const handleToggleRelation = useCallback(async (name: string) => {
    setSelectedNames((prev) => {
      const next = new Set(prev)
      if (next.has(name)) {
        next.delete(name)
      } else {
        next.add(name)
        const rel = allRelations.find((r) => r.name === name)
        if (rel && rel.data.length === 0) {
          // Check if it's a view-only relation (not in base relations)
          const isViewOnly = !relations.some((r) => r.name === name)
          if (isViewOnly) {
            setLoadingRelations((prev) => new Set(prev).add(name))
            deduplicatedLoad(name, () => loadViewAsRelation(name))
          } else {
            setLoadingRelations((prev) => new Set(prev).add(name))
            deduplicatedLoad(name, () => loadRelationData(name)).finally(() => {
              setLoadingRelations((prev) => {
                const s = new Set(prev)
                s.delete(name)
                return s
              })
            })
          }
        }
      }
      return next
    })
  }, [allRelations, relations, loadRelationData, loadViewAsRelation, deduplicatedLoad])

  const handleSelectAll = useCallback(async () => {
    const gen = ++loadGenRef.current
    const names = new Set(graphRelations.map((r) => r.name))
    setSelectedNames(names)
    const toLoad = graphRelations.filter((r) => r.data.length === 0)
    if (toLoad.length > 0) {
      setLoadingRelations(new Set(toLoad.map((r) => r.name)))
      const baseNames = new Set(relations.map((r) => r.name))
      await Promise.all(toLoad.map((r) =>
        deduplicatedLoad(r.name, () =>
          baseNames.has(r.name) ? loadRelationData(r.name) : loadViewAsRelation(r.name)
        )
      ))
      // Only clear loading if this is still the active operation
      if (gen === loadGenRef.current) setLoadingRelations(new Set())
    }
  }, [graphRelations, relations, loadRelationData, loadViewAsRelation, deduplicatedLoad])

  const handleDeselectAll = useCallback(() => {
    ++loadGenRef.current // invalidate any in-flight select-all
    setSelectedNames(new Set())
    setLoadingRelations(new Set())
  }, [])

  const handleFilterRelation = useCallback((relation: string) => {
    setSelectedNames(new Set([relation]))
    // Load data if needed
    const rel = allRelations.find((r) => r.name === relation)
    if (rel && rel.data.length === 0) {
      const isViewOnly = !relations.some((r) => r.name === relation)
      if (isViewOnly) {
        deduplicatedLoad(relation, () => loadViewAsRelation(relation))
      } else {
        deduplicatedLoad(relation, () => loadRelationData(relation))
      }
    }
  }, [allRelations, relations, loadRelationData, loadViewAsRelation, deduplicatedLoad])

  // Auto-select relation from URL search params (e.g. ?select=knows)
  useEffect(() => {
    const name = searchParams.get("select")
    if (!name || initialSelectHandled.current) return
    const rel = allRelations.find((r) => r.name === name && r.arity >= 1)
    if (!rel) return
    initialSelectHandled.current = true
    handleFilterRelation(name)
  }, [searchParams, allRelations, handleFilterRelation])

  const { elements, stats } = useMemo(
    () => buildGraphElements(allRelations, selectedNames, grouped),
    [allRelations, selectedNames, grouped]
  )

  const activeRelationNames = useMemo(
    () => Array.from(selectedNames).filter((n) =>
      allRelations.some((r) => r.name === n && r.data.length > 0)
    ),
    [selectedNames, allRelations]
  )

  return (
    <AppShell>
      {!selectedKnowledgeGraph ? (
        <div className="flex flex-1 items-center justify-center p-8">
          <div className="text-center">
            <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-muted">
              <AlertCircle className="h-6 w-6 text-muted-foreground" />
            </div>
            <h2 className="text-lg font-semibold">No Knowledge Graph Selected</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Select a knowledge graph from the header dropdown to visualize it.
            </p>
          </div>
        </div>
      ) : (
        <div className="flex flex-1 h-full overflow-hidden">
          {/* Sidebar */}
          <div className="w-72 flex-shrink-0 border-r border-border/50 bg-muted/20 h-full flex flex-col">
            <div className="p-2 border-b border-border/50 flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-2">
                KG Graph
              </span>
              <Button
                variant="ghost"
                size="sm"
                onClick={async () => {
                  const gen = ++loadGenRef.current
                  // Refresh metadata and reload selected relation data in parallel
                  const toReload = Array.from(selectedNames)
                  if (toReload.length > 0) setLoadingRelations(new Set(toReload))
                  const baseNames = new Set(relations.map((r) => r.name))
                  await Promise.all([
                    refreshCurrentKnowledgeGraph(),
                    ...toReload.map((name) =>
                      deduplicatedLoad(name, () =>
                        baseNames.has(name) ? loadRelationData(name) : loadViewAsRelation(name)
                      )
                    ),
                  ])
                  if (gen === loadGenRef.current) setLoadingRelations(new Set())
                }}
                disabled={isRefreshing}
                className="h-7 px-2"
              >
                <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? "animate-spin" : ""}`} />
              </Button>
            </div>
            <div className="flex-1 overflow-hidden">
              <GraphSidebar
                relations={allRelations}
                selectedNames={selectedNames}
                onToggleRelation={handleToggleRelation}
                onSelectAll={handleSelectAll}
                onDeselectAll={handleDeselectAll}
                loadingRelations={loadingRelations}
                stats={elements.length > 0 ? stats : null}
              />
            </div>
          </div>

          {/* Graph canvas */}
          <div className="flex-1 overflow-hidden h-full">
            <GraphCanvas
              elements={elements}
              stats={stats}
              relationNames={activeRelationNames}
              grouped={grouped}
              onGroupedChange={setGrouped}
              onFilterRelation={handleFilterRelation}
            />
          </div>
        </div>
      )}
    </AppShell>
  )
}

export default function GraphPage() {
  return (
    <Suspense>
      <GraphPageInner />
    </Suspense>
  )
}
