"use client"

import { useState, useEffect, useMemo, useCallback } from "react"
import { AppShell } from "@/components/app-shell"
import { GraphSidebar } from "@/components/graph-sidebar"
import { GraphCanvas } from "@/components/graph-canvas"
import { useDatalogStore } from "@/lib/datalog-store"
import { buildGraphElements } from "@/lib/graph-utils"
import { AlertCircle, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"

export default function GraphPage() {
  const {
    selectedKnowledgeGraph,
    relations,
    isRefreshing,
    refreshCurrentKnowledgeGraph,
    loadRelationData,
  } = useDatalogStore()

  const [selectedNames, setSelectedNames] = useState<Set<string>>(new Set())
  const [loadingRelations, setLoadingRelations] = useState<Set<string>>(new Set())

  const binaryRelations = useMemo(
    () => relations.filter((r) => r.arity === 2 && !r.isView),
    [relations]
  )

  useEffect(() => {
    if (selectedKnowledgeGraph) {
      refreshCurrentKnowledgeGraph()
    }
  }, [selectedKnowledgeGraph?.name])

  const handleToggleRelation = useCallback(async (name: string) => {
    setSelectedNames((prev) => {
      const next = new Set(prev)
      if (next.has(name)) {
        next.delete(name)
      } else {
        next.add(name)
        // Load data if not yet loaded
        const rel = relations.find((r) => r.name === name)
        if (rel && rel.data.length === 0) {
          setLoadingRelations((prev) => new Set(prev).add(name))
          loadRelationData(name).finally(() => {
            setLoadingRelations((prev) => {
              const s = new Set(prev)
              s.delete(name)
              return s
            })
          })
        }
      }
      return next
    })
  }, [relations, loadRelationData])

  const handleSelectAll = useCallback(async () => {
    const names = new Set(binaryRelations.map((r) => r.name))
    setSelectedNames(names)
    const toLoad = binaryRelations.filter((r) => r.data.length === 0)
    if (toLoad.length > 0) {
      setLoadingRelations(new Set(toLoad.map((r) => r.name)))
      await Promise.all(toLoad.map((r) => loadRelationData(r.name)))
      setLoadingRelations(new Set())
    }
  }, [binaryRelations, loadRelationData])

  const handleDeselectAll = useCallback(() => {
    setSelectedNames(new Set())
  }, [])

  const { elements, stats } = useMemo(
    () => buildGraphElements(relations, selectedNames),
    [relations, selectedNames]
  )

  const activeRelationNames = useMemo(
    () => Array.from(selectedNames).filter((n) =>
      relations.some((r) => r.name === n && r.data.length > 0)
    ),
    [selectedNames, relations]
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
                onClick={() => refreshCurrentKnowledgeGraph()}
                disabled={isRefreshing}
                className="h-7 px-2"
              >
                <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? "animate-spin" : ""}`} />
              </Button>
            </div>
            <div className="flex-1 overflow-hidden">
              <GraphSidebar
                relations={relations}
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
            />
          </div>
        </div>
      )}
    </AppShell>
  )
}
