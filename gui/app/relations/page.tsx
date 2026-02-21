"use client"

import { useState, useEffect } from "react"
import { AppShell } from "@/components/app-shell"
import { RelationsExplorer } from "@/components/relations-explorer"
import { RelationDetailPanel } from "@/components/relation-detail-panel"
import { ViewDetailPanel } from "@/components/view-detail-panel"
import { useDatalogStore, type Relation, type View } from "@/lib/datalog-store"
import { AlertCircle, Network, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"

export default function RelationsPage() {
  const {
    selectedKnowledgeGraph,
    relations,
    views,
    isRefreshing,
    refreshCurrentKnowledgeGraph
  } = useDatalogStore()
  const [selectedRelation, setSelectedRelation] = useState<Relation | null>(null)
  const [selectedView, setSelectedView] = useState<View | null>(null)

  // Auto-refresh data when the page loads or becomes visible
  useEffect(() => {
    if (selectedKnowledgeGraph) {
      refreshCurrentKnowledgeGraph()
    }
  }, [selectedKnowledgeGraph?.name]) // Only refresh when KG changes, not on every render

  // Keep selection in sync with store data (e.g., after loadRelationData updates tupleCount/columns)
  useEffect(() => {
    if (selectedRelation) {
      const updated = relations.find(r => r.name === selectedRelation.name)
      if (updated) {
        // Only update if data actually changed to avoid unnecessary re-renders
        if (updated !== selectedRelation) setSelectedRelation(updated)
      } else {
        setSelectedRelation(null)
      }
    }
    if (selectedView) {
      if (!views.find(v => v.name === selectedView.name)) {
        setSelectedView(null)
      }
    }
  }, [relations, views])

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
              Select a knowledge graph from the header dropdown to view relations.
            </p>
          </div>
        </div>
      ) : (
        <div className="flex flex-1 h-full overflow-hidden">
          {/* Relations/Views explorer */}
          <div className="w-72 flex-shrink-0 border-r border-border/50 bg-muted/20 h-full flex flex-col">
            {/* Refresh button */}
            <div className="p-2 border-b border-border/50 flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-2">
                Explorer
              </span>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => refreshCurrentKnowledgeGraph()}
                disabled={isRefreshing}
                className="h-7 px-2"
              >
                <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
              </Button>
            </div>
            <div className="flex-1 overflow-hidden">
              <RelationsExplorer
                relations={relations}
                views={views}
                selectedRelationId={selectedRelation?.id}
                selectedViewId={selectedView?.id}
                onSelectRelation={(r) => {
                  setSelectedRelation(r)
                  setSelectedView(null)
                }}
                onSelectView={(v) => {
                  setSelectedView(v)
                  setSelectedRelation(null)
                }}
              />
            </div>
          </div>

          {/* Detail panel */}
          <div className="flex-1 overflow-hidden h-full">
            {selectedRelation && <RelationDetailPanel relation={selectedRelation} />}
            {selectedView && <ViewDetailPanel view={selectedView} relations={relations} />}
            {!selectedRelation && !selectedView && (
              <div className="flex h-full items-center justify-center bg-muted/10">
                <div className="text-center">
                  <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-muted">
                    <Network className="h-6 w-6 text-muted-foreground" />
                  </div>
                  <p className="text-sm font-medium text-muted-foreground">Select a relation or view</p>
                  <p className="mt-1 text-xs text-muted-foreground">Choose from the list to inspect schema and data</p>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </AppShell>
  )
}
