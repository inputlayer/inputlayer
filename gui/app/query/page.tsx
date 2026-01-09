"use client"

import { useState, useCallback } from "react"
import { AppShell } from "@/components/app-shell"
import { QueryEditorPanel } from "@/components/query-editor-panel"
import { QueryResultsPanel } from "@/components/query-results-panel"
import { QuerySidebar } from "@/components/query-sidebar"
import { useDatalogStore } from "@/lib/datalog-store"
import { InputLayerClient } from "@inputlayer/api-client"
import { Zap } from "lucide-react"

export interface QueryResult {
  data: (string | number | boolean)[][]
  columns: string[]
  executionTime: number
  query: string
  timestamp: Date
}

export interface ExplainResult {
  plan: string
  optimizations: string[]
  query: string
}

const client = new InputLayerClient({ baseUrl: "/api/v1" })

export default function QueryPage() {
  const { selectedKnowledgeGraph, executeQuery } = useDatalogStore()
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null)
  const [explainResult, setExplainResult] = useState<ExplainResult | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [isExplaining, setIsExplaining] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [activeQuery, setActiveQuery] = useState("")

  const handleExecuteQuery = useCallback(
    async (query: string) => {
      setIsExecuting(true)
      setError(null)
      setActiveQuery(query)
      setExplainResult(null) // Clear explain when executing

      try {
        const result = await executeQuery(query)

        if (result.status === "error") {
          setError(result.error || "Query execution failed")
          setQueryResult(null)
        } else {
          setQueryResult({
            data: result.data,
            columns: result.columns,
            executionTime: result.executionTime,
            query: result.query,
            timestamp: result.timestamp,
          })
        }
      } catch (err) {
        setError(String(err))
        setQueryResult(null)
      }

      setIsExecuting(false)
    },
    [executeQuery],
  )

  const handleExplainQuery = useCallback(
    async (query: string) => {
      if (!selectedKnowledgeGraph) {
        setError("No knowledge graph selected")
        return
      }

      setIsExplaining(true)
      setError(null)
      setActiveQuery(query)
      setQueryResult(null) // Clear results when explaining

      try {
        const result = await client.query.explain({
          query,
          knowledgeGraph: selectedKnowledgeGraph.name,
        })

        setExplainResult({
          plan: result.plan,
          optimizations: result.optimizations,
          query,
        })
      } catch (err) {
        console.error("Explain failed:", err)
        setError(err instanceof Error ? err.message : "Failed to explain query")
        setExplainResult(null)
      }

      setIsExplaining(false)
    },
    [selectedKnowledgeGraph],
  )

  return (
    <AppShell>
      <div className="flex h-full flex-1 overflow-hidden">
        {/* Main editor area */}
        <div className="flex flex-1 flex-col overflow-hidden">
          {/* Toolbar */}
          <div className="flex h-10 flex-shrink-0 items-center justify-between border-b border-border/50 bg-muted/30 px-3">
            <div className="flex items-center gap-2">
              <Zap className="h-3.5 w-3.5 text-primary" />
              <span className="text-xs font-medium">Query Editor</span>
            </div>
            {selectedKnowledgeGraph && <span className="text-xs text-muted-foreground">{selectedKnowledgeGraph.name}</span>}
          </div>

          <div className="flex flex-1 flex-col overflow-hidden">
            {/* Editor takes ~40% */}
            <div className="h-[280px] flex-shrink-0 border-b border-border/50">
              <QueryEditorPanel
                onExecute={handleExecuteQuery}
                onExplain={handleExplainQuery}
                isExecuting={isExecuting}
                isExplaining={isExplaining}
              />
            </div>
            {/* Results take remaining space */}
            <div className="min-h-0 flex-1 overflow-hidden">
              <QueryResultsPanel
                result={queryResult}
                explainResult={explainResult}
                error={error}
                isExecuting={isExecuting}
                isExplaining={isExplaining}
                activeQuery={activeQuery}
              />
            </div>
          </div>
        </div>

        <aside className="h-full w-72 flex-shrink-0 border-l border-border/50 bg-muted/20">
          <QuerySidebar onSelectQuery={handleExecuteQuery} />
        </aside>
      </div>
    </AppShell>
  )
}
