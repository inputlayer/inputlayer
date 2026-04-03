"use client"

import { useState, useCallback, useMemo, useEffect, Suspense } from "react"
import { useSearchParams } from "next/navigation"
import { AppShell } from "@/components/app-shell"
import { QueryEditorPanel } from "@/components/query-editor-panel"
import { QueryResultsPanel } from "@/components/query-results-panel"
import { QuerySidebar } from "@/components/query-sidebar"
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable"
import { useDatalogStore } from "@/lib/datalog-store"
import type { QueryResult, ValidationError } from "@/lib/datalog-store"
import { Zap, AlertTriangle, PanelRightClose, PanelRightOpen, GripHorizontal } from "lucide-react"
import { Button } from "@/components/ui/button"

export type { QueryResult }

export interface StructuredError {
  message: string
  validationErrors?: ValidationError[]
}

function useSidebarOpen() {
  const [open, setOpen] = useState(() => {
    if (typeof window === "undefined") return true
    const stored = localStorage.getItem("il-sidebar-open")
    return stored !== "false"
  })
  const toggle = useCallback(() => {
    setOpen((prev) => {
      const next = !prev
      try { localStorage.setItem("il-sidebar-open", String(next)) } catch {}
      return next
    })
  }, [])
  return [open, toggle] as const
}

export default function QueryPage() {
  return (
    <Suspense>
      <QueryPageInner />
    </Suspense>
  )
}

function QueryPageInner() {
  const { selectedKnowledgeGraph, executeQuery, setEditorContent, cancelCurrentQuery, executeInternalQuery, loadExample } = useDatalogStore()
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [error, setError] = useState<StructuredError | null>(null)
  const [activeQuery, setActiveQuery] = useState("")
  const [sidebarOpen, toggleSidebar] = useSidebarOpen()
  const [pendingExample, setPendingExample] = useState<string | null>(null)
  const searchParams = useSearchParams()

  // Handle ?example=XXX URL parameter
  useEffect(() => {
    const exampleId = searchParams.get("example")
    if (exampleId && selectedKnowledgeGraph) {
      // Start the agent for this example
      executeInternalQuery(`.agent start ${exampleId}`).catch(() => {})
    }
  }, [searchParams, selectedKnowledgeGraph, executeInternalQuery])

  const handleExecuteQuery = useCallback(
    async (query: string) => {
      setIsExecuting(true)
      setError(null)
      setActiveQuery(query)
      try {
        const result = await executeQuery(query)

        if (result.status === "error") {
          setError({
            message: result.error || "Query execution failed",
            validationErrors: result.validationErrors,
          })
          setQueryResult(null)
        } else {
          setQueryResult(result)
        }
      } catch (err) {
        setError({ message: String(err) })
        setQueryResult(null)
      }

      setIsExecuting(false)
    },
    [executeQuery],
  )

  const errorLines = useMemo(
    () => new Set(error?.validationErrors?.map((e) => e.line) ?? []),
    [error],
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
              {selectedKnowledgeGraph && (
                <>
                  <div className="h-3 w-px bg-border" />
                  <span className="text-xs text-muted-foreground">{selectedKnowledgeGraph.name}</span>
                </>
              )}
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={toggleSidebar}
              className="h-7 w-7 p-0 ml-auto"
              aria-label={sidebarOpen ? "Hide sidebar" : "Show sidebar"}
            >
              {sidebarOpen ? <PanelRightClose className="h-3.5 w-3.5" /> : <PanelRightOpen className="h-3.5 w-3.5" />}
            </Button>
          </div>

          {!selectedKnowledgeGraph && (
            <div className="flex items-center gap-2 border-b border-amber-500/20 bg-amber-500/5 px-3 py-1.5 text-xs text-amber-700 dark:text-amber-400">
              <AlertTriangle className="h-3 w-3 flex-shrink-0" />
              <span>No knowledge graph selected - select one from the header to run queries</span>
            </div>
          )}

          <ResizablePanelGroup direction="vertical" className="flex-1">
            <ResizablePanel defaultSize={38} minSize={15} className="overflow-hidden">
              <QueryEditorPanel
                onExecute={handleExecuteQuery}
                onCancel={cancelCurrentQuery}
                isExecuting={isExecuting}
                errorLines={errorLines}
              />
            </ResizablePanel>
            <ResizableHandle className="relative border-t border-b border-border/50 bg-muted/30 hover:bg-primary/10 transition-colors data-[resize-handle-active]:bg-primary/20">
              <GripHorizontal className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground/40" />
            </ResizableHandle>
            <ResizablePanel defaultSize={62} minSize={20} className="overflow-hidden">
              <QueryResultsPanel
                result={queryResult}
                error={error}
                isExecuting={isExecuting}
                activeQuery={activeQuery}
                sidebarOpen={sidebarOpen}
                onStartExample={() => {
                  if (!sidebarOpen) toggleSidebar()
                }}
              />
            </ResizablePanel>
          </ResizablePanelGroup>
        </div>

        {sidebarOpen && (
          <aside className="h-full flex-shrink-0 border-l border-border/50 bg-muted/20" style={{ width: 360, minWidth: 280, maxWidth: 520, resize: "horizontal", overflow: "hidden" }}>
            <QuerySidebar
              onSelectQuery={handleExecuteQuery}
              onLoadQuery={setEditorContent}
              pendingExample={pendingExample}
              onPendingExampleHandled={() => setPendingExample(null)}
            />
          </aside>
        )}
      </div>
    </AppShell>
  )
}
