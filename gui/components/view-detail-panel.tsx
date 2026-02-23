"use client"

import { useState, useEffect, useCallback, useRef } from "react"
import { Eye, Copy, Check, Download, Table, GitBranch, Gauge, Code, RefreshCw, Loader2, Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { cn } from "@/lib/utils"
import { toast } from "sonner"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import type { View, Relation } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { ViewDataTab } from "@/components/view-data-tab"
import { ViewGraphTab } from "@/components/view-graph-tab"
import { ViewPerformanceTab } from "@/components/view-performance-tab"

interface ViewDetailPanelProps {
  view: View
  relations: Relation[]
  onNavigate?: (name: string) => void
}

export function ViewDetailPanel({ view, relations, onNavigate }: ViewDetailPanelProps) {
  const { loadViewData, dropRule } = useDatalogStore()
  const [copied, setCopied] = useState(false)
  const [showDropConfirm, setShowDropConfirm] = useState(false)
  const [isDropping, setIsDropping] = useState(false)
  const [activeTab, setActiveTab] = useState("data")
  const [loading, setLoading] = useState(false)
  const [loadingSlow, setLoadingSlow] = useState(false)
  const slowTimerRef = useRef<ReturnType<typeof setTimeout>>(null)

  const handleRefresh = useCallback(async () => {
    setLoading(true)
    setLoadingSlow(false)
    slowTimerRef.current = setTimeout(() => setLoadingSlow(true), 5000)
    try {
      await loadViewData(view.name)
    } finally {
      clearTimeout(slowTimerRef.current)
      setLoading(false)
      setLoadingSlow(false)
    }
  }, [loadViewData, view.name])

  // Load view data (definition, dependencies, etc.) when a view is selected
  useEffect(() => {
    handleRefresh()
  }, [handleRefresh])

  const handleCopy = async () => {
    await navigator.clipboard.writeText(view.definition)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const handleExport = () => {
    const content = `-- View: ${view.name}\n-- Dependencies: ${view.dependencies.join(", ") || "none"}\n\n${view.definition}`
    const blob = new Blob([content], { type: "text/plain" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${view.name}.dl`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex h-full flex-col">
      {/* Header - same structure as RelationDetailPanel */}
      <div className="flex items-center justify-between border-b border-border/50 bg-muted/30 px-4 py-3 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-chart-2/10">
            <Eye className="h-5 w-5 text-chart-2" />
          </div>
          <div>
            <h2 className="font-semibold font-mono">{view.name}</h2>
            <p className="text-xs text-muted-foreground">
              Rule{view.arity > 0 && <> • Arity {view.arity}</>} • {view.dependencies.length} {view.dependencies.length === 1 ? "dependency" : "dependencies"}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={loading}
            className="h-8 gap-1.5 bg-transparent"
          >
            {loading ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" />
            )}
            Refresh
          </Button>
          <Button variant="outline" size="sm" onClick={handleCopy} className="h-8 gap-1.5 bg-transparent" disabled={!view.definition}>
            {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
            Copy
          </Button>
          <Button variant="outline" size="sm" onClick={handleExport} className="h-8 gap-1.5 bg-transparent" disabled={!view.definition}>
            <Download className="h-3.5 w-3.5" />
            Export
          </Button>
          <div className="w-px h-5 bg-border/50" />
          <Button
            variant="destructive"
            size="sm"
            onClick={() => setShowDropConfirm(true)}
            disabled={isDropping}
            className="h-8 gap-1.5"
          >
            {isDropping ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Trash2 className="h-3.5 w-3.5" />}
            Drop Rule
          </Button>
          <AlertDialog open={showDropConfirm} onOpenChange={(open) => { if (!open) setShowDropConfirm(false) }}>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Drop rule</AlertDialogTitle>
                <AlertDialogDescription>
                  Are you sure you want to drop rule &ldquo;{view.name}&rdquo;? All clauses will be removed and derived data will no longer be computed. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel disabled={isDropping}>Cancel</AlertDialogCancel>
                <AlertDialogAction
                  onClick={async () => {
                    setIsDropping(true)
                    try {
                      await dropRule(view.name, view.isSession)
                      toast.success(`Dropped rule "${view.name}"`)
                    } catch (e) {
                      const msg = e instanceof Error ? e.message : "Failed to drop rule"
                      toast.error(msg)
                    } finally {
                      setIsDropping(false)
                      setShowDropConfirm(false)
                    }
                  }}
                  disabled={isDropping}
                  className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                >
                  {isDropping ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                  Drop
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>

      {/* Definition preview */}
      <div className="border-b border-border/50 p-4 flex-shrink-0">
        <div className="flex items-center gap-2 mb-2">
          <Code className="h-3.5 w-3.5 text-muted-foreground" />
          <h3 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Definition</h3>
        </div>
        {loading && !view.definition ? (
          <pre className="font-mono text-sm text-muted-foreground bg-muted/30 rounded-md px-3 py-2">
            {loadingSlow ? "Loading is taking longer than expected..." : "Loading definition..."}
          </pre>
        ) : view.definition ? (
          <pre
            className="font-mono text-sm bg-[var(--code-bg)] rounded-md px-3 py-2 overflow-x-auto whitespace-pre-wrap leading-relaxed"
            dangerouslySetInnerHTML={{ __html: highlightToHtml(view.definition) }}
          />
        ) : (
          <pre className="font-mono text-sm text-muted-foreground bg-muted/30 rounded-md px-3 py-2">
            No definition available
          </pre>
        )}
      </div>

      {/* Dependencies */}
      <div className="border-b border-border/50 p-4 flex-shrink-0">
        <h3 className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Dependencies</h3>
        <div className="flex flex-wrap gap-2">
          {view.dependencies.map((dep) => (
            <Badge
              key={dep}
              variant="secondary"
              className={cn("gap-1.5 font-mono text-xs", onNavigate && "cursor-pointer hover:bg-secondary/80 transition-colors")}
              onClick={() => onNavigate?.(dep)}
            >
              <div className="h-1.5 w-1.5 rounded-full bg-chart-1" />
              {dep}
            </Badge>
          ))}
        </div>
      </div>

      {/* Tabs section - fills remaining space */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col overflow-hidden min-h-0">
        <div className="border-b border-border/50 px-4 flex-shrink-0">
          <TabsList className="h-10 bg-transparent p-0 gap-4">
            <TabsTrigger
              value="data"
              className={cn(
                "h-10 px-0 pb-3 pt-2.5 rounded-none border-b-2 border-transparent",
                "data-[state=active]:bg-transparent data-[state=active]:shadow-none data-[state=active]:border-primary",
              )}
            >
              <Table className="h-4 w-4 mr-2" />
              Data
            </TabsTrigger>
            <TabsTrigger
              value="graph"
              className={cn(
                "h-10 px-0 pb-3 pt-2.5 rounded-none border-b-2 border-transparent",
                "data-[state=active]:bg-transparent data-[state=active]:shadow-none data-[state=active]:border-primary",
              )}
            >
              <GitBranch className="h-4 w-4 mr-2" />
              Dependency Graph
            </TabsTrigger>
            <TabsTrigger
              value="performance"
              className={cn(
                "h-10 px-0 pb-3 pt-2.5 rounded-none border-b-2 border-transparent",
                "data-[state=active]:bg-transparent data-[state=active]:shadow-none data-[state=active]:border-primary",
              )}
            >
              <Gauge className="h-4 w-4 mr-2" />
              Performance
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="data" className="flex-1 m-0 overflow-hidden">
          <ViewDataTab view={view} />
        </TabsContent>
        <TabsContent value="graph" className="flex-1 m-0 overflow-hidden">
          <ViewGraphTab view={view} relations={relations} />
        </TabsContent>
        <TabsContent value="performance" className="flex-1 m-0 overflow-hidden">
          <ViewPerformanceTab view={view} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
