"use client"

import type { View } from "@/lib/datalog-store"
import { GitBranch, Code, Info, Eye } from "lucide-react"

interface ViewPerformanceTabProps {
  view: View
}

export function ViewPerformanceTab({ view }: ViewPerformanceTabProps) {
  // Derive what we can from view metadata
  const definitionLines = view.definition.split("\n").length
  const isRecursive = view.dependencies.includes(view.name)
  const dependencyCount = view.dependencies.length

  return (
    <div className="h-full overflow-auto p-4 space-y-6">
      {/* Info banner */}
      <div className="rounded-lg border border-border/50 bg-muted/10 px-4 py-3">
        <div className="flex items-center gap-2 text-sm">
          <Info className="h-4 w-4 text-muted-foreground" />
          <span className="text-muted-foreground">
            Detailed performance metrics are computed on-demand during query execution.
          </span>
        </div>
      </div>

      {/* View Analysis cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <GitBranch className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Dependencies</span>
          </div>
          <p className="text-2xl font-bold tabular-nums">{dependencyCount}</p>
          <p className="text-xs text-muted-foreground mt-1">
            {dependencyCount === 0 ? "No dependencies" : dependencyCount === 1 ? "relation" : "relations"}
          </p>
        </div>

        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <Code className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Definition Size</span>
          </div>
          <p className="text-2xl font-bold tabular-nums">{definitionLines}</p>
          <p className="text-xs text-muted-foreground mt-1">{definitionLines === 1 ? "line" : "lines"}</p>
        </div>

        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <Eye className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Type</span>
          </div>
          <p className="text-2xl font-bold">{isRecursive ? "Recursive" : "Standard"}</p>
          <p className="text-xs text-muted-foreground mt-1">
            {isRecursive ? "Self-referential view" : "Non-recursive view"}
          </p>
        </div>
      </div>

      {/* Dependencies list */}
      {dependencyCount > 0 && (
        <div className="rounded-lg border border-border/50">
          <div className="border-b border-border/50 px-4 py-3">
            <h3 className="text-sm font-medium">Dependency Analysis</h3>
            <p className="text-xs text-muted-foreground mt-0.5">Relations this view depends on</p>
          </div>
          <div className="p-4">
            <div className="flex flex-wrap gap-2">
              {view.dependencies.map((dep) => (
                <div
                  key={dep}
                  className="flex items-center gap-2 rounded-md border border-border/50 bg-muted/30 px-3 py-1.5"
                >
                  <div
                    className={`h-2 w-2 rounded-full ${dep === view.name ? "bg-amber-500" : "bg-chart-1"}`}
                  />
                  <span className="font-mono text-xs">{dep}</span>
                  {dep === view.name && (
                    <span className="text-[10px] text-amber-500">(self)</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Definition preview */}
      <div className="rounded-lg border border-border/50">
        <div className="border-b border-border/50 px-4 py-3">
          <h3 className="text-sm font-medium">View Definition</h3>
          <p className="text-xs text-muted-foreground mt-0.5">Datalog rules defining this view</p>
        </div>
        <div className="p-4">
          <pre className="rounded-md bg-muted/30 p-3 font-mono text-xs text-foreground overflow-x-auto">
            {view.definition || "No definition available"}
          </pre>
        </div>
      </div>
    </div>
  )
}
