"use client"

import { Clock, Download, Rows3, AlertCircle, CheckCircle2, Loader2, Copy, Check, Lightbulb, Sparkles } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { useState } from "react"
import type { QueryResult, ExplainResult } from "@/app/query/page"

interface QueryResultsPanelProps {
  result: QueryResult | null
  explainResult?: ExplainResult | null
  error: string | null
  isExecuting: boolean
  isExplaining?: boolean
  activeQuery: string
}

export function QueryResultsPanel({ result, explainResult, error, isExecuting, isExplaining = false, activeQuery }: QueryResultsPanelProps) {
  const [copied, setCopied] = useState(false)

  const handleExport = () => {
    if (!result) return
    const csv = [result.columns.join(","), ...result.data.map((row) => row.join(","))].join("\n")
    const blob = new Blob([csv], { type: "text/csv" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "query-results.csv"
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleCopyResults = async () => {
    if (!result) return
    const text = [result.columns.join("\t"), ...result.data.map((row) => row.join("\t"))].join("\n")
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  if (isExecuting) {
    return (
      <div className="flex h-full items-center justify-center bg-background">
        <div className="text-center">
          <Loader2 className="mx-auto h-8 w-8 animate-spin text-primary" />
          <p className="mt-3 text-sm font-medium">Executing query...</p>
          <p className="mt-1 text-xs text-muted-foreground font-mono max-w-md truncate px-4">
            {activeQuery.slice(0, 60)}...
          </p>
        </div>
      </div>
    )
  }

  if (isExplaining) {
    return (
      <div className="flex h-full items-center justify-center bg-background">
        <div className="text-center">
          <Lightbulb className="mx-auto h-8 w-8 animate-pulse text-amber-500" />
          <p className="mt-3 text-sm font-medium">Analyzing query plan...</p>
          <p className="mt-1 text-xs text-muted-foreground font-mono max-w-md truncate px-4">
            {activeQuery.slice(0, 60)}...
          </p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full flex-col bg-background">
        <div className="flex h-9 items-center gap-2 border-b border-border/50 bg-destructive/5 px-3">
          <AlertCircle className="h-3.5 w-3.5 text-destructive" />
          <span className="text-xs font-medium text-destructive">Error</span>
        </div>
        <div className="flex flex-1 items-center justify-center p-6">
          <div className="max-w-md rounded-lg border border-destructive/20 bg-destructive/5 p-4">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-destructive">Query Failed</p>
                <p className="mt-1 text-sm text-muted-foreground">{error}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  // Show explain results
  if (explainResult) {
    return (
      <div className="flex h-full flex-col bg-background">
        {/* Explain toolbar */}
        <div className="flex h-9 items-center justify-between border-b border-border/50 bg-amber-500/5 px-3">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              <Lightbulb className="h-3.5 w-3.5 text-amber-500" />
              <span className="text-xs font-medium text-amber-600 dark:text-amber-400">Query Plan</span>
            </div>
            {explainResult.optimizations.length > 0 && (
              <>
                <div className="h-3 w-px bg-border" />
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Sparkles className="h-3 w-3 text-emerald-500" />
                  {explainResult.optimizations.length} optimization{explainResult.optimizations.length !== 1 ? "s" : ""}
                </div>
              </>
            )}
          </div>
        </div>

        {/* Explain content */}
        <div className="flex-1 overflow-auto p-4 space-y-4">
          {/* Query Plan */}
          <div className="rounded-lg border border-border/50">
            <div className="border-b border-border/50 px-4 py-2 bg-muted/30">
              <h3 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Execution Plan</h3>
            </div>
            <pre className="p-4 font-mono text-sm text-foreground overflow-x-auto whitespace-pre-wrap">
              {explainResult.plan || "No plan available"}
            </pre>
          </div>

          {/* Optimizations */}
          {explainResult.optimizations.length > 0 && (
            <div className="rounded-lg border border-border/50">
              <div className="border-b border-border/50 px-4 py-2 bg-muted/30">
                <h3 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Optimizations Applied</h3>
              </div>
              <ul className="p-4 space-y-2">
                {explainResult.optimizations.map((opt, idx) => (
                  <li key={idx} className="flex items-start gap-2 text-sm">
                    <Sparkles className="h-4 w-4 text-emerald-500 flex-shrink-0 mt-0.5" />
                    <span>{opt}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    )
  }

  if (!result) {
    return (
      <div className="flex h-full items-center justify-center bg-muted/20">
        <div className="text-center">
          <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-muted">
            <Rows3 className="h-5 w-5 text-muted-foreground" />
          </div>
          <p className="text-sm font-medium text-muted-foreground">No results yet</p>
          <p className="mt-1 text-xs text-muted-foreground">Run a query to see results here</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col bg-background">
      {/* Results toolbar */}
      <div className="flex h-9 items-center justify-between border-b border-border/50 bg-muted/30 px-3">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <CheckCircle2 className="h-3.5 w-3.5 text-success" />
            <span className="text-xs font-medium text-success">Success</span>
          </div>
          <div className="h-3 w-px bg-border" />
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <span className="flex items-center gap-1">
              <Rows3 className="h-3 w-3" />
              {result.data.length} rows
            </span>
            <span className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {result.executionTime}ms
            </span>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={handleCopyResults} className="h-7 gap-1.5 px-2 text-xs">
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
            Copy
          </Button>
          <Button variant="ghost" size="sm" onClick={handleExport} className="h-7 gap-1.5 px-2 text-xs">
            <Download className="h-3 w-3" />
            Export
          </Button>
        </div>
      </div>

      {/* Results table */}
      <div className="flex-1 overflow-auto scrollbar-thin">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-muted/80 backdrop-blur-sm">
              <th className="w-12 border-b border-r border-border/50 px-3 py-2 text-center text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                #
              </th>
              {result.columns.map((col) => (
                <th
                  key={col}
                  className="border-b border-r border-border/50 px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground last:border-r-0"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.data.map((row, rowIndex) => (
              <tr key={rowIndex} className="group transition-colors hover:bg-muted/50">
                <td className="border-b border-r border-border/30 px-3 py-2 text-center font-mono text-[10px] text-muted-foreground">
                  {rowIndex + 1}
                </td>
                {row.map((cell, cellIndex) => (
                  <td
                    key={cellIndex}
                    className="border-b border-r border-border/30 px-3 py-2 font-mono text-xs last:border-r-0"
                  >
                    {typeof cell === "boolean" ? (
                      <Badge
                        variant="outline"
                        className={cn(
                          "text-[10px] font-mono",
                          cell ? "border-success/50 text-success bg-success/10" : "border-muted-foreground/50",
                        )}
                      >
                        {cell.toString()}
                      </Badge>
                    ) : typeof cell === "number" ? (
                      <span className="text-[var(--code-variable)]">{cell}</span>
                    ) : (
                      <span>{String(cell)}</span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
