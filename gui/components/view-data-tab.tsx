"use client"

import { useEffect, useState, useCallback, useRef } from "react"
import type { View } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"
import { generateVariables } from "@/lib/ws-parsers"
import { Download, RefreshCw, Rows3, Loader2, AlertCircle, FileJson } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

interface ViewDataTabProps {
  view: View
}

interface ViewData {
  columns: string[]
  data: (string | number | boolean | null)[][]
  totalCount: number
}

export function ViewDataTab({ view }: ViewDataTabProps) {
  const { selectedKnowledgeGraph, executeInternalQuery } = useDatalogStore()
  const [viewData, setViewData] = useState<ViewData>({ columns: [], data: [], totalCount: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const pageSize = 100
  // Track the arity we last queried with to avoid duplicate queries
  const lastQueriedRef = useRef<string>("")

  const loadData = useCallback(async () => {
    if (!selectedKnowledgeGraph) {
      setError("No knowledge graph selected")
      setLoading(false)
      return
    }

    // Wait until loadViewData has set the correct arity
    if (view.arity <= 0) return

    const queryKey = `${view.name}:${view.arity}`
    if (lastQueriedRef.current === queryKey && viewData.data.length > 0) return
    lastQueriedRef.current = queryKey

    setLoading(true)
    setError(null)

    try {
      const vars = generateVariables(view.arity)
      const query = `?${view.name}(${vars.join(", ")})`
      const result = await executeInternalQuery(query)

      if (result.status === "error") {
        setError(result.error || "Failed to load view data")
      } else {
        setViewData({
          columns: result.columns,
          data: result.data,
          totalCount: result.data.length,
        })
        setPage(0)
      }
    } catch (err) {
      console.error("Failed to load view data:", err)
      setError(err instanceof Error ? err.message : "Failed to load view data")
    } finally {
      setLoading(false)
    }
  }, [selectedKnowledgeGraph, view.name, view.arity, executeInternalQuery, viewData.data.length])

  // Load data when arity becomes available or view changes
  useEffect(() => {
    if (view.arity > 0) {
      loadData()
    }
  }, [view.name, view.arity]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleRefresh = useCallback(async () => {
    lastQueriedRef.current = "" // force re-query
    await loadData()
  }, [loadData])

  const handleExportCsv = () => {
    if (viewData.data.length === 0) return

    const escapeCell = (v: unknown) => {
      const s = v === null ? "" : String(v)
      return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s
    }
    const csvContent = [
      viewData.columns.map(escapeCell).join(","),
      ...viewData.data.map((row) => row.map(escapeCell).join(",")),
    ].join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${view.name}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleExportJson = () => {
    if (viewData.data.length === 0) return
    const rows = viewData.data.map((row) => {
      const obj: Record<string, string | number | boolean | null> = {}
      viewData.columns.forEach((col, i) => { obj[col] = row[i] })
      return obj
    })
    const json = JSON.stringify(rows, null, 2)
    const blob = new Blob([json], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${view.name}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex items-center justify-between border-b border-border/50 bg-muted/10 px-4 py-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Rows3 className="h-3.5 w-3.5" />
          <span>{viewData.totalCount.toLocaleString()} rows</span>
          <span className="text-border">•</span>
          <span>{viewData.columns.length} columns</span>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-xs"
            onClick={handleRefresh}
            disabled={loading}
          >
            <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-xs"
            onClick={handleExportCsv}
            disabled={viewData.data.length === 0}
          >
            <Download className="h-3 w-3" />
            CSV
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-xs"
            onClick={handleExportJson}
            disabled={viewData.data.length === 0}
          >
            <FileJson className="h-3 w-3" />
            JSON
          </Button>
        </div>
      </div>

      {/* Data table */}
      <div className="flex-1 overflow-auto">
        {view.arity <= 0 || loading ? (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <Loader2 className="mx-auto h-8 w-8 text-muted-foreground/50 animate-spin" />
              <p className="mt-2 text-sm text-muted-foreground">Loading view data...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <AlertCircle className="mx-auto h-8 w-8 text-destructive/50" />
              <p className="mt-2 text-sm text-destructive">{error}</p>
              <Button variant="outline" size="sm" className="mt-4" onClick={handleRefresh}>
                Retry
              </Button>
            </div>
          </div>
        ) : viewData.data.length === 0 ? (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <Rows3 className="mx-auto h-8 w-8 text-muted-foreground/50" />
              <p className="mt-2 text-sm text-muted-foreground">No data in this view</p>
              <p className="text-xs text-muted-foreground">The view computation returned empty results</p>
            </div>
          </div>
        ) : (() => {
          const totalPages = Math.max(1, Math.ceil(viewData.data.length / pageSize))
          const safePage = Math.min(page, totalPages - 1)
          const displayData = viewData.data.slice(safePage * pageSize, (safePage + 1) * pageSize)
          return (
            <>
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-muted/50 border-b border-border/50">
                  <tr>
                    <th className="w-12 px-3 py-2 text-left text-xs font-medium text-muted-foreground">#</th>
                    {viewData.columns.map((col, idx) => (
                      <th key={`${col}-${idx}`} className="px-3 py-2 text-left text-xs font-medium text-muted-foreground">
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/30">
                  {displayData.map((row, i) => (
                    <tr key={i} className="hover:bg-muted/30 transition-colors">
                      <td className="px-3 py-2 text-xs text-muted-foreground tabular-nums">{safePage * pageSize + i + 1}</td>
                      {row.map((cell, j) => (
                        <td key={j} className="px-3 py-2 font-mono text-xs">
                          {cell === null ? (
                            <span className="italic text-muted-foreground/50">null</span>
                          ) : typeof cell === "boolean" ? (
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
              {totalPages > 1 && (
                <div className="flex items-center justify-between border-t border-border/50 bg-muted/10 px-4 py-2 flex-shrink-0">
                  <p className="text-[10px] text-muted-foreground">
                    Showing {safePage * pageSize + 1}–{Math.min((safePage + 1) * pageSize, viewData.data.length)} of {viewData.data.length} rows
                  </p>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2 text-[10px]"
                      disabled={safePage === 0}
                      onClick={() => setPage(safePage - 1)}
                    >
                      Prev
                    </Button>
                    <span className="text-[10px] text-muted-foreground">
                      Page {safePage + 1} of {totalPages}
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2 text-[10px]"
                      disabled={safePage >= totalPages - 1}
                      onClick={() => setPage(safePage + 1)}
                    >
                      Next
                    </Button>
                  </div>
                </div>
              )}
            </>
          )
        })()}
      </div>
    </div>
  )
}
