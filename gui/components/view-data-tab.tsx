"use client"

import { useEffect, useState, useCallback } from "react"
import type { View } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"
import { generateVariables } from "@/lib/ws-parsers"
import { Download, RefreshCw, Rows3, Loader2, AlertCircle } from "lucide-react"
import { Button } from "@/components/ui/button"

interface ViewDataTabProps {
  view: View
}

interface ViewData {
  columns: string[]
  data: (string | number | boolean | null)[][]
  totalCount: number
}

export function ViewDataTab({ view }: ViewDataTabProps) {
  const { selectedKnowledgeGraph, executeQuery } = useDatalogStore()
  const [viewData, setViewData] = useState<ViewData>({ columns: [], data: [], totalCount: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadData = useCallback(async () => {
    if (!selectedKnowledgeGraph) {
      setError("No knowledge graph selected")
      setLoading(false)
      return
    }

    setLoading(true)
    setError(null)

    try {
      // Query the view data using generated variable names
      const vars = generateVariables(view.arity > 0 ? view.arity : 2)
      const query = `?${view.name}(${vars.join(", ")})`
      const result = await executeQuery(query)

      if (result.status === "error") {
        setError(result.error || "Failed to load view data")
      } else {
        setViewData({
          columns: result.columns,
          data: result.data,
          totalCount: result.data.length,
        })
      }
    } catch (err) {
      console.error("Failed to load view data:", err)
      setError(err instanceof Error ? err.message : "Failed to load view data")
    } finally {
      setLoading(false)
    }
  }, [selectedKnowledgeGraph, view.name, view.arity, executeQuery])

  useEffect(() => {
    loadData()
  }, [loadData])

  const handleExport = () => {
    if (viewData.data.length === 0) return

    const csvContent = [
      viewData.columns.join(","),
      ...viewData.data.map((row) => row.map((cell) => JSON.stringify(cell)).join(",")),
    ].join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${view.name}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex items-center justify-between border-b border-border/50 bg-muted/10 px-4 py-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Rows3 className="h-3.5 w-3.5" />
          <span>{viewData.totalCount} rows</span>
          <span className="text-border">•</span>
          <span>{viewData.columns.length} columns</span>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-xs"
            onClick={loadData}
            disabled={loading}
          >
            <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-xs"
            onClick={handleExport}
            disabled={viewData.data.length === 0}
          >
            <Download className="h-3 w-3" />
            Export
          </Button>
        </div>
      </div>

      {/* Data table */}
      <div className="flex-1 overflow-auto">
        {loading ? (
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
              <Button variant="outline" size="sm" className="mt-4" onClick={loadData}>
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
        ) : (
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
              {viewData.data.map((row, i) => (
                <tr key={i} className="hover:bg-muted/30 transition-colors">
                  <td className="px-3 py-2 text-xs text-muted-foreground tabular-nums">{i + 1}</td>
                  {row.map((cell, j) => (
                    <td key={j} className="px-3 py-2 font-mono text-xs">
                      {String(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
