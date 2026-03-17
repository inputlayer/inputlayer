"use client"

import { useEffect, useState, useCallback, useRef } from "react"
import type { View } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"
import { generateVariables } from "@/lib/ws-parsers"
import { Loader2, AlertCircle, Share2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { QueryResultGraph } from "@/components/query-result-graph"

interface ViewDataGraphTabProps {
  view: View
}

export function ViewDataGraphTab({ view }: ViewDataGraphTabProps) {
  const { selectedKnowledgeGraph, executeInternalQuery } = useDatalogStore()
  const [data, setData] = useState<(string | number | boolean | null)[][]>([])
  const [columns, setColumns] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const requestIdRef = useRef(0)

  const loadData = useCallback(async () => {
    if (!selectedKnowledgeGraph) return
    if (view.arity < 1) return

    const id = ++requestIdRef.current
    setLoading(true)
    setError(null)
    try {
      const vars = generateVariables(view.arity)
      const query = `?${view.name}(${vars.join(", ")})`
      const result = await executeInternalQuery(query)
      // Discard result if a newer request has been issued
      if (id !== requestIdRef.current) return
      if (result.status === "error") {
        setError(result.error || "Failed to load view data")
      } else {
        setData(result.data)
        setColumns(result.columns)
      }
    } catch (err) {
      if (id !== requestIdRef.current) return
      setError(err instanceof Error ? err.message : "Failed to load data")
    } finally {
      if (id === requestIdRef.current) setLoading(false)
    }
  }, [selectedKnowledgeGraph, view.name, view.arity, executeInternalQuery])

  useEffect(() => {
    if (view.arity >= 1) loadData()
  }, [view.name, view.arity, loadData])

  if (view.arity < 1) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center">
          <Share2 className="mx-auto h-8 w-8 text-muted-foreground/50" />
          <p className="mt-2 text-sm text-muted-foreground">No data to visualize</p>
        </div>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground/50" aria-label="Loading" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center">
          <AlertCircle className="mx-auto h-8 w-8 text-destructive/50" />
          <p className="mt-2 text-sm text-destructive">{error}</p>
          <Button variant="outline" size="sm" className="mt-4" onClick={() => loadData()}>
            Retry
          </Button>
        </div>
      </div>
    )
  }

  return <QueryResultGraph data={data} columns={columns} name={view.name} />
}
