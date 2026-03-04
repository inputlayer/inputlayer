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
  const lastQueriedRef = useRef<string>("")

  const loadData = useCallback(async () => {
    if (!selectedKnowledgeGraph) return
    if (view.arity < 1) return

    const queryKey = `${view.name}:${view.arity}`
    if (lastQueriedRef.current === queryKey && data.length > 0) return
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
        setData(result.data)
        setColumns(result.columns)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load data")
    } finally {
      setLoading(false)
    }
  }, [selectedKnowledgeGraph, view.name, view.arity, executeInternalQuery, data.length])

  useEffect(() => {
    if (view.arity >= 1) loadData()
  }, [view.name, view.arity]) // eslint-disable-line react-hooks/exhaustive-deps

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
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground/50" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center">
          <AlertCircle className="mx-auto h-8 w-8 text-destructive/50" />
          <p className="mt-2 text-sm text-destructive">{error}</p>
          <Button variant="outline" size="sm" className="mt-4" onClick={() => { lastQueriedRef.current = ""; loadData() }}>
            Retry
          </Button>
        </div>
      </div>
    )
  }

  return <QueryResultGraph data={data} columns={columns} name={view.name} />
}
