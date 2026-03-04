"use client"

import { useMemo } from "react"
import { GraphCanvas } from "@/components/graph-canvas"
import { buildQueryGraphElements } from "@/lib/graph-utils"
import { Share2 } from "lucide-react"

interface QueryResultGraphProps {
  data: (string | number | boolean | null)[][]
  columns: string[]
  name?: string
}

export function QueryResultGraph({ data, columns, name }: QueryResultGraphProps) {
  const { elements, stats, relationNames } = useMemo(
    () => buildQueryGraphElements(data, columns, name),
    [data, columns, name]
  )

  if (data.length === 0 && columns.length !== 1) {
    return (
      <div className="flex h-full items-center justify-center bg-muted/10">
        <div className="text-center">
          <Share2 className="mx-auto h-12 w-12 text-muted-foreground/30" />
          <p className="mt-3 text-sm font-medium text-muted-foreground">No data to visualize</p>
          <p className="mt-1 text-xs text-muted-foreground/70">
            The query returned no rows
          </p>
        </div>
      </div>
    )
  }

  return (
    <GraphCanvas
      elements={elements}
      stats={stats}
      relationNames={relationNames}
    />
  )
}
