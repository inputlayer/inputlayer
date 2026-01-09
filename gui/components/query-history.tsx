"use client"

import { Clock, Play } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { useDatalogStore } from "@/lib/datalog-store"
import { formatDistanceToNow } from "date-fns"

export function QueryHistory() {
  const { queryHistory } = useDatalogStore()

  const formatTimestamp = (timestamp: Date | string) => {
    try {
      const date = timestamp instanceof Date ? timestamp : new Date(timestamp)
      if (isNaN(date.getTime())) {
        return "just now"
      }
      return formatDistanceToNow(date, { addSuffix: true })
    } catch {
      return "just now"
    }
  }

  if (queryHistory.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">Query History</CardTitle>
          <CardDescription className="text-xs">Your recent queries will appear here</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
            <Clock className="h-8 w-8 mb-2" />
            <p className="text-sm">No queries yet</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm">Query History</CardTitle>
        <CardDescription className="text-xs">{queryHistory.length} recent queries</CardDescription>
      </CardHeader>
      <CardContent className="space-y-2 max-h-[400px] overflow-auto">
        {queryHistory.map((item, index) => (
          <div
            key={index}
            className="rounded-md border p-2 text-xs cursor-pointer hover:bg-accent transition-colors group"
          >
            <div className="flex items-center justify-between mb-1">
              <span className="text-muted-foreground">{formatTimestamp(item.timestamp)}</span>
              <Play className="h-3 w-3 opacity-0 group-hover:opacity-100 transition-opacity" />
            </div>
            <pre className="font-mono overflow-x-auto whitespace-pre-wrap text-foreground">
              {item.query.slice(0, 100)}
              {item.query.length > 100 ? "..." : ""}
            </pre>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
