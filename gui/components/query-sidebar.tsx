"use client"

import { useState } from "react"
import { History, Clock, Search, X, CheckCircle2, XCircle } from "lucide-react"
import { useIQLStore } from "@/lib/iql-store"
import { formatDistanceToNow } from "date-fns"
import { formatTime } from "@/lib/ui-utils"
import { Input } from "@/components/ui/input"

interface QuerySidebarProps {
  onSelectQuery: (query: string) => void | Promise<void>
  onLoadQuery?: (query: string) => void
}

export function QuerySidebar({ onSelectQuery, onLoadQuery }: QuerySidebarProps) {
  const { queryHistory } = useIQLStore()

  return (
    <div className="flex h-full flex-col bg-background">
      <div className="flex items-center gap-1.5 border-b px-3 py-2 text-xs font-medium">
        <History className="h-3 w-3" />
        History
        {queryHistory.length > 0 && (
          <span className="text-[10px] text-muted-foreground">({queryHistory.length})</span>
        )}
      </div>

      <div className="flex-1 overflow-auto">
        <HistoryPanel queryHistory={queryHistory} onSelectQuery={onSelectQuery} onLoadQuery={onLoadQuery ?? onSelectQuery} />
      </div>
    </div>
  )
}

// --- History Panel ---

function HistoryPanel({
  queryHistory,
  onSelectQuery,
  onLoadQuery,
}: {
  queryHistory: Array<{ id: string; query: string; status: string; executionTime: number; timestamp: Date; error?: string }>
  onSelectQuery: (query: string) => void
  onLoadQuery: (query: string) => void
}) {
  const [search, setSearch] = useState("")

  const filteredHistory = queryHistory.filter(
    (item) => item.query && item.query.toLowerCase().includes(search.toLowerCase()),
  )

  return (
    <div className="p-2 space-y-2">
      {queryHistory.length > 0 && (
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
          <Input
            placeholder="Search history..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-7 pl-7 text-xs"
          />
          {search && (
            <button onClick={() => setSearch("")} className="absolute right-2 top-1/2 -translate-y-1/2">
              <X className="h-3 w-3 text-muted-foreground" />
            </button>
          )}
        </div>
      )}

      {filteredHistory.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-8 text-center">
          <Clock className="mb-2 h-5 w-5 text-muted-foreground/30" />
          <p className="text-xs text-muted-foreground">
            {queryHistory.length === 0 ? "No queries yet" : "No matching queries"}
          </p>
        </div>
      ) : (
        <div className="space-y-1">
          {filteredHistory.map((item) => (
            <button
              key={item.id}
              onClick={() => onLoadQuery(item.query)}
              className="group w-full rounded-md p-2 text-left transition-colors hover:bg-muted"
            >
              <div className="flex items-center gap-1.5">
                {item.status === "success" ? (
                  <CheckCircle2 className="h-3 w-3 flex-shrink-0 text-emerald-500" />
                ) : (
                  <XCircle className="h-3 w-3 flex-shrink-0 text-red-500" />
                )}
                <span className="flex-1 truncate font-mono text-[11px]">{item.query}</span>
              </div>
              <div className="mt-0.5 flex items-center gap-2 pl-[18px] text-[10px] text-muted-foreground">
                <span>{formatTime(item.executionTime)}</span>
                <span>{formatDistanceToNow(item.timestamp, { addSuffix: true })}</span>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
