"use client"

import type React from "react"

import { useState } from "react"
import { Play, Trash2, Copy, Check } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Textarea } from "@/components/ui/textarea"
import { useDatalogStore } from "@/lib/datalog-store"

interface QueryEditorProps {
  onExecute: (query: string) => void
  isExecuting: boolean
}

export function QueryEditor({ onExecute, isExecuting }: QueryEditorProps) {
  const [query, setQuery] = useState(`% Example Datalog Query
?- user(X, Name, Email).`)
  const [copied, setCopied] = useState(false)
  const { addQueryToHistory } = useDatalogStore()

  const handleExecute = () => {
    if (query.trim()) {
      addQueryToHistory(query, "Success")
      onExecute(query)
    }
  }

  const handleCopy = async () => {
    await navigator.clipboard.writeText(query)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const handleClear = () => {
    setQuery("")
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault()
      handleExecute()
    }
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
        <CardTitle className="text-base">Query</CardTitle>
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" onClick={handleCopy} className="h-8 w-8">
            {copied ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
          </Button>
          <Button variant="ghost" size="icon" onClick={handleClear} className="h-8 w-8">
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="relative">
          <Textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Enter your Datalog query..."
            className="min-h-[200px] font-mono text-sm resize-none bg-muted/50"
          />
          <div className="absolute bottom-2 right-2 text-xs text-muted-foreground">
            {navigator.platform.includes("Mac") ? "⌘" : "Ctrl"} + Enter to run
          </div>
        </div>
        <div className="flex items-center justify-between">
          <p className="text-xs text-muted-foreground">
            {query.split("\n").length} lines • {query.length} characters
          </p>
          <Button onClick={handleExecute} disabled={isExecuting || !query.trim()}>
            <Play className="mr-2 h-4 w-4" />
            {isExecuting ? "Executing..." : "Execute Query"}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
