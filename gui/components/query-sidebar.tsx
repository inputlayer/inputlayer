"use client"

import { useState, useMemo, useRef, useEffect } from "react"
import { History, Bot, Clock, Search, X, ChevronRight, CheckCircle2, XCircle, Play, Send, Loader2, Sparkles } from "lucide-react"
import { useDatalogStore } from "@/lib/datalog-store"
import { formatDistanceToNow } from "date-fns"
import { cn } from "@/lib/utils"
import { formatTime } from "@/lib/ui-utils"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"

// Teaching examples - these map to the engine's agent examples
const TEACHING_EXAMPLES = [
  { id: "flights", name: "Flight Reachability", category: "Complete Tour", difficulty: "beginner" as const, description: "Build a flight network from scratch. Learn facts, rules, recursion, provenance, and incremental updates." },
  { id: "retraction", name: "Correct Retraction", category: "Truth Maintenance", difficulty: "intermediate" as const, description: "The diamond problem - why removing one reason shouldn't remove a conclusion that has other support." },
  { id: "provenance", name: "AI Agent Procurement Audit", category: "Explainability", difficulty: "intermediate" as const, description: "An AI agent approves purchases. A regulator asks: why? Show the proof." },
  { id: "incremental", name: "Company Access Control", category: "Incremental Updates", difficulty: "intermediate" as const, description: "Build access control. Watch how one new hire automatically gets the right permissions." },
  { id: "rules_vectors", name: "Smart Product Recommendations", category: "Hybrid Reasoning", difficulty: "advanced" as const, description: "A customer asks for printer ink. Vector search recommends the wrong brand. Rules fix it." },
  { id: "agentic_ai", name: "Customer Churn Detection", category: "Agentic AI", difficulty: "advanced" as const, description: "Build a churn risk system. When the VP asks 'why?', show the proof." },
  { id: "schemas", name: "Schemas & Column Types", category: "Data Modeling", difficulty: "beginner" as const, description: "Why schemas matter. Typed columns, self-documenting data, and cleaner query results." },
]

const DIFFICULTY_COLORS = {
  beginner: "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  intermediate: "bg-amber-500/15 text-amber-600 dark:text-amber-400 border-amber-500/30",
  advanced: "bg-violet-500/15 text-violet-600 dark:text-violet-400 border-violet-500/30",
} as const

interface ChatMessage {
  role: "user" | "assistant"
  content: string
  suggestedQuery?: string
}

interface QuerySidebarProps {
  onSelectQuery: (query: string) => void | Promise<void>
  onLoadQuery?: (query: string) => void
  pendingExample?: string | null
  onPendingExampleHandled?: () => void
}

export function QuerySidebar({ onSelectQuery, onLoadQuery, pendingExample, onPendingExampleHandled }: QuerySidebarProps) {
  const [activeTab, setActiveTab] = useState<"agent" | "history">("agent")
  const { queryHistory, executeQuery, setEditorContent } = useDatalogStore()

  return (
    <div className="flex h-full flex-col bg-background">
      {/* Tab header */}
      <div className="flex border-b">
        <button
          onClick={() => setActiveTab("agent")}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors",
            activeTab === "agent"
              ? "border-b-2 border-primary text-primary"
              : "text-muted-foreground hover:text-foreground"
          )}
        >
          <Sparkles className="h-3 w-3" />
          Learn
        </button>
        <button
          onClick={() => setActiveTab("history")}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors",
            activeTab === "history"
              ? "border-b-2 border-primary text-primary"
              : "text-muted-foreground hover:text-foreground"
          )}
        >
          <History className="h-3 w-3" />
          History
          {queryHistory.length > 0 && (
            <span className="text-[10px] text-muted-foreground">({queryHistory.length})</span>
          )}
        </button>
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-auto">
        {activeTab === "agent" ? (
          <AgentPanel onSelectQuery={onSelectQuery} pendingExample={pendingExample} onPendingExampleHandled={onPendingExampleHandled} />
        ) : (
          <HistoryPanel queryHistory={queryHistory} onSelectQuery={onSelectQuery} onLoadQuery={onLoadQuery ?? onSelectQuery} />
        )}
      </div>
    </div>
  )
}

// --- Agent Panel (teaches IQL through examples + Claude) ---

// Persist agent state across panel open/close
const agentStateCache: { example: string | null; messages: ChatMessage[] } = { example: null, messages: [] }

function AgentPanel({ onSelectQuery, pendingExample, onPendingExampleHandled }: { onSelectQuery: (query: string) => void; pendingExample?: string | null; onPendingExampleHandled?: () => void }) {
  const { executeQuery, executeInternalQuery, setEditorContent, loadExample, createKnowledgeGraph, deleteKnowledgeGraph, loadKnowledgeGraph, username } = useDatalogStore()
  const [messages, setMessages] = useState<ChatMessage[]>(agentStateCache.messages)
  const [input, setInput] = useState("")
  const [loading, setLoading] = useState(false)
  const [activeExample, setActiveExample] = useState<string | null>(agentStateCache.example)
  const lessonDone = useRef(false)
  const lastExecutedQuery = useRef<string | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  // Sync state to cache so it survives panel close/reopen
  useEffect(() => {
    agentStateCache.example = activeExample
    agentStateCache.messages = messages
  }, [activeExample, messages])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  // Auto-start a pending example (from welcome panel or URL parameter)
  useEffect(() => {
    if (pendingExample && !loading && !activeExample) {
      startExample(pendingExample)
      onPendingExampleHandled?.()
    }
  }, [pendingExample]) // eslint-disable-line react-hooks/exhaustive-deps

  /** Parse an agent response: content (row 0), optional suggested_query and done markers (remaining rows). */
  const parseAgentResponse = (data: (string | number | boolean | null)[][]) => {
    const content = String(data[0]?.[0] ?? "")
    let suggestedQuery: string | undefined
    let done = false
    for (let i = 1; i < data.length; i++) {
      const row = String(data[i]?.[0] ?? "")
      if (row.startsWith("suggested_query:")) suggestedQuery = row.slice("suggested_query:".length)
      if (row === "done:true") done = true
    }
    return { content, suggestedQuery, done }
  }

  const startExample = async (exampleId: string) => {
    setLoading(true)
    setActiveExample(exampleId)
    setMessages([])
    lessonDone.current = false

    try {
      // Create a per-user KG for this lesson and switch to it (updates header)
      const userSlug = (username || "anon").replace(/[^a-z0-9_]/g, "_")
      const kgName = `learn_${userSlug}_${exampleId.replace(/[^a-z0-9_]/g, "_")}`
      try { await deleteKnowledgeGraph(kgName) } catch { /* ignore if doesn't exist */ }
      await createKnowledgeGraph(kgName)
      await loadKnowledgeGraph(kgName)

      // Set editor to show the KG context
      setEditorContent(`// ${TEACHING_EXAMPLES.find(e => e.id === exampleId)?.name ?? exampleId}\n// Follow the Learn panel to build this knowledge graph step by step.\n`)

      // Start the scripted lesson
      const response = await executeInternalQuery(`.agent start ${exampleId}`)
      if (response.data && response.data.length > 0) {
        const { content, suggestedQuery, done } = parseAgentResponse(response.data)
        if (done && !suggestedQuery) lessonDone.current = true
        setMessages([{ role: "assistant", content, suggestedQuery }])
      }
    } catch (e) {
      setMessages([{
        role: "assistant",
        content: `Failed to start example: ${e instanceof Error ? e.message : String(e)}`,
      }])
    }

    setLoading(false)
    inputRef.current?.focus()
  }

  const sendMessage = async () => {
    if (!input.trim() || loading) return
    const userMsg = input.trim()
    setInput("")
    setMessages((prev) => [...prev, { role: "user", content: userMsg }])
    setLoading(true)

    try {
      // Include the last executed query as context so Claude knows what the user is referring to
      const ctx = lastExecutedQuery.current ? `[last query: ${lastExecutedQuery.current}] ` : ""
      const response = await executeInternalQuery(`.agent ${ctx}${userMsg}`)
      if (response.data && response.data.length > 0) {
        const { content, suggestedQuery, done } = parseAgentResponse(response.data)
        if (done && !suggestedQuery) lessonDone.current = true
        setMessages((prev) => [...prev, { role: "assistant", content, suggestedQuery }])
      }
    } catch (e) {
      setMessages((prev) => [...prev, {
        role: "assistant",
        content: `Error: ${e instanceof Error ? e.message : String(e)}`,
      }])
    }

    setLoading(false)
    inputRef.current?.focus()
  }

  const runSuggestedQuery = async (query: string) => {
    setEditorContent(query)
    lastExecutedQuery.current = query
    await onSelectQuery(query)

    // Don't auto-advance if the lesson is already complete
    if (lessonDone.current) return

    // Advance to the next step now that the query has completed
    try {
      const response = await executeInternalQuery(".agent next")
      if (response.data && response.data.length > 0) {
        const { content, suggestedQuery, done } = parseAgentResponse(response.data)
        if (done && !suggestedQuery) lessonDone.current = true
        setMessages((prev) => [...prev, { role: "assistant", content, suggestedQuery }])
      }
    } catch { /* ignore */ }
  }

  // Parse agent message into segments (text + code blocks)
  const renderMessage = (content: string) => {
    const segments: Array<{ type: "text" | "code"; content: string; lang?: string }> = []
    let remaining = content
    while (remaining.length > 0) {
      const codeStart = remaining.search(/```(\w*)\n?/)
      if (codeStart === -1) {
        segments.push({ type: "text", content: remaining })
        break
      }
      if (codeStart > 0) {
        segments.push({ type: "text", content: remaining.slice(0, codeStart) })
      }
      const langMatch = remaining.slice(codeStart).match(/```(\w*)\n?/)
      const lang = langMatch?.[1] || ""
      const codeContentStart = codeStart + (langMatch?.[0].length ?? 3)
      const codeEnd = remaining.indexOf("```", codeContentStart)
      if (codeEnd === -1) {
        segments.push({ type: "text", content: remaining.slice(codeStart) })
        break
      }
      segments.push({ type: "code", content: remaining.slice(codeContentStart, codeEnd).trim(), lang })
      remaining = remaining.slice(codeEnd + 3)
      continue
    }

    return segments.map((seg, i) => {
      if (seg.type === "code") {
        return (
          <button
            key={i}
            onClick={() => runSuggestedQuery(seg.content)}
            className="my-1.5 flex w-full items-center gap-2 rounded-md border border-primary/30 bg-primary/5 px-3 py-2.5 text-left font-mono text-xs text-primary hover:bg-primary/10 transition-colors"
          >
            <Play className="h-3 w-3 flex-shrink-0" />
            <span className="flex-1">{seg.content}</span>
          </button>
        )
      }
      // Render text with basic formatting (bold with **)
      const parts = seg.content.split(/(\*\*[^*]+\*\*)/)
      return (
        <span key={i}>
          {parts.map((part, j) => {
            if (part.startsWith("**") && part.endsWith("**")) {
              return <strong key={j} className="font-semibold">{part.slice(2, -2)}</strong>
            }
            return <span key={j}>{part}</span>
          })}
        </span>
      )
    })
  }

  // Show example list when no active conversation
  if (!activeExample) {
    return (
      <div className="p-3 space-y-2">
        <div className="text-center py-2">
          <Sparkles className="h-5 w-5 mx-auto mb-1.5 text-primary" />
          <p className="text-xs font-medium">Learn InputLayer</p>
          <p className="text-[10px] text-muted-foreground mt-0.5">Pick a topic to start a guided tutorial</p>
        </div>
        {TEACHING_EXAMPLES.map((ex) => (
          <button
            key={ex.id}
            onClick={() => startExample(ex.id)}
            disabled={loading}
            className="w-full rounded-lg border bg-card p-3 text-left transition-all hover:border-primary/50 hover:shadow-sm disabled:opacity-50"
          >
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <p className="text-sm font-medium">{ex.name}</p>
                <p className="mt-0.5 text-xs text-muted-foreground leading-relaxed">{ex.description}</p>
              </div>
              <Badge variant="outline" className={cn("text-[9px] px-1.5 py-0 flex-shrink-0", DIFFICULTY_COLORS[ex.difficulty])}>
                {ex.difficulty}
              </Badge>
            </div>
            <p className="mt-1 text-[10px] text-muted-foreground/60">{ex.category}</p>
          </button>
        ))}
      </div>
    )
  }

  // Active conversation
  const currentExample = TEACHING_EXAMPLES.find((e) => e.id === activeExample)

  return (
    <div className="flex h-full flex-col">
      {/* Chat header */}
      <div className="flex items-center justify-between border-b px-3 py-2">
        <div className="flex items-center gap-2 min-w-0">
          <Bot className="h-3.5 w-3.5 text-primary flex-shrink-0" />
          <span className="text-xs font-medium truncate">{currentExample?.name ?? activeExample}</span>
        </div>
        <Button variant="ghost" size="sm" className="h-5 px-1.5 text-[10px]" onClick={() => { setActiveExample(null); setMessages([]) }}>
          <X className="h-3 w-3" />
        </Button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-auto p-3 space-y-3">
        {messages.map((msg, i) => (
          <div key={i} className={cn("text-sm leading-relaxed", msg.role === "user" ? "text-right" : "")}>
            {msg.role === "user" ? (
              <div className="inline-block rounded-lg bg-primary text-primary-foreground px-3 py-2 max-w-[85%] text-left">
                {msg.content}
              </div>
            ) : (
              <div className="rounded-lg bg-muted/50 px-3 py-2.5 text-sm leading-relaxed">
                {renderMessage(msg.content)}
              </div>
            )}
          </div>
        ))}
        {loading && (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            Thinking...
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="border-t p-2">
        <div className="flex gap-1.5">
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && sendMessage()}
            placeholder="Ask about this example..."
            className="h-8 text-xs"
            disabled={loading}
          />
          <Button size="sm" className="h-8 w-8 p-0" onClick={sendMessage} disabled={loading || !input.trim()}>
            <Send className="h-3 w-3" />
          </Button>
        </div>
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
