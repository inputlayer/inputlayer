"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { AppShell } from "@/components/app-shell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { GatewaySettings } from "@/components/gateway-settings"
import { useGatewayStore } from "@/lib/gateway-store"
import {
  useChatStore,
  conversationTitle,
  findingCount,
  type ChatTurn,
  type Conversation,
} from "@/lib/chat-store"
import { useIQLStore, type InstalledOntology } from "@/lib/iql-store"
import {
  chatCompletion,
  gatewayHealth,
  subscribeEvents,
  type ChatMessage,
  type Finding,
  type GatewayEvent,
  type Report,
} from "@/lib/gateway-client"
import {
  Send,
  Settings2,
  ShieldCheck,
  ShieldAlert,
  Loader2,
  ChevronRight,
  Radio,
  AlertTriangle,
  Plus,
  Trash2,
  MessagesSquare,
} from "lucide-react"

type Turn = ChatTurn

/** Highlight the quoted spans a finding cites inside a message. */
function HighlightedText({ text, spans }: { text: string; spans: string[] }) {
  if (spans.length === 0) return <>{text}</>
  const escaped = spans
    .filter((span) => span.length > 0)
    .map((span) => span.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
  if (escaped.length === 0) return <>{text}</>
  const parts = text.split(new RegExp(`(${escaped.join("|")})`, "g"))
  return (
    <>
      {parts.map((part, index) =>
        spans.includes(part) ? (
          <mark
            key={index}
            className="rounded bg-amber-400/30 px-0.5 text-foreground dark:bg-amber-400/20"
          >
            {part}
          </mark>
        ) : (
          <span key={index}>{part}</span>
        ),
      )}
    </>
  )
}

function ProofTree({ proof }: { proof: unknown }) {
  const [open, setOpen] = useState(false)
  if (!proof || (Array.isArray(proof) && proof.length === 0)) return null
  const nodes =
    Array.isArray(proof) && proof[0] && typeof proof[0] === "object"
      ? ((proof[0] as { nodes?: Record<string, unknown> }).nodes ?? {})
      : {}
  const entries = Object.entries(nodes)
  return (
    <div className="mt-2">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
      >
        <ChevronRight className={`h-3 w-3 transition-transform ${open ? "rotate-90" : ""}`} />
        Proof ({entries.length} nodes, from the engine)
      </button>
      {open && (
        <div className="mt-1.5 space-y-1 rounded-md bg-muted/50 p-2 font-mono text-[11px] leading-relaxed">
          {entries.map(([id, raw]) => {
            const node = raw as {
              kind?: string
              rule_id?: string
              conclusion?: { pred?: string; args?: string[] }
            }
            const conclusion = node.conclusion
              ? `${node.conclusion.pred}(${(node.conclusion.args ?? []).join(", ")})`
              : ""
            return (
              <div key={id} className="flex gap-2">
                <span className="text-muted-foreground">{node.kind === "rule" ? "rule" : "fact"}</span>
                <span className="min-w-0 break-all">
                  {conclusion}
                  {node.rule_id && (
                    <span className="block text-muted-foreground">{node.rule_id}</span>
                  )}
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function FindingCard({ finding, messages }: { finding: Finding; messages: Turn[] }) {
  return (
    <div className="rounded-md border border-amber-500/40 bg-amber-500/5 p-3 text-sm space-y-2">
      <div className="flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400" />
        <span className="font-medium">{finding.title ?? finding.view}</span>
        {finding.blocking && <Badge variant="outline">blocking</Badge>}
      </div>
      <div className="space-y-1">
        {finding.spans.map((span, index) => {
          const messageIndex = Number(span.message)
          const turn = messages[messageIndex]
          return (
            <div key={index} className="text-xs">
              <span className="text-muted-foreground">
                message {span.message}
                {turn ? ` (${turn.role})` : ""}:{" "}
              </span>
              <span className="italic">
                <HighlightedText text={span.surface} spans={[span.surface]} />
              </span>
            </div>
          )
        })}
      </div>
      <ProofTree proof={finding.proof} />
    </div>
  )
}

export default function ChatPage() {
  const { url, apiKey, hydrate } = useGatewayStore()
  const knowledgeGraphs = useIQLStore((s) => s.knowledgeGraphs)
  const selectedKg = useIQLStore((s) => s.selectedKnowledgeGraph)
  const listInstalledOntologies = useIQLStore((s) => s.listInstalledOntologies)

  const {
    conversations,
    activeId,
    hydrate: hydrateChats,
    startConversation,
    selectConversation,
    deleteConversation,
    setTurns: persistTurns,
    updateSettings,
  } = useChatStore()

  const [settingsOpen, setSettingsOpen] = useState(false)
  const [installed, setInstalled] = useState<InstalledOntology[]>([])
  const [trace, setTrace] = useState(false)
  const [draft, setDraft] = useState("")
  const [sending, setSending] = useState(false)
  const [events, setEvents] = useState<GatewayEvent[]>([])
  const transcriptRef = useRef<HTMLDivElement>(null)

  useEffect(() => hydrate(), [hydrate])
  useEffect(() => hydrateChats(), [hydrateChats])

  const active: Conversation | null = activeId ? (conversations[activeId] ?? null) : null
  const conversation = active?.id ?? ""
  const turns = useMemo(() => active?.turns ?? [], [active])
  const kgName = active?.kg ?? ""
  const ontology = active?.ontology ?? ""
  const mode = active?.mode ?? "annotate"

  // Start a conversation on first visit so the page is never empty-handed.
  useEffect(() => {
    if (!useChatStore.getState().hydrated) return
    if (activeId) return
    const kg = selectedKg?.name ?? knowledgeGraphs[0]?.name ?? ""
    if (Object.keys(conversations).length === 0) {
      startConversation({ kg, ontology: "", mode: "annotate" })
    } else {
      const newest = Object.values(conversations).sort((a, b) => b.updatedAt - a.updatedAt)[0]
      selectConversation(newest.id)
    }
  }, [activeId, conversations, knowledgeGraphs, selectedKg, startConversation, selectConversation])

  const setKgName = (kg: string) => activeId && updateSettings(activeId, { kg })
  const setOntology = (name: string) => activeId && updateSettings(activeId, { ontology: name })
  const setMode = (next: "annotate" | "enforce") =>
    activeId && updateSettings(activeId, { mode: next })
  const setTurns = (next: Turn[]) => activeId && persistTurns(activeId, next)

  // A conversation created before any KG was known adopts one later.
  useEffect(() => {
    if (activeId && !kgName && (selectedKg?.name || knowledgeGraphs[0]?.name)) {
      updateSettings(activeId, { kg: selectedKg?.name ?? knowledgeGraphs[0].name })
    }
  }, [activeId, kgName, selectedKg, knowledgeGraphs, updateSettings])

  // What is installed in the chosen KG decides what we can evaluate against.
  useEffect(() => {
    if (!kgName) return
    let cancelled = false
    void (async () => {
      try {
        const packs = await listInstalledOntologies(kgName)
        if (cancelled) return
        setInstalled(packs)
        if (!packs.some((pack) => pack.name === ontology)) {
          setOntology(packs[0]?.name ?? "")
        }
      } catch {
        if (!cancelled) setInstalled([])
      }
    })()
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kgName, listInstalledOntologies])

  // Live evaluation events for this conversation.
  useEffect(() => {
    if (!url || !apiKey || !conversation) return
    // Switching conversations resubscribes: the gateway replays that
    // conversation's recent events from its ring buffer.
    setEvents([])
    const close = subscribeEvents({ url, apiKey }, conversation, (event) =>
      setEvents((current) => [...current.slice(-99), event]),
    )
    return close
  }, [url, apiKey, conversation])

  useEffect(() => {
    transcriptRef.current?.scrollTo({ top: transcriptRef.current.scrollHeight })
  }, [turns])

  // What this gateway actually serves, so a version drift is visible BEFORE
  // a turn fails: an evaluation is refused when the knowledge graph's
  // installed pack is not the one the gateway loaded.
  const [served, setServed] = useState<Record<string, string>>({})
  useEffect(() => {
    if (!url) return
    let cancelled = false
    void (async () => {
      try {
        const health = await gatewayHealth({ url, apiKey })
        if (cancelled) return
        const map: Record<string, string> = {}
        for (const entry of health.ontologies) {
          const [name, version] = entry.split("@")
          if (name && version) map[name] = version
        }
        setServed(map)
      } catch {
        if (!cancelled) setServed({})
      }
    })()
    return () => {
      cancelled = true
    }
  }, [url, apiKey])

  const versionDrift = useMemo(() => {
    if (!ontology) return null
    const installedPack = installed.find((pack) => pack.name === ontology)
    const servedVersion = served[ontology]
    if (!installedPack || !servedVersion || installedPack.version === servedVersion) return null
    return { installed: installedPack.version, served: servedVersion }
  }, [ontology, installed, served])

  const selections = useMemo(
    () => (kgName && ontology ? [`${kgName}/${ontology}`] : []),
    [kgName, ontology],
  )

  const send = useCallback(async () => {
    const text = draft.trim()
    if (!text || sending) return
    const history: Turn[] = [...turns, { role: "user", content: text }]
    setTurns(history)
    setDraft("")
    setSending(true)
    try {
      const messages: ChatMessage[] = history.map((turn) => ({
        role: turn.role,
        content: turn.content,
      }))
      const result = await chatCompletion(
        { url, apiKey },
        { messages, selections, conversation, mode, trace },
      )
      setTurns([
        ...history,
        {
          role: "assistant",
          content: result.content || (result.refusal ? "" : "(empty response)"),
          reports: result.reports,
          refusal: result.refusal,
        },
      ])
    } catch (error) {
      setTurns([
        ...history,
        {
          role: "assistant",
          content: "",
          refusal: {
            type: "error",
            message: error instanceof Error ? error.message : String(error),
          },
        },
      ])
    } finally {
      setSending(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draft, sending, turns, url, apiKey, selections, conversation, mode, trace, activeId])

  const configured = Boolean(url && apiKey)

  const history = useMemo(
    () => Object.values(conversations).sort((a, b) => b.updatedAt - a.updatedAt),
    [conversations],
  )

  return (
    <AppShell>
      <div className="flex flex-1 overflow-hidden">
        {/* Conversation history */}
        <div className="hidden w-60 flex-col border-r border-border/50 bg-muted/20 md:flex">
          <div className="flex items-center justify-between border-b border-border/50 px-3 py-2">
            <span className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-muted-foreground">
              <MessagesSquare className="h-4 w-4" />
              Conversations
            </span>
            <Button
              variant="ghost"
              size="sm"
              title="New conversation"
              onClick={() =>
                startConversation({
                  kg: kgName || (selectedKg?.name ?? knowledgeGraphs[0]?.name ?? ""),
                  ontology,
                  mode,
                })
              }
            >
              <Plus className="h-4 w-4" />
            </Button>
          </div>
          <div className="flex-1 overflow-auto p-2 space-y-1">
            {history.length === 0 && (
              <p className="px-2 py-1 text-xs text-muted-foreground">No conversations yet.</p>
            )}
            {history.map((item) => {
              const findings = findingCount(item)
              return (
                <div
                  key={item.id}
                  className={`group rounded-md px-2 py-1.5 text-sm cursor-pointer ${
                    item.id === activeId ? "bg-teal-500/10" : "hover:bg-muted"
                  }`}
                  onClick={() => selectConversation(item.id)}
                >
                  <div className="flex items-start justify-between gap-1">
                    <span className="min-w-0 flex-1 truncate">{conversationTitle(item)}</span>
                    <button
                      type="button"
                      className="opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-foreground"
                      title="Delete conversation"
                      onClick={(event) => {
                        event.stopPropagation()
                        deleteConversation(item.id)
                      }}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </div>
                  <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
                    <span className="font-mono truncate">{item.id}</span>
                    {findings > 0 && (
                      <span className="text-amber-600 dark:text-amber-400">{findings}</span>
                    )}
                  </div>
                  <div className="text-[11px] text-muted-foreground">
                    {item.kg}
                    {item.ontology ? `/${item.ontology}` : ""} - {item.turns.length} turn(s)
                  </div>
                </div>
              )
            })}
          </div>
          <p className="border-t border-border/50 px-3 py-2 text-[11px] leading-snug text-muted-foreground">
            The conversation id namespaces its facts in the knowledge graph - reopening one
            continues the same accumulated conversation.
          </p>
        </div>

        {/* Transcript */}
        <div className="flex flex-1 flex-col overflow-hidden">
          <div className="flex items-center gap-2 border-b border-border/50 px-4 py-2">
            <select
              className="rounded-md border border-input bg-background px-2 py-1 text-sm"
              value={kgName}
              onChange={(event) => setKgName(event.target.value)}
            >
              {knowledgeGraphs.map((kg) => (
                <option key={kg.name} value={kg.name}>
                  {kg.name}
                </option>
              ))}
            </select>
            <span className="text-muted-foreground">/</span>
            <select
              className="rounded-md border border-input bg-background px-2 py-1 text-sm"
              value={ontology}
              onChange={(event) => setOntology(event.target.value)}
            >
              {installed.length === 0 && <option value="">no ontology installed</option>}
              {installed.map((pack) => (
                <option key={pack.name} value={pack.name}>
                  {pack.name}@{pack.version}
                </option>
              ))}
            </select>
            <Button
              variant={mode === "enforce" ? "default" : "ghost"}
              size="sm"
              onClick={() => setMode(mode === "enforce" ? "annotate" : "enforce")}
              title="Enforce refuses to complete over blocking findings"
            >
              {mode === "enforce" ? (
                <ShieldCheck className="h-4 w-4 mr-1.5" />
              ) : (
                <ShieldAlert className="h-4 w-4 mr-1.5" />
              )}
              {mode}
            </Button>
            <Button
              variant={trace ? "default" : "ghost"}
              size="sm"
              onClick={() => setTrace(!trace)}
              title="Return the extracted claims and mapped IQL"
            >
              trace
            </Button>
            <div className="ml-auto flex items-center gap-2">
              <span className="font-mono text-xs text-muted-foreground">{conversation}</span>
              <Button variant="ghost" size="sm" onClick={() => setSettingsOpen(true)}>
                <Settings2 className="h-4 w-4" />
              </Button>
            </div>
          </div>

          <div ref={transcriptRef} className="flex-1 space-y-4 overflow-auto p-4">
            {!configured && (
              <div className="rounded-lg border border-border/60 p-4 text-sm">
                <p className="font-medium">Connect the gateway</p>
                <p className="mt-1 text-muted-foreground">
                  Set the gateway URL and API key to start a verified conversation.
                </p>
                <Button className="mt-3" size="sm" onClick={() => setSettingsOpen(true)}>
                  <Settings2 className="h-4 w-4 mr-1.5" />
                  Gateway settings
                </Button>
              </div>
            )}
            {versionDrift && (
              <div className="rounded-lg border border-amber-500/50 bg-amber-500/5 p-3 text-sm">
                <p className="font-medium text-amber-600 dark:text-amber-400">
                  Version drift: this knowledge graph is on {ontology}@{versionDrift.installed},
                  the gateway serves {versionDrift.served}
                </p>
                <p className="mt-1 text-muted-foreground">
                  Evaluation will be refused until they match, so findings stay attributable to
                  the rules that derived them. Upgrade it in the Ontologies tab.
                </p>
              </div>
            )}
            {turns.map((turn, index) => (
              <div key={index} className="space-y-2">
                <div
                  className={
                    turn.role === "user"
                      ? "ml-auto max-w-[80%] rounded-lg bg-teal-500/10 px-3 py-2 text-sm"
                      : "max-w-[85%] rounded-lg bg-muted px-3 py-2 text-sm"
                  }
                >
                  {turn.role === "assistant" && turn.content ? (
                    // Models answer in markdown; render it, but keep the
                    // quoted-span highlighting on the plain text nodes so a
                    // finding's evidence is still visible in the reply.
                    <div className="chat-markdown">
                      <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        components={{
                          p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                          ul: ({ children }) => (
                            <ul className="mb-2 list-disc space-y-1 pl-5 last:mb-0">{children}</ul>
                          ),
                          ol: ({ children }) => (
                            <ol className="mb-2 list-decimal space-y-1 pl-5 last:mb-0">
                              {children}
                            </ol>
                          ),
                          li: ({ children }) => <li className="leading-snug">{children}</li>,
                          h1: ({ children }) => (
                            <h1 className="mb-2 mt-1 text-base font-semibold">{children}</h1>
                          ),
                          h2: ({ children }) => (
                            <h2 className="mb-2 mt-1 text-sm font-semibold">{children}</h2>
                          ),
                          h3: ({ children }) => (
                            <h3 className="mb-1 mt-1 text-sm font-semibold">{children}</h3>
                          ),
                          strong: ({ children }) => (
                            <strong className="font-semibold">{children}</strong>
                          ),
                          code: ({ children }) => (
                            <code className="rounded bg-background/70 px-1 py-0.5 font-mono text-[12px]">
                              {children}
                            </code>
                          ),
                          pre: ({ children }) => (
                            <pre className="mb-2 overflow-x-auto rounded bg-background/70 p-2 font-mono text-[12px]">
                              {children}
                            </pre>
                          ),
                          table: ({ children }) => (
                            <div className="mb-2 overflow-x-auto">
                              <table className="w-full border-collapse text-[13px]">{children}</table>
                            </div>
                          ),
                          th: ({ children }) => (
                            <th className="border border-border/60 px-2 py-1 text-left font-medium">
                              {children}
                            </th>
                          ),
                          td: ({ children }) => (
                            <td className="border border-border/60 px-2 py-1">{children}</td>
                          ),
                          blockquote: ({ children }) => (
                            <blockquote className="mb-2 border-l-2 border-border pl-3 text-muted-foreground">
                              {children}
                            </blockquote>
                          ),
                          a: ({ href, children }) => (
                            <a
                              href={href}
                              target="_blank"
                              rel="noreferrer"
                              className="text-teal-600 underline dark:text-teal-400"
                            >
                              {children}
                            </a>
                          ),
                        }}
                      >
                        {turn.content}
                      </ReactMarkdown>
                    </div>
                  ) : (
                    <HighlightedText
                      text={turn.content}
                      spans={(turn.reports ?? []).flatMap((report) =>
                        report.findings.flatMap((finding) =>
                          finding.spans.map((span) => span.surface),
                        ),
                      )}
                    />
                  )}
                  {turn.refusal && (
                    <div className="space-y-1 text-sm">
                      <span className="font-medium text-red-600 dark:text-red-400">
                        {turn.refusal.type === "consistency_violation"
                          ? "Completion refused - the conversation conflicts"
                          : turn.refusal.type === "verification_unavailable"
                            ? "Completion refused - evaluation did not complete"
                            : turn.refusal.type}
                      </span>
                      <p className="text-muted-foreground">{turn.refusal.message}</p>
                      {(turn.reports ?? [])
                        .filter((report) => report.status !== "complete" && report.reason)
                        .map((report, index) => (
                          <p key={index} className="text-muted-foreground">
                            <span className="font-mono">
                              {report.kg}/{report.ontology}
                            </span>
                            : {report.reason}
                          </p>
                        ))}
                    </div>
                  )}
                </div>
                {(turn.reports ?? []).map((report, reportIndex) => (
                  <div key={reportIndex} className="max-w-[85%] space-y-2">
                    <div className="flex items-center gap-2 text-xs text-muted-foreground">
                      <Badge variant={report.status === "complete" ? "secondary" : "outline"}>
                        {report.status}
                      </Badge>
                      <span className="font-mono">
                        {report.kg}/{report.ontology}
                      </span>
                      {report.reason && <span>{report.reason}</span>}
                    </div>
                    {report.findings.map((finding, findingIndex) => (
                      <FindingCard key={findingIndex} finding={finding} messages={turns} />
                    ))}
                    {report.dropped.length > 0 && (
                      <p className="text-xs text-muted-foreground">
                        {report.dropped.length} extraction row(s) dropped (unquoted or malformed)
                      </p>
                    )}
                    {report.trace && (
                      <details className="rounded-md bg-muted/50 p-2 text-xs">
                        <summary className="cursor-pointer text-muted-foreground">
                          Translation ({report.trace.statements?.length ?? 0} statements,
                          extract {report.trace.extract_ms}ms, engine {report.trace.engine_ms}ms)
                        </summary>
                        <pre className="mt-2 overflow-x-auto font-mono text-[11px]">
                          {(report.trace.statements ?? []).join("\n")}
                        </pre>
                      </details>
                    )}
                  </div>
                ))}
              </div>
            ))}
            {sending && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Completing and evaluating...
              </div>
            )}
          </div>

          <div className="flex gap-2 border-t border-border/50 p-3">
            <Input
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault()
                  void send()
                }
              }}
              placeholder={configured ? "Say something..." : "Configure the gateway first"}
              disabled={!configured || sending}
            />
            <Button onClick={() => void send()} disabled={!configured || sending || !draft.trim()}>
              <Send className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* Live events */}
        <div className="hidden w-80 flex-col border-l border-border/50 bg-muted/20 lg:flex">
          <div className="flex items-center gap-2 border-b border-border/50 px-3 py-2">
            <Radio className="h-4 w-4 text-teal-500" />
            <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Evaluation events
            </span>
          </div>
          <div className="flex-1 space-y-2 overflow-auto p-3 text-xs">
            {events.length === 0 && (
              <p className="text-muted-foreground">
                Events for this conversation appear here as turns are evaluated.
              </p>
            )}
            {events.map((event, index) => (
              <div key={index} className="rounded border border-border/50 bg-background p-2">
                <div className="flex items-center gap-2">
                  <Badge variant="outline">{event.type}</Badge>
                  {event.ontology ? (
                    <span className="font-mono text-muted-foreground">
                      {String(event.ontology)}
                    </span>
                  ) : null}
                </div>
                {event.type === "translation" && (
                  <p className="mt-1 text-muted-foreground">
                    {(event.tuples as unknown[] | undefined)?.length ?? 0} tuples stored
                  </p>
                )}
                {event.type === "finding" && (
                  <p className="mt-1">{String(event.title ?? "finding")}</p>
                )}
                {event.type === "report" && (
                  <p className="mt-1 text-muted-foreground">
                    {String(event.status)} - {String(event.findings ?? 0)} finding(s)
                  </p>
                )}
                {event.type === "lagged" && (
                  <p className="mt-1 text-amber-600 dark:text-amber-400">
                    {String(event.skipped)} event(s) skipped
                  </p>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      <GatewaySettings open={settingsOpen} onOpenChange={setSettingsOpen} />
    </AppShell>
  )
}
