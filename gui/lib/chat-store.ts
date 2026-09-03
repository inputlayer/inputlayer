"use client"

import { create } from "zustand"
import type { Report } from "./gateway-client"

/**
 * Conversations survive navigation and reload.
 *
 * The conversation id is not cosmetic: the gateway namespaces every fact it
 * stores with it, so resuming an id continues the SAME accumulated
 * conversation in the knowledge graph (and replays that conversation's
 * events from the gateway's ring buffer). Losing the id would strand those
 * facts under a name nothing references.
 */

export interface ChatTurn {
  role: "user" | "assistant"
  content: string
  reports?: Report[]
  refusal?: { type: string; message: string }
}

export interface Conversation {
  id: string
  createdAt: number
  updatedAt: number
  kg: string
  ontology: string
  mode: "annotate" | "enforce"
  turns: ChatTurn[]
}

const STORAGE_KEY = "inputlayer_chat_conversations"
const ACTIVE_KEY = "inputlayer_chat_active"
const MAX_CONVERSATIONS = 50

function load(): Record<string, Conversation> {
  if (typeof window === "undefined") return {}
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    return raw ? (JSON.parse(raw) as Record<string, Conversation>) : {}
  } catch {
    return {}
  }
}

function persist(conversations: Record<string, Conversation>, activeId: string | null): void {
  if (typeof window === "undefined") return
  try {
    // Keep the newest N so a long-lived browser does not grow without bound.
    const trimmed = Object.values(conversations)
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .slice(0, MAX_CONVERSATIONS)
    const map: Record<string, Conversation> = {}
    for (const conversation of trimmed) map[conversation.id] = conversation
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(map))
    if (activeId) window.localStorage.setItem(ACTIVE_KEY, activeId)
    else window.localStorage.removeItem(ACTIVE_KEY)
  } catch {
    // Private mode or quota: conversations simply stay in memory.
  }
}

export function newConversationId(): string {
  return `studio-${Math.random().toString(36).slice(2, 10)}`
}

/** First user message, for the history list. */
export function conversationTitle(conversation: Conversation): string {
  const first = conversation.turns.find((turn) => turn.role === "user")
  if (!first) return "New conversation"
  return first.content.length > 48 ? `${first.content.slice(0, 48)}...` : first.content
}

export function findingCount(conversation: Conversation): number {
  return conversation.turns.reduce(
    (total, turn) =>
      total + (turn.reports ?? []).reduce((sum, report) => sum + report.findings.length, 0),
    0,
  )
}

interface ChatState {
  conversations: Record<string, Conversation>
  activeId: string | null
  hydrated: boolean
  hydrate: () => void
  startConversation: (defaults: { kg: string; ontology: string; mode: "annotate" | "enforce" }) => string
  selectConversation: (id: string) => void
  deleteConversation: (id: string) => void
  setTurns: (id: string, turns: ChatTurn[]) => void
  updateSettings: (
    id: string,
    settings: Partial<Pick<Conversation, "kg" | "ontology" | "mode">>,
  ) => void
  active: () => Conversation | null
}

export const useChatStore = create<ChatState>((set, get) => ({
  conversations: {},
  activeId: null,
  hydrated: false,

  hydrate: () => {
    if (get().hydrated) return
    const conversations = load()
    let activeId: string | null = null
    try {
      activeId = typeof window !== "undefined" ? window.localStorage.getItem(ACTIVE_KEY) : null
    } catch {
      activeId = null
    }
    if (activeId && !conversations[activeId]) activeId = null
    set({ conversations, activeId, hydrated: true })
  },

  startConversation: (defaults) => {
    const id = newConversationId()
    const now = Date.now()
    const conversation: Conversation = {
      id,
      createdAt: now,
      updatedAt: now,
      turns: [],
      ...defaults,
    }
    const conversations = { ...get().conversations, [id]: conversation }
    persist(conversations, id)
    set({ conversations, activeId: id })
    return id
  },

  selectConversation: (id: string) => {
    if (!get().conversations[id]) return
    persist(get().conversations, id)
    set({ activeId: id })
  },

  deleteConversation: (id: string) => {
    const conversations = { ...get().conversations }
    delete conversations[id]
    const activeId = get().activeId === id ? null : get().activeId
    persist(conversations, activeId)
    set({ conversations, activeId })
  },

  setTurns: (id: string, turns: ChatTurn[]) => {
    const existing = get().conversations[id]
    if (!existing) return
    const conversations = {
      ...get().conversations,
      [id]: { ...existing, turns, updatedAt: Date.now() },
    }
    persist(conversations, get().activeId)
    set({ conversations })
  },

  updateSettings: (id, settings) => {
    const existing = get().conversations[id]
    if (!existing) return
    const conversations = { ...get().conversations, [id]: { ...existing, ...settings } }
    persist(conversations, get().activeId)
    set({ conversations })
  },

  active: () => {
    const { activeId, conversations } = get()
    return activeId ? (conversations[activeId] ?? null) : null
  },
}))
