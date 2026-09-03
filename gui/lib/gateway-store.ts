"use client"

import { create } from "zustand"
import { defaultGatewayUrl, type GatewayConfig } from "./gateway-client"

/**
 * Gateway settings, persisted the same way the engine connection is: the
 * URL in localStorage, the bearer key in sessionStorage (it is a
 * credential, and a shared browser should not keep it after the tab
 * closes).
 */
const URL_KEY = "inputlayer_gateway_url"
const TOKEN_KEY = "inputlayer_gateway_token"

function safeGet(storage: "local" | "session", key: string): string | null {
  if (typeof window === "undefined") return null
  try {
    return (storage === "local" ? window.localStorage : window.sessionStorage).getItem(key)
  } catch {
    return null
  }
}

function safeSet(storage: "local" | "session", key: string, value: string): void {
  if (typeof window === "undefined") return
  try {
    const store = storage === "local" ? window.localStorage : window.sessionStorage
    if (value) store.setItem(key, value)
    else store.removeItem(key)
  } catch {
    // Private mode or a full quota: settings simply do not persist.
  }
}

interface GatewayState {
  url: string
  apiKey: string
  hydrated: boolean
  setUrl: (url: string) => void
  setApiKey: (apiKey: string) => void
  hydrate: () => void
  config: () => GatewayConfig
}

export const useGatewayStore = create<GatewayState>((set, get) => ({
  url: "",
  apiKey: "",
  hydrated: false,

  setUrl: (url: string) => {
    safeSet("local", URL_KEY, url)
    set({ url })
  },

  setApiKey: (apiKey: string) => {
    safeSet("session", TOKEN_KEY, apiKey)
    set({ apiKey })
  },

  hydrate: () => {
    if (get().hydrated) return
    set({
      url: safeGet("local", URL_KEY) ?? defaultGatewayUrl(),
      apiKey: safeGet("session", TOKEN_KEY) ?? "",
      hydrated: true,
    })
  },

  config: () => ({ url: get().url, apiKey: get().apiKey }),
}))
