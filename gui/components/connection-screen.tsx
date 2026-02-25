"use client"

import type React from "react"

import { useState } from "react"
import { Server, Loader2, Database, User, Lock } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { cn } from "@/lib/utils"
import { useDatalogStore } from "@/lib/datalog-store"
import { ThemeToggle } from "@/components/theme-toggle"
import { Logo } from "@/components/logo"

export function ConnectionScreen() {
  const [host, setHost] = useState(() => {
    if (typeof window === "undefined") return "localhost"
    try {
      const stored = localStorage.getItem("inputlayer_connection")
      if (stored) { const p = JSON.parse(stored); return p.host || "localhost" }
    } catch {}
    return "localhost"
  })
  const [port, setPort] = useState(() => {
    if (typeof window === "undefined") return "8080"
    try {
      const stored = localStorage.getItem("inputlayer_connection")
      if (stored) { const p = JSON.parse(stored); return String(p.port || 8080) }
    } catch {}
    return "8080"
  })
  const [database, setDatabase] = useState("")
  const [username, setUsername] = useState(() => {
    if (typeof window === "undefined") return ""
    try {
      const stored = localStorage.getItem("inputlayer_connection")
      if (stored) { const p = JSON.parse(stored); return p.username || "" }
    } catch {}
    return ""
  })
  const [password, setPassword] = useState("")
  const [isConnecting, setIsConnecting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [portError, setPortError] = useState<string | null>(null)

  const { connect } = useDatalogStore()

  const handleConnect = async () => {
    const portNum = parseInt(port)
    if (isNaN(portNum) || portNum < 1 || portNum > 65535) {
      setPortError("Port must be between 1 and 65535")
      return
    }
    setPortError(null)
    setIsConnecting(true)
    setError(null)
    try {
      await connect(host, portNum, database || "default", username, password)
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to connect. Please check your connection details."
      setError(msg)
    } finally {
      setIsConnecting(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !isConnecting) {
      handleConnect()
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <div className="fixed top-4 right-4">
        <ThemeToggle />
      </div>
      <div className="w-full max-w-md">
        <div className="mb-8 flex flex-col items-center text-center">
          <div className="mb-4">
            <Logo size="lg" />
          </div>
          <p className="mt-1.5 text-sm text-muted-foreground">Connect to an InputLayer server to get started</p>
        </div>

        {/* Connection form */}
        <div className="rounded-xl border border-border bg-card p-6 shadow-sm">
          <div className="mb-6 flex items-center gap-3 rounded-lg bg-muted/50 px-4 py-3">
            <Server className="h-5 w-5 text-muted-foreground" />
            <div>
              <p className="text-sm font-medium">Server Connection</p>
              <p className="text-xs text-muted-foreground">Enter your InputLayer server details</p>
            </div>
          </div>

          <div className="space-y-4" onKeyDown={handleKeyDown}>
            {/* Host and Port */}
            <div className="grid grid-cols-3 gap-3">
              <div className="col-span-2 space-y-2">
                <Label htmlFor="host" className="text-xs font-medium text-muted-foreground">
                  Host
                </Label>
                <Input
                  id="host"
                  value={host}
                  onChange={(e) => setHost(e.target.value)}
                  placeholder="localhost"
                  className="h-10"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="port" className="text-xs font-medium text-muted-foreground">
                  Port
                </Label>
                <Input
                  id="port"
                  type="number"
                  min={1}
                  max={65535}
                  value={port}
                  onChange={(e) => { setPort(e.target.value); setPortError(null) }}
                  placeholder="8080"
                  className={cn("h-10", portError && "border-destructive")}
                />
                {portError && <p className="text-[10px] text-destructive mt-1">{portError}</p>}
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="database" className="text-xs font-medium text-muted-foreground">
                Knowledge Graph
              </Label>
              <div className="relative">
                <Database className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="database"
                  value={database}
                  onChange={(e) => setDatabase(e.target.value)}
                  placeholder="default"
                  className="h-10 pl-10"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label htmlFor="username" className="text-xs font-medium text-muted-foreground">
                  Username <span className="text-muted-foreground/50">(optional)</span>
                </Label>
                <div className="relative">
                  <User className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    id="username"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="admin"
                    className="h-10 pl-10"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label htmlFor="password" className="text-xs font-medium text-muted-foreground">
                  Password <span className="text-muted-foreground/50">(optional)</span>
                </Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    id="password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder=""
                    className="h-10 pl-10"
                  />
                </div>
              </div>
            </div>

            {error && <div className="rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">{error}</div>}

            <Button onClick={handleConnect} disabled={isConnecting || !host || !port} className="mt-2 h-10 w-full">
              {isConnecting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Connecting...
                </>
              ) : (
                <>
                  <Database className="mr-2 h-4 w-4" />
                  Connect to Server
                </>
              )}
            </Button>
          </div>
        </div>

        {/* Footer hint */}
        <p className="mt-4 text-center text-xs text-muted-foreground">
          Press <kbd className="rounded bg-muted px-1.5 py-0.5 font-mono text-[10px]">Enter</kbd> to connect
        </p>
      </div>
    </div>
  )
}
