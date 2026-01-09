"use client"

import { type ReactNode, useEffect } from "react"
import { useDatalogStore } from "@/lib/datalog-store"
import { ConnectionScreen } from "@/components/connection-screen"
import { Header } from "@/components/header"
import { SidebarNav } from "@/components/sidebar-nav"
import { Loader2 } from "lucide-react"

interface AppShellProps {
  children: ReactNode
}

export function AppShell({ children }: AppShellProps) {
  const { connection, isInitialized, initFromStorage } = useDatalogStore()

  // Try to restore session from localStorage on mount
  useEffect(() => {
    initFromStorage()
  }, [initFromStorage])

  // Show loading state while initializing
  if (!isInitialized) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="flex flex-col items-center gap-3">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          <p className="text-sm text-muted-foreground">Restoring session...</p>
        </div>
      </div>
    )
  }

  // Show connection screen if not connected
  if (!connection || connection.status !== "connected") {
    return <ConnectionScreen />
  }

  return (
    <div className="flex h-screen flex-col bg-background">
      <Header />
      <div className="flex flex-1 overflow-hidden">
        <SidebarNav />
        <main className="flex flex-1 flex-col overflow-hidden">{children}</main>
      </div>
    </div>
  )
}
