"use client"

import { type ReactNode, useEffect } from "react"
import { useDatalogStore } from "@/lib/datalog-store"
import { ConnectionScreen } from "@/components/connection-screen"
import { SplashScreen } from "@/components/splash-screen"
import { Header } from "@/components/header"
import { MinimalHeader } from "@/components/minimal-header"
import { SidebarNav } from "@/components/sidebar-nav"
import { ErrorBoundary } from "@/components/error-boundary"

interface AppShellProps {
  children: ReactNode
  /** When false, skip connection/splash gates (e.g. docs page works offline) */
  requireConnection?: boolean
}

export function AppShell({ children, requireConnection = true }: AppShellProps) {
  const { connection, isInitialized, isRestoringSession, initFromStorage } = useDatalogStore()

  // Try to restore session from localStorage on mount
  useEffect(() => {
    initFromStorage()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (requireConnection) {
    // Show splash while initializing or restoring a saved session
    if (!isInitialized || isRestoringSession) {
      return <SplashScreen />
    }

    // Show connection screen if disconnected (no active connection)
    if (!connection || connection.status === "disconnected") {
      return <ConnectionScreen />
    }

    // Show splash during connecting/reconnecting states
    if (connection.status === "connecting" || connection.status === "reconnecting") {
      const msg = connection.status === "reconnecting" ? "Reconnecting..." : "Connecting..."
      return <SplashScreen status={msg} />
    }
  }

  const isConnected = connection && connection.status === "connected"

  return (
    <ErrorBoundary>
      <div className="flex h-screen flex-col bg-background">
        {isConnected ? <Header /> : <MinimalHeader />}
        <div className="flex flex-1 overflow-hidden">
          <SidebarNav connectedOnly={!!isConnected} />
          <main className="flex flex-1 flex-col overflow-hidden">{children}</main>
        </div>
      </div>
    </ErrorBoundary>
  )
}
