"use client"

import { type ReactNode, useEffect } from "react"
import { useDatalogStore } from "@/lib/datalog-store"
import { ConnectionScreen } from "@/components/connection-screen"
import { SplashScreen } from "@/components/splash-screen"
import { Header } from "@/components/header"
import { SidebarNav } from "@/components/sidebar-nav"
import { ErrorBoundary } from "@/components/error-boundary"

interface AppShellProps {
  children: ReactNode
}

export function AppShell({ children }: AppShellProps) {
  const { connection, isInitialized, isRestoringSession, initFromStorage } = useDatalogStore()

  // Try to restore session from localStorage on mount
  useEffect(() => {
    initFromStorage()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

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

  return (
    <ErrorBoundary>
      <div className="flex h-screen flex-col bg-background">
        <Header />
        <div className="flex flex-1 overflow-hidden">
          <SidebarNav />
          <main className="flex flex-1 flex-col overflow-hidden">{children}</main>
        </div>
      </div>
    </ErrorBoundary>
  )
}
