"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { useDatalogStore } from "@/lib/datalog-store"
import { ConnectionScreen } from "@/components/connection-screen"
import { SplashScreen } from "@/components/splash-screen"

export default function HomePage() {
  const router = useRouter()
  const { connection, isInitialized, isRestoringSession, initFromStorage } = useDatalogStore()

  useEffect(() => {
    initFromStorage()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Redirect to /query once we have an active connection
  useEffect(() => {
    if (isInitialized && !isRestoringSession && connection?.status === "connected") {
      router.replace("/query")
    }
  }, [isInitialized, isRestoringSession, connection?.status, router])

  // Show splash while initializing or restoring
  if (!isInitialized || isRestoringSession) {
    return <SplashScreen />
  }

  // Show splash while connecting/reconnecting
  if (connection?.status === "connecting" || connection?.status === "reconnecting") {
    const msg = connection.status === "reconnecting" ? "Reconnecting..." : "Connecting..."
    return <SplashScreen status={msg} />
  }

  // Not connected — show login screen at "/"
  return <ConnectionScreen />
}
