"use client"

import { Check, X, Loader2, LogOut } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { useDatalogStore } from "@/lib/datalog-store"
import { cn } from "@/lib/utils"

export function ConnectionStatus() {
  const { connection, disconnect } = useDatalogStore()

  const status = connection?.status || "disconnected"

  const statusConfig = {
    connected: {
      icon: Check,
      label: "Connected",
      className: "bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20",
    },
    disconnected: {
      icon: X,
      label: "Disconnected",
      className: "bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20",
    },
    connecting: {
      icon: Loader2,
      label: "Connecting",
      className: "bg-yellow-500/10 text-yellow-600 dark:text-yellow-400 border-yellow-500/20",
    },
    reconnecting: {
      icon: Loader2,
      label: "Reconnecting",
      className: "bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20",
    },
  }

  const config = statusConfig[status]
  const Icon = config.icon

  if (status !== "connected") {
    return (
      <Badge variant="secondary" className={cn("border", config.className)}>
        <Icon className={cn("mr-1.5 h-3 w-3", (status === "connecting" || status === "reconnecting") && "animate-spin")} />
        {config.label}
      </Badge>
    )
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" className="h-auto p-0">
          <Badge variant="secondary" className={cn("border cursor-pointer", config.className)}>
            <Icon className="mr-1.5 h-3 w-3" />
            {config.label}
            {connection?.name && <span className="ml-1.5 opacity-70">({connection.name})</span>}
          </Badge>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem
          onClick={disconnect}
          className="text-destructive focus:text-destructive"
        >
          <LogOut className="mr-2 h-3.5 w-3.5" />
          Disconnect
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
