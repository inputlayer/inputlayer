"use client"

import Image from "next/image"
import { Database, ChevronDown, Search, Check } from "lucide-react"
import { ThemeToggle } from "@/components/theme-toggle"
import { ConnectionStatus } from "@/components/connection-status"
import { useDatalogStore } from "@/lib/datalog-store"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { useState } from "react"
import { Input } from "@/components/ui/input"

export function Header() {
  const { selectedKnowledgeGraph, connection, knowledgeGraphs, loadKnowledgeGraph } = useDatalogStore()
  const [searchQuery, setSearchQuery] = useState("")

  const filteredKnowledgeGraphs = knowledgeGraphs.filter((kg) => kg.name.toLowerCase().includes(searchQuery.toLowerCase()))

  return (
    <header className="sticky top-0 z-50 w-full border-b border-border/50 bg-background/80 backdrop-blur-xl">
      <div className="flex h-12 items-center px-4">
        <div className="flex items-center gap-4">
          <Image src="/logo.png" alt="InputLayer" width={120} height={28} className="dark:invert-0" priority />

          {/* Knowledge Graph breadcrumb after logo */}
          {connection && (
            <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
              <div className="h-4 w-px bg-border" />
              <span className="ml-2 text-foreground/80">{connection.host}</span>
              <span className="text-border">/</span>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="h-6 gap-1.5 px-2 text-sm font-medium">
                    <Database className="h-3 w-3 text-primary" />
                    {selectedKnowledgeGraph ? selectedKnowledgeGraph.name : "Select KG"}
                    <ChevronDown className="h-3 w-3 opacity-50" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-64">
                  <div className="p-2">
                    <div className="relative">
                      <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        placeholder="Search knowledge graphs..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="h-8 pl-8 text-sm"
                      />
                    </div>
                  </div>
                  <DropdownMenuSeparator />
                  <div className="max-h-64 overflow-y-auto">
                    {filteredKnowledgeGraphs.length === 0 ? (
                      <div className="px-2 py-4 text-center text-sm text-muted-foreground">No knowledge graphs found</div>
                    ) : (
                      filteredKnowledgeGraphs.map((kg) => (
                        <DropdownMenuItem
                          key={kg.id}
                          onClick={() => {
                            loadKnowledgeGraph(kg.name)
                            setSearchQuery("")
                          }}
                          className="flex items-center justify-between gap-2"
                        >
                          <div className="flex items-center gap-2">
                            <Database className="h-3.5 w-3.5 text-muted-foreground" />
                            <div>
                              <span className="font-medium">{kg.name}</span>
                              <p className="text-xs text-muted-foreground">
                                {kg.relationsCount} relations • {kg.viewsCount} views
                              </p>
                            </div>
                          </div>
                          {selectedKnowledgeGraph?.id === kg.id && <Check className="h-4 w-4 text-primary" />}
                        </DropdownMenuItem>
                      ))
                    )}
                  </div>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          )}
        </div>

        <div className="flex-1" />

        {/* Right section - status and theme toggle */}
        <div className="flex items-center gap-2">
          <ConnectionStatus />
          <div className="h-4 w-px bg-border" />
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
