"use client"

import { Database, ChevronDown, Search, Check, Plus, Trash2, Loader2 } from "lucide-react"
import { Logo } from "@/components/logo"
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
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { useState } from "react"
import { Input } from "@/components/ui/input"
import { toast } from "sonner"

const KG_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export function Header() {
  const { selectedKnowledgeGraph, connection, knowledgeGraphs, loadKnowledgeGraph, createKnowledgeGraph, deleteKnowledgeGraph } = useDatalogStore()
  const [searchQuery, setSearchQuery] = useState("")
  const [newKgName, setNewKgName] = useState("")
  const [newKgError, setNewKgError] = useState<string | null>(null)
  const [isCreating, setIsCreating] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)
  const [isDeleting, setIsDeleting] = useState(false)
  const [dropdownOpen, setDropdownOpen] = useState(false)

  const filteredKnowledgeGraphs = knowledgeGraphs.filter((kg) => kg.name.toLowerCase().includes(searchQuery.toLowerCase()))

  const validateKgName = (name: string): string | null => {
    if (!name.trim()) return "Name is required"
    if (!KG_NAME_RE.test(name)) return "Must start with a letter/underscore, alphanumeric only"
    if (name === "." || name === "..") return "Invalid name"
    return null
  }

  const handleCreateKg = async () => {
    const error = validateKgName(newKgName)
    if (error) {
      setNewKgError(error)
      return
    }
    setIsCreating(true)
    setNewKgError(null)
    try {
      await createKnowledgeGraph(newKgName)
      await loadKnowledgeGraph(newKgName)
      setNewKgName("")
      toast.success(`Knowledge graph "${newKgName}" created`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to create knowledge graph"
      setNewKgError(msg)
    } finally {
      setIsCreating(false)
    }
  }

  const handleDeleteKg = async () => {
    if (!deleteTarget) return
    setIsDeleting(true)
    try {
      await deleteKnowledgeGraph(deleteTarget)
      toast.success(`Knowledge graph "${deleteTarget}" deleted`)
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to delete knowledge graph"
      toast.error(msg)
    } finally {
      setIsDeleting(false)
      setDeleteTarget(null)
    }
  }

  return (
    <header className="sticky top-0 z-50 w-full border-b border-border/50 bg-background/80 backdrop-blur-xl">
      <div className="flex h-12 items-center px-4">
        <div className="flex items-center gap-4">
          <Logo size="sm" />

          {/* Knowledge Graph breadcrumb after logo */}
          {connection && (
            <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
              <div className="h-4 w-px bg-border" />
              <span className="ml-2 text-foreground/80">{connection.host}</span>
              <span className="text-border">/</span>
              <DropdownMenu open={dropdownOpen} onOpenChange={(open) => {
                setDropdownOpen(open)
                if (!open) { setSearchQuery(""); setNewKgName(""); setNewKgError(null) }
              }}>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="h-6 gap-1.5 px-2 text-sm font-medium">
                    <Database className="h-3 w-3 text-primary" />
                    {selectedKnowledgeGraph ? selectedKnowledgeGraph.name : "Select KG"}
                    <ChevronDown className="h-3 w-3 opacity-50" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-72">
                  <div className="p-2">
                    <div className="relative">
                      <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        placeholder="Search knowledge graphs..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="h-8 pl-8 text-sm"
                        aria-label="Search knowledge graphs"
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
                          <div className="flex items-center gap-2 flex-1 min-w-0">
                            <Database className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                            <div className="min-w-0 flex-1">
                              <span className="font-medium">{kg.name}</span>
                              <p className="text-xs text-muted-foreground">
                                {kg.relationsCount} relations • {kg.viewsCount} views
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-1 flex-shrink-0">
                            {selectedKnowledgeGraph?.id === kg.id && <Check className="h-4 w-4 text-primary" />}
                            <button
                              onClick={(e) => {
                                e.stopPropagation()
                                e.preventDefault()
                                setDeleteTarget(kg.name)
                                setDropdownOpen(false)
                              }}
                              className="rounded p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive transition-colors opacity-0 group-hover:opacity-100"
                              title={`Delete ${kg.name}`}
                            >
                              <Trash2 className="h-3 w-3" />
                            </button>
                          </div>
                        </DropdownMenuItem>
                      ))
                    )}
                  </div>
                  <DropdownMenuSeparator />
                  {/* Create new KG */}
                  <div className="p-2">
                    <div className="flex gap-1.5">
                      <Input
                        placeholder="New knowledge graph..."
                        value={newKgName}
                        onChange={(e) => { setNewKgName(e.target.value); setNewKgError(null) }}
                        onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); handleCreateKg() } }}
                        className="h-7 text-xs flex-1"
                        aria-label="New knowledge graph name"
                      />
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 w-7 p-0 flex-shrink-0"
                        onClick={handleCreateKg}
                        disabled={isCreating || !newKgName.trim()}
                      >
                        {isCreating ? <Loader2 className="h-3 w-3 animate-spin" /> : <Plus className="h-3 w-3" />}
                      </Button>
                    </div>
                    {newKgError && (
                      <p className="mt-1 text-[10px] text-destructive">{newKgError}</p>
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

      {/* Delete KG confirmation dialog */}
      <AlertDialog open={deleteTarget !== null} onOpenChange={(open) => { if (!open) setDeleteTarget(null) }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete knowledge graph</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete &quot;{deleteTarget}&quot;? This action cannot be undone and all data will be permanently removed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isDeleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteKg}
              disabled={isDeleting}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {isDeleting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </header>
  )
}
