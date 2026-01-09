"use client"

import { useState, useEffect, useCallback } from "react"
import { AppShell } from "@/components/app-shell"
import { useDatalogStore, type KnowledgeGraph } from "@/lib/datalog-store"
import { InputLayerClient } from "@inputlayer/api-client"
import {
  Database,
  Plus,
  Trash2,
  RefreshCw,
  Loader2,
  AlertCircle,
  Network,
  Eye,
  Check,
  X,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
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
import { cn } from "@/lib/utils"

const client = new InputLayerClient({ baseUrl: "/api/v1" })

export default function KnowledgeGraphPage() {
  const { knowledgeGraphs, setKnowledgeGraphs, selectedKnowledgeGraph, loadKnowledgeGraph } = useDatalogStore()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [newKgName, setNewKgName] = useState("")
  const [newKgDescription, setNewKgDescription] = useState("")
  const [creating, setCreating] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [kgToDelete, setKgToDelete] = useState<KnowledgeGraph | null>(null)

  const loadKnowledgeGraphs = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await client.knowledgeGraphs.list()
      const kgs: KnowledgeGraph[] = result.knowledgeGraphs.map((kg, idx) => ({
        id: String(idx + 1),
        name: kg.name,
        description: kg.description,
        relationsCount: kg.relationsCount,
        viewsCount: kg.viewsCount,
      }))
      setKnowledgeGraphs(kgs)
    } catch (err) {
      console.error("Failed to load knowledge graphs:", err)
      setError(err instanceof Error ? err.message : "Failed to load knowledge graphs")
    } finally {
      setLoading(false)
    }
  }, [setKnowledgeGraphs])

  useEffect(() => {
    loadKnowledgeGraphs()
  }, [loadKnowledgeGraphs])

  const handleCreate = async () => {
    if (!newKgName.trim()) return

    setCreating(true)
    try {
      await client.knowledgeGraphs.create({
        name: newKgName.trim(),
        description: newKgDescription.trim() || undefined,
      })
      setNewKgName("")
      setNewKgDescription("")
      setCreateDialogOpen(false)
      await loadKnowledgeGraphs()
    } catch (err) {
      console.error("Failed to create knowledge graph:", err)
      setError(err instanceof Error ? err.message : "Failed to create knowledge graph")
    } finally {
      setCreating(false)
    }
  }

  const handleDelete = async () => {
    if (!kgToDelete) return

    setDeleting(true)
    try {
      await client.knowledgeGraphs.delete(kgToDelete.name)
      setDeleteDialogOpen(false)
      setKgToDelete(null)
      await loadKnowledgeGraphs()
    } catch (err) {
      console.error("Failed to delete knowledge graph:", err)
      setError(err instanceof Error ? err.message : "Failed to delete knowledge graph")
    } finally {
      setDeleting(false)
    }
  }

  const confirmDelete = (kg: KnowledgeGraph) => {
    setKgToDelete(kg)
    setDeleteDialogOpen(true)
  }

  return (
    <AppShell>
      <div className="flex h-full flex-col">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border/50 bg-muted/30 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
              <Database className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h1 className="text-lg font-semibold">Knowledge Graph Management</h1>
              <p className="text-xs text-muted-foreground">
                Create, manage, and monitor your knowledge graphs
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={loadKnowledgeGraphs}
              disabled={loading}
              className="gap-1.5"
            >
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              Refresh
            </Button>
            <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
              <DialogTrigger asChild>
                <Button size="sm" className="gap-1.5">
                  <Plus className="h-4 w-4" />
                  Create Knowledge Graph
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Create New Knowledge Graph</DialogTitle>
                  <DialogDescription>
                    Create a new knowledge graph to store relations and views.
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4 py-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Name</label>
                    <Input
                      placeholder="my_knowledge_graph"
                      value={newKgName}
                      onChange={(e) => setNewKgName(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      Use lowercase letters, numbers, and underscores
                    </p>
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Description (optional)</label>
                    <Input
                      placeholder="A brief description..."
                      value={newKgDescription}
                      onChange={(e) => setNewKgDescription(e.target.value)}
                    />
                  </div>
                </div>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                    Cancel
                  </Button>
                  <Button onClick={handleCreate} disabled={creating || !newKgName.trim()}>
                    {creating && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                    Create
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-6">
          {error && (
            <div className="mb-4 flex items-center gap-2 rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive">
              <AlertCircle className="h-4 w-4" />
              {error}
              <Button
                variant="ghost"
                size="sm"
                className="ml-auto h-6 w-6 p-0"
                onClick={() => setError(null)}
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          )}

          {loading ? (
            <div className="flex h-64 items-center justify-center">
              <div className="text-center">
                <Loader2 className="mx-auto h-8 w-8 animate-spin text-muted-foreground" />
                <p className="mt-2 text-sm text-muted-foreground">Loading knowledge graphs...</p>
              </div>
            </div>
          ) : knowledgeGraphs.length === 0 ? (
            <div className="flex h-64 items-center justify-center">
              <div className="text-center">
                <Database className="mx-auto h-12 w-12 text-muted-foreground/50" />
                <h3 className="mt-4 text-lg font-medium">No knowledge graphs</h3>
                <p className="mt-1 text-sm text-muted-foreground">
                  Create your first knowledge graph to get started
                </p>
                <Button className="mt-4" onClick={() => setCreateDialogOpen(true)}>
                  <Plus className="mr-2 h-4 w-4" />
                  Create Knowledge Graph
                </Button>
              </div>
            </div>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {knowledgeGraphs.map((kg) => (
                <div
                  key={kg.id}
                  className={cn(
                    "group relative rounded-lg border border-border/50 bg-card p-4 transition-all hover:border-border hover:shadow-sm",
                    selectedKnowledgeGraph?.name === kg.name && "border-primary/50 bg-primary/5"
                  )}
                >
                  {/* Selection indicator */}
                  {selectedKnowledgeGraph?.name === kg.name && (
                    <div className="absolute -right-1 -top-1 flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground">
                      <Check className="h-3 w-3" />
                    </div>
                  )}

                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-muted">
                        <Database className="h-5 w-5 text-muted-foreground" />
                      </div>
                      <div>
                        <h3 className="font-mono font-medium">{kg.name}</h3>
                        {kg.description && (
                          <p className="text-xs text-muted-foreground">{kg.description}</p>
                        )}
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-8 w-8 p-0 opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive hover:bg-destructive/10"
                      onClick={() => confirmDelete(kg)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>

                  <div className="mt-4 flex items-center gap-4">
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <Network className="h-3.5 w-3.5" />
                      <span>{kg.relationsCount} relations</span>
                    </div>
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <Eye className="h-3.5 w-3.5" />
                      <span>{kg.viewsCount} views</span>
                    </div>
                  </div>

                  <div className="mt-4 flex items-center gap-2">
                    {selectedKnowledgeGraph?.name === kg.name ? (
                      <Badge variant="secondary" className="text-xs">
                        Selected
                      </Badge>
                    ) : (
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 text-xs"
                        onClick={() => loadKnowledgeGraph(kg.name)}
                      >
                        Select
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Delete confirmation dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Knowledge Graph</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete <strong>{kgToDelete?.name}</strong>? This action
              cannot be undone. All relations and views in this knowledge graph will be permanently
              deleted.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {deleting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </AppShell>
  )
}
