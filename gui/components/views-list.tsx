"use client"

import { Eye, GitBranch } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import type { View } from "@/lib/datalog-store"
import { cn } from "@/lib/utils"

interface ViewsListProps {
  views: View[]
  selectedId: string | undefined
  onSelect: (view: View) => void
}

export function ViewsList({ views, selectedId, onSelect }: ViewsListProps) {
  if (views.length === 0) {
    return (
      <Card>
        <CardContent className="flex flex-col items-center justify-center py-12">
          <Eye className="h-8 w-8 text-muted-foreground" />
          <p className="mt-2 text-sm text-muted-foreground">No views found</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-2">
      {views.map((view) => (
        <Card
          key={view.id}
          className={cn(
            "cursor-pointer transition-all hover:border-primary/50",
            selectedId === view.id && "border-primary ring-1 ring-primary",
          )}
          onClick={() => onSelect(view)}
        >
          <CardContent className="p-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Eye className="h-4 w-4 text-accent" />
                <span className="font-medium font-mono text-sm">{view.name}</span>
              </div>
              <Badge variant="outline" className="text-xs">
                computed
              </Badge>
            </div>
            <div className="mt-2 flex items-center gap-3 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <GitBranch className="h-3 w-3" />
                {view.dependencies.length} deps
              </span>
              <span>{view.computationSteps.length} steps</span>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
