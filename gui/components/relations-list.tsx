"use client"

import { Network, Rows3 } from "lucide-react"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import type { Relation } from "@/lib/datalog-store"
import { cn } from "@/lib/utils"

interface RelationsListProps {
  relations: Relation[]
  selectedId: string | undefined
  onSelect: (relation: Relation) => void
}

export function RelationsList({ relations, selectedId, onSelect }: RelationsListProps) {
  if (relations.length === 0) {
    return (
      <Card>
        <CardContent className="flex flex-col items-center justify-center py-12">
          <Network className="h-8 w-8 text-muted-foreground" />
          <p className="mt-2 text-sm text-muted-foreground">No relations found</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-2">
      {relations.map((relation) => (
        <Card
          key={relation.id}
          className={cn(
            "cursor-pointer transition-all hover:border-primary/50",
            selectedId === relation.id && "border-primary ring-1 ring-primary",
          )}
          onClick={() => onSelect(relation)}
        >
          <CardContent className="p-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Network className="h-4 w-4 text-primary" />
                <span className="font-medium font-mono text-sm">{relation.name}</span>
              </div>
              <Badge variant="secondary" className="text-xs">
                arity {relation.arity}
              </Badge>
            </div>
            <div className="mt-2 flex items-center gap-3 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Rows3 className="h-3 w-3" />
                {relation.tupleCount.toLocaleString()} tuples
              </span>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
