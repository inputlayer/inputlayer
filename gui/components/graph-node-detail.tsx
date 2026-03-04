"use client"

import { X, Link2, Network } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"

export interface NodeDetailData {
  id: string
  label: string
  degree: number
  relations: string[]
  neighbors: { label: string; relation: string; direction: "in" | "out" }[]
}

interface GraphNodeDetailProps {
  node: NodeDetailData | null
  onClose: () => void
}

export function GraphNodeDetail({ node, onClose }: GraphNodeDetailProps) {
  if (!node) return null

  return (
    <div className="absolute top-4 right-4 z-10 w-72 rounded-lg border border-border/50 bg-background/95 backdrop-blur-sm shadow-lg">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border/50 px-3 py-2">
        <div className="flex items-center gap-2 min-w-0">
          <div className="h-2.5 w-2.5 rounded-full bg-teal-400 flex-shrink-0" />
          <span className="font-mono text-sm font-medium truncate">{node.label}</span>
        </div>
        <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={onClose}>
          <X className="h-3.5 w-3.5" />
        </Button>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-4 px-3 py-2 border-b border-border/50">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <Link2 className="h-3 w-3" />
          {node.degree} connections
        </div>
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <Network className="h-3 w-3" />
          {node.relations.length} relation{node.relations.length !== 1 ? "s" : ""}
        </div>
      </div>

      {/* Relations */}
      <div className="px-3 py-2 border-b border-border/50">
        <h4 className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground mb-1.5">
          Relations
        </h4>
        <div className="flex flex-wrap gap-1">
          {node.relations.map((rel) => (
            <Badge key={rel} variant="secondary" className="text-[10px] font-mono">
              {rel}
            </Badge>
          ))}
        </div>
      </div>

      {/* Neighbors */}
      <div className="px-3 py-2 max-h-48 overflow-auto scrollbar-thin">
        <h4 className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground mb-1.5">
          Connections ({Math.min(node.neighbors.length, 20)}
          {node.neighbors.length > 20 ? ` of ${node.neighbors.length}` : ""})
        </h4>
        <div className="space-y-1">
          {node.neighbors.slice(0, 20).map((neighbor, i) => (
            <div key={i} className="flex items-center gap-2 text-xs">
              <span className={`text-[10px] ${neighbor.direction === "out" ? "text-emerald-500" : "text-blue-500"}`}>
                {neighbor.direction === "out" ? "\u2192" : "\u2190"}
              </span>
              <span className="font-mono truncate flex-1">{neighbor.label}</span>
              <span className="text-[10px] text-muted-foreground">{neighbor.relation}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
