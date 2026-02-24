"use client"

import type React from "react"
import { useEffect, useRef, useState, useCallback } from "react"
import type { View, Relation } from "@/lib/datalog-store"
import { useTheme } from "next-themes"
import { ZoomIn, ZoomOut, Maximize2 } from "lucide-react"
import { Button } from "@/components/ui/button"

interface ViewGraphTabProps {
  view: View
  relations: Relation[]
}

interface Node {
  id: string
  label: string
  type: "relation" | "view" | "operation"
  x: number
  y: number
}

interface Edge {
  from: string
  to: string
}

export function ViewGraphTab({ view, relations }: ViewGraphTabProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const nodesRef = useRef<Node[]>([])
  const [hoveredNode, setHoveredNode] = useState<string | null>(null)
  const [zoom, setZoom] = useState(1)
  const [pan, setPan] = useState({ x: 0, y: 0 })
  const [dragging, setDragging] = useState(false)
  const dragStartRef = useRef({ x: 0, y: 0, panX: 0, panY: 0 })
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  const drawGraph = useCallback(() => {
    if (!mounted) return

    const canvas = canvasRef.current
    const container = containerRef.current
    if (!canvas || !container) return

    const ctx = canvas.getContext("2d")
    if (!ctx) return

    // Set canvas size to container size
    const rect = container.getBoundingClientRect()
    canvas.width = rect.width * 2 // Higher res
    canvas.height = rect.height * 2
    ctx.scale(2, 2)

    const isDark = resolvedTheme === "dark"

    const colors = {
      background: isDark ? "#0a0a0a" : "#fafafa",
      relationNode: "#3b82f6",
      viewNode: "#a855f7",
      operationNode: "#10b981",
      edge: isDark ? "#333" : "#e5e5e5",
      edgeActive: isDark ? "#555" : "#d4d4d4",
      text: isDark ? "#fafafa" : "#0a0a0a",
      textSecondary: isDark ? "#a1a1aa" : "#71717a",
    }

    const nodes: Node[] = []
    const edges: Edge[] = []

    const width = rect.width
    const height = rect.height
    const centerX = width / 2

    // Add dependency relations at top
    view.dependencies.forEach((dep, index) => {
      const spacing = width / (view.dependencies.length + 1)
      nodes.push({
        id: dep,
        label: dep,
        type: "relation",
        x: spacing * (index + 1),
        y: 80,
      })
    })

    // Add computation steps
    view.computationSteps.forEach((step, index) => {
      const y = 160 + index * 90
      nodes.push({
        id: step.id,
        label: step.operation,
        type: "operation",
        x: centerX,
        y: y,
      })

      step.inputs.forEach((input) => {
        const inputNode = nodes.find((n) => n.id === input || n.label === input)
        if (inputNode) {
          edges.push({ from: inputNode.id, to: step.id })
        }
      })
    })

    // Add view output
    const lastY = nodes.length > 0 ? Math.max(...nodes.map((n) => n.y)) + 90 : 250
    nodes.push({
      id: view.id,
      label: view.name,
      type: "view",
      x: centerX,
      y: lastY,
    })

    if (view.computationSteps.length > 0) {
      // Connect last step to output view node
      edges.push({
        from: view.computationSteps[view.computationSteps.length - 1].id,
        to: view.id,
      })
    } else if (view.dependencies.length > 0) {
      // Fallback: no computation steps but have dependencies - draw direct edges
      view.dependencies.forEach((dep) => {
        edges.push({ from: dep, to: view.id })
      })
    }

    // Clear canvas
    ctx.fillStyle = colors.background
    ctx.fillRect(0, 0, width, height)

    // Apply zoom and pan
    ctx.save()
    ctx.translate(width / 2 + pan.x, height / 2 + pan.y)
    ctx.scale(zoom, zoom)
    ctx.translate(-width / 2, -height / 2)

    // Draw edges with gradient
    edges.forEach((edge) => {
      const from = nodes.find((n) => n.id === edge.from)
      const to = nodes.find((n) => n.id === edge.to)
      if (from && to) {
        const gradient = ctx.createLinearGradient(from.x, from.y, to.x, to.y)
        gradient.addColorStop(0, colors.edgeActive)
        gradient.addColorStop(1, colors.edge)

        ctx.strokeStyle = gradient
        ctx.lineWidth = 2
        ctx.beginPath()
        ctx.moveTo(from.x, from.y + 24)

        const midY = (from.y + 24 + to.y - 24) / 2
        ctx.bezierCurveTo(from.x, midY, to.x, midY, to.x, to.y - 24)
        ctx.stroke()

        // Arrow
        ctx.fillStyle = colors.edge
        ctx.beginPath()
        ctx.moveTo(to.x, to.y - 24)
        ctx.lineTo(to.x - 5, to.y - 32)
        ctx.lineTo(to.x + 5, to.y - 32)
        ctx.closePath()
        ctx.fill()
      }
    })

    // Draw nodes
    nodes.forEach((node) => {
      const isHovered = hoveredNode === node.id
      const nodeHeight = 48
      const nodeWidth = Math.max(100, node.label.length * 10 + 32)

      // Shadow
      if (isHovered) {
        ctx.shadowColor =
          node.type === "relation" ? colors.relationNode : node.type === "view" ? colors.viewNode : colors.operationNode
        ctx.shadowBlur = 20
        ctx.shadowOffsetX = 0
        ctx.shadowOffsetY = 4
      }

      // Node background
      ctx.beginPath()
      const radius = 8
      const x = node.x - nodeWidth / 2
      const y = node.y - nodeHeight / 2
      ctx.roundRect(x, y, nodeWidth, nodeHeight, radius)

      if (node.type === "relation") {
        ctx.fillStyle = colors.relationNode
      } else if (node.type === "view") {
        ctx.fillStyle = colors.viewNode
      } else {
        ctx.fillStyle = colors.operationNode
      }
      ctx.fill()

      // Reset shadow
      ctx.shadowColor = "transparent"
      ctx.shadowBlur = 0

      // Border when hovered
      if (isHovered) {
        ctx.strokeStyle = "#fff"
        ctx.lineWidth = 2
        ctx.stroke()
      }

      // Node label
      ctx.fillStyle = "#fff"
      ctx.font = "600 12px ui-monospace, monospace"
      ctx.textAlign = "center"
      ctx.textBaseline = "middle"
      ctx.fillText(node.label, node.x, node.y - 4)

      // Type label
      ctx.fillStyle = "rgba(255,255,255,0.7)"
      ctx.font = "500 10px ui-sans-serif, sans-serif"
      ctx.fillText(node.type.toUpperCase(), node.x, node.y + 12)
    })

    ctx.restore()

    // Store nodes for hover detection
    nodesRef.current = nodes
  }, [view, relations, hoveredNode, resolvedTheme, mounted, zoom, pan])

  useEffect(() => {
    drawGraph()
  }, [drawGraph])

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const resizeObserver = new ResizeObserver(() => {
      drawGraph()
    })
    resizeObserver.observe(container)

    return () => resizeObserver.disconnect()
  }, [drawGraph])

  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    // Handle drag-to-pan
    if (dragging) {
      const dx = e.clientX - dragStartRef.current.x
      const dy = e.clientY - dragStartRef.current.y
      setPan({ x: dragStartRef.current.panX + dx, y: dragStartRef.current.panY + dy })
      return
    }

    const canvas = canvasRef.current
    const container = containerRef.current
    if (!canvas || !container) return

    const canvasRect = canvas.getBoundingClientRect()
    const containerRect = container.getBoundingClientRect()
    const screenX = e.clientX - canvasRect.left
    const screenY = e.clientY - canvasRect.top
    const width = containerRect.width
    const height = containerRect.height

    // Inverse the zoom+pan transform
    const nodeX = (screenX - width / 2 - pan.x) / zoom + width / 2
    const nodeY = (screenY - height / 2 - pan.y) / zoom + height / 2

    const nodes = nodesRef.current
    const hovered = nodes.find((node) => {
      const nodeWidth = Math.max(100, node.label.length * 10 + 32) / 2
      const dx = Math.abs(nodeX - node.x)
      const dy = Math.abs(nodeY - node.y)
      return dx < nodeWidth && dy < 24
    })

    setHoveredNode(hovered?.id || null)
  }

  const handleMouseDown = (e: React.MouseEvent<HTMLCanvasElement>) => {
    setDragging(true)
    dragStartRef.current = { x: e.clientX, y: e.clientY, panX: pan.x, panY: pan.y }
  }

  const handleMouseUp = () => {
    setDragging(false)
  }

  if (!mounted) {
    return (
      <div className="flex h-full items-center justify-center bg-muted/10">
        <p className="text-sm text-muted-foreground">Loading graph...</p>
      </div>
    )
  }

  return (
    <div className="relative h-full" ref={containerRef}>
      <canvas
        ref={canvasRef}
        className={`h-full w-full ${dragging ? "cursor-grabbing" : "cursor-grab"}`}
        role="img"
        aria-label={`Rule dependency graph for ${view.name}`}
        tabIndex={0}
        onMouseMove={handleMouseMove}
        onMouseDown={handleMouseDown}
        onMouseUp={handleMouseUp}
        onMouseLeave={() => { setHoveredNode(null); setDragging(false) }}
      >
        Dependency graph: {view.name} depends on {view.dependencies.join(", ") || "no other relations"}
      </canvas>

      {/* Zoom controls */}
      <div className="absolute bottom-4 right-4 flex items-center gap-1 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm p-1">
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => setZoom(Math.max(0.5, zoom - 0.25))}>
          <ZoomOut className="h-3.5 w-3.5" />
        </Button>
        <span className="text-xs font-medium w-12 text-center">{Math.round(zoom * 100)}%</span>
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => setZoom(Math.min(2, zoom + 0.25))}>
          <ZoomIn className="h-3.5 w-3.5" />
        </Button>
        <div className="w-px h-4 bg-border mx-1" />
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }) }}>
          <Maximize2 className="h-3.5 w-3.5" />
        </Button>
      </div>

      {/* Legend */}
      <div className="absolute bottom-4 left-4 flex items-center gap-4 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm px-3 py-2">
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded bg-blue-500" />
          <span className="text-xs text-muted-foreground">Relation</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded bg-emerald-500" />
          <span className="text-xs text-muted-foreground">Operation</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="h-3 w-3 rounded bg-purple-500" />
          <span className="text-xs text-muted-foreground">View</span>
        </div>
      </div>
    </div>
  )
}
