"use client"

import { useRef, useState, useEffect, useCallback, useMemo } from "react"
import dynamic from "next/dynamic"
import { useTheme } from "next-themes"
import { ZoomIn, ZoomOut, Maximize2, LayoutGrid, Share2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select"
import { GraphNodeDetail, type NodeDetailData } from "@/components/graph-node-detail"
import type { CytoscapeElement, GraphStats } from "@/lib/graph-utils"
import { EDGE_COLORS } from "@/lib/graph-utils"
import type cytoscape from "cytoscape"

const CytoscapeComponent = dynamic(() => import("react-cytoscapejs"), { ssr: false })

type LayoutName = "cola" | "cose" | "circle" | "concentric" | "breadthfirst" | "grid"

const LAYOUT_OPTIONS: { value: LayoutName; label: string }[] = [
  { value: "cola", label: "Force-Directed" },
  { value: "cose", label: "Physics" },
  { value: "circle", label: "Circle" },
  { value: "concentric", label: "Concentric" },
  { value: "breadthfirst", label: "Hierarchical" },
  { value: "grid", label: "Grid" },
]

interface GraphCanvasProps {
  elements: CytoscapeElement[]
  stats: GraphStats
  relationNames: string[]
}

export function GraphCanvas({ elements, stats, relationNames }: GraphCanvasProps) {
  const cyRef = useRef<cytoscape.Core | null>(null)
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const [layout, setLayout] = useState<LayoutName>("cola")
  const [selectedNode, setSelectedNode] = useState<NodeDetailData | null>(null)
  const [zoomLabel, setZoomLabel] = useState("100%")

  useEffect(() => { setMounted(true) }, [])

  // Register cola layout plugin (once, client-side only)
  useEffect(() => {
    if (typeof window === "undefined") return
    import("cytoscape").then((Cytoscape) => {
      import("cytoscape-cola").then((cola) => {
        try { Cytoscape.default.use(cola.default) } catch { /* already registered */ }
      })
    })
  }, [])

  const isDark = resolvedTheme === "dark"

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stylesheet: any[] = useMemo(() => [
    {
      selector: "node",
      style: {
        label: "data(label)",
        "background-color": isDark ? "#2dd4bf" : "#14b8a6",
        color: isDark ? "#e5e7eb" : "#1f2937",
        "font-size": "10px",
        "font-family": "ui-monospace, monospace",
        "text-valign": "bottom" as const,
        "text-margin-y": 6,
        width: "mapData(degree, 1, 20, 20, 50)",
        height: "mapData(degree, 1, 20, 20, 50)",
        "border-width": 2,
        "border-color": isDark ? "#0d9488" : "#0f766e",
        "text-max-width": "80px",
        "text-wrap": "ellipsis" as const,
      },
    },
    {
      selector: "node:selected",
      style: {
        "background-color": isDark ? "#e879f9" : "#d946ef",
        "border-color": isDark ? "#f0abfc" : "#c026d3",
        "border-width": 3,
      },
    },
    {
      selector: "edge",
      style: {
        width: 1.5,
        "line-color": isDark ? "#4b5563" : "#d1d5db",
        "target-arrow-color": isDark ? "#4b5563" : "#d1d5db",
        "target-arrow-shape": "triangle" as const,
        "curve-style": "bezier" as const,
        "arrow-scale": 0.8,
        label: elements.length < 200 ? "data(label)" : "",
        "font-size": "8px",
        "text-rotation": "autorotate" as const,
        color: isDark ? "#9ca3af" : "#6b7280",
        "text-background-color": isDark ? "#0a0a0a" : "#ffffff",
        "text-background-opacity": 0.8,
        "text-background-padding": "2px" as unknown as number,
      },
    },
    ...relationNames.map((name, i) => ({
      selector: `edge[relation = "${name}"]`,
      style: {
        "line-color": EDGE_COLORS[i % EDGE_COLORS.length],
        "target-arrow-color": EDGE_COLORS[i % EDGE_COLORS.length],
      },
    })),
  ], [isDark, relationNames, elements.length])

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const layoutConfig = useMemo((): any => {
    const configs: Record<LayoutName, any> = {
      cola: {
        name: "cola",
        animate: true,
        maxSimulationTime: 3000,
        nodeSpacing: 30,
        edgeLength: 150,
        randomize: true,
      },
      cose: {
        name: "cose",
        animate: true,
        animationDuration: 1000,
        nodeRepulsion: () => 8000,
        idealEdgeLength: () => 100,
        randomize: true,
      },
      circle: { name: "circle", animate: true },
      concentric: {
        name: "concentric",
        animate: true,
        concentric: (node: { degree: () => number }) => node.degree(),
        levelWidth: () => 2,
      },
      breadthfirst: { name: "breadthfirst", animate: true, spacingFactor: 1.5 },
      grid: { name: "grid", animate: true },
    }
    return configs[layout]
  }, [layout])

  const handleCy = useCallback((cy: cytoscape.Core) => {
    cyRef.current = cy

    cy.on("tap", "node", (evt) => {
      const node = evt.target
      const nodeData = node.data()
      const neighbors = node.connectedEdges().map((edge: cytoscape.EdgeSingular) => {
        const edgeData = edge.data()
        const isSource = edgeData.source === nodeData.id
        return {
          label: isSource
            ? cy.getElementById(edgeData.target).data("label")
            : cy.getElementById(edgeData.source).data("label"),
          relation: edgeData.relation,
          direction: (isSource ? "out" : "in") as "in" | "out",
        }
      })
      setSelectedNode({
        id: nodeData.id,
        label: nodeData.label,
        degree: nodeData.degree,
        relations: nodeData.relations,
        neighbors,
      })
    })

    cy.on("tap", (evt) => {
      if (evt.target === cy) setSelectedNode(null)
    })

    cy.on("zoom", () => {
      setZoomLabel(`${Math.round(cy.zoom() * 100)}%`)
    })
  }, [])

  useEffect(() => {
    if (cyRef.current && elements.length > 0) {
      cyRef.current.layout(layoutConfig).run()
    }
  }, [layoutConfig, elements])

  if (!mounted) {
    return (
      <div className="flex h-full items-center justify-center bg-muted/10">
        <p className="text-sm text-muted-foreground">Loading graph...</p>
      </div>
    )
  }

  if (elements.length === 0) {
    return (
      <div className="flex h-full items-center justify-center bg-muted/10">
        <div className="text-center">
          <Share2 className="mx-auto h-12 w-12 text-muted-foreground/30" />
          <p className="mt-3 text-sm font-medium text-muted-foreground">No data to visualize</p>
          <p className="mt-1 text-xs text-muted-foreground/70">
            Select binary relations from the sidebar to build the graph
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="relative h-full w-full">
      <CytoscapeComponent
        elements={elements as unknown as cytoscape.ElementDefinition[]}
        stylesheet={stylesheet}
        layout={layoutConfig}
        cy={handleCy}
        style={{ width: "100%", height: "100%" }}
        className={isDark ? "bg-[#0a0a0a]" : "bg-[#fafafa]"}
      />

      <GraphNodeDetail node={selectedNode} onClose={() => setSelectedNode(null)} />

      {/* Layout selector */}
      <div className="absolute top-4 left-4 z-10">
        <Select value={layout} onValueChange={(v) => setLayout(v as LayoutName)}>
          <SelectTrigger size="sm" className="h-8 w-40 bg-background/90 backdrop-blur-sm border-border/50">
            <LayoutGrid className="h-3.5 w-3.5 mr-1.5" />
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LAYOUT_OPTIONS.map((opt) => (
              <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Zoom controls */}
      <div className="absolute bottom-4 right-4 flex items-center gap-1 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm p-1">
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0"
          onClick={() => { const cy = cyRef.current; if (cy) cy.zoom({ level: Math.max(0.1, cy.zoom() - 0.25), renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 } }) }}>
          <ZoomOut className="h-3.5 w-3.5" />
        </Button>
        <span className="text-xs font-medium w-12 text-center">{zoomLabel}</span>
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0"
          onClick={() => { const cy = cyRef.current; if (cy) cy.zoom({ level: Math.min(3, cy.zoom() + 0.25), renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 } }) }}>
          <ZoomIn className="h-3.5 w-3.5" />
        </Button>
        <div className="w-px h-4 bg-border mx-1" />
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0"
          onClick={() => cyRef.current?.fit(undefined, 50)}>
          <Maximize2 className="h-3.5 w-3.5" />
        </Button>
      </div>

      {/* Legend */}
      {relationNames.length > 0 && (
        <div className="absolute bottom-4 left-4 flex flex-wrap gap-x-3 gap-y-1.5 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm px-3 py-2 max-w-sm">
          {relationNames.map((name, i) => (
            <div key={name} className="flex items-center gap-1.5">
              <div
                className="h-2.5 w-2.5 rounded-full flex-shrink-0"
                style={{ backgroundColor: EDGE_COLORS[i % EDGE_COLORS.length] }}
              />
              <span className="text-[10px] text-muted-foreground font-mono">{name}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
