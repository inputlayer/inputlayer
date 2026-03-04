"use client"

import { useRef, useState, useEffect, useCallback, useMemo } from "react"
import dynamic from "next/dynamic"
import { useTheme } from "next-themes"
import { ZoomIn, ZoomOut, Maximize2, Minimize2, LayoutGrid, Share2, Focus, Search, X, Download, ImageDown, FileCode2, Tag, Tags, Boxes } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select"
import { GraphNodeDetail, type NodeDetailData } from "@/components/graph-node-detail"
import type { CytoscapeElement, GraphStats } from "@/lib/graph-utils"
import { EDGE_COLORS, NODE_COLORS } from "@/lib/graph-utils"
import type cytoscape from "cytoscape"

const CytoscapeComponent = dynamic(() => import("react-cytoscapejs"), { ssr: false })

type LayoutName = "cola" | "fcose" | "dagre" | "elk" | "cose" | "cose-bilkent" | "euler" | "spread" | "d3-force" | "avsdf" | "circle" | "concentric" | "breadthfirst" | "grid"

const LAYOUT_OPTIONS: { value: LayoutName; label: string }[] = [
  { value: "cola", label: "Cola (Force)" },
  { value: "fcose", label: "fCoSE (Fast)" },
  { value: "cose-bilkent", label: "CoSE Bilkent" },
  { value: "euler", label: "Euler (Spring)" },
  { value: "spread", label: "Spread" },
  { value: "d3-force", label: "D3 Force" },
  { value: "dagre", label: "Dagre (Layered)" },
  { value: "elk", label: "ELK (Hierarchical)" },
  { value: "cose", label: "CoSE (Physics)" },
  { value: "avsdf", label: "AVSDF (Circular)" },
  { value: "circle", label: "Circle" },
  { value: "concentric", label: "Concentric" },
  { value: "breadthfirst", label: "Breadthfirst" },
  { value: "grid", label: "Grid" },
]

/** Layouts that support Cytoscape compound (parent) nodes */
const COMPOUND_LAYOUTS: Set<LayoutName> = new Set(["cola", "fcose", "cose-bilkent", "elk", "dagre"])

interface GraphCanvasProps {
  elements: CytoscapeElement[]
  stats: GraphStats
  relationNames: string[]
  grouped?: boolean
  onGroupedChange?: (grouped: boolean) => void
  onFilterRelation?: (relation: string) => void
}

export function GraphCanvas({ elements, stats, relationNames, grouped = false, onGroupedChange, onFilterRelation }: GraphCanvasProps) {
  const cyRef = useRef<cytoscape.Core | null>(null)
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const [layout, setLayout] = useState<LayoutName>("cola")
  const [selectedNode, setSelectedNode] = useState<NodeDetailData | null>(null)
  const [zoomLabel, setZoomLabel] = useState("100%")
  const [fadeEnabled, setFadeEnabled] = useState(true)
  const fadeEnabledRef = useRef(true)
  const [showEdgeLabels, setShowEdgeLabels] = useState(true)
  const [searchOpen, setSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState("")
  const [searchMatches, setSearchMatches] = useState<number>(0)
  const [searchIndex, setSearchIndex] = useState(0)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  // Auto-switch to compound-compatible layout when grouping is enabled
  useEffect(() => {
    if (grouped && !COMPOUND_LAYOUTS.has(layout)) {
      setLayout("cola")
    }
  }, [grouped, layout])

  useEffect(() => { setMounted(true) }, [])

  const isDark = resolvedTheme === "dark"

  const handleFadeToggle = useCallback((enabled: boolean) => {
    setFadeEnabled(enabled)
    fadeEnabledRef.current = enabled
    if (!enabled && cyRef.current) {
      cyRef.current.batch(() => {
        cyRef.current!.elements().removeStyle("opacity")
      })
    }
  }, [])

  // Node search
  const handleSearch = useCallback((query: string) => {
    setSearchQuery(query)
    const cy = cyRef.current
    if (!cy) return
    cy.elements().removeClass("search-match search-dim")
    if (!query.trim()) {
      setSearchMatches(0)
      setSearchIndex(0)
      return
    }
    const q = query.toLowerCase()
    const matches = cy.nodes().filter((n) => n.data("label")?.toLowerCase().includes(q))
    setSearchMatches(matches.length)
    setSearchIndex(0)
    if (matches.length > 0) {
      cy.elements().addClass("search-dim")
      matches.removeClass("search-dim").addClass("search-match")
      cy.animate({ fit: { eles: matches, padding: 60 } }, { duration: 400 })
    }
  }, [])

  const handleSearchNav = useCallback((direction: 1 | -1) => {
    const cy = cyRef.current
    if (!cy || searchMatches === 0) return
    const matches = cy.nodes(".search-match")
    const next = (searchIndex + direction + matches.length) % matches.length
    setSearchIndex(next)
    const node = matches[next]
    cy.animate({ center: { eles: node }, zoom: Math.max(cy.zoom(), 1.5) }, { duration: 300 })
  }, [searchMatches, searchIndex])

  const closeSearch = useCallback(() => {
    setSearchOpen(false)
    setSearchQuery("")
    setSearchMatches(0)
    setSearchIndex(0)
    cyRef.current?.elements().removeClass("search-match search-dim")
  }, [])

  // Export
  const handleExportPng = useCallback(() => {
    const cy = cyRef.current
    if (!cy) return
    const png = cy.png({ output: "blob", scale: 2, bg: isDark ? "#0a0a0a" : "#fafafa" })
    const url = URL.createObjectURL(png as Blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "graph.png"
    a.click()
    URL.revokeObjectURL(url)
  }, [isDark])

  const handleExportSvg = useCallback(() => {
    const cy = cyRef.current
    if (!cy) return
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const svg = (cy as any).svg({ scale: 1, full: true, bg: isDark ? "#0a0a0a" : "#fafafa" })
    const blob = new Blob([svg], { type: "image/svg+xml" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "graph.svg"
    a.click()
    URL.revokeObjectURL(url)
  }, [isDark])

  // Fullscreen
  const toggleFullscreen = useCallback(() => {
    const el = containerRef.current
    if (!el) return
    if (!document.fullscreenElement) {
      el.requestFullscreen().then(() => setIsFullscreen(true)).catch(() => {})
    } else {
      document.exitFullscreen().then(() => setIsFullscreen(false)).catch(() => {})
    }
  }, [])

  useEffect(() => {
    const handler = () => setIsFullscreen(!!document.fullscreenElement)
    document.addEventListener("fullscreenchange", handler)
    return () => document.removeEventListener("fullscreenchange", handler)
  }, [])

  // Keyboard shortcuts for graph
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Only handle when this component is visible
      if (!containerRef.current || !containerRef.current.offsetParent) return
      if ((e.metaKey || e.ctrlKey) && e.key === "f") {
        e.preventDefault()
        setSearchOpen(true)
        setTimeout(() => searchInputRef.current?.focus(), 50)
      }
      if (e.key === "Escape" && searchOpen) {
        closeSearch()
      }
    }
    document.addEventListener("keydown", handler)
    return () => document.removeEventListener("keydown", handler)
  }, [searchOpen, closeSearch])

  // Register layout plugins (once, client-side only)
  useEffect(() => {
    if (typeof window === "undefined") return
    import("cytoscape").then((Cytoscape) => {
      const register = (plugin: { default: cytoscape.Ext }) => {
        try { Cytoscape.default.use(plugin.default) } catch { /* already registered */ }
      }
      import("cytoscape-cola").then(register)
      import("cytoscape-fcose" as string).then(register)
      import("cytoscape-dagre" as string).then(register)
      import("cytoscape-elk" as string).then(register)
      import("cytoscape-cose-bilkent" as string).then(register)
      import("cytoscape-euler" as string).then(register)
      import("cytoscape-spread" as string).then(register)
      import("cytoscape-d3-force" as string).then(register)
      import("cytoscape-avsdf" as string).then(register)
      import("cytoscape-svg" as string).then(register)
    })
  }, [])

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stylesheet: any[] = useMemo(() => [
    {
      selector: "node",
      style: {
        label: "data(label)",
        "background-color": isDark ? "#2dd4bf" : "#14b8a6",
        color: isDark ? "#e5e7eb" : "#1f2937",
        "font-size": "10px",
        "font-family": "var(--font-mono)",
        "text-valign": "bottom" as const,
        "text-margin-y": 6,
        "text-background-color": isDark ? "#1a1a2e" : "#ffffff",
        "text-background-opacity": 0.85,
        "text-background-padding": "3px" as unknown as number,
        "text-background-shape": "roundrectangle" as const,
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
        label: showEdgeLabels && elements.length < 500 ? "data(label)" : "",
        "font-size": "8px",
        "text-rotation": "autorotate" as const,
        color: isDark ? "#9ca3af" : "#6b7280",
        "text-background-color": isDark ? "#0a0a0a" : "#ffffff",
        "text-background-opacity": 0.8,
        "text-background-padding": "2px" as unknown as number,
      },
    },
    {
      selector: ".search-match",
      style: {
        "background-color": "#facc15",
        "border-color": "#eab308",
        "border-width": 3,
        "z-index": 999,
      },
    },
    {
      selector: ".search-dim",
      style: {
        opacity: 0.15,
      },
    },
    ...relationNames.map((name, i) => ({
      selector: `edge[relation = "${name}"]`,
      style: {
        "line-color": EDGE_COLORS[i % EDGE_COLORS.length],
        "target-arrow-color": EDGE_COLORS[i % EDGE_COLORS.length],
      },
    })),
    // Node clustering: color nodes by primary relation when multiple relations are active
    ...(relationNames.length > 1 ? relationNames.map((name, i) => ({
      selector: `node[primaryRelation = "${name}"]`,
      style: {
        "background-color": NODE_COLORS[i % NODE_COLORS.length],
        "border-color": NODE_COLORS[i % NODE_COLORS.length],
      },
    })) : []),
    // Compound parent nodes (visible when grouped)
    ...(grouped && relationNames.length > 1 ? [
      {
        selector: ":parent",
        style: {
          "background-opacity": 0.06,
          "background-color": isDark ? "#2dd4bf" : "#14b8a6",
          "border-width": 1.5,
          "border-style": "dashed" as const,
          "border-color": isDark ? "#374151" : "#d1d5db",
          "border-opacity": 0.6,
          shape: "roundrectangle" as const,
          "text-valign": "top" as const,
          "text-halign": "center" as const,
          "text-margin-y": -4,
          label: "data(label)",
          "font-size": "11px",
          "font-family": "var(--font-mono)",
          "font-weight": "bold" as const,
          color: isDark ? "#9ca3af" : "#6b7280",
          padding: "24px" as unknown as number,
        },
      },
      ...relationNames.map((name, i) => ({
        selector: `node#group_${name}`,
        style: {
          "background-color": NODE_COLORS[i % NODE_COLORS.length],
          "border-color": NODE_COLORS[i % NODE_COLORS.length],
        },
      })),
    ] : []),
  ], [isDark, relationNames, elements.length, showEdgeLabels, grouped])

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
      fcose: {
        name: "fcose",
        animate: true,
        animationDuration: 800,
        quality: "default",
        nodeRepulsion: () => 8000,
        idealEdgeLength: () => 120,
        nodeSeparation: 75,
        randomize: true,
      },
      dagre: {
        name: "dagre",
        animate: true,
        rankDir: "TB",
        nodeSep: 50,
        rankSep: 80,
        edgeSep: 20,
      },
      elk: {
        name: "elk",
        animate: true,
        elk: {
          algorithm: "layered",
          "elk.direction": "DOWN",
          "elk.spacing.nodeNode": "50",
          "elk.layered.spacing.nodeNodeBetweenLayers": "80",
        },
      },
      "cose-bilkent": {
        name: "cose-bilkent",
        animate: "end",
        animationDuration: 800,
        nodeRepulsion: 8000,
        idealEdgeLength: 120,
        edgeElasticity: 0.45,
        nestingFactor: 0.1,
        gravity: 0.25,
        numIter: 2500,
        randomize: true,
      },
      euler: {
        name: "euler",
        animate: true,
        animationDuration: 1000,
        springLength: 120,
        springCoeff: 0.0008,
        gravity: -1.2,
        randomize: true,
      },
      spread: {
        name: "spread",
        animate: true,
        minDist: 40,
      },
      "d3-force": {
        name: "d3-force",
        animate: true,
        fixedAfterDragging: true,
        linkId: (d: { id: string }) => d.id,
        linkDistance: 120,
        manyBodyStrength: -300,
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
      avsdf: {
        name: "avsdf",
        animate: true,
        nodeSeparation: 80,
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

      // Ignore compound parent nodes
      if (node.isParent()) return

      // Fade unrelated elements using direct style (faster than classes)
      if (fadeEnabledRef.current) {
        const neighborhood = node.closedNeighborhood()
        cy.batch(() => {
          cy.elements().not(neighborhood).style("opacity", 0.12)
          neighborhood.style("opacity", 1)
        })
      }

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
      if (evt.target === cy) {
        cy.batch(() => {
          cy.elements().removeStyle("opacity")
        })
        setSelectedNode(null)
      }
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
    <div ref={containerRef} className="relative h-full w-full">
      <CytoscapeComponent
        key={`cy-${grouped && relationNames.length > 1}`}
        elements={elements as unknown as cytoscape.ElementDefinition[]}
        stylesheet={stylesheet}
        layout={layoutConfig}
        cy={handleCy}
        style={{ width: "100%", height: "100%" }}
        className={isDark ? "bg-[#0a0a0a]" : "bg-[#fafafa]"}
      />

      <GraphNodeDetail
        node={selectedNode}
        onClose={() => {
          if (cyRef.current) {
            cyRef.current.batch(() => {
              cyRef.current!.elements().removeStyle("opacity")
            })
          }
          setSelectedNode(null)
        }}
        onHoverRelation={(rel) => {
          const cy = cyRef.current
          if (!cy || !selectedNode) return
          if (!rel) {
            if (fadeEnabledRef.current) {
              const node = cy.getElementById(selectedNode.id)
              const neighborhood = node.closedNeighborhood()
              cy.batch(() => {
                cy.elements().not(neighborhood).style("opacity", 0.12)
                neighborhood.style("opacity", 1)
              })
            } else {
              cy.batch(() => { cy.elements().removeStyle("opacity") })
            }
            return
          }
          const node = cy.getElementById(selectedNode.id)
          const relEdges = node.connectedEdges(`[relation = "${rel}"]`)
          const relNodes = relEdges.connectedNodes()
          cy.batch(() => {
            cy.elements().style("opacity", 0.08)
            node.style("opacity", 1)
            relEdges.style("opacity", 1)
            relNodes.style("opacity", 1)
          })
        }}
        onClickRelation={onFilterRelation ? (rel) => {
          onFilterRelation(rel)
        } : undefined}
      />

      {/* Top controls */}
      <div className="absolute top-4 left-4 z-10 flex items-center gap-2">
        <Select value={layout} onValueChange={(v) => setLayout(v as LayoutName)}>
          <SelectTrigger size="sm" className="h-8 w-40 bg-background/90 backdrop-blur-sm border-border/50 hover:bg-teal-500/10">
            <LayoutGrid className="h-3.5 w-3.5 mr-1.5" />
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LAYOUT_OPTIONS
              .filter((opt) => !grouped || COMPOUND_LAYOUTS.has(opt.value))
              .map((opt) => (
                <SelectItem key={opt.value} value={opt.value} className="focus:bg-teal-500/10 focus:text-teal-600 dark:focus:text-teal-400">{opt.label}</SelectItem>
              ))}
          </SelectContent>
        </Select>
        <label className="flex items-center gap-1.5 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm px-2.5 h-8 cursor-pointer hover:bg-teal-500/10">
          <Focus className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-[10px] text-muted-foreground">Focus</span>
          <Switch checked={fadeEnabled} onCheckedChange={handleFadeToggle} className="scale-75" />
        </label>
        <Button
          variant="ghost"
          size="sm"
          className="h-8 w-8 p-0 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg hover:bg-teal-500/10 hover:text-teal-600 dark:hover:text-teal-400"
          onClick={() => setShowEdgeLabels((v) => !v)}
          title={showEdgeLabels ? "Hide edge labels" : "Show edge labels"}
        >
          {showEdgeLabels ? <Tag className="h-3.5 w-3.5" /> : <Tags className="h-3.5 w-3.5 text-muted-foreground line-through" />}
        </Button>
        {onGroupedChange && relationNames.length > 1 && (
          <Button
            variant="ghost"
            size="sm"
            className={`h-8 w-8 p-0 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg hover:bg-teal-500/10 hover:text-teal-600 dark:hover:text-teal-400 ${grouped ? "text-teal-500" : ""}`}
            onClick={() => onGroupedChange(!grouped)}
            title={grouped ? "Ungroup nodes" : "Group nodes by relation"}
          >
            <Boxes className="h-3.5 w-3.5" />
          </Button>
        )}
        <Button
          variant="ghost"
          size="sm"
          className="h-8 w-8 p-0 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg hover:bg-teal-500/10 hover:text-teal-600 dark:hover:text-teal-400"
          onClick={() => { setSearchOpen(true); setTimeout(() => searchInputRef.current?.focus(), 50) }}
          title="Search nodes (Ctrl+F)"
        >
          <Search className="h-3.5 w-3.5" />
        </Button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg hover:bg-teal-500/10 hover:text-teal-600 dark:hover:text-teal-400"
              title="Export graph"
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            <DropdownMenuItem onClick={handleExportPng} className="focus:bg-teal-500/10 focus:text-teal-600 dark:focus:text-teal-400">
              <ImageDown className="h-3.5 w-3.5 mr-2" />
              Export as PNG
            </DropdownMenuItem>
            <DropdownMenuItem onClick={handleExportSvg} className="focus:bg-teal-500/10 focus:text-teal-600 dark:focus:text-teal-400">
              <FileCode2 className="h-3.5 w-3.5 mr-2" />
              Export as SVG
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
        <Button
          variant="ghost"
          size="sm"
          className="h-8 w-8 p-0 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg hover:bg-teal-500/10 hover:text-teal-600 dark:hover:text-teal-400"
          onClick={toggleFullscreen}
          title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
        >
          {isFullscreen ? <Minimize2 className="h-3.5 w-3.5" /> : <Maximize2 className="h-3.5 w-3.5" />}
        </Button>
      </div>

      {/* Search bar */}
      {searchOpen && (
        <div className="absolute top-14 left-4 z-10 flex items-center gap-1.5 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm px-2 py-1.5">
          <Search className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
          <Input
            ref={searchInputRef}
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handleSearchNav(e.shiftKey ? -1 : 1)
              if (e.key === "Escape") closeSearch()
            }}
            placeholder="Search nodes..."
            className="h-6 w-44 border-0 bg-transparent px-1 text-xs focus-visible:ring-0"
          />
          {searchQuery && (
            <span className="text-[10px] text-muted-foreground whitespace-nowrap">
              {searchMatches > 0 ? `${searchIndex + 1}/${searchMatches}` : "0 found"}
            </span>
          )}
          <Button variant="ghost" size="sm" className="h-5 w-5 p-0" onClick={closeSearch}>
            <X className="h-3 w-3" />
          </Button>
        </div>
      )}

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
        <div className="absolute bottom-4 left-4 rounded-lg border border-border/50 bg-background/90 backdrop-blur-sm px-3 py-2 max-w-sm">
          {relationNames.length > 1 && (
            <div className="flex flex-wrap gap-x-3 gap-y-1.5 mb-1.5 pb-1.5 border-b border-border/30">
              {relationNames.map((name, i) => (
                <div key={`node-${name}`} className="flex items-center gap-1.5">
                  <div
                    className="h-2.5 w-2.5 rounded-full flex-shrink-0"
                    style={{ backgroundColor: NODE_COLORS[i % NODE_COLORS.length] }}
                  />
                  <span className="text-[10px] text-muted-foreground font-mono">{name}</span>
                </div>
              ))}
            </div>
          )}
          <div className="flex flex-wrap gap-x-3 gap-y-1.5">
            {relationNames.map((name, i) => (
              <div key={`edge-${name}`} className="flex items-center gap-1.5">
                <div
                  className="h-0.5 w-3 rounded flex-shrink-0"
                  style={{ backgroundColor: EDGE_COLORS[i % EDGE_COLORS.length] }}
                />
                <span className="text-[10px] text-muted-foreground font-mono">{name}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
