import type { Relation } from "@/lib/datalog-store"

export interface GraphNode {
  data: {
    id: string
    label: string
    degree: number
    relations: string[]
  }
}

export interface GraphEdge {
  data: {
    id: string
    source: string
    target: string
    label: string
    relation: string
  }
}

export type CytoscapeElement = GraphNode | GraphEdge

export interface GraphStats {
  nodeCount: number
  edgeCount: number
  relationCount: number
  truncated: boolean
  totalEdges: number
}

/** Maximum nodes before truncating */
export const MAX_NODES = 500
/** Maximum edges before truncating */
export const MAX_EDGES = 2000

/**
 * Transform loaded binary relations into Cytoscape elements.
 * Each binary relation (arity 2) becomes edges; column values become nodes.
 */
export function buildGraphElements(
  relations: Relation[],
  selectedRelationNames: Set<string>,
): { elements: CytoscapeElement[]; stats: GraphStats } {
  const nodeMap = new Map<string, { relations: Set<string>; degree: number }>()
  const edges: GraphEdge[] = []
  let totalEdges = 0

  const binaryRelations = relations.filter(
    (r) => r.arity === 2 && selectedRelationNames.has(r.name) && r.data.length > 0
  )

  for (const rel of binaryRelations) {
    for (const row of rel.data) {
      totalEdges++
      if (edges.length >= MAX_EDGES) continue

      const sourceVal = String(row[0] ?? "null")
      const targetVal = String(row[1] ?? "null")
      const sourceId = `n_${sourceVal}`
      const targetId = `n_${targetVal}`

      if (!nodeMap.has(sourceId)) {
        nodeMap.set(sourceId, { relations: new Set(), degree: 0 })
      }
      const sourceEntry = nodeMap.get(sourceId)!
      sourceEntry.relations.add(rel.name)
      sourceEntry.degree++

      if (!nodeMap.has(targetId)) {
        nodeMap.set(targetId, { relations: new Set(), degree: 0 })
      }
      const targetEntry = nodeMap.get(targetId)!
      targetEntry.relations.add(rel.name)
      targetEntry.degree++

      edges.push({
        data: {
          id: `e_${rel.name}_${edges.length}`,
          source: sourceId,
          target: targetId,
          label: rel.name,
          relation: rel.name,
        },
      })
    }
  }

  const truncated = nodeMap.size > MAX_NODES || totalEdges > MAX_EDGES
  let nodeEntries = Array.from(nodeMap.entries())

  if (nodeEntries.length > MAX_NODES) {
    nodeEntries.sort((a, b) => b[1].degree - a[1].degree)
    nodeEntries = nodeEntries.slice(0, MAX_NODES)
  }

  const keptNodeIds = new Set(nodeEntries.map(([id]) => id))

  const nodes: GraphNode[] = nodeEntries.map(([id, entry]) => ({
    data: {
      id,
      label: id.slice(2), // Remove "n_" prefix
      degree: entry.degree,
      relations: Array.from(entry.relations),
    },
  }))

  const keptEdges = edges.filter(
    (e) => keptNodeIds.has(e.data.source) && keptNodeIds.has(e.data.target)
  )

  return {
    elements: [...nodes, ...keptEdges],
    stats: {
      nodeCount: nodes.length,
      edgeCount: keptEdges.length,
      relationCount: binaryRelations.length,
      truncated,
      totalEdges,
    },
  }
}

/** Raw hex colors for Cytoscape (cannot parse CSS custom properties) */
export const EDGE_COLORS = [
  "#2dd4bf", // teal / aquamarine
  "#d946ef", // fuchsia / magenta
  "#5eead4", // teal-300
  "#e879f9", // fuchsia-400
  "#14b8a6", // teal-500
  "#a855f7", // violet
  "#99f6e4", // teal-200
  "#f0abfc", // fuchsia-300
]

export function getRelationColor(relationName: string, allNames: string[]): string {
  const index = allNames.indexOf(relationName)
  return EDGE_COLORS[index % EDGE_COLORS.length]
}
