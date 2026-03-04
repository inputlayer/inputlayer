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
export const MAX_NODES = 1000
/** Maximum edges before truncating */
export const MAX_EDGES = 5000

/**
 * Transform loaded binary relations into Cytoscape elements.
 * Each binary relation (arity 2) becomes edges; column values become nodes.
 */
export function buildGraphElements(
  relations: Relation[],
  selectedRelationNames: Set<string>,
  grouped = false,
): { elements: CytoscapeElement[]; stats: GraphStats } {
  const nodeMap = new Map<string, { relations: Set<string>; degree: number }>()
  const edges: GraphEdge[] = []
  let totalEdges = 0

  const graphRelations = relations.filter(
    (r) => r.arity >= 1 && selectedRelationNames.has(r.name) && r.data.length > 0
  )

  for (const rel of graphRelations) {
    const arity = rel.arity

    if (arity === 1) {
      // Unary: nodes only
      for (const row of rel.data) {
        const val = String(row[0] ?? "null")
        const id = `n_${val}`
        if (!nodeMap.has(id)) {
          nodeMap.set(id, { relations: new Set(), degree: 0 })
        }
        nodeMap.get(id)!.relations.add(rel.name)
      }
      continue
    }

    // Arity 2+: source → target with optional edge label
    for (const row of rel.data) {
      totalEdges++
      if (edges.length >= MAX_EDGES) continue

      const sourceVal = String(row[0] ?? "null")
      const targetVal = String(row[arity === 2 ? 1 : arity - 1] ?? "null")
      const sourceId = `n_${sourceVal}`
      const targetId = `n_${targetVal}`

      // Edge label from middle columns for arity 3+
      let edgeLabel = rel.name
      if (arity === 3) {
        edgeLabel = String(row[1] ?? rel.name)
      } else if (arity > 3) {
        edgeLabel = row.slice(1, arity - 1).map((v) => String(v ?? "")).join(", ")
      }

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
          label: edgeLabel,
          relation: arity >= 3 ? edgeLabel : rel.name,
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

  const activeRelNames = Array.from(new Set(graphRelations.map((r) => r.name)))
  const useGrouping = grouped && activeRelNames.length > 1

  const nodes: GraphNode[] = nodeEntries.map(([id, entry]) => ({
    data: {
      id,
      label: id.slice(2), // Remove "n_" prefix
      degree: entry.degree,
      relations: Array.from(entry.relations),
      primaryRelation: Array.from(entry.relations)[0] || "",
      ...(useGrouping ? { parent: `group_${Array.from(entry.relations)[0]}` } : {}),
    },
  }))

  // Create parent (compound) nodes for each relation when grouped
  const parentNodes: GraphNode[] = useGrouping
    ? activeRelNames.map((name) => ({
        data: { id: `group_${name}`, label: name, degree: 0, relations: [name], primaryRelation: name },
      }))
    : []

  const keptEdges = edges.filter(
    (e) => keptNodeIds.has(e.data.source) && keptNodeIds.has(e.data.target)
  )

  return {
    elements: [...parentNodes, ...nodes, ...keptEdges],
    stats: {
      nodeCount: nodes.length,
      edgeCount: keptEdges.length,
      relationCount: graphRelations.length,
      truncated,
      totalEdges,
    },
  }
}

/**
 * Transform query/view result data of any arity into Cytoscape elements.
 *
 * - Arity 1: nodes only (set of values)
 * - Arity 2: col[0] → col[1]
 * - Arity 3: col[0] → col[2], col[1] as edge label (subject, predicate, object)
 * - Arity 4+: col[0] → col[last], middle columns joined as edge label
 */
export function buildQueryGraphElements(
  data: (string | number | boolean | null)[][],
  columns: string[],
  name?: string,
): { elements: CytoscapeElement[]; stats: GraphStats; relationNames: string[] } {
  const arity = columns.length
  const nodeMap = new Map<string, { degree: number }>()
  const edges: GraphEdge[] = []
  let totalEdges = 0

  if (arity === 1) {
    // Arity 1: nodes only
    for (const row of data) {
      const val = String(row[0] ?? "null")
      const id = `n_${val}`
      if (!nodeMap.has(id)) nodeMap.set(id, { degree: 0 })
    }
  } else {
    // Arity 2+: source → target with optional edge label
    for (const row of data) {
      totalEdges++
      if (edges.length >= MAX_EDGES) continue

      const sourceVal = String(row[0] ?? "null")
      const targetVal = String(row[arity === 2 ? 1 : arity - 1] ?? "null")
      const sourceId = `n_${sourceVal}`
      const targetId = `n_${targetVal}`

      // Edge label from middle columns (arity 3+)
      let edgeLabel = ""
      if (arity === 3) {
        edgeLabel = String(row[1] ?? "")
      } else if (arity > 3) {
        edgeLabel = row.slice(1, arity - 1).map((v) => String(v ?? "")).join(", ")
      }

      if (!nodeMap.has(sourceId)) nodeMap.set(sourceId, { degree: 0 })
      nodeMap.get(sourceId)!.degree++

      if (!nodeMap.has(targetId)) nodeMap.set(targetId, { degree: 0 })
      nodeMap.get(targetId)!.degree++

      edges.push({
        data: {
          id: `e_${edges.length}`,
          source: sourceId,
          target: targetId,
          label: edgeLabel,
          relation: edgeLabel || name || "result",
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
      label: id.slice(2),
      degree: entry.degree,
      relations: [name || "result"],
    },
  }))

  const keptEdges = edges.filter(
    (e) => keptNodeIds.has(e.data.source) && keptNodeIds.has(e.data.target)
  )

  // Collect unique edge labels for legend (arity 3+)
  const uniqueRelations = new Set(keptEdges.map((e) => e.data.relation))

  return {
    elements: [...nodes, ...keptEdges],
    stats: {
      nodeCount: nodes.length,
      edgeCount: keptEdges.length,
      relationCount: uniqueRelations.size,
      truncated,
      totalEdges,
    },
    relationNames: arity >= 3 ? Array.from(uniqueRelations) : [],
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

/** Node colors for relation clustering (aquamarine → violet → magenta gradient) */
export const NODE_COLORS = [
  "#2dd4bf", // aquamarine / teal
  "#14b8a6", // teal-500
  "#0d9488", // teal-600
  "#0891b2", // cyan-600
  "#6366f1", // indigo
  "#8b5cf6", // violet
  "#a855f7", // purple
  "#c026d3", // fuchsia-600
  "#d946ef", // magenta / fuchsia
  "#e879f9", // fuchsia-400
]

export function getRelationColor(relationName: string, allNames: string[]): string {
  const index = allNames.indexOf(relationName)
  return EDGE_COLORS[index % EDGE_COLORS.length]
}
