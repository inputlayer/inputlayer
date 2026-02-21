import type { WsResultMessage } from "./ws-types"

// ── Types used by store ─────────────────────────────────────────────────────

export interface ParsedKnowledgeGraph {
  name: string
  isCurrent: boolean
}

export interface ParsedRelation {
  name: string
  arity: number
  columns: string[]
  tupleCount: number
}

export interface ParsedRule {
  name: string
  clauseCount: number
}

// ── Parsers ─────────────────────────────────────────────────────────────────

/**
 * Parse `.kg list` result.
 * Rows are message strings: "Knowledge Graphs:", "  default", "  mykg *"
 * The " *" suffix marks the current KG.
 */
export function parseKgList(result: WsResultMessage): ParsedKnowledgeGraph[] {
  const kgs: ParsedKnowledgeGraph[] = []
  for (const row of result.rows) {
    const msg = String(row[0]).trim()
    // Skip header and empty lines
    if (!msg || msg === "Knowledge Graphs:" || msg === "No knowledge graphs found.") continue
    const isCurrent = msg.endsWith(" *")
    const name = isCurrent ? msg.slice(0, -2).trim() : msg
    if (name) kgs.push({ name, isCurrent })
  }
  return kgs
}

/**
 * Parse `.rel` result.
 * New format: "  name (arity: N, columns: [col1, col2], tuples: M)"
 * Legacy format: "  name (arity: N)"
 */
export function parseRelList(result: WsResultMessage): ParsedRelation[] {
  const rels: ParsedRelation[] = []
  for (const row of result.rows) {
    const msg = String(row[0]).trim()
    if (!msg || msg === "Relations:" || msg === "No relations in current knowledge graph.") continue
    // New format: "name (arity: N, columns: [col1, col2, ...], tuples: M)"
    const fullMatch = msg.match(/^(\S+)\s+\(arity:\s*(\d+),\s*columns:\s*\[([^\]]*)\],\s*tuples:\s*(\d+)\)/)
    if (fullMatch) {
      const columns = fullMatch[3] ? fullMatch[3].split(",").map((c) => c.trim()).filter(Boolean) : []
      rels.push({
        name: fullMatch[1],
        arity: parseInt(fullMatch[2], 10),
        columns,
        tupleCount: parseInt(fullMatch[4], 10),
      })
      continue
    }
    // Legacy format: "name (arity: N)"
    const legacyMatch = msg.match(/^(\S+)\s+\(arity:\s*(\d+)\)/)
    if (legacyMatch) {
      rels.push({ name: legacyMatch[1], arity: parseInt(legacyMatch[2], 10), columns: [], tupleCount: 0 })
    }
  }
  return rels
}

/**
 * Parse `.rule list` result.
 * Rows are message strings: "Rules:", "  path (2 clause(s))", ...
 */
export function parseRuleList(result: WsResultMessage): ParsedRule[] {
  const rules: ParsedRule[] = []
  for (const row of result.rows) {
    const msg = String(row[0]).trim()
    if (!msg || msg === "Rules:" || msg === "No rules defined.") continue
    // Match pattern: "name (N clause(s))"
    const match = msg.match(/^(\S+)\s+\((\d+)\s+clause/)
    if (match) {
      rules.push({ name: match[1], clauseCount: parseInt(match[2], 10) })
    }
  }
  return rules
}

/**
 * Parse `.rule show <name>` result.
 * Rows are message strings containing the rule definition text.
 */
export function parseRuleDefinition(result: WsResultMessage): string {
  return result.rows
    .map((row) => String(row[0]))
    .filter((msg) => !msg.startsWith("Rule '") || !msg.endsWith("' not found."))
    .join("\n")
}

/**
 * Generate Datalog variable names for a given arity.
 * arity 1 → ["A"], arity 3 → ["A", "B", "C"], arity 27 → ["A", "B", ..., "Z", "A1"]
 */
export function generateVariables(arity: number): string[] {
  const vars: string[] = []
  for (let i = 0; i < arity; i++) {
    const letter = String.fromCharCode(65 + (i % 26))
    const suffix = i < 26 ? "" : String(Math.floor(i / 26))
    vars.push(letter + suffix)
  }
  return vars
}
