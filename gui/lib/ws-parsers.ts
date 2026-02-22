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
  columnTypes: string[]
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
    // New format with types: "name (arity: N, columns: [col1: type1, col2: type2], tuples: M)"
    // Also handles legacy without types: "name (arity: N, columns: [col1, col2], tuples: M)"
    const fullMatch = msg.match(/^(\S+)\s+\(arity:\s*(\d+),\s*columns:\s*\[([^\]]*)\],\s*tuples:\s*(\d+)\)/)
    if (fullMatch) {
      const rawEntries = fullMatch[3] ? fullMatch[3].split(",").map(e => e.trim()) : []
      const columns: string[] = []
      const columnTypes: string[] = []
      for (let i = 0; i < rawEntries.length; i++) {
        const entry = rawEntries[i]
        const colonIdx = entry.indexOf(":")
        if (colonIdx !== -1) {
          columns.push(entry.substring(0, colonIdx).trim() || `col${i}`)
          columnTypes.push(entry.substring(colonIdx + 1).trim() || "any")
        } else {
          columns.push(entry || `col${i}`)
          columnTypes.push("any")
        }
      }
      rels.push({
        name: fullMatch[1],
        arity: parseInt(fullMatch[2], 10),
        columns,
        columnTypes,
        tupleCount: parseInt(fullMatch[4], 10),
      })
      continue
    }
    // Legacy format: "name (arity: N)"
    const legacyMatch = msg.match(/^(\S+)\s+\(arity:\s*(\d+)\)/)
    if (legacyMatch) {
      rels.push({ name: legacyMatch[1], arity: parseInt(legacyMatch[2], 10), columns: [], columnTypes: [], tupleCount: 0 })
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
 * Parse `.rule def <name>` result.
 * Rows are message strings containing the rule definition text.
 */
export function parseRuleDefinition(result: WsResultMessage): string {
  return result.rows
    .map((row) => String(row[0]))
    .filter((msg) => !(msg.startsWith("Rule '") && msg.endsWith("' not found.")))
    .join("\n")
}

// Built-in function names to exclude from dependency extraction.
// These match word(...) patterns in rule bodies but are not relation references.
const BUILTIN_FUNCTION_NAMES = new Set([
  // Distance functions
  "euclidean", "cosine", "dot", "manhattan",
  // Vector operations
  "normalize", "vec_dim", "vec_add", "vec_scale",
  // LSH functions
  "lsh_bucket", "lsh_probes", "lsh_multi_probe",
  // Temporal functions
  "time_now", "time_diff", "time_add", "time_sub", "time_decay", "time_decay_linear",
  "time_before", "time_after", "time_between", "within_last",
  "intervals_overlap", "interval_contains", "interval_duration", "point_in_interval",
  // Quantization
  "quantize_linear", "quantize_symmetric", "dequantize", "dequantize_scaled",
  // Int8 distance
  "euclidean_int8", "cosine_int8", "dot_int8", "manhattan_int8",
  // Math
  "abs", "abs_int64", "abs_float64", "sqrt", "pow", "log", "exp",
  "sin", "cos", "tan", "floor", "ceil", "sign",
  // Type conversion
  "to_float", "to_int",
  // String functions
  "len", "upper", "lower", "trim", "substr", "replace", "concat",
  // Scalar min/max
  "min_val", "max_val",
  // HNSW
  "hnsw_nearest",
])

/**
 * Extract dependency relation names from a rule definition string.
 * Parses lines like "  1. path(X, Y) <- edge(X, Y)." to extract body relation names.
 * Includes the rule's own name if self-referential (recursive).
 * Excludes built-in function names.
 */
export function parseDependenciesFromDefinition(definition: string, ruleName: string): string[] {
  const deps = new Set<string>()
  for (const line of definition.split("\n")) {
    // Match clause lines: "  1. head(...) <- body1(...), body2(...), ..."
    const arrowIdx = line.indexOf("<-")
    if (arrowIdx === -1) continue
    const body = line.substring(arrowIdx + 2)
    // Extract relation names: word chars immediately before "("
    const matches = body.matchAll(/(\w+)\s*\(/g)
    for (const m of matches) {
      const name = m[1]
      if (!BUILTIN_FUNCTION_NAMES.has(name)) {
        deps.add(name)
      }
    }
  }
  return Array.from(deps)
}

/**
 * Find the index of the matching closing paren for the opening paren at `openIdx`.
 * Handles nested parentheses. Returns -1 if not found.
 */
function findMatchingParen(s: string, openIdx: number): number {
  let depth = 1
  for (let i = openIdx + 1; i < s.length; i++) {
    if (s[i] === "(") depth++
    else if (s[i] === ")") { depth--; if (depth === 0) return i }
  }
  return -1
}

/**
 * Count top-level comma-separated arguments in a string.
 * Handles nested parens: "X, abs(Y), Z" → 3, not 4.
 */
function countTopLevelArgs(s: string): number {
  const trimmed = s.trim()
  if (!trimmed) return 0
  let count = 1
  let depth = 0
  for (const ch of trimmed) {
    if (ch === "(") depth++
    else if (ch === ")") depth--
    else if (ch === "," && depth === 0) count++
  }
  return count
}

/**
 * Parse a rule definition into structured clauses.
 * Input format from `.rule def`:
 *   "Rule: path\nClauses:\n  1. path(X, Y) <- edge(X, Y).\n  2. path(X, Z) <- edge(X, Y), path(Y, Z)."
 * Returns array of { head, headArity, body } where body contains relation names referenced in the clause body.
 * Handles nested parentheses in head (e.g., result(abs(X), Y)).
 */
export function parseRuleClauses(definition: string): { head: string; headArity: number; body: string[] }[] {
  const clauses: { head: string; headArity: number; body: string[] }[] = []
  for (const line of definition.split("\n")) {
    // Match the numbered prefix and extract the head name
    const prefixMatch = line.match(/^\s*\d+\.\s*(\w+)\s*\(/)
    if (!prefixMatch) continue

    const head = prefixMatch[1]
    // Find the opening paren position after the head name
    const openParenIdx = line.indexOf("(", prefixMatch.index! + prefixMatch[0].length - 1)
    if (openParenIdx === -1) continue
    // Find matching closing paren (handles nesting)
    const closeParenIdx = findMatchingParen(line, openParenIdx)
    if (closeParenIdx === -1) continue

    const headArgs = line.substring(openParenIdx + 1, closeParenIdx)
    const headArity = countTopLevelArgs(headArgs)

    // Find `<-` after the head
    const arrowIdx = line.indexOf("<-", closeParenIdx)
    if (arrowIdx === -1) continue

    // Body is everything from after `<-` to the trailing `.`
    let bodyStr = line.substring(arrowIdx + 2).trim()
    if (bodyStr.endsWith(".")) bodyStr = bodyStr.slice(0, -1)

    const bodyRels: string[] = []
    const matches = bodyStr.matchAll(/(\w+)\s*\(/g)
    for (const m of matches) {
      if (!BUILTIN_FUNCTION_NAMES.has(m[1])) {
        bodyRels.push(m[1])
      }
    }
    clauses.push({ head, headArity, body: bodyRels })
  }
  return clauses
}

/**
 * Parse `.session` result to extract relation names referenced by session rules/facts.
 * Lines look like:
 *   "  path(X,Y) <- edge(X,Y)."   → extracts "path"
 *   "  edge(1, 2)."               → extracts "edge"
 */
export function parseSessionNames(result: WsResultMessage): string[] {
  const names = new Set<string>()
  for (const row of result.rows) {
    const msg = String(row[0]).trim()
    if (!msg || msg === "Session rules:" || msg === "Session facts:" ||
        msg === "No session rules." || msg === "No session facts." ||
        msg.startsWith("Session:")) continue
    // Extract the leading relation/rule name: word chars before "("
    const match = msg.match(/^(\w+)\s*\(/)
    if (match) {
      names.add(match[1])
    }
  }
  return Array.from(names)
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
