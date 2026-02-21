import type { Relation, View } from "./datalog-store"

export type CompletionKind = "relation" | "view" | "column" | "function" | "aggregate" | "keyword" | "meta"

export interface CompletionItem {
  label: string
  kind: CompletionKind
  detail?: string
  insertText: string
}

// Built-in functions from src/ast/mod.rs BuiltinFunc enum
const BUILTIN_FUNCTIONS: CompletionItem[] = [
  // Distance functions
  { label: "euclidean", kind: "function", detail: "euclidean(v1, v2) → float", insertText: "euclidean(v1, v2)" },
  { label: "cosine", kind: "function", detail: "cosine(v1, v2) → float", insertText: "cosine(v1, v2)" },
  { label: "dot", kind: "function", detail: "dot(v1, v2) → float", insertText: "dot(v1, v2)" },
  { label: "manhattan", kind: "function", detail: "manhattan(v1, v2) → float", insertText: "manhattan(v1, v2)" },
  // Vector operations
  { label: "normalize", kind: "function", detail: "normalize(v) → vector", insertText: "normalize(v)" },
  { label: "vec_dim", kind: "function", detail: "vec_dim(v) → int", insertText: "vec_dim(v)" },
  { label: "vec_add", kind: "function", detail: "vec_add(v1, v2) → vector", insertText: "vec_add(v1, v2)" },
  { label: "vec_scale", kind: "function", detail: "vec_scale(v, s) → vector", insertText: "vec_scale(v, s)" },
  // LSH functions
  { label: "lsh_bucket", kind: "function", detail: "lsh_bucket(v, bands, rows) → int", insertText: "lsh_bucket(v, bands, rows)" },
  { label: "lsh_probes", kind: "function", detail: "lsh_probes(v, bands, rows) → int", insertText: "lsh_probes(v, bands, rows)" },
  { label: "lsh_multi_probe", kind: "function", detail: "lsh_multi_probe(v, bands, rows, probes) → int", insertText: "lsh_multi_probe(v, bands, rows, probes)" },
  // Temporal functions
  { label: "time_now", kind: "function", detail: "time_now() → timestamp", insertText: "time_now()" },
  { label: "time_diff", kind: "function", detail: "time_diff(t1, t2) → int", insertText: "time_diff(t1, t2)" },
  { label: "time_add", kind: "function", detail: "time_add(t, ms) → timestamp", insertText: "time_add(t, ms)" },
  { label: "time_sub", kind: "function", detail: "time_sub(t, ms) → timestamp", insertText: "time_sub(t, ms)" },
  { label: "time_decay", kind: "function", detail: "time_decay(t, now, half_life) → float", insertText: "time_decay(t, now, half_life)" },
  { label: "time_decay_linear", kind: "function", detail: "time_decay_linear(t, now, window) → float", insertText: "time_decay_linear(t, now, window)" },
  { label: "time_before", kind: "function", detail: "time_before(t1, t2) → bool", insertText: "time_before(t1, t2)" },
  { label: "time_after", kind: "function", detail: "time_after(t1, t2) → bool", insertText: "time_after(t1, t2)" },
  { label: "time_between", kind: "function", detail: "time_between(t, start, end) → bool", insertText: "time_between(t, start, end)" },
  { label: "within_last", kind: "function", detail: "within_last(t, now, window) → bool", insertText: "within_last(t, now, window)" },
  { label: "intervals_overlap", kind: "function", detail: "intervals_overlap(s1, e1, s2, e2) → bool", insertText: "intervals_overlap(s1, e1, s2, e2)" },
  { label: "interval_contains", kind: "function", detail: "interval_contains(s, e, t) → bool", insertText: "interval_contains(s, e, t)" },
  { label: "interval_duration", kind: "function", detail: "interval_duration(s, e) → int", insertText: "interval_duration(s, e)" },
  { label: "point_in_interval", kind: "function", detail: "point_in_interval(t, s, e) → bool", insertText: "point_in_interval(t, s, e)" },
  // Quantization
  { label: "quantize_linear", kind: "function", detail: "quantize_linear(v, bits) → vector", insertText: "quantize_linear(v, bits)" },
  { label: "quantize_symmetric", kind: "function", detail: "quantize_symmetric(v, bits) → vector", insertText: "quantize_symmetric(v, bits)" },
  { label: "dequantize", kind: "function", detail: "dequantize(v) → vector", insertText: "dequantize(v)" },
  { label: "dequantize_scaled", kind: "function", detail: "dequantize_scaled(v, scale) → vector", insertText: "dequantize_scaled(v, scale)" },
  // Int8 distance
  { label: "euclidean_int8", kind: "function", detail: "euclidean_int8(v1, v2) → float", insertText: "euclidean_int8(v1, v2)" },
  { label: "cosine_int8", kind: "function", detail: "cosine_int8(v1, v2) → float", insertText: "cosine_int8(v1, v2)" },
  { label: "dot_int8", kind: "function", detail: "dot_int8(v1, v2) → float", insertText: "dot_int8(v1, v2)" },
  { label: "manhattan_int8", kind: "function", detail: "manhattan_int8(v1, v2) → float", insertText: "manhattan_int8(v1, v2)" },
  // Math
  { label: "abs", kind: "function", detail: "abs(x) → number", insertText: "abs(x)" },
  { label: "abs_int64", kind: "function", detail: "abs_int64(x) → int", insertText: "abs_int64(x)" },
  { label: "abs_float64", kind: "function", detail: "abs_float64(x) → float", insertText: "abs_float64(x)" },
  { label: "sqrt", kind: "function", detail: "sqrt(x) → float", insertText: "sqrt(x)" },
  { label: "pow", kind: "function", detail: "pow(base, exp) → float", insertText: "pow(base, exp)" },
  { label: "log", kind: "function", detail: "log(x) → float", insertText: "log(x)" },
  { label: "exp", kind: "function", detail: "exp(x) → float", insertText: "exp(x)" },
  { label: "sin", kind: "function", detail: "sin(x) → float", insertText: "sin(x)" },
  { label: "cos", kind: "function", detail: "cos(x) → float", insertText: "cos(x)" },
  { label: "tan", kind: "function", detail: "tan(x) → float", insertText: "tan(x)" },
  { label: "floor", kind: "function", detail: "floor(x) → int", insertText: "floor(x)" },
  { label: "ceil", kind: "function", detail: "ceil(x) → int", insertText: "ceil(x)" },
  { label: "sign", kind: "function", detail: "sign(x) → int", insertText: "sign(x)" },
  // Type conversion
  { label: "to_float", kind: "function", detail: "to_float(x) → float", insertText: "to_float(x)" },
  { label: "to_int", kind: "function", detail: "to_int(x) → int", insertText: "to_int(x)" },
  // String functions
  { label: "len", kind: "function", detail: "len(s) → int", insertText: "len(s)" },
  { label: "upper", kind: "function", detail: "upper(s) → string", insertText: "upper(s)" },
  { label: "lower", kind: "function", detail: "lower(s) → string", insertText: "lower(s)" },
  { label: "trim", kind: "function", detail: "trim(s) → string", insertText: "trim(s)" },
  { label: "substr", kind: "function", detail: "substr(s, start, len) → string", insertText: "substr(s, start, len)" },
  { label: "replace", kind: "function", detail: "replace(s, find, repl) → string", insertText: "replace(s, find, repl)" },
  { label: "concat", kind: "function", detail: "concat(s1, s2, ...) → string", insertText: "concat(s1, s2)" },
  // Scalar min/max
  { label: "min_val", kind: "function", detail: "min_val(a, b) → same type", insertText: "min_val(a, b)" },
  { label: "max_val", kind: "function", detail: "max_val(a, b) → same type", insertText: "max_val(a, b)" },
  // HNSW nearest neighbor search
  { label: "hnsw_nearest", kind: "function", detail: "hnsw_nearest(idx, vec, k, Id, Dist)", insertText: "hnsw_nearest(idx, vec, k, Id, Dist)" },
]

const AGGREGATE_FUNCTIONS: CompletionItem[] = [
  { label: "count", kind: "aggregate", detail: "count<Var>", insertText: "count<Var>" },
  { label: "count_distinct", kind: "aggregate", detail: "count_distinct<Var>", insertText: "count_distinct<Var>" },
  { label: "sum", kind: "aggregate", detail: "sum<Var>", insertText: "sum<Var>" },
  { label: "min", kind: "aggregate", detail: "min<Var>", insertText: "min<Var>" },
  { label: "max", kind: "aggregate", detail: "max<Var>", insertText: "max<Var>" },
  { label: "avg", kind: "aggregate", detail: "avg<Var>", insertText: "avg<Var>" },
  { label: "top_k", kind: "aggregate", detail: "top_k<k, Var, OrderVar:desc>", insertText: "top_k<k, Var, OrderVar:desc>" },
  { label: "top_k_threshold", kind: "aggregate", detail: "top_k_threshold<k, threshold, Var, OrderVar:desc>", insertText: "top_k_threshold<k, threshold, Var, OrderVar:desc>" },
  { label: "within_radius", kind: "aggregate", detail: "within_radius<radius, Var, DistVar>", insertText: "within_radius<radius, Var, DistVar>" },
]

const KEYWORDS: CompletionItem[] = [
  { label: "true", kind: "keyword", detail: "boolean", insertText: "true" },
  { label: "false", kind: "keyword", detail: "boolean", insertText: "false" },
  { label: "<-", kind: "keyword", detail: "rule arrow", insertText: "<-" },
]

// Meta commands (dot commands) — work via WebSocket execute
const META_COMMANDS: CompletionItem[] = [
  // Knowledge graph commands
  { label: ".kg", kind: "meta", detail: "show current knowledge graph", insertText: ".kg" },
  { label: ".kg list", kind: "meta", detail: "list all knowledge graphs", insertText: ".kg list" },
  { label: ".kg create", kind: "meta", detail: ".kg create <name>", insertText: ".kg create " },
  { label: ".kg use", kind: "meta", detail: ".kg use <name>", insertText: ".kg use " },
  { label: ".kg drop", kind: "meta", detail: ".kg drop <name>", insertText: ".kg drop " },
  // Relation commands
  { label: ".rel", kind: "meta", detail: "list all relations", insertText: ".rel" },
  // Rule commands
  { label: ".rule", kind: "meta", detail: "list all rules", insertText: ".rule" },
  { label: ".rule list", kind: "meta", detail: "list all rules", insertText: ".rule list" },
  { label: ".rule def", kind: "meta", detail: ".rule def <name> — show definition", insertText: ".rule def " },
  { label: ".rule drop", kind: "meta", detail: ".rule drop <name>", insertText: ".rule drop " },
  { label: ".rule drop prefix", kind: "meta", detail: ".rule drop prefix <p>", insertText: ".rule drop prefix " },
  { label: ".rule edit", kind: "meta", detail: ".rule edit <name> <idx> <rule>", insertText: ".rule edit " },
  { label: ".rule clear", kind: "meta", detail: ".rule clear <name>", insertText: ".rule clear " },
  { label: ".rule remove", kind: "meta", detail: ".rule remove <name> <idx>", insertText: ".rule remove " },
  // Session commands
  { label: ".session", kind: "meta", detail: "list session rules", insertText: ".session" },
  { label: ".session clear", kind: "meta", detail: "clear all session rules", insertText: ".session clear" },
  { label: ".session drop", kind: "meta", detail: ".session drop <n|name>", insertText: ".session drop " },
  // Index commands
  { label: ".index", kind: "meta", detail: "list all indexes", insertText: ".index" },
  { label: ".index list", kind: "meta", detail: "list all indexes", insertText: ".index list" },
  { label: ".index create", kind: "meta", detail: ".index create <name> on <rel>(<col>)", insertText: ".index create " },
  { label: ".index drop", kind: "meta", detail: ".index drop <name>", insertText: ".index drop " },
  { label: ".index stats", kind: "meta", detail: ".index stats <name>", insertText: ".index stats " },
  { label: ".index rebuild", kind: "meta", detail: ".index rebuild <name>", insertText: ".index rebuild " },
  // Clear commands
  { label: ".clear prefix", kind: "meta", detail: ".clear prefix <prefix>", insertText: ".clear prefix " },
  // System commands
  { label: ".compact", kind: "meta", detail: "compact storage", insertText: ".compact" },
  { label: ".status", kind: "meta", detail: "show system status", insertText: ".status" },
  { label: ".explain", kind: "meta", detail: ".explain <query> — show plan", insertText: ".explain " },
  { label: ".load", kind: "meta", detail: ".load <file> [--replace|--merge]", insertText: ".load " },
  { label: ".help", kind: "meta", detail: "show help", insertText: ".help" },
]

const SEPARATOR_CHARS = new Set([" ", "(", ")", ",", "\n", "\t", "<", ">", "=", "!", "+", "-", "*", "/", "%"])

/** Check if column names are auto-generated (col0, col1, ...) */
function isGenericColumns(columns: string[]): boolean {
  return columns.length > 0 && columns.every((c, i) => c === `col${i}`)
}

/**
 * Convert a column name to a Datalog variable (capitalize first letter).
 * e.g., "name" → "Name", "id" → "Id", "salary" → "Salary"
 */
function colToVariable(col: string): string {
  if (col.length === 0) return col
  return col.charAt(0).toUpperCase() + col.slice(1)
}

/**
 * Extract the word being typed at the cursor position.
 * Returns the prefix string and the start index of that word.
 */
export function extractWordAtCursor(text: string, cursorPos: number): { prefix: string; startIndex: number } {
  let start = cursorPos
  while (start > 0 && !SEPARATOR_CHARS.has(text[start - 1])) {
    start--
  }
  // Special case: include leading dot for meta commands
  if (start > 0 && text[start - 1] === "." && start === cursorPos) {
    start--
  }
  const prefix = text.substring(start, cursorPos)
  return { prefix, startIndex: start }
}

/**
 * Extract meta command context from the current line.
 * If the line starts with a dot, returns the full text from the dot to cursor
 * along with the start index (the dot position).
 */
function extractMetaContext(text: string, cursorPos: number): { metaPrefix: string; metaStartIndex: number } | null {
  // Find start of current line
  let lineStart = cursorPos
  while (lineStart > 0 && text[lineStart - 1] !== "\n") {
    lineStart--
  }
  const lineText = text.substring(lineStart, cursorPos)
  const trimmedStart = lineText.length - lineText.trimStart().length
  const trimmed = lineText.trimStart()
  if (trimmed.startsWith(".")) {
    return { metaPrefix: trimmed, metaStartIndex: lineStart + trimmedStart }
  }
  return null
}

/**
 * Generate variable names from arity: 1→["A"], 3→["A","B","C"]
 */
function arityToVars(arity: number): string[] {
  return Array.from({ length: arity }, (_, i) => String.fromCharCode(65 + (i % 26)) + (i >= 26 ? String(Math.floor(i / 26)) : ""))
}

/**
 * When cursor is right after `(`, look back for a relation/view name and
 * suggest the full argument template (column-derived variables or generic A,B,C).
 */
function getParenArgSuggestion(
  text: string,
  cursorPos: number,
  relations: Relation[],
  views: View[]
): { items: CompletionItem[]; startIndex: number } | null {
  // Walk back from before the `(` to find the relation name
  let nameEnd = cursorPos - 1 // points at `(`
  let nameStart = nameEnd
  while (nameStart > 0 && !SEPARATOR_CHARS.has(text[nameStart - 1])) {
    nameStart--
  }
  // Skip leading ?
  let effectiveStart = nameStart
  if (effectiveStart < nameEnd && text[effectiveStart] === "?") effectiveStart++

  const name = text.substring(effectiveStart, nameEnd)
  if (!name) return null

  // Check if there's already content right after `(` (don't suggest if args already present)
  // Only look at the immediate next char, not all remaining text (which may span multiple lines)
  const nextChar = cursorPos < text.length ? text[cursorPos] : ""
  const hasArgsAlready = nextChar !== "" && nextChar !== ")" && nextChar !== " " && nextChar !== "\n" && nextChar !== "\t"
  if (hasArgsAlready) return null

  const closeParen = nextChar === ")" ? "" : ")"

  const rel = relations.find((r) => r.name === name)
  if (rel) {
    const vars = rel.columns.length > 0 && !isGenericColumns(rel.columns)
      ? rel.columns.map(colToVariable)
      : arityToVars(rel.arity)
    const template = vars.join(", ")
    return {
      items: [{
        label: template,
        kind: "column" as CompletionKind,
        detail: `${rel.name} arguments`,
        insertText: template + closeParen,
      }],
      startIndex: cursorPos,
    }
  }

  const view = views.find((v) => v.name === name)
  if (view && view.arity > 0) {
    const vars = arityToVars(view.arity)
    const template = vars.join(", ")
    return {
      items: [{
        label: template,
        kind: "column" as CompletionKind,
        detail: `${view.name} arguments`,
        insertText: template + closeParen,
      }],
      startIndex: cursorPos,
    }
  }

  return null
}

/**
 * Get completion suggestions based on the current prefix and available data.
 */
export function getCompletions(
  text: string,
  cursorPos: number,
  relations: Relation[],
  views: View[],
  forceShow = false
): { items: CompletionItem[]; startIndex: number } {
  // Meta commands — check first since they use full-line context, not just current word
  const metaCtx = extractMetaContext(text, cursorPos)
  if (metaCtx) {
    const metaLower = metaCtx.metaPrefix.toLowerCase()
    const items: CompletionItem[] = []
    for (const item of META_COMMANDS) {
      if (item.label.toLowerCase().startsWith(metaLower) || item.insertText.toLowerCase().startsWith(metaLower)) {
        // Skip exact matches (already typed the full command)
        if (item.insertText.toLowerCase() === metaLower || item.insertText.toLowerCase() === metaLower + " ") continue
        items.push(item)
      }
    }
    return { items, startIndex: metaCtx.metaStartIndex }
  }

  // After opening paren: suggest argument template for the relation/view
  // Matches: relation(, ?relation(, +relation(, -relation(
  if (cursorPos > 0 && text[cursorPos - 1] === "(") {
    const parenArgSuggestion = getParenArgSuggestion(text, cursorPos, relations, views)
    if (parenArgSuggestion) return parenArgSuggestion
  }

  const { prefix, startIndex } = extractWordAtCursor(text, cursorPos)

  // After typing ? or + or - show all relations/views immediately
  // ? is not a separator so it appears in prefix; + and - are separators so check char before cursor
  const charBefore = cursorPos > 0 ? text[cursorPos - 1] : ""
  const isOperatorPrefix =
    prefix === "?" ||
    (prefix === "" && (charBefore === "+" || charBefore === "-"))

  if (!forceShow && !isOperatorPrefix) {
    // Don't suggest for empty prefix, single char, variables (uppercase), or numbers
    if (prefix.length < 1) return { items: [], startIndex }
    if (prefix.length === 1 && !prefix.startsWith(".")) return { items: [], startIndex }
    if (/^[A-Z]/.test(prefix)) return { items: [], startIndex }
    if (/^[0-9]/.test(prefix)) return { items: [], startIndex }
  }

  const lowerPrefix = prefix.toLowerCase()
  const items: CompletionItem[] = []

  // After ?, +, - only show relations and views with expanded columns
  if (isOperatorPrefix) {
    for (const rel of relations) {
      const hasRealColumns = rel.columns.length > 0 && !isGenericColumns(rel.columns)
      const detail = hasRealColumns
        ? `(${rel.columns.join(", ")}) — ${rel.tupleCount} rows`
        : `arity ${rel.arity} — ${rel.tupleCount} rows`
      const vars = hasRealColumns
        ? `(${rel.columns.map(colToVariable).join(", ")})`
        : ""
      items.push({
        label: rel.name,
        kind: "relation",
        detail,
        insertText: rel.name + vars,
      })
    }
    for (const view of views) {
      const vars = view.arity > 0
        ? `(${Array.from({ length: view.arity }, (_, i) => String.fromCharCode(65 + i)).join(", ")})`
        : ""
      items.push({
        label: view.name,
        kind: "view",
        detail: view.arity > 0 ? `view — arity ${view.arity}` : "view",
        insertText: view.name + vars,
      })
    }
    return { items, startIndex: cursorPos }
  }

  // Relations
  for (const rel of relations) {
    if (rel.name.toLowerCase().startsWith(lowerPrefix)) {
      const hasReal = rel.columns.length > 0 && !isGenericColumns(rel.columns)
      const detail = hasReal
        ? `(${rel.columns.join(", ")}) — ${rel.tupleCount} rows`
        : `arity ${rel.arity} — ${rel.tupleCount} rows`
      items.push({
        label: rel.name,
        kind: "relation",
        detail,
        insertText: rel.name,
      })
    }
  }

  // Views
  for (const view of views) {
    if (view.name.toLowerCase().startsWith(lowerPrefix)) {
      items.push({
        label: view.name,
        kind: "view",
        detail: "view",
        insertText: view.name,
      })
    }
  }

  // Column names (from all relations, skip auto-generated col0/col1/...)
  const seenColumns = new Set<string>()
  for (const rel of relations) {
    if (isGenericColumns(rel.columns)) continue
    for (const col of rel.columns) {
      if (col.toLowerCase().startsWith(lowerPrefix) && !seenColumns.has(col)) {
        seenColumns.add(col)
        items.push({
          label: col,
          kind: "column",
          detail: `column in ${rel.name}`,
          insertText: col,
        })
      }
    }
  }

  // Built-in functions
  for (const fn of BUILTIN_FUNCTIONS) {
    if (fn.label.startsWith(lowerPrefix)) {
      items.push(fn)
    }
  }

  // Aggregates
  for (const agg of AGGREGATE_FUNCTIONS) {
    if (agg.label.startsWith(lowerPrefix)) {
      items.push(agg)
    }
  }

  // Keywords
  for (const kw of KEYWORDS) {
    if (kw.label.startsWith(lowerPrefix)) {
      items.push(kw)
    }
  }

  return { items, startIndex }
}

/**
 * Calculate pixel coordinates of the cursor position in a textarea.
 * Uses a mirror div technique to measure text layout.
 */
export function getCursorCoordinates(
  textarea: HTMLTextAreaElement,
  position: number
): { top: number; left: number } {
  const mirror = document.createElement("div")
  const style = window.getComputedStyle(textarea)

  // Copy relevant styles
  const properties = [
    "fontFamily", "fontSize", "fontWeight", "fontStyle",
    "letterSpacing", "lineHeight", "textTransform",
    "wordSpacing", "textIndent", "paddingTop", "paddingRight",
    "paddingBottom", "paddingLeft", "borderTopWidth", "borderRightWidth",
    "borderBottomWidth", "borderLeftWidth", "boxSizing",
  ] as const

  mirror.style.position = "absolute"
  mirror.style.visibility = "hidden"
  mirror.style.whiteSpace = "pre-wrap"
  mirror.style.wordWrap = "break-word"
  mirror.style.overflow = "hidden"
  mirror.style.width = `${textarea.clientWidth}px`

  for (const prop of properties) {
    mirror.style[prop] = style[prop]
  }

  const textBefore = textarea.value.substring(0, position)
  const textNode = document.createTextNode(textBefore)
  const span = document.createElement("span")
  span.textContent = textarea.value.substring(position) || "."

  mirror.appendChild(textNode)
  mirror.appendChild(span)
  document.body.appendChild(mirror)

  const spanRect = span.offsetTop
  const spanLeft = span.offsetLeft

  document.body.removeChild(mirror)

  return {
    top: spanRect - textarea.scrollTop,
    left: spanLeft - textarea.scrollLeft,
  }
}
