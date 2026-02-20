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
  { label: "euclidean", kind: "function", detail: "euclidean(v1, v2) → float", insertText: "euclidean" },
  { label: "cosine", kind: "function", detail: "cosine(v1, v2) → float", insertText: "cosine" },
  { label: "dot", kind: "function", detail: "dot(v1, v2) → float", insertText: "dot" },
  { label: "manhattan", kind: "function", detail: "manhattan(v1, v2) → float", insertText: "manhattan" },
  // Vector operations
  { label: "normalize", kind: "function", detail: "normalize(v) → vector", insertText: "normalize" },
  { label: "vec_dim", kind: "function", detail: "vec_dim(v) → int", insertText: "vec_dim" },
  { label: "vec_add", kind: "function", detail: "vec_add(v1, v2) → vector", insertText: "vec_add" },
  { label: "vec_scale", kind: "function", detail: "vec_scale(v, s) → vector", insertText: "vec_scale" },
  // LSH functions
  { label: "lsh_bucket", kind: "function", detail: "lsh_bucket(v, bands, rows) → int", insertText: "lsh_bucket" },
  { label: "lsh_probes", kind: "function", detail: "lsh_probes(v, bands, rows) → int", insertText: "lsh_probes" },
  { label: "lsh_multi_probe", kind: "function", detail: "lsh_multi_probe(v, bands, rows, probes) → int", insertText: "lsh_multi_probe" },
  // Temporal functions
  { label: "time_now", kind: "function", detail: "time_now() → timestamp", insertText: "time_now" },
  { label: "time_diff", kind: "function", detail: "time_diff(t1, t2) → int", insertText: "time_diff" },
  { label: "time_add", kind: "function", detail: "time_add(t, ms) → timestamp", insertText: "time_add" },
  { label: "time_sub", kind: "function", detail: "time_sub(t, ms) → timestamp", insertText: "time_sub" },
  { label: "time_decay", kind: "function", detail: "time_decay(t, now, half_life) → float", insertText: "time_decay" },
  { label: "time_decay_linear", kind: "function", detail: "time_decay_linear(t, now, window) → float", insertText: "time_decay_linear" },
  { label: "time_before", kind: "function", detail: "time_before(t1, t2) → bool", insertText: "time_before" },
  { label: "time_after", kind: "function", detail: "time_after(t1, t2) → bool", insertText: "time_after" },
  { label: "time_between", kind: "function", detail: "time_between(t, start, end) → bool", insertText: "time_between" },
  { label: "within_last", kind: "function", detail: "within_last(t, now, window) → bool", insertText: "within_last" },
  { label: "intervals_overlap", kind: "function", detail: "intervals_overlap(s1, e1, s2, e2) → bool", insertText: "intervals_overlap" },
  { label: "interval_contains", kind: "function", detail: "interval_contains(s, e, t) → bool", insertText: "interval_contains" },
  { label: "interval_duration", kind: "function", detail: "interval_duration(s, e) → int", insertText: "interval_duration" },
  { label: "point_in_interval", kind: "function", detail: "point_in_interval(t, s, e) → bool", insertText: "point_in_interval" },
  // Quantization
  { label: "quantize_linear", kind: "function", detail: "quantize_linear(v, bits) → vector", insertText: "quantize_linear" },
  { label: "quantize_symmetric", kind: "function", detail: "quantize_symmetric(v, bits) → vector", insertText: "quantize_symmetric" },
  { label: "dequantize", kind: "function", detail: "dequantize(v) → vector", insertText: "dequantize" },
  { label: "dequantize_scaled", kind: "function", detail: "dequantize_scaled(v, scale) → vector", insertText: "dequantize_scaled" },
  // Int8 distance
  { label: "euclidean_int8", kind: "function", detail: "euclidean_int8(v1, v2) → float", insertText: "euclidean_int8" },
  { label: "cosine_int8", kind: "function", detail: "cosine_int8(v1, v2) → float", insertText: "cosine_int8" },
  { label: "dot_int8", kind: "function", detail: "dot_int8(v1, v2) → float", insertText: "dot_int8" },
  { label: "manhattan_int8", kind: "function", detail: "manhattan_int8(v1, v2) → float", insertText: "manhattan_int8" },
  // Math
  { label: "abs", kind: "function", detail: "abs(x) → number", insertText: "abs" },
  { label: "abs_int64", kind: "function", detail: "abs_int64(x) → int", insertText: "abs_int64" },
  { label: "abs_float64", kind: "function", detail: "abs_float64(x) → float", insertText: "abs_float64" },
  { label: "sqrt", kind: "function", detail: "sqrt(x) → float", insertText: "sqrt" },
  { label: "pow", kind: "function", detail: "pow(base, exp) → float", insertText: "pow" },
  { label: "log", kind: "function", detail: "log(x) → float", insertText: "log" },
  { label: "exp", kind: "function", detail: "exp(x) → float", insertText: "exp" },
  { label: "sin", kind: "function", detail: "sin(x) → float", insertText: "sin" },
  { label: "cos", kind: "function", detail: "cos(x) → float", insertText: "cos" },
  { label: "tan", kind: "function", detail: "tan(x) → float", insertText: "tan" },
  { label: "floor", kind: "function", detail: "floor(x) → int", insertText: "floor" },
  { label: "ceil", kind: "function", detail: "ceil(x) → int", insertText: "ceil" },
  { label: "sign", kind: "function", detail: "sign(x) → int", insertText: "sign" },
  // Type conversion
  { label: "to_float", kind: "function", detail: "to_float(x) → float", insertText: "to_float" },
  { label: "to_int", kind: "function", detail: "to_int(x) → int", insertText: "to_int" },
  // String functions
  { label: "len", kind: "function", detail: "len(s) → int", insertText: "len" },
  { label: "upper", kind: "function", detail: "upper(s) → string", insertText: "upper" },
  { label: "lower", kind: "function", detail: "lower(s) → string", insertText: "lower" },
  { label: "trim", kind: "function", detail: "trim(s) → string", insertText: "trim" },
  { label: "substr", kind: "function", detail: "substr(s, start, len) → string", insertText: "substr" },
  { label: "replace", kind: "function", detail: "replace(s, find, repl) → string", insertText: "replace" },
  { label: "concat", kind: "function", detail: "concat(s1, s2, ...) → string", insertText: "concat" },
  // Scalar min/max
  { label: "min_val", kind: "function", detail: "min_val(a, b) → same type", insertText: "min_val" },
  { label: "max_val", kind: "function", detail: "max_val(a, b) → same type", insertText: "max_val" },
]

const AGGREGATE_FUNCTIONS: CompletionItem[] = [
  { label: "count", kind: "aggregate", detail: "count<Var>", insertText: "count" },
  { label: "count_distinct", kind: "aggregate", detail: "count_distinct<Var>", insertText: "count_distinct" },
  { label: "sum", kind: "aggregate", detail: "sum<Var>", insertText: "sum" },
  { label: "min", kind: "aggregate", detail: "min<Var>", insertText: "min" },
  { label: "max", kind: "aggregate", detail: "max<Var>", insertText: "max" },
  { label: "avg", kind: "aggregate", detail: "avg<Var>", insertText: "avg" },
  { label: "top_k", kind: "aggregate", detail: "top_k<k, Var, OrderVar:desc>", insertText: "top_k" },
  { label: "top_k_threshold", kind: "aggregate", detail: "top_k_threshold<k, threshold, Var, OrderVar:desc>", insertText: "top_k_threshold" },
  { label: "within_radius", kind: "aggregate", detail: "within_radius<radius, Var, DistVar>", insertText: "within_radius" },
]

const KEYWORDS: CompletionItem[] = [
  { label: "true", kind: "keyword", detail: "boolean", insertText: "true" },
  { label: "false", kind: "keyword", detail: "boolean", insertText: "false" },
  { label: "<-", kind: "keyword", detail: "rule arrow", insertText: "<-" },
]

// Meta commands are REPL-only (not supported in the query API/GUI).
const META_COMMANDS: CompletionItem[] = []

const SEPARATOR_CHARS = new Set([" ", "(", ")", ",", "\n", "\t", "<", ">", "=", "!", "+", "-", "*", "/", "%"])

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
 * Check if prefix looks like a meta command (starts with .)
 */
function isMetaPrefix(text: string, startIndex: number): boolean {
  // Walk back to the start of the line
  let lineStart = startIndex
  while (lineStart > 0 && text[lineStart - 1] !== "\n") {
    lineStart--
  }
  const linePrefix = text.substring(lineStart, startIndex).trim()
  return linePrefix === "" || linePrefix.startsWith(".")
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
      const detail = rel.columns.length > 0
        ? `(${rel.columns.join(", ")}) — ${rel.tupleCount} rows`
        : `arity ${rel.arity} — ${rel.tupleCount} rows`
      const vars = rel.columns.length > 0
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
      items.push({
        label: view.name,
        kind: "view",
        detail: "view",
        insertText: view.name,
      })
    }
    return { items, startIndex: cursorPos }
  }

  // Meta commands — only at the beginning of a line
  if (prefix.startsWith(".") && isMetaPrefix(text, startIndex)) {
    for (const item of META_COMMANDS) {
      if (item.label.toLowerCase().startsWith(lowerPrefix)) {
        items.push(item)
      }
    }
    // Meta commands don't mix with other completions
    return { items, startIndex }
  }

  // Relations
  for (const rel of relations) {
    if (rel.name.toLowerCase().startsWith(lowerPrefix)) {
      const detail = rel.columns.length > 0
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

  // Column names (from all relations)
  const seenColumns = new Set<string>()
  for (const rel of relations) {
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
